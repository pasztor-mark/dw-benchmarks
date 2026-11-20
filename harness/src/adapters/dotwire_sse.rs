use super::dotwire::DotwireTarget;
use crate::adapter::{Ack, Conn, Delivery, SetupPlan, Target};
use crate::body;
use crate::config::RunConfig;
use crate::results::{Fixtures, UserCred};
use anyhow::{bail, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq)]
enum Parsed {
    Message(u64),
    Revoked,
}

#[derive(Default)]
struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    fn feed(&mut self, chunk: &[u8]) -> Vec<Parsed> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(end) = self.buf.windows(2).position(|w| w == b"\n\n") {
            let block: Vec<u8> = self.buf.drain(..end + 2).collect();
            let block = String::from_utf8_lossy(&block);
            let (mut event, mut data) = (None, String::new());
            for line in block.lines() {
                if line.starts_with(':') {
                    continue;
                }
                if let Some(v) = line.strip_prefix("event:") {
                    event = Some(v.trim().to_string());
                } else if let Some(v) = line.strip_prefix("data:") {
                    data.push_str(v.strip_prefix(' ').unwrap_or(v));
                }
            }
            match event.as_deref() {
                Some("revoked") => out.push(Parsed::Revoked),
                Some("message") => {
                    let Ok(v) = serde_json::from_str::<Value>(&data) else { continue };
                    if v["replayed"].as_bool() == Some(true) {
                        continue;
                    }
                    if let Some(id) = v["content"].as_str().and_then(body::decode) {
                        out.push(Parsed::Message(id));
                    }
                }
                _ => {}
            }
        }
        out
    }
}

pub struct DotwireSseTarget {
    inner: DotwireTarget,
    streams: reqwest::Client,
}

impl DotwireSseTarget {
    pub fn new(cfg: &RunConfig) -> Result<Self> {
        Ok(Self {
            inner: DotwireTarget::new(cfg)?,
            streams: reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).build()?,
        })
    }
}

struct SseConn {
    streams: reqwest::Client,
    base_url: String,
    token: String,
    tx: Arc<Mutex<Option<mpsc::UnboundedSender<Delivery>>>>,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

#[async_trait]
impl Conn for SseConn {
    async fn subscribe(&mut self, room: &str) -> Result<()> {
        let Some(tx) = self.tx.lock().unwrap().clone() else { bail!("connection closed") };
        let url = format!("{}/rooms/{room}/events?access_token={}", self.base_url, self.token);
        let mut resp = tokio::time::timeout(RESPONSE_TIMEOUT, self.streams.get(url).send()).await??;
        if resp.status().as_u16() != 200 {
            bail!("sse subscribe failed: {} {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        let (shared, stop) = (self.tx.clone(), self.stop.clone());
        let mut stopped = self.stop.subscribe();
        self.tasks.push(tokio::spawn(async move {
            let mut parser = SseParser::default();
            'stream: loop {
                let chunk = tokio::select! {
                    c = resp.chunk() => c,
                    _ = stopped.changed() => break,
                };
                let Ok(Some(chunk)) = chunk else { break };
                let at = Instant::now();
                for p in parser.feed(&chunk) {
                    match p {
                        Parsed::Message(msg_id) => {
                            let _ = tx.send(Delivery { msg_id, at });
                        }
                        Parsed::Revoked => break 'stream,
                    }
                }
            }
            shared.lock().unwrap().take();
            let _ = stop.send(true);
        }));
        Ok(())
    }

    async fn close(self: Box<Self>) {
        self.tx.lock().unwrap().take();
        for t in &self.tasks {
            t.abort();
        }
    }
}

#[async_trait]
impl Target for DotwireSseTarget {
    async fn setup(&self, plan: &SetupPlan) -> Result<Fixtures> {
        let mut fx = self.inner.setup(plan).await?;
        fx.target = "dotwire-sse".into();
        Ok(fx)
    }

    async fn connect(&self, user: &UserCred, deliveries: mpsc::UnboundedSender<Delivery>) -> Result<Box<dyn Conn>> {
        Ok(Box::new(SseConn {
            streams: self.streams.clone(),
            base_url: self.inner.base_url.clone(),
            token: self.inner.token(&user.id)?,
            tx: Arc::new(Mutex::new(Some(deliveries))),
            stop: watch::channel(false).0,
            tasks: Vec::new(),
        }))
    }

    async fn send(&self, user: &UserCred, room: &str, body: String) -> Result<Ack> {
        self.inner.send(user, room, body).await
    }

    async fn history(&self, user: &UserCred, room: &str, limit: usize) -> Result<usize> {
        self.inner.history(user, room, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(seq: u64, content: &str, extra: &str) -> String {
        format!("id: {seq}\nevent: message\ndata: {{\"roomId\":\"r\",\"seq\":{seq},\"senderId\":\"u1\",\"time\":\"t\",\"content\":\"{content}\"{extra}}}\n\n")
    }

    #[test]
    fn parses_events() {
        let mut p = SseParser::default();
        let input = format!(": ping\n\n{}", msg(3, "dwb:7:..", ""));
        let (a, b) = input.as_bytes().split_at(20);
        let mut got = p.feed(a);
        got.extend(p.feed(b));
        assert_eq!(got, vec![Parsed::Message(7)]);
    }

    #[test]
    fn ignores_replayed() {
        let mut p = SseParser::default();
        assert!(p.feed(msg(3, "dwb:7:..", ",\"replayed\":true").as_bytes()).is_empty());
        assert_eq!(p.feed(msg(4, "dwb:8:..", ",\"replayed\":false").as_bytes()), vec![Parsed::Message(8)]);
        assert!(p.feed(msg(5, "hello", "").as_bytes()).is_empty());
        assert_eq!(p.feed(b"event: revoked\ndata: {\"roomId\":\"r\"}\n\n"), vec![Parsed::Revoked]);
    }
}
