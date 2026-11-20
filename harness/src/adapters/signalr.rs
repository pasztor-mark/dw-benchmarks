use anyhow::{anyhow, bail, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval_at, Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub const SEP: char = '\x1e';
const PING_EVERY: Duration = Duration::from_secs(15);
const PING: &str = "{\"type\":6}\x1e";

enum Cmd {
    Invoke { target: String, args: Vec<Value>, reply: oneshot::Sender<Result<()>> },
    Close,
}

pub struct Client {
    cmd: mpsc::UnboundedSender<Cmd>,
}

pub fn split_frames(buf: &mut String) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(i) = buf.find(SEP) {
        let rest = buf.split_off(i + 1);
        buf.pop();
        out.push(std::mem::replace(buf, rest));
    }
    out
}

fn frame_error(v: &Value) -> Option<String> {
    v.get("error").and_then(Value::as_str).map(str::to_string)
}

impl Client {
    pub async fn start<S, F>(mut ws: WebSocketStream<S>, on_push: F) -> Result<Client>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: Fn(&str, &[Value]) + Send + 'static,
    {
        ws.send(Message::Text(format!("{{\"protocol\":\"json\",\"version\":1}}{SEP}").into())).await?;
        let mut buf = String::new();
        let pending_frames = loop {
            let msg = ws.next().await.ok_or_else(|| anyhow!("closed during handshake"))??;
            let Message::Text(t) = msg else { continue };
            buf.push_str(&t);
            let mut frames = split_frames(&mut buf);
            if frames.is_empty() {
                continue;
            }
            let first: Value = serde_json::from_str(&frames.remove(0))?;
            if let Some(e) = frame_error(&first) {
                bail!("handshake rejected: {e}");
            }
            break frames;
        };
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(run(ws, rx, on_push, buf, pending_frames));
        Ok(Client { cmd: tx })
    }

    pub async fn invoke(&self, target: &str, args: Vec<Value>) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.cmd
            .send(Cmd::Invoke { target: target.into(), args, reply })
            .map_err(|_| anyhow!("connection closed"))?;
        rx.await.map_err(|_| anyhow!("connection closed"))?
    }

    pub fn is_closed(&self) -> bool {
        self.cmd.is_closed()
    }

    pub fn close(&self) {
        let _ = self.cmd.send(Cmd::Close);
    }
}

async fn run<S, F>(
    mut ws: WebSocketStream<S>,
    mut cmds: mpsc::UnboundedReceiver<Cmd>,
    on_push: F,
    mut buf: String,
    first: Vec<String>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(&str, &[Value]) + Send + 'static,
{
    let mut pending: HashMap<String, oneshot::Sender<Result<()>>> = HashMap::new();
    let mut next_id = 0u64;
    let mut ping = interval_at(Instant::now() + PING_EVERY, PING_EVERY);
    let mut frames = first;
    loop {
        for f in frames.drain(..) {
            let Ok(v) = serde_json::from_str::<Value>(&f) else { continue };
            match v.get("type").and_then(Value::as_u64) {
                Some(1) => {
                    if let (Some(t), Some(a)) = (v["target"].as_str(), v["arguments"].as_array()) {
                        on_push(t, a);
                    }
                }
                Some(3) => {
                    if let Some(reply) = v["invocationId"].as_str().and_then(|id| pending.remove(id)) {
                        let _ = reply.send(match frame_error(&v) {
                            Some(e) => Err(anyhow!("invocation failed: {e}")),
                            None => Ok(()),
                        });
                    }
                }
                Some(6) => {
                    if ws.send(Message::Text(PING.into())).await.is_err() {
                        return;
                    }
                }
                Some(7) => return,
                _ => {}
            }
        }
        tokio::select! {
            _ = ping.tick() => {
                if ws.send(Message::Text(PING.into())).await.is_err() {
                    return;
                }
            }
            cmd = cmds.recv() => match cmd {
                Some(Cmd::Invoke { target, args, reply }) => {
                    next_id += 1;
                    let id = next_id.to_string();
                    let text = format!("{}{SEP}", json!({"type": 1, "invocationId": id, "target": target, "arguments": args}));
                    if ws.send(Message::Text(text.into())).await.is_err() {
                        let _ = reply.send(Err(anyhow!("connection closed")));
                        return;
                    }
                    pending.insert(id, reply);
                }
                Some(Cmd::Close) | None => {
                    let _ = ws.send(Message::Text(format!("{{\"type\":7}}{SEP}").into())).await;
                    let _ = ws.close(None).await;
                    return;
                }
            },
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    buf.push_str(&t);
                    frames = split_frames(&mut buf);
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => {}
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;

    #[test]
    fn split_frames() {
        let mut buf = "{\"type\":6}\x1e{\"type\":1,\"target\":\"x\"}\x1e{\"type\"".to_string();
        let frames = super::split_frames(&mut buf);
        assert_eq!(frames, vec!["{\"type\":6}", "{\"type\":1,\"target\":\"x\"}"]);
        assert_eq!(buf, "{\"type\"");
        buf.push_str(":6}\x1e");
        assert_eq!(super::split_frames(&mut buf), vec!["{\"type\":6}"]);
        assert!(buf.is_empty());
    }

    async fn text(ws: &mut WebSocketStream<tokio::io::DuplexStream>) -> String {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => t.to_string(),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ping_scheduled() {
        let (a, b) = tokio::io::duplex(65536);
        let mut server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
        let client_ws = WebSocketStream::from_raw_socket(a, Role::Client, None).await;
        let srv = tokio::spawn(async move {
            assert_eq!(text(&mut server).await, "{\"protocol\":\"json\",\"version\":1}\x1e");
            server.send(Message::Text("{}\x1e".into())).await.unwrap();
            let start = tokio::time::Instant::now();
            assert_eq!(text(&mut server).await, "{\"type\":6}\x1e");
            let elapsed = start.elapsed();
            assert!(elapsed >= Duration::from_secs(15) && elapsed < Duration::from_secs(16), "{elapsed:?}");
            server
        });
        let _client = Client::start(client_ws, |_, _| {}).await.unwrap();
        srv.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn handshake_error_surfaces() {
        let (a, b) = tokio::io::duplex(65536);
        let mut server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
        let client_ws = WebSocketStream::from_raw_socket(a, Role::Client, None).await;
        tokio::spawn(async move {
            let _ = text(&mut server).await;
            server.send(Message::Text("{\"error\":\"bad protocol\"}\x1e".into())).await.unwrap();
            server
        });
        let err = Client::start(client_ws, |_, _| {}).await.err().expect("must fail");
        assert!(err.to_string().contains("bad protocol"), "{err}");
    }
}
