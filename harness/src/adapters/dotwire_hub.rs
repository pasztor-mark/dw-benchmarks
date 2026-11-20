use super::dotwire::DotwireTarget;
use super::signalr;
use super::tinode::{Live, Registry};
use crate::adapter::{Ack, Conn, Delivery, SetupPlan, Target};
use crate::config::RunConfig;
use crate::results::{Fixtures, UserCred};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

impl Live for signalr::Client {
    fn is_alive(&self) -> bool {
        !self.is_closed()
    }
}

pub struct DotwireHubTarget {
    inner: DotwireTarget,
    senders: Arc<Registry<signalr::Client>>,
}

impl DotwireHubTarget {
    pub fn new(cfg: &RunConfig) -> Result<Self> {
        Ok(Self { inner: DotwireTarget::new(cfg)?, senders: Arc::default() })
    }
}

struct HubConn {
    user: String,
    client: Arc<signalr::Client>,
    senders: Arc<Registry<signalr::Client>>,
}

#[async_trait]
impl Conn for HubConn {
    async fn subscribe(&mut self, room: &str) -> Result<()> {
        self.client.invoke("Subscribe", vec![json!(room)]).await
    }

    async fn close(self: Box<Self>) {
        self.senders.unregister(&self.user, &self.client);
        self.client.close();
    }
}

#[async_trait]
impl Target for DotwireHubTarget {
    async fn setup(&self, plan: &SetupPlan) -> Result<Fixtures> {
        let mut fx = self.inner.setup(plan).await?;
        fx.target = "dotwire-hub".into();
        Ok(fx)
    }

    async fn connect(&self, user: &UserCred, deliveries: mpsc::UnboundedSender<Delivery>) -> Result<Box<dyn Conn>> {
        let client = Arc::new(self.inner.open_hub(user, Some(deliveries)).await?);
        self.senders.register(&user.id, client.clone());
        Ok(Box::new(HubConn { user: user.id.clone(), client, senders: self.senders.clone() }))
    }

    async fn send(&self, user: &UserCred, room: &str, body: String) -> Result<Ack> {
        let client = self
            .senders
            .get_or_open(&user.id, || async {
                let client = Arc::new(self.inner.open_hub(user, None).await?);
                client.invoke("Subscribe", vec![json!(room)]).await?;
                Ok(client)
            })
            .await?;
        client.invoke("Send", vec![json!(room), json!(body), Value::Null]).await?;
        Ok(Ack { at: Instant::now() })
    }

    async fn history(&self, user: &UserCred, room: &str, limit: usize) -> Result<usize> {
        self.inner.history(user, room, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    type Server = WebSocketStream<tokio::io::DuplexStream>;

    async fn text(ws: &mut Server) -> String {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => t.to_string(),
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn pair() -> (Server, signalr::Client) {
        let (a, b) = tokio::io::duplex(65536);
        let mut server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
        let client_ws = WebSocketStream::from_raw_socket(a, Role::Client, None).await;
        let handshake = tokio::spawn(async move {
            let _ = text(&mut server).await;
            server.send(Message::Text("{}\x1e".into())).await.unwrap();
            server
        });
        let client = signalr::Client::start(client_ws, |_, _| {}).await.unwrap();
        (handshake.await.unwrap(), client)
    }

    fn target() -> DotwireHubTarget {
        let cfg = RunConfig::load(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/configs/smoke.toml"))).unwrap();
        DotwireHubTarget {
            inner: DotwireTarget::with_pem(&cfg, include_bytes!("test_key.pem")).unwrap(),
            senders: Arc::default(),
        }
    }

    fn user() -> UserCred {
        UserCred { id: "u000001".into(), token: None }
    }

    #[tokio::test]
    async fn send_invokes_hub_send_with_null_idempotency_key() {
        let (mut server, client) = pair().await;
        let target = target();
        target.senders.register("u000001", Arc::new(client));
        let srv = tokio::spawn(async move {
            let invocation = text(&mut server).await;
            let frame: serde_json::Value = serde_json::from_str(invocation.trim_end_matches('\x1e')).unwrap();
            let id = frame["invocationId"].as_str().unwrap().to_string();
            server
                .send(Message::Text(format!("{{\"type\":3,\"invocationId\":\"{id}\"}}\x1e").into()))
                .await
                .unwrap();
            (frame, server)
        });
        assert!(target.send(&user(), "room-1", "dwb:1:x".into()).await.is_ok());
        let (frame, _server) = srv.await.unwrap();
        assert_eq!(frame["target"], "Send");
        assert_eq!(frame["arguments"], serde_json::json!(["room-1", "dwb:1:x", null]));
    }

    #[tokio::test]
    async fn send_error_completion_is_err() {
        let (mut server, client) = pair().await;
        let target = target();
        target.senders.register("u000001", Arc::new(client));
        tokio::spawn(async move {
            let invocation = text(&mut server).await;
            let frame: serde_json::Value = serde_json::from_str(invocation.trim_end_matches('\x1e')).unwrap();
            let id = frame["invocationId"].as_str().unwrap().to_string();
            server
                .send(Message::Text(
                    format!("{{\"type\":3,\"invocationId\":\"{id}\",\"error\":\"RateLimited\"}}\x1e").into(),
                ))
                .await
                .unwrap();
            let _keep = server;
            std::future::pending::<()>().await;
        });
        let err = target.send(&user(), "room-1", "b".into()).await.err().unwrap().to_string();
        assert!(err.contains("RateLimited"), "{err}");
    }

    #[tokio::test]
    async fn closed_client_reports_closed() {
        let (_server, client) = pair().await;
        assert!(!client.is_closed());
        client.close();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(client.is_closed());
    }
}
