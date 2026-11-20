use super::signalr;
use crate::adapter::{Ack, Conn, Delivery, SetupPlan, Target};
use crate::body;
use crate::config::RunConfig;
use crate::results::{Fixtures, RoomFixture, UserCred};
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const LIFETIME_S: u64 = 14_400;
const REFRESH_AFTER: Duration = Duration::from_secs(3_600);
const ADMIN_USER: &str = "host-admin";
const ADMIN_CHUNK: usize = 500;

pub fn mint_token(key: &EncodingKey, kid: &str, iss: &str, aud: &str, sub: &str, role: &str, iat: u64) -> Result<String> {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.to_string());
    let claims = json!({"sub": sub, "dw:role": role, "iss": iss, "aud": aud, "iat": iat, "exp": iat + LIFETIME_S});
    Ok(jsonwebtoken::encode(&header, &claims, key)?)
}

fn room_guid(index: usize) -> String {
    format!("00000000-0000-4000-8000-{index:012}")
}

pub struct DotwireTarget {
    pub(super) http: reqwest::Client,
    pub(super) base_url: String,
    ws_url: String,
    key: EncodingKey,
    mints: AtomicU64,
    kid: String,
    issuer: String,
    audience: String,
    tokens: Mutex<HashMap<String, (Instant, String)>>,
}

impl DotwireTarget {
    pub fn new(cfg: &RunConfig) -> Result<Self> {
        let t = &cfg.target;
        let path = t.jwt_private_key_path.as_deref().context("target.jwt_private_key_path is required for dotwire")?;
        let pem = std::fs::read(path).with_context(|| format!("reading {path}"))?;
        Self::with_pem(cfg, &pem)
    }

    pub(super) fn with_pem(cfg: &RunConfig, pem: &[u8]) -> Result<Self> {
        let t = &cfg.target;
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(256)
                .build()?,
            base_url: t.base_url.trim_end_matches('/').to_string(),
            ws_url: t.ws_url.trim_end_matches('/').to_string(),
            key: EncodingKey::from_rsa_pem(pem)?,
            mints: AtomicU64::new(0),
            kid: t.jwt_kid.clone(),
            issuer: t.jwt_issuer.clone(),
            audience: t.jwt_audience.clone(),
            tokens: Mutex::default(),
        })
    }

    pub(super) async fn open_hub(
        &self,
        user: &UserCred,
        deliveries: Option<mpsc::UnboundedSender<Delivery>>,
    ) -> Result<signalr::Client> {
        let token = self.token(&user.id)?;
        let url = format!("{}/hub/rooms?access_token={token}", self.ws_url);
        let (ws, _) = tokio_tungstenite::connect_async(url).await?;
        signalr::Client::start(ws, move |target, args| {
            if target != "ReceiveMessage" {
                return;
            }
            let Some(deliveries) = &deliveries else { return };
            let Some(content) = args.first().and_then(|a| a["content"].as_str()) else { return };
            if let Some(msg_id) = body::decode(content) {
                let _ = deliveries.send(Delivery { msg_id, at: Instant::now() });
            }
        })
        .await
    }

    pub(super) fn token(&self, user: &str) -> Result<String> {
        if let Some((at, tok)) = self.tokens.lock().unwrap().get(user) {
            if at.elapsed() < REFRESH_AFTER {
                return Ok(tok.clone());
            }
        }
        let role = if user == ADMIN_USER { "admin" } else { "member" };
        let iat = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let tok = mint_token(&self.key, &self.kid, &self.issuer, &self.audience, user, role, iat)?;
        self.mints.fetch_add(1, Ordering::Relaxed);
        self.tokens.lock().unwrap().insert(user.to_string(), (Instant::now(), tok.clone()));
        Ok(tok)
    }
}

struct DotwireConn {
    client: signalr::Client,
}

#[async_trait]
impl Conn for DotwireConn {
    async fn subscribe(&mut self, room: &str) -> Result<()> {
        self.client.invoke("Subscribe", vec![json!(room)]).await
    }

    async fn close(self: Box<Self>) {
        self.client.close();
    }
}

#[async_trait]
impl Target for DotwireTarget {
    async fn setup(&self, plan: &SetupPlan) -> Result<Fixtures> {
        let mut ids: HashMap<String, String> = HashMap::new();
        let mut users = Vec::new();
        let mut rooms = Vec::new();
        let admin = self.token(ADMIN_USER)?;
        for (index, (_, members)) in plan.rooms.iter().enumerate() {
            let mut mapped = Vec::new();
            for m in members {
                let next = ids.len() + 1;
                let id = ids.entry(m.clone()).or_insert_with(|| format!("u{next:06}")).clone();
                if !users.iter().any(|u: &UserCred| u.id == id) {
                    users.push(UserCred { id: id.clone(), token: None });
                }
                mapped.push(id);
            }
            let room = room_guid(index);
            for chunk in mapped.chunks(ADMIN_CHUNK) {
                let resp = self
                    .http
                    .post(format!("{}/admin/rooms/{room}/members", self.base_url))
                    .bearer_auth(&admin)
                    .json(&json!({"userIds": chunk, "ensureMemberRole": true}))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    bail!("add members to {room}: {} {}", resp.status(), resp.text().await.unwrap_or_default());
                }
            }
            rooms.push(RoomFixture { id: room, members: mapped });
        }
        Ok(Fixtures {
            target: "dotwire".into(),
            scenario: String::new(),
            users,
            rooms,
            meta: serde_json::Map::new(),
        })
    }

    async fn connect(&self, user: &UserCred, deliveries: mpsc::UnboundedSender<Delivery>) -> Result<Box<dyn Conn>> {
        let client = self.open_hub(user, Some(deliveries)).await?;
        Ok(Box::new(DotwireConn { client }))
    }

    async fn send(&self, user: &UserCred, room: &str, body: String) -> Result<Ack> {
        let resp = self
            .http
            .post(format!("{}/rooms/{room}/messages", self.base_url))
            .bearer_auth(self.token(&user.id)?)
            .json(&json!({"content": body}))
            .send()
            .await?;
        let at = Instant::now();
        if resp.status().as_u16() != 202 {
            bail!("send failed: {} {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        Ok(Ack { at })
    }

    async fn history(&self, user: &UserCred, room: &str, limit: usize) -> Result<usize> {
        let resp = self
            .http
            .get(format!("{}/rooms/{room}/messages?limit={limit}", self.base_url))
            .bearer_auth(self.token(&user.id)?)
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("history failed: {}", resp.status());
        }
        let v: Value = resp.json().await?;
        Ok(v["messages"].as_array().map_or(0, Vec::len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn jwt_claims() {
        let key = EncodingKey::from_rsa_pem(include_bytes!("test_key.pem")).unwrap();
        let tok = mint_token(&key, "bench", "dwbench", "dotwire", "u000002", "member", 1_700_000_000).unwrap();
        let part = |i: usize| -> serde_json::Value {
            let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(tok.split('.').nth(i).unwrap()).unwrap();
            serde_json::from_slice(&raw).unwrap()
        };
        let (h, c) = (part(0), part(1));
        assert_eq!(h["kid"], "bench");
        assert_eq!(h["alg"], "RS256");
        assert_eq!(c["dw:role"], "member");
        assert_eq!(c["sub"], "u000002");
        assert_eq!(c["iss"], "dwbench");
        assert_eq!(c["aud"], "dotwire");
        assert_eq!(c["exp"].as_u64().unwrap() - c["iat"].as_u64().unwrap(), 14400);
    }

    #[test]
    fn token_cached_and_key_parsed_once() {
        let cfg = RunConfig::load(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/configs/smoke.toml"))).unwrap();
        let target = DotwireTarget::with_pem(&cfg, include_bytes!("test_key.pem")).unwrap();
        let a = target.token("u000001").unwrap();
        let b = target.token("u000001").unwrap();
        assert_eq!(a, b);
        assert_eq!(target.mints.load(Ordering::Relaxed), 1);
        target.token("u000002").unwrap();
        assert_eq!(target.mints.load(Ordering::Relaxed), 2);
    }
}
