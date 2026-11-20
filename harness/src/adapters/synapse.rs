use crate::adapter::{Ack, Conn, Delivery, SetupPlan, Target};
use crate::body;
use crate::config::RunConfig;
use crate::results::{Fixtures, RoomFixture, UserCred};
use crate::retry::Backoff;
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt};
use hmac::{Hmac, KeyInit, Mac};
use rand::RngExt;
use serde_json::{json, Map, Value};
use sha1::Sha1;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const SYNC_LONG_POLL_MS: u64 = 30_000;
const SYNC_TIMEOUT: Duration = Duration::from_secs(45);
const SETUP_CONCURRENCY: usize = 32;
const FILTER: &str = r#"{"room":{"timeline":{"limit":1},"state":{"lazy_load_members":true}},"presence":{"types":[]},"account_data":{"types":[]}}"#;

const SYNC_FILTER: &str = r#"{"room":{"timeline":{"limit":1000},"state":{"lazy_load_members":true}},"presence":{"types":[]},"account_data":{"types":[]}}"#;

pub(crate) fn limited_rooms(resp: &Value) -> usize {
    resp["rooms"]["join"]
        .as_object()
        .map_or(0, |j| j.values().filter(|r| r["timeline"]["limited"] == true).count())
}

pub(crate) fn txn_id(prefix: &str, user: &str, msg_id: u64, n: u64) -> String {
    format!("{prefix}-{user}-{msg_id}-{n}")
}

pub(crate) fn register_mac(secret: &str, nonce: &str, user: &str, password: &str) -> String {
    let mut mac = <Hmac<Sha1> as KeyInit>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    for part in [nonce, user, password] {
        mac.update(part.as_bytes());
        mac.update(&[0]);
    }
    mac.update(b"notadmin");
    hex::encode(mac.finalize().into_bytes())
}

pub(crate) fn handle_sync(resp: &Value, first: bool, own: &str) -> (Vec<u64>, Vec<String>) {
    let mut ids = Vec::new();
    let mut rooms = Vec::new();
    let Some(joined) = resp["rooms"]["join"].as_object() else { return (ids, rooms) };
    for (room, data) in joined {
        rooms.push(room.clone());
        if first {
            continue;
        }
        for e in data["timeline"]["events"].as_array().into_iter().flatten() {
            if e["type"] == "m.room.message" && e["sender"] != own {
                if let Some(id) = e["content"]["body"].as_str().and_then(body::decode) {
                    ids.push(id);
                }
            }
        }
    }
    (ids, rooms)
}

pub(crate) fn join_error(user: &str, room: &str, status: u16, body: &str) -> String {
    format!("synapse join failed for {user} in {room}: {status} {body}")
}

fn enc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

struct Account {
    id: String,
    matrix_id: String,
    token: String,
    filter_id: String,
}

pub struct SynapseTarget {
    http: reqwest::Client,
    base_url: String,
    secret: String,
    server_name: Option<String>,
    run_prefix: String,
    counter: AtomicU64,
    limited: Arc<AtomicU64>,
    whoami: Mutex<HashMap<String, String>>,
}

impl SynapseTarget {
    pub fn new(cfg: &RunConfig) -> Result<Self> {
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
        let salt: u32 = rand::rng().random();
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(REQUEST_TIMEOUT)
                .pool_max_idle_per_host(256)
                .build()?,
            base_url: cfg.target.base_url.trim_end_matches('/').to_string(),
            secret: cfg.target.admin_secret.clone().context("target.admin_secret (DWBENCH_ADMIN_SECRET) is required for synapse")?,
            server_name: cfg.target.synapse_server_name.clone(),
            run_prefix: format!("{stamp:x}{salt:08x}"),
            counter: AtomicU64::new(0),
            limited: Arc::default(),
            whoami: Mutex::default(),
        })
    }

    async fn json_of(what: &str, resp: reqwest::Response) -> Result<Value> {
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("synapse {what} failed: {} {text}", status.as_u16());
        }
        serde_json::from_str(&text).with_context(|| format!("synapse {what}: bad json"))
    }

    async fn create_account(&self, id: String) -> Result<Account> {
        let nonce = Self::json_of(
            "register nonce",
            self.http.get(format!("{}/_synapse/admin/v1/register", self.base_url)).send().await?,
        )
        .await?["nonce"]
            .as_str()
            .context("register nonce missing")?
            .to_string();
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
        let username = format!("{id}_{stamp:x}");
        let password = format!("{:032x}", rand::rng().random::<u128>());
        let mac = register_mac(&self.secret, &nonce, &username, &password);
        let reg = Self::json_of(
            &format!("register {id}"),
            self.http
                .post(format!("{}/_synapse/admin/v1/register", self.base_url))
                .json(&json!({"nonce": nonce, "username": username, "password": password, "admin": false, "mac": mac}))
                .send()
                .await?,
        )
        .await?;
        let token = reg["access_token"].as_str().context("register response without access_token")?.to_string();
        let matrix_id = reg["user_id"].as_str().context("register response without user_id")?.to_string();
        if let Some(name) = &self.server_name {
            if !matrix_id.ends_with(&format!(":{name}")) {
                bail!("registered user {matrix_id} is not on server {name}");
            }
        }
        let filter: Value = serde_json::from_str(FILTER)?;
        let filter_id = Self::json_of(
            &format!("filter upload {id}"),
            self.http
                .post(format!("{}/_matrix/client/v3/user/{}/filter", self.base_url, enc(&matrix_id)))
                .bearer_auth(&token)
                .json(&filter)
                .send()
                .await?,
        )
        .await?["filter_id"]
            .as_str()
            .context("filter response without filter_id")?
            .to_string();
        Ok(Account { id, matrix_id, token, filter_id })
    }

    async fn matrix_id(&self, token: &str) -> Result<String> {
        if let Some(v) = self.whoami.lock().unwrap().get(token) {
            return Ok(v.clone());
        }
        let v = Self::json_of(
            "whoami",
            self.http
                .get(format!("{}/_matrix/client/v3/account/whoami", self.base_url))
                .bearer_auth(token)
                .send()
                .await?,
        )
        .await?["user_id"]
            .as_str()
            .context("whoami without user_id")?
            .to_string();
        self.whoami.lock().unwrap().insert(token.to_string(), v.clone());
        Ok(v)
    }
}

async fn sync_once(http: &reqwest::Client, base_url: &str, token: &str, since: Option<&str>) -> Result<Value> {
    let url = match since {
        None => format!("{base_url}/_matrix/client/v3/sync?timeout=0&filter={}", enc(FILTER)),
        Some(s) => format!(
            "{base_url}/_matrix/client/v3/sync?timeout={SYNC_LONG_POLL_MS}&since={}&filter={}",
            enc(s),
            enc(SYNC_FILTER)
        ),
    };
    let resp = http.get(url).bearer_auth(token).timeout(SYNC_TIMEOUT).send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        bail!("sync failed: {} {text}", status.as_u16());
    }
    Ok(serde_json::from_str(&text)?)
}

struct SynapseConn {
    rooms: Arc<Mutex<HashSet<String>>>,
    changed: Arc<watch::Sender<u64>>,
    task: JoinHandle<()>,
    limited: Arc<AtomicU64>,
}

#[async_trait]
impl Conn for SynapseConn {
    async fn subscribe(&mut self, room: &str) -> Result<()> {
        let mut rx = self.changed.subscribe();
        let deadline = tokio::time::Instant::now() + REQUEST_TIMEOUT;
        loop {
            if self.rooms.lock().unwrap().contains(room) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, rx.changed()).await.is_err() {
                bail!("synapse sync never showed room {room}");
            }
        }
    }

    async fn close(self: Box<Self>) {
        self.task.abort();
        let n = self.limited.load(Ordering::Relaxed);
        if n > 0 {
            eprintln!("synapse: {n} limited timelines so far across connections (events beyond the sync limit were lost)");
        }
    }
}

#[async_trait]
impl Target for SynapseTarget {
    async fn setup(&self, plan: &SetupPlan) -> Result<Fixtures> {
        let mut ids: HashMap<String, String> = HashMap::new();
        let mut order = Vec::new();
        let mut rooms_mapped = Vec::new();
        for (_, members) in &plan.rooms {
            let mut mapped = Vec::new();
            for m in members {
                let next = ids.len() + 1;
                let id = ids
                    .entry(m.clone())
                    .or_insert_with(|| {
                        let id = format!("u{next:06}");
                        order.push(id.clone());
                        id
                    })
                    .clone();
                mapped.push(id);
            }
            rooms_mapped.push(mapped);
        }
        let accounts: Vec<Account> = futures_util::stream::iter(order.into_iter().map(|id| self.create_account(id)))
            .buffered(SETUP_CONCURRENCY)
            .try_collect()
            .await?;
        let by_id: HashMap<&str, &Account> = accounts.iter().map(|a| (a.id.as_str(), a)).collect();
        let mut rooms = Vec::new();
        for mapped in rooms_mapped {
            let owner = by_id[mapped[0].as_str()];
            let created = Self::json_of(
                &format!("createRoom for {}", owner.id),
                self.http
                    .post(format!("{}/_matrix/client/v3/createRoom", self.base_url))
                    .bearer_auth(&owner.token)
                    .json(&json!({"preset": "public_chat"}))
                    .send()
                    .await?,
            )
            .await?;
            let room = created["room_id"].as_str().context("createRoom returned no room_id")?.to_string();
            let joins: Vec<(String, String)> =
                mapped[1..].iter().map(|m| (m.clone(), by_id[m.as_str()].token.clone())).collect();
            futures_util::stream::iter(joins.into_iter().map(|(user, token)| {
                let room = room.clone();
                async move {
                    let resp = self
                        .http
                        .post(format!("{}/_matrix/client/v3/join/{}", self.base_url, enc(&room)))
                        .bearer_auth(&token)
                        .json(&json!({}))
                        .send()
                        .await?;
                    let status = resp.status();
                    if !status.is_success() {
                        let text = resp.text().await.unwrap_or_default();
                        return Err(anyhow!(join_error(&user, &room, status.as_u16(), &text)));
                    }
                    Ok(())
                }
            }))
            .buffered(SETUP_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;
            rooms.push(RoomFixture { id: room, members: mapped });
        }
        let mut meta = Map::new();
        meta.insert(
            "matrix_users".into(),
            Value::Object(accounts.iter().map(|a| (a.id.clone(), json!(a.matrix_id))).collect()),
        );
        meta.insert(
            "filter_ids".into(),
            Value::Object(accounts.iter().map(|a| (a.id.clone(), json!(a.filter_id))).collect()),
        );
        meta.insert("room_ids".into(), json!(rooms.iter().map(|r| r.id.clone()).collect::<Vec<_>>()));
        Ok(Fixtures {
            target: "synapse".into(),
            scenario: String::new(),
            users: accounts.iter().map(|a| UserCred { id: a.id.clone(), token: Some(a.token.clone()) }).collect(),
            rooms,
            meta,
        })
    }

    async fn connect(&self, user: &UserCred, deliveries: mpsc::UnboundedSender<Delivery>) -> Result<Box<dyn Conn>> {
        let token = user.token.clone().context("synapse user has no token")?;
        let own = self.matrix_id(&token).await?;
        let first = sync_once(&self.http, &self.base_url, &token, None).await?;
        let mut since = first["next_batch"].as_str().context("sync without next_batch")?.to_string();
        let (_, joined) = handle_sync(&first, true, &own);
        let rooms: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(joined.into_iter().collect()));
        let (tx, _) = watch::channel(0u64);
        let changed = Arc::new(tx);
        let task = {
            let (http, base_url, rooms, changed) = (self.http.clone(), self.base_url.clone(), rooms.clone(), changed.clone());
            let limited = self.limited.clone();
            tokio::spawn(async move {
                let mut deliveries = Some(deliveries);
                let mut backoff = Backoff::new();
                let mut failures = 0u32;
                loop {
                    match sync_once(&http, &base_url, &token, Some(&since)).await {
                        Ok(resp) => {
                            let at = Instant::now();
                            failures = 0;
                            backoff.reset();
                            if let Some(n) = resp["next_batch"].as_str() {
                                since = n.to_string();
                            }
                            limited.fetch_add(limited_rooms(&resp) as u64, Ordering::Relaxed);
                            let (ids, joined) = handle_sync(&resp, false, &own);
                            if !joined.is_empty() {
                                rooms.lock().unwrap().extend(joined);
                                changed.send_modify(|v| *v += 1);
                            }
                            if let Some(tx) = &deliveries {
                                for msg_id in ids {
                                    let _ = tx.send(Delivery { msg_id, at });
                                }
                            }
                        }
                        Err(_) => {
                            failures += 1;
                            if failures > 1 {
                                deliveries = None;
                            }
                            tokio::time::sleep(backoff.next_delay()).await;
                        }
                    }
                }
            })
        };
        Ok(Box::new(SynapseConn { rooms, changed, task, limited: self.limited.clone() }))
    }

    async fn send(&self, user: &UserCred, room: &str, body: String) -> Result<Ack> {
        let token = user.token.as_deref().context("synapse user has no token")?;
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let txn = txn_id(&self.run_prefix, &user.id, body::decode(&body).unwrap_or(0), n);
        let resp = self
            .http
            .put(format!("{}/_matrix/client/v3/rooms/{}/send/m.room.message/{txn}", self.base_url, enc(room)))
            .bearer_auth(token)
            .json(&json!({"msgtype": "m.text", "body": body}))
            .send()
            .await?;
        let at = Instant::now();
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("send failed: {} {text}", status.as_u16());
        }
        Ok(Ack { at })
    }

    async fn history(&self, user: &UserCred, room: &str, limit: usize) -> Result<usize> {
        let token = user.token.as_deref().context("synapse user has no token")?;
        let resp = self
            .http
            .get(format!("{}/_matrix/client/v3/rooms/{}/messages?dir=b&limit={limit}", self.base_url, enc(room)))
            .bearer_auth(token)
            .send()
            .await?;
        let v = Self::json_of("history", resp).await?;
        Ok(v["chunk"].as_array().map_or(0, |c| c.iter().filter(|e| e["type"] == "m.room.message").count()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body;
    use serde_json::json;

    fn fixture() -> Value {
        json!({
            "next_batch": "s2",
            "rooms": {"join": {"!r1:bench.local": {"timeline": {"events": [
                {"type": "m.room.message", "sender": "@b:bench.local", "content": {"msgtype": "m.text", "body": body::encode(7, 64)}},
                {"type": "m.room.member", "sender": "@c:bench.local", "content": {"membership": "join"}},
                {"type": "m.room.message", "sender": "@b:bench.local", "content": {"msgtype": "m.text", "body": body::encode(8, 64)}},
                {"type": "m.room.message", "sender": "@b:bench.local", "content": {"msgtype": "m.text", "body": "hello"}},
                {"type": "m.room.message", "sender": "@me:bench.local", "content": {"msgtype": "m.text", "body": body::encode(9, 64)}}
            ]}}}}
        })
    }

    #[test]
    fn parse_sync() {
        let (ids, rooms) = handle_sync(&fixture(), false, "@me:bench.local");
        assert_eq!(ids, vec![7, 8]);
        assert_eq!(rooms, vec!["!r1:bench.local".to_string()]);
    }

    #[test]
    fn initial_sync_not_counted() {
        let (ids, rooms) = handle_sync(&fixture(), true, "@me:bench.local");
        assert!(ids.is_empty());
        assert_eq!(rooms, vec!["!r1:bench.local".to_string()]);
    }

    #[test]
    fn limited_timelines_counted() {
        let resp = json!({"rooms": {"join": {
            "!a:x": {"timeline": {"limited": true, "events": []}},
            "!b:x": {"timeline": {"limited": false, "events": []}},
            "!c:x": {"timeline": {"events": []}}
        }}});
        assert_eq!(limited_rooms(&resp), 1);
        assert_eq!(limited_rooms(&json!({})), 0);
    }

    #[test]
    fn txn_ids_unique_for_same_user_and_msg() {
        assert_ne!(txn_id("p", "u1", 5, 0), txn_id("p", "u1", 5, 1));
    }

    #[test]
    fn register_mac_matches_openssl() {
        assert_eq!(
            register_mac("secret1", "nonce1", "alice", "pw"),
            "85ebf277b69c22df711ab79a03ec2085cb05780c"
        );
    }

    #[test]
    fn join_error_is_fatal() {
        assert_eq!(
            join_error("u000003", "!r:bench.local", 403, "{\"errcode\":\"M_FORBIDDEN\"}"),
            "synapse join failed for u000003 in !r:bench.local: 403 {\"errcode\":\"M_FORBIDDEN\"}"
        );
    }
}
