use crate::adapter::{Ack, Conn, Delivery, SetupPlan, Target};
use crate::body;
use crate::config::RunConfig;
use crate::results::{Fixtures, RoomFixture, UserCred};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use base64::Engine;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, OnceCell};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SETUP_CONCURRENCY: usize = 32;
const PASSWORD: &str = "benchpw123";

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<(Value, Instant)>>>>;

fn parse_data(frame: &Value) -> Option<u64> {
    frame["data"]["content"].as_str().and_then(body::decode)
}

fn ctrl_of(frame: &Value) -> (u64, &str) {
    let c = &frame["ctrl"];
    (c["code"].as_u64().unwrap_or(0), c["text"].as_str().unwrap_or(""))
}

fn check_join(user: &str, topic: &str, frame: &Value) -> Result<()> {
    let (code, text) = ctrl_of(frame);
    if code >= 300 || code == 0 {
        bail!("tinode join failed for {user} in {topic}: {code} {text}");
    }
    Ok(())
}

fn check_ok(what: &str, frame: &Value, allow_not_modified: bool) -> Result<()> {
    let (code, text) = ctrl_of(frame);
    if (code > 0 && code < 300) || (allow_not_modified && code == 304) {
        return Ok(());
    }
    bail!("tinode {what} failed: {code} {text}")
}

struct Session {
    sink: tokio::sync::Mutex<Sink>,
    pending: Pending,
    next_id: AtomicU64,
    alive: Arc<AtomicBool>,
    hist: Arc<Mutex<Option<usize>>>,
    hist_lock: tokio::sync::Mutex<()>,
    subscribed: Mutex<HashSet<String>>,
    reader: JoinHandle<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Session {
    async fn open(ws_url: &str, api_key: &str, deliveries: Option<mpsc::UnboundedSender<Delivery>>) -> Result<Arc<Session>> {
        let url = format!("{ws_url}/v0/channels?apikey={api_key}");
        let (ws, _) = tokio::time::timeout(REQUEST_TIMEOUT, tokio_tungstenite::connect_async(url))
            .await
            .context("tinode connect timed out")??;
        let (sink, mut stream) = ws.split();
        let pending: Pending = Arc::default();
        let alive = Arc::new(AtomicBool::new(true));
        let hist: Arc<Mutex<Option<usize>>> = Arc::default();
        let reader = {
            let (pending, alive, hist) = (pending.clone(), alive.clone(), hist.clone());
            tokio::spawn(async move {
                while let Ok(Some(msg)) = stream.try_next().await {
                    let at = Instant::now();
                    let Message::Text(t) = msg else { continue };
                    let Ok(frame) = serde_json::from_str::<Value>(&t) else { continue };
                    if frame.get("ctrl").is_some() {
                        if let Some(tx) = frame["ctrl"]["id"].as_str().and_then(|id| pending.lock().unwrap().remove(id)) {
                            let _ = tx.send((frame, at));
                        }
                    } else if frame.get("data").is_some() {
                        if let Some(n) = hist.lock().unwrap().as_mut() {
                            *n += 1;
                        }
                        if let (Some(tx), Some(msg_id)) = (&deliveries, parse_data(&frame)) {
                            let _ = tx.send(Delivery { msg_id, at });
                        }
                    }
                }
                alive.store(false, Ordering::Relaxed);
                pending.lock().unwrap().clear();
            })
        };
        let s = Arc::new(Session {
            sink: tokio::sync::Mutex::new(sink),
            pending,
            next_id: AtomicU64::new(1),
            alive,
            hist,
            hist_lock: tokio::sync::Mutex::new(()),
            subscribed: Mutex::default(),
            reader,
        });
        let hi = s.request("hi", json!({"ver": "0.25", "ua": "dwbench/0.1"})).await?;
        check_ok("handshake", &hi.0, false)?;
        Ok(s)
    }

    async fn request(&self, kind: &str, mut body: Value) -> Result<(Value, Instant)> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        body["id"] = json!(id);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.clone(), tx);
        let mut frame = Map::new();
        frame.insert(kind.into(), body);
        let sent = tokio::time::timeout(REQUEST_TIMEOUT, async {
            self.sink.lock().await.send(Message::Text(Value::Object(frame).to_string().into())).await
        })
        .await;
        match sent {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.pending.lock().unwrap().remove(&id);
                return Err(e.into());
            }
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                bail!("tinode {kind} write timed out");
            }
        }
        match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(_)) => bail!("tinode connection closed during {kind}"),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                bail!("tinode {kind} timed out")
            }
        }
    }

    async fn login(&self, token: &str) -> Result<()> {
        let (frame, _) = self.request("login", json!({"scheme": "token", "secret": token})).await?;
        check_ok("login", &frame, false)
    }

    async fn ensure_subscribed(&self, topic: &str) -> Result<()> {
        if self.subscribed.lock().unwrap().contains(topic) {
            return Ok(());
        }
        let (frame, _) = self.request("sub", json!({"topic": topic})).await?;
        check_ok(&format!("subscribe {topic}"), &frame, true)?;
        self.subscribed.lock().unwrap().insert(topic.to_string());
        Ok(())
    }
}

pub(super) trait Live {
    fn is_alive(&self) -> bool;
}

impl Live for Session {
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }
}

pub(super) struct Registry<T> {
    map: Mutex<HashMap<String, Arc<OnceCell<Arc<T>>>>>,
}

impl<T> Default for Registry<T> {
    fn default() -> Self {
        Self { map: Mutex::default() }
    }
}

impl<T: Live> Registry<T> {
    pub(super) fn register(&self, user: &str, item: Arc<T>) {
        self.map.lock().unwrap().insert(user.to_string(), Arc::new(OnceCell::new_with(Some(item))));
    }

    pub(super) fn unregister(&self, user: &str, item: &Arc<T>) {
        let mut map = self.map.lock().unwrap();
        if map.get(user).and_then(|c| c.get()).is_some_and(|s| Arc::ptr_eq(s, item)) {
            map.remove(user);
        }
    }

    pub(super) async fn get_or_open<F, Fut>(&self, user: &str, open: F) -> Result<Arc<T>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Arc<T>>>,
    {
        let cell = {
            let mut map = self.map.lock().unwrap();
            if map.get(user).and_then(|c| c.get()).is_some_and(|s| !s.is_alive()) {
                map.remove(user);
            }
            map.entry(user.to_string()).or_default().clone()
        };
        Ok(cell.get_or_try_init(open).await?.clone())
    }
}

struct Account {
    id: String,
    tinode_id: String,
    token: String,
    session: Arc<Session>,
}

pub struct TinodeTarget {
    ws_url: String,
    api_key: String,
    senders: Arc<Registry<Session>>,
    history: Registry<Session>,
}

impl TinodeTarget {
    pub fn new(cfg: &RunConfig) -> Result<Self> {
        Ok(Self {
            ws_url: cfg.target.ws_url.trim_end_matches('/').to_string(),
            api_key: cfg.target.tinode_api_key.clone().context("target.tinode_api_key is required for tinode")?,
            senders: Arc::default(),
            history: Registry::default(),
        })
    }

    async fn create_account(&self, id: String) -> Result<Account> {
        let session = Session::open(&self.ws_url, &self.api_key, None).await?;
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
        let login = format!("{id}_{stamp:x}");
        let secret = base64::engine::general_purpose::STANDARD.encode(format!("{login}:{PASSWORD}"));
        let (frame, _) = session
            .request(
                "acc",
                json!({"user": "new", "scheme": "basic", "secret": secret, "login": true, "desc": {"public": {"fn": id}}}),
            )
            .await?;
        check_ok(&format!("create account {id}"), &frame, false)?;
        let params = &frame["ctrl"]["params"];
        let tinode_id = params["user"].as_str().context("acc response without user")?.to_string();
        let token = params["token"].as_str().context("acc response without token")?.to_string();
        Ok(Account { id, tinode_id, token, session })
    }

    async fn plain_session(&self, user: &UserCred, registry: &Registry<Session>) -> Result<Arc<Session>> {
        let token = user.token.as_deref().context("tinode user has no token")?;
        registry
            .get_or_open(&user.id, || async {
                let s = Session::open(&self.ws_url, &self.api_key, None).await?;
                s.login(token).await?;
                Ok(s)
            })
            .await
    }
}

struct TinodeConn {
    user: String,
    session: Arc<Session>,
    senders: Arc<Registry<Session>>,
}

#[async_trait]
impl Conn for TinodeConn {
    async fn subscribe(&mut self, room: &str) -> Result<()> {
        self.session.ensure_subscribed(room).await
    }

    async fn close(self: Box<Self>) {
        self.session.alive.store(false, Ordering::Relaxed);
        self.senders.unregister(&self.user, &self.session);
        self.session.reader.abort();
    }
}

#[async_trait]
impl Target for TinodeTarget {
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
            let (frame, _) = owner.session.request("sub", json!({"topic": "new"})).await?;
            check_join(&owner.id, "new", &frame)?;
            let topic = frame["ctrl"]["topic"].as_str().context("group creation returned no topic")?.to_string();
            let joins: Vec<(Arc<Session>, String, String)> = mapped[1..]
                .iter()
                .map(|m| (by_id[m.as_str()].session.clone(), m.clone(), topic.clone()))
                .collect();
            futures_util::stream::iter(joins.into_iter().map(|(session, user, topic)| async move {
                let (frame, _) = session.request("sub", json!({"topic": topic})).await?;
                check_join(&user, &topic, &frame)
            }))
            .buffered(SETUP_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;
            rooms.push(RoomFixture { id: topic, members: mapped });
        }
        let mut meta = Map::new();
        meta.insert(
            "tinode_users".into(),
            Value::Object(accounts.iter().map(|a| (a.id.clone(), json!(a.tinode_id))).collect()),
        );
        Ok(Fixtures {
            target: "tinode".into(),
            scenario: String::new(),
            users: accounts.iter().map(|a| UserCred { id: a.id.clone(), token: Some(a.token.clone()) }).collect(),
            rooms,
            meta,
        })
    }

    async fn connect(&self, user: &UserCred, deliveries: mpsc::UnboundedSender<Delivery>) -> Result<Box<dyn Conn>> {
        let token = user.token.as_deref().context("tinode user has no token")?;
        let session = Session::open(&self.ws_url, &self.api_key, Some(deliveries)).await?;
        session.login(token).await?;
        self.senders.register(&user.id, session.clone());
        Ok(Box::new(TinodeConn { user: user.id.clone(), session, senders: self.senders.clone() }))
    }

    async fn send(&self, user: &UserCred, room: &str, body: String) -> Result<Ack> {
        let session = self.plain_session(user, &self.senders).await?;
        session.ensure_subscribed(room).await?;
        let (frame, at) = session.request("pub", json!({"topic": room, "noecho": true, "content": body})).await?;
        let (code, text) = ctrl_of(&frame);
        if code != 202 {
            bail!("send failed: {code} {text}");
        }
        Ok(Ack { at })
    }

    async fn history(&self, user: &UserCred, room: &str, limit: usize) -> Result<usize> {
        let session = self.plain_session(user, &self.history).await?;
        session.ensure_subscribed(room).await?;
        let _guard = session.hist_lock.lock().await;
        *session.hist.lock().unwrap() = Some(0);
        let res = session
            .request("get", json!({"topic": room, "what": "data", "data": {"limit": limit}}))
            .await;
        let n = session.hist.lock().unwrap().take().unwrap_or(0);
        let (frame, _) = res?;
        let (code, text) = ctrl_of(&frame);
        if code >= 300 && code != 304 {
            return Err(anyhow!("history failed: {code} {text}"));
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_data() {
        let f = json!({"data": {"topic": "grp1", "from": "usr1", "seq": 3, "content": "dwb:9:.."}});
        assert_eq!(super::parse_data(&f), Some(9));
        let f = json!({"data": {"topic": "grp1", "seq": 4, "content": "hello"}});
        assert_eq!(super::parse_data(&f), None);
        assert_eq!(super::parse_data(&json!({"ctrl": {"id": "1", "code": 200}})), None);
    }

    struct Fake(AtomicBool);

    impl Live for Fake {
        fn is_alive(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }
    }

    #[tokio::test]
    async fn send_session_selection() {
        let reg: Registry<Fake> = Registry::default();
        let opens = AtomicU64::new(0);
        let open = || async {
            opens.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(Fake(AtomicBool::new(true))))
        };
        let conn = Arc::new(Fake(AtomicBool::new(true)));
        reg.register("a", conn.clone());
        assert!(Arc::ptr_eq(&reg.get_or_open("a", open).await.unwrap(), &conn));
        assert_eq!(opens.load(Ordering::Relaxed), 0);
        let lazy = reg.get_or_open("b", open).await.unwrap();
        assert!(Arc::ptr_eq(&reg.get_or_open("b", open).await.unwrap(), &lazy));
        assert_eq!(opens.load(Ordering::Relaxed), 1);
        conn.0.store(false, Ordering::Relaxed);
        reg.unregister("a", &conn);
        let fresh = reg.get_or_open("a", open).await.unwrap();
        assert!(!Arc::ptr_eq(&fresh, &conn));
        assert_eq!(opens.load(Ordering::Relaxed), 2);
        fresh.0.store(false, Ordering::Relaxed);
        let replaced = reg.get_or_open("a", open).await.unwrap();
        assert!(!Arc::ptr_eq(&replaced, &fresh));
        assert_eq!(opens.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn join_error_is_fatal() {
        let f = json!({"ctrl": {"id": "5", "code": 403, "text": "access denied"}});
        let err = check_join("u000003", "grp9", &f).unwrap_err().to_string();
        assert_eq!(err, "tinode join failed for u000003 in grp9: 403 access denied");
        assert!(check_join("u1", "grp9", &json!({"ctrl": {"id": "5", "code": 200, "text": "ok"}})).is_ok());
    }
}
