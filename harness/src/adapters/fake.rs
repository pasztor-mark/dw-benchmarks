use crate::adapter::{Ack, Conn, Delivery, SetupPlan, Target};
use crate::body;
use crate::results::{Fixtures, RoomFixture, UserCred};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

type Subs = Arc<Mutex<HashMap<String, Vec<(String, mpsc::UnboundedSender<Delivery>)>>>>;

type Conns = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Delivery>>>>;

pub struct FakeTarget {
    pub latency: Duration,
    pub drop_every: Option<u64>,
    pub history_returns: Option<usize>,
    pub fail_sends: bool,
    sends: AtomicU64,
    subs: Subs,
    conns: Conns,
}

impl FakeTarget {
    pub fn new(latency: Duration, drop_every: Option<u64>) -> Self {
        Self {
            latency,
            drop_every,
            history_returns: None,
            fail_sends: false,
            sends: AtomicU64::new(0),
            subs: Arc::default(),
            conns: Arc::default(),
        }
    }

    #[cfg(test)]
    pub fn restart(&self) {
        self.subs.lock().unwrap().clear();
        self.conns.lock().unwrap().clear();
    }
}

struct FakeConn {
    user: String,
    subs: Subs,
    conns: Conns,
}

#[async_trait]
impl Conn for FakeConn {
    async fn subscribe(&mut self, room: &str) -> anyhow::Result<()> {
        let tx = self
            .conns
            .lock()
            .unwrap()
            .get(&self.user)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("connection dropped"))?;
        self.subs.lock().unwrap().entry(room.to_string()).or_default().push((self.user.clone(), tx));
        Ok(())
    }

    async fn close(self: Box<Self>) {
        self.conns.lock().unwrap().remove(&self.user);
        for members in self.subs.lock().unwrap().values_mut() {
            members.retain(|(u, _)| u != &self.user);
        }
    }
}

#[async_trait]
impl Target for FakeTarget {
    async fn setup(&self, plan: &SetupPlan) -> anyhow::Result<Fixtures> {
        let mut users: Vec<String> = plan.rooms.iter().flat_map(|(_, m)| m.clone()).collect();
        users.sort();
        users.dedup();
        Ok(Fixtures {
            target: "fake".into(),
            scenario: String::new(),
            users: users.into_iter().map(|id| UserCred { id, token: None }).collect(),
            rooms: plan
                .rooms
                .iter()
                .map(|(key, members)| RoomFixture { id: key.clone(), members: members.clone() })
                .collect(),
            meta: serde_json::Map::new(),
        })
    }

    async fn connect(
        &self,
        user: &UserCred,
        deliveries: mpsc::UnboundedSender<Delivery>,
    ) -> anyhow::Result<Box<dyn Conn>> {
        self.conns.lock().unwrap().insert(user.id.clone(), deliveries);
        Ok(Box::new(FakeConn { user: user.id.clone(), subs: self.subs.clone(), conns: self.conns.clone() }))
    }

    async fn send(&self, user: &UserCred, room: &str, body: String) -> anyhow::Result<Ack> {
        let n = self.sends.fetch_add(1, Ordering::SeqCst) + 1;
        let msg_id = body::decode(&body).ok_or_else(|| anyhow::anyhow!("undecodable body"))?;
        if self.fail_sends {
            anyhow::bail!("send failed");
        }
        if self.drop_every.is_some_and(|d| n.is_multiple_of(d)) {
            return Ok(Ack { at: Instant::now() });
        }
        let targets: Vec<_> = self
            .subs
            .lock()
            .unwrap()
            .get(room)
            .map(|m| m.iter().filter(|(u, _)| u != &user.id).map(|(_, tx)| tx.clone()).collect())
            .unwrap_or_default();
        let latency = self.latency;
        tokio::spawn(async move {
            tokio::time::sleep(latency).await;
            for tx in targets {
                let _ = tx.send(Delivery { msg_id, at: Instant::now() });
            }
        });
        Ok(Ack { at: Instant::now() })
    }

    async fn history(&self, _user: &UserCred, _room: &str, limit: usize) -> anyhow::Result<usize> {
        Ok(self.history_returns.unwrap_or(limit))
    }
}
