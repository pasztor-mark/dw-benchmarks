use crate::config::RunConfig;
use crate::results::{Fixtures, UserCred};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

pub struct Delivery {
    pub msg_id: u64,
    pub at: Instant,
}

pub struct Ack {
    pub at: Instant,
}

pub struct SetupPlan {
    pub rooms: Vec<(String, Vec<String>)>,
}

#[async_trait]
pub trait Target: Send + Sync {
    async fn setup(&self, plan: &SetupPlan) -> anyhow::Result<Fixtures>;
    async fn connect(
        &self,
        user: &UserCred,
        deliveries: mpsc::UnboundedSender<Delivery>,
    ) -> anyhow::Result<Box<dyn Conn>>;
    async fn send(&self, user: &UserCred, room: &str, body: String) -> anyhow::Result<Ack>;
    async fn history(&self, user: &UserCred, room: &str, limit: usize) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait Conn: Send {
    async fn subscribe(&mut self, room: &str) -> anyhow::Result<()>;
    async fn close(self: Box<Self>);
}

pub fn build_target(name: &str, cfg: &RunConfig) -> anyhow::Result<Arc<dyn Target>> {
    match name {
        "fake" => Ok(Arc::new(crate::adapters::fake::FakeTarget::new(
            std::time::Duration::from_millis(1),
            None,
        ))),
        "dotwire" => Ok(Arc::new(crate::adapters::dotwire::DotwireTarget::new(cfg)?)),
        "dotwire-sse" => Ok(Arc::new(crate::adapters::dotwire_sse::DotwireSseTarget::new(cfg)?)),
        "dotwire-hub" => Ok(Arc::new(crate::adapters::dotwire_hub::DotwireHubTarget::new(cfg)?)),
        "tinode" => Ok(Arc::new(crate::adapters::tinode::TinodeTarget::new(cfg)?)),
        "synapse" => Ok(Arc::new(crate::adapters::synapse::SynapseTarget::new(cfg)?)),
        other => anyhow::bail!("target {other} not implemented"),
    }
}
