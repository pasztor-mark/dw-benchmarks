use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TargetEndpoints {
    pub base_url: String,
    pub ws_url: String,
    pub admin_secret: Option<String>,
    pub jwt_private_key_path: Option<String>,
    pub jwt_kid: String,
    pub jwt_issuer: String,
    pub jwt_audience: String,
    pub tinode_api_key: Option<String>,
    pub synapse_server_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StepTiming {
    pub warmup_s: u64,
    pub measure_s: u64,
    pub drain_s: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LatencyLadderScale {
    pub rooms: usize,
    pub members: usize,
    pub rates: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FanoutScale {
    pub sizes: Vec<usize>,
    pub rate: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectionsScale {
    pub levels: Vec<usize>,
    pub connect_rate: u64,
    pub hold_s: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReconnectScale {
    pub users: usize,
    pub rate_per_room: f64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HistoryScale {
    pub rooms: usize,
    pub per_room: usize,
    pub rates: Vec<u64>,
    pub limit: usize,
    #[serde(default = "serial")]
    pub preload_concurrency: usize,
}

fn serial() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ScenarioScales {
    pub latency_ladder: LatencyLadderScale,
    pub fanout: FanoutScale,
    pub connections: ConnectionsScale,
    pub reconnect: ReconnectScale,
    pub history: HistoryScale,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub target: TargetEndpoints,
    pub payload_bytes: usize,
    pub inflight_cap: usize,
    pub step: StepTiming,
    pub scenarios: ScenarioScales,
}

impl RunConfig {
    pub fn load(path: &Path) -> Result<RunConfig> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut cfg: RunConfig = toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        if let Ok(v) = std::env::var("DWBENCH_BASE_URL") {
            cfg.target.base_url = v;
        }
        if let Ok(v) = std::env::var("DWBENCH_WS_URL") {
            cfg.target.ws_url = v;
        }
        if let Ok(v) = std::env::var("DWBENCH_TINODE_API_KEY") {
            cfg.target.tinode_api_key = Some(v);
        }
        if let Some(v) = std::env::var("DWBENCH_ADMIN_SECRET").ok().filter(|v| !v.is_empty()) {
            cfg.target.admin_secret = Some(v);
        }
        if let Some(v) = std::env::var("DWBENCH_SYNAPSE_SERVER_NAME").ok().filter(|v| !v.is_empty()) {
            cfg.target.synapse_server_name = Some(v);
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SMOKE: &str = include_str!("../configs/smoke.toml");

    #[test]
    fn smoke_toml_parses() {
        let cfg = RunConfig::load(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/smoke.toml"
        )))
        .expect("smoke.toml must parse");
        assert_eq!(cfg.scenarios.latency_ladder.rates, vec![2, 5]);
        assert_eq!(cfg.step.measure_s, 5);
    }

    #[test]
    fn full_toml_parses() {
        let cfg = RunConfig::load(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/full.toml"
        )))
        .expect("full.toml must parse");
        assert_eq!(cfg.scenarios.latency_ladder.rates, vec![100, 500, 1000, 2000, 5000, 10000]);
        assert_eq!((cfg.step.warmup_s, cfg.step.measure_s, cfg.step.drain_s), (30, 120, 10));
        assert_eq!((cfg.payload_bytes, cfg.inflight_cap), (256, 10000));
        assert_eq!(cfg.scenarios.history.preload_concurrency, 1);
    }

    #[test]
    fn synapse_toml_parses() {
        let cfg = RunConfig::load(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/synapse.toml"
        )))
        .expect("synapse.toml must parse");
        assert_eq!(cfg.scenarios.latency_ladder.rates, vec![10, 25, 50, 100]);
        assert_eq!(cfg.scenarios.fanout.rate, 2);
        assert_eq!(cfg.scenarios.history.preload_concurrency, 32);
    }

    #[test]
    fn unknown_key_rejected() {
        let text = format!("bogus = 1\n{SMOKE}");
        let err = toml::from_str::<RunConfig>(&text).expect_err("unknown key must be rejected");
        assert!(err.to_string().contains("bogus"), "error was: {err}");
    }
}
