use super::{finish_step, groups_from, run_step, summary, RunMeta, StepSpec};
use crate::adapter::Target;
use crate::config::RunConfig;
use crate::results::{Derived, Fixtures, Summary};
use anyhow::Result;
use std::sync::Arc;

pub async fn run(target: Arc<dyn Target>, fixtures: &Fixtures, cfg: &RunConfig, meta: &RunMeta) -> Result<Summary> {
    let scale = &cfg.scenarios.latency_ladder;
    let (rooms, members) = (scale.rooms as u64, scale.members as u64);
    let mut steps = Vec::new();
    let mut best: Option<f64> = None;
    let mut bad_run = 0;
    for (index, &rate) in scale.rates.iter().enumerate() {
        let spec = StepSpec {
            index,
            label: format!("rate={rate}"),
            params: serde_json::json!({"rate": rate, "rooms": scale.rooms, "members": scale.members})
                .as_object()
                .unwrap()
                .clone(),
            rate,
            groups: groups_from(fixtures, scale.rooms, |_| scale.members)?,
            pick: Arc::new(move |i| ((i % rooms) as usize, ((i / rooms) % members) as usize)),
            track_last: false,
        };
        let out = run_step(target.clone(), cfg, &spec, meta.smoke).await;
        finish_step(meta, &out);
        let s = &out.step;
        let failing = s.errors > s.sent;
        let good = !failing
            && !s.saturated
            && !s.invalid
            && s.expected > 0
            && s.latency_us.p99 < 250_000
            && (s.lost as f64 / s.expected as f64) < 0.001;
        if good {
            best = Some(best.map_or(rate as f64, |b| b.max(rate as f64)));
        }
        bad_run = if s.saturated || s.invalid { bad_run + 1 } else { 0 };
        steps.push(out.step);
        if failing || bad_run >= 2 {
            break;
        }
    }
    Ok(summary(meta, steps, Derived { max_sustainable_rate: best }))
}
