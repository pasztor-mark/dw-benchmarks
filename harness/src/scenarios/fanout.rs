use super::{finish_step, groups_from, run_step, summary, RunMeta, StepSpec};
use crate::adapter::Target;
use crate::config::RunConfig;
use crate::results::{Derived, Fixtures, Summary};
use anyhow::Result;
use std::sync::Arc;

pub async fn run(target: Arc<dyn Target>, fixtures: &Fixtures, cfg: &RunConfig, meta: &RunMeta) -> Result<Summary> {
    let scale = &cfg.scenarios.fanout;
    let mut steps = Vec::new();
    for (index, &size) in scale.sizes.iter().enumerate() {
        let spec = StepSpec {
            index,
            label: format!("size={size}"),
            params: serde_json::json!({"size": size, "rate": scale.rate}).as_object().unwrap().clone(),
            rate: scale.rate,
            groups: groups_from(fixtures, 1, |_| size)?,
            pick: Arc::new(|_| (0, 0)),
            track_last: true,
        };
        let out = run_step(target.clone(), cfg, &spec, meta.smoke).await;
        finish_step(meta, &out);
        steps.push(out.step);
    }
    Ok(summary(meta, steps, Derived::default()))
}
