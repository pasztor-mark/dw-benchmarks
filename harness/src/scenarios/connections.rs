use super::{apply_guards, blank_step, connect_sub, finish_step, new_hist, open_loop_collect, pct, placements, summary, RunMeta, StepOut};
use crate::adapter::{Conn, Target};
use crate::config::RunConfig;
use crate::guards::CpuMeter;
use crate::results::{Derived, Fixtures, Summary};
use anyhow::Result;
use futures_util::FutureExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub async fn run(target: Arc<dyn Target>, fixtures: &Fixtures, cfg: &RunConfig, meta: &RunMeta) -> Result<Summary> {
    let sc = &cfg.scenarios.connections;
    anyhow::ensure!(sc.connect_rate > 0, "connect_rate must be positive");
    let all = placements(fixtures)?;
    let max = sc.levels.iter().copied().max().unwrap_or(0);
    anyhow::ensure!(all.len() >= max, "fixtures have {} users, need {max}", all.len());
    let mut live: Vec<Box<dyn Conn>> = Vec::new();
    let mut attempted = 0usize;
    let mut steps = Vec::new();
    for (index, &level) in sc.levels.iter().enumerate() {
        let batch = Arc::new(all[attempted.min(level)..level].to_vec());
        attempted = level.max(attempted);
        let n = batch.len() as u64;
        let mut step = blank_step(
            index,
            format!("level={level}"),
            serde_json::json!({"level": level, "connect_rate": sc.connect_rate, "hold_s": sc.hold_s})
                .as_object()
                .unwrap()
                .clone(),
        );
        let cpu = CpuMeter::start();
        let (report, results) = open_loop_collect(
            sc.connect_rate,
            Duration::from_secs_f64(n as f64 / sc.connect_rate as f64),
            cfg.inflight_cap,
            {
                let target = target.clone();
                move |i, intended| {
                    let (user, room) = batch[i as usize].clone();
                    let target = target.clone();
                    let intended = intended.into_std();
                    async move {
                        let (tx, mut rx) = mpsc::unbounded_channel();
                        let c = connect_sub(&target, &user, &room, tx).await?;
                        tokio::spawn(async move { while rx.recv().await.is_some() {} });
                        anyhow::Ok((Instant::now().saturating_duration_since(intended).as_micros() as u64, c))
                    }
                    .boxed()
                }
            },
        )
        .await;
        let cpu = cpu.stop();
        let mut hist = new_hist();
        let mut failed = report.scheduled - report.started;
        for r in results {
            match r {
                Ok((us, c)) => {
                    hist.saturating_record(us);
                    live.push(c);
                }
                Err(e) => {
                    eprintln!("connect error: {e:#}");
                    failed += 1;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(sc.hold_s)).await;
        step.latency_us = pct(&hist);
        step.sched_lag_us = pct(&report.lag);
        step.saturated = report.saturated;
        step.errors = failed;
        step.extra.insert("connected".into(), live.len().into());
        step.extra.insert("failed".into(), failed.into());
        step.extra.insert("level".into(), level.into());
        apply_guards(&mut step, cpu, meta.smoke);
        let out = StepOut { step, latency: hist };
        finish_step(meta, &out);
        steps.push(out.step);
    }
    for c in live {
        c.close().await;
    }
    Ok(summary(meta, steps, Derived::default()))
}
