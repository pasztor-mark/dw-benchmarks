use super::{apply_guards, blank_step, finish_step, new_hist, open_loop_collect, pct, placements, summary, RunMeta, StepOut, SEND_TIMEOUT};
use crate::adapter::Target;
use crate::body;
use crate::config::RunConfig;
use crate::guards::CpuMeter;
use crate::results::{Derived, Fixtures, Summary};
use anyhow::{Context, Result};
use futures_util::{FutureExt, TryStreamExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

pub async fn preload(target: Arc<dyn Target>, fixtures: &Fixtures, cfg: &RunConfig) -> Result<()> {
    let per_room = cfg.scenarios.history.per_room;
    futures_util::stream::iter(placements(fixtures)?.into_iter().map(Ok))
        .try_for_each_concurrent(cfg.scenarios.history.preload_concurrency, |(user, room)| {
            let target = target.clone();
            async move {
                for i in 0..per_room {
                    timeout(SEND_TIMEOUT, target.send(&user, &room, body::encode(i as u64 + 1, cfg.payload_bytes)))
                        .await
                        .with_context(|| format!("preload timed out in {room}"))?
                        .with_context(|| format!("preload failed in {room}"))?;
                }
                anyhow::Ok(())
            }
        })
        .await
}

pub async fn run(target: Arc<dyn Target>, fixtures: &Fixtures, cfg: &RunConfig, meta: &RunMeta) -> Result<Summary> {
    let sc = &cfg.scenarios.history;
    let all = placements(fixtures)?;
    anyhow::ensure!(all.len() >= sc.rooms && sc.rooms > 0, "fixtures have {} rooms, need {}", all.len(), sc.rooms);
    let who = Arc::new(all[..sc.rooms].to_vec());
    let want = sc.limit.min(sc.per_room);
    let mut steps = Vec::new();
    for (index, &rate) in sc.rates.iter().enumerate() {
        let mut step = blank_step(
            index,
            format!("rate={rate}"),
            serde_json::json!({"rate": rate, "limit": sc.limit, "per_room": sc.per_room, "rooms": sc.rooms})
                .as_object()
                .unwrap()
                .clone(),
        );
        let cpu = CpuMeter::start();
        let (report, results) = open_loop_collect(rate, Duration::from_secs(cfg.step.measure_s), cfg.inflight_cap, {
            let (target, who, limit) = (target.clone(), who.clone(), sc.limit);
            move |i, intended| {
                let (user, room) = who[i as usize % who.len()].clone();
                let target = target.clone();
                let intended = intended.into_std();
                async move {
                    let n = timeout(SEND_TIMEOUT, target.history(&user, &room, limit)).await??;
                    anyhow::Ok((Instant::now().saturating_duration_since(intended).as_micros() as u64, n))
                }
                .boxed()
            }
        })
        .await;
        let cpu = cpu.stop();
        let mut hist = new_hist();
        let (mut failed, mut short) = (report.scheduled - report.started, 0u64);
        for r in results {
            match r {
                Ok((us, n)) => {
                    hist.saturating_record(us);
                    if n < want {
                        short += 1;
                    }
                }
                Err(e) => {
                    eprintln!("history error: {e:#}");
                    failed += 1;
                }
            }
        }
        step.latency_us = pct(&hist);
        step.sched_lag_us = pct(&report.lag);
        step.saturated = report.saturated;
        step.sent = report.started;
        step.errors = failed + short;
        step.extra.insert("short_pages".into(), short.into());
        step.extra.insert("unsent".into(), (report.scheduled - report.started).into());
        apply_guards(&mut step, cpu, meta.smoke);
        let out = StepOut { step, latency: hist };
        finish_step(meta, &out);
        steps.push(out.step);
    }
    Ok(summary(meta, steps, Derived::default()))
}
