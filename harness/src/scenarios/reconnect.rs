use super::{apply_guards, blank_step, connect_sub, finish_step, new_hist, pct, placements, summary, wire_id, RunMeta, StepOut, SEND_TIMEOUT};
use crate::adapter::Target;
use crate::body;
use crate::config::RunConfig;
use crate::guards::CpuMeter;
use crate::results::{Derived, Fixtures, Summary, UserCred};
use crate::retry::Backoff;
use anyhow::{Context, Result};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::timeout;

const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
pub struct Timing {
    pub settle: Duration,
    pub ceiling: Duration,
}

pub async fn run(
    target: Arc<dyn Target>,
    fixtures: &Fixtures,
    cfg: &RunConfig,
    meta: &RunMeta,
    restart_cmd: Option<&str>,
) -> Result<Summary> {
    let timing = Timing { settle: Duration::from_secs(5), ceiling: Duration::from_secs(120) };
    run_with(target, fixtures, cfg, meta, restart_cmd, timing).await
}

pub async fn run_with(
    target: Arc<dyn Target>,
    fixtures: &Fixtures,
    cfg: &RunConfig,
    meta: &RunMeta,
    restart_cmd: Option<&str>,
    timing: Timing,
) -> Result<Summary> {
    let cmd = restart_cmd.context("reconnect-storm requires --restart-cmd")?;
    let sc = &cfg.scenarios.reconnect;
    anyhow::ensure!(sc.rate_per_room > 0.0, "rate_per_room must be positive");
    let all = placements(fixtures)?;
    anyhow::ensure!(all.len() >= sc.users, "fixtures have {} users, need {}", all.len(), sc.users);
    let all = &all[..sc.users];
    let started = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let first: Arc<Mutex<Option<Instant>>> = Arc::default();
    let mut tasks = Vec::new();
    let mut senders: Vec<(UserCred, String)> = Vec::new();
    for (user, room) in all {
        if !senders.iter().any(|(_, r)| r == room) {
            senders.push((user.clone(), room.clone()));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let conn = connect_sub(&target, user, room, tx).await.with_context(|| format!("initial connect for {}", user.id))?;
        let (target, user, room) = (target.clone(), user.clone(), room.clone());
        let (recovered, first) = (started.clone(), first.clone());
        tasks.push(tokio::spawn(async move {
            let (mut conn, mut rx, mut reconnected, mut backoff) = (Some(conn), rx, false, Backoff::new());
            loop {
                match rx.recv().await {
                    Some(d) => {
                        if reconnected {
                            first.lock().unwrap().get_or_insert(d.at);
                        }
                    }
                    None => {
                        if let Some(c) = conn.take() {
                            let _ = timeout(CLOSE_TIMEOUT, c.close()).await;
                        }
                        loop {
                            tokio::time::sleep(backoff.next_delay()).await;
                            let (tx, nrx) = mpsc::unbounded_channel();
                            if let Ok(c) = connect_sub(&target, &user, &room, tx).await {
                                conn = Some(c);
                                rx = nrx;
                                backoff.reset();
                                if !reconnected {
                                    recovered.lock().unwrap().push(Instant::now());
                                }
                                reconnected = true;
                                break;
                            }
                        }
                    }
                }
            }
        }));
    }

    tokio::time::sleep(timing.settle).await;
    let cpu = CpuMeter::start();
    let started_at = chrono::Utc::now();
    let t0 = Instant::now();
    let payload = cfg.payload_bytes;
    let period = Duration::from_secs_f64(1.0 / sc.rate_per_room);
    for (user, room) in senders {
        let target = target.clone();
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            let mut i = 0u64;
            loop {
                tick.tick().await;
                i += 1;
                let (target, user, room) = (target.clone(), user.clone(), room.clone());
                tokio::spawn(async move {
                    let _ = timeout(SEND_TIMEOUT, target.send(&user, &room, body::encode(wire_id(0, i), payload))).await;
                });
            }
        }));
    }
    let cmd_ok = matches!(
        timeout(timing.ceiling, tokio::process::Command::new("sh").arg("-c").arg(cmd).status()).await,
        Ok(Ok(s)) if s.success()
    );
    while cmd_ok
        && (started.lock().unwrap().len() < all.len() || first.lock().unwrap().is_none())
        && t0.elapsed() < timing.ceiling
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let cpu = cpu.stop();
    for t in tasks {
        t.abort();
    }

    let mut done: Vec<Duration> = started.lock().unwrap().iter().map(|at| at.saturating_duration_since(t0)).collect();
    done.sort();
    let users = all.len();
    let need99 = (users as f64 * 0.99).ceil() as usize;
    let ms = |d: &Duration| serde_json::Value::from(d.as_millis() as u64);
    let mut hist = new_hist();
    for d in &done {
        hist.saturating_record((d.as_micros() as u64).max(1));
    }
    let mut step = blank_step(
        0,
        "restart".into(),
        serde_json::json!({"users": users, "rate_per_room": sc.rate_per_room}).as_object().unwrap().clone(),
    );
    step.started_at = started_at;
    step.latency_us = pct(&hist);
    step.errors = (users - done.len()) as u64;
    let extra = &mut step.extra;
    extra.insert("t99_ms".into(), if done.len() >= need99 && need99 > 0 { ms(&done[need99 - 1]) } else { serde_json::Value::Null });
    extra.insert("t100_ms".into(), if done.len() == users && users > 0 { ms(&done[users - 1]) } else { serde_json::Value::Null });
    extra.insert(
        "first_delivery_ms".into(),
        first.lock().unwrap().map_or(serde_json::Value::Null, |at| ms(&at.saturating_duration_since(t0))),
    );
    extra.insert("never_recovered".into(), (users - done.len()).into());
    if !cmd_ok {
        step.invalid_reasons.push("restart_cmd_failed".into());
    }
    apply_guards(&mut step, cpu, meta.smoke);
    let out = StepOut { step, latency: hist };
    finish_step(meta, &out);
    Ok(summary(meta, vec![out.step], Derived::default()))
}
