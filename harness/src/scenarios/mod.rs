use crate::accounting::{SendTable, StepRecorder};
use crate::adapter::{Conn, Delivery, SetupPlan, Target};
use crate::body;
use crate::config::RunConfig;
use crate::guards::{self, CpuMeter};
use crate::results::{self, Derived, Fixtures, Percentiles, Step, Summary, UserCred};
use crate::schedule::run_open_loop;
use anyhow::{Context, Result};
use futures_util::FutureExt;
use hdrhistogram::Histogram;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::timeout;

pub mod connections;
pub mod fanout;
pub mod history;
pub mod latency_ladder;
pub mod reconnect;

const WIRE_BASE: u64 = 1 << 32;
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const FOREIGN_SLOT: u32 = u32::MAX;

pub struct RunMeta {
    pub target: String,
    pub scenario: String,
    pub variant: Option<String>,
    pub disk: String,
    pub rep: u32,
    pub local: bool,
    pub smoke: bool,
    pub out: Option<PathBuf>,
}

pub struct Group {
    pub room: String,
    pub members: Vec<UserCred>,
}

pub struct StepSpec {
    pub index: usize,
    pub label: String,
    pub params: serde_json::Map<String, serde_json::Value>,
    pub rate: u64,
    pub groups: Vec<Group>,
    pub pick: Arc<dyn Fn(u64) -> (usize, usize) + Send + Sync>,
    pub track_last: bool,
}

pub struct StepOut {
    pub step: Step,
    pub latency: Histogram<u64>,
}

pub fn setup_plan(scenario: &str, cfg: &RunConfig) -> Result<SetupPlan> {
    let users = |room: usize, n: usize| (0..n).map(|m| format!("u{room}_{m}")).collect::<Vec<_>>();
    let rooms = match scenario {
        "latency-ladder" => {
            let l = &cfg.scenarios.latency_ladder;
            (0..l.rooms).map(|r| (format!("r{r}"), users(r, l.members))).collect()
        }
        "fanout" => {
            let n = cfg.scenarios.fanout.sizes.iter().copied().max().unwrap_or(0);
            vec![("r0".to_string(), users(0, n))]
        }
        "connections" => chunked(cfg.scenarios.connections.levels.iter().copied().max().unwrap_or(0)),
        "reconnect-storm" => chunked(cfg.scenarios.reconnect.users),
        "history" => (0..cfg.scenarios.history.rooms).map(|r| (format!("r{r}"), users(r, 1))).collect(),
        other => anyhow::bail!("scenario {other} not implemented"),
    };
    Ok(SetupPlan { rooms })
}

fn chunked(n: usize) -> Vec<(String, Vec<String>)> {
    (0..n.div_ceil(10))
        .map(|r| (format!("r{r}"), (r * 10..n.min(r * 10 + 10)).map(|u| format!("u{r}_{u}")).collect()))
        .collect()
}

pub fn placements(fixtures: &Fixtures) -> Result<Vec<(UserCred, String)>> {
    let users: HashMap<&str, &UserCred> = fixtures.users.iter().map(|u| (u.id.as_str(), u)).collect();
    fixtures
        .rooms
        .iter()
        .flat_map(|r| r.members.iter().map(move |m| (m, &r.id)))
        .map(|(m, room)| {
            let user = users.get(m.as_str()).map(|u| (*u).clone()).with_context(|| format!("unknown user {m}"))?;
            Ok((user, room.clone()))
        })
        .collect()
}

pub async fn connect_sub(
    target: &Arc<dyn Target>,
    user: &UserCred,
    room: &str,
    tx: mpsc::UnboundedSender<Delivery>,
) -> Result<Box<dyn Conn>> {
    timeout(CONNECT_TIMEOUT, async {
        let mut c = target.connect(user, tx).await?;
        c.subscribe(room).await?;
        anyhow::Ok(c)
    })
    .await?
}

pub async fn open_loop_collect<T: Send + 'static>(
    rate: u64,
    duration: Duration,
    cap: usize,
    mut make: impl FnMut(u64, tokio::time::Instant) -> futures_util::future::BoxFuture<'static, T>,
) -> (crate::schedule::ScheduleReport, Vec<T>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let report = run_open_loop(rate, duration, cap, move |i, intended| {
        let fut = make(i, intended);
        let tx = tx.clone();
        async move {
            let _ = tx.send(fut.await);
        }
        .boxed()
    })
    .await;
    let mut out = Vec::new();
    while let Some(v) = rx.recv().await {
        out.push(v);
    }
    (report, out)
}

pub fn blank_step(index: usize, label: String, params: serde_json::Map<String, serde_json::Value>) -> Step {
    let zero = Percentiles::default();
    Step {
        index,
        label,
        params,
        started_at: chrono::Utc::now(),
        ended_at: chrono::Utc::now(),
        sent: 0,
        expected: 0,
        received: 0,
        lost: 0,
        duplicates: 0,
        errors: 0,
        latency_us: zero.clone(),
        service_us: zero.clone(),
        ack_us: zero.clone(),
        sched_lag_us: zero,
        harness_cpu: 0.0,
        saturated: false,
        invalid: false,
        invalid_reasons: Vec::new(),
        extra: serde_json::Map::new(),
    }
}

pub fn apply_guards(step: &mut Step, cpu: f64, smoke: bool) {
    let (invalid, reasons) = guards::evaluate(cpu, step.sched_lag_us.p99, step.saturated);
    step.harness_cpu = cpu;
    step.invalid_reasons.extend(reasons);
    step.invalid = (invalid && !smoke) || step.invalid_reasons.iter().any(|r| r == "restart_cmd_failed");
    if smoke {
        step.extra.insert("smoke".into(), true.into());
    }
    step.ended_at = chrono::Utc::now();
}

pub fn groups_from(fixtures: &Fixtures, rooms: usize, members: impl Fn(usize) -> usize) -> Result<Vec<Group>> {
    let users: HashMap<&str, &UserCred> = fixtures.users.iter().map(|u| (u.id.as_str(), u)).collect();
    anyhow::ensure!(fixtures.rooms.len() >= rooms, "fixtures have {} rooms, need {rooms}", fixtures.rooms.len());
    fixtures
        .rooms
        .iter()
        .take(rooms)
        .enumerate()
        .map(|(i, room)| {
            let want = members(i);
            anyhow::ensure!(room.members.len() >= want, "room {} has {} members, need {want}", room.id, room.members.len());
            let members = room.members[..want]
                .iter()
                .map(|id| users.get(id.as_str()).map(|u| (*u).clone()).with_context(|| format!("unknown user {id}")))
                .collect::<Result<Vec<_>>>()?;
            Ok(Group { room: room.id.clone(), members })
        })
        .collect()
}

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

pub fn pct(h: &Histogram<u64>) -> Percentiles {
    Percentiles {
        count: h.len(),
        p50: h.value_at_quantile(0.5),
        p95: h.value_at_quantile(0.95),
        p99: h.value_at_quantile(0.99),
        p999: h.value_at_quantile(0.999),
        max: h.max(),
    }
}

#[derive(Debug, PartialEq)]
enum WireId {
    Warmup,
    Foreign,
    Mine(u64),
}

fn wire_id(step: usize, id: u64) -> u64 {
    WIRE_BASE * (step as u64 + 1) + id
}

fn classify(step: usize, msg_id: u64) -> WireId {
    match msg_id / WIRE_BASE {
        0 => WireId::Warmup,
        p if p == step as u64 + 1 => WireId::Mine(msg_id % WIRE_BASE),
        _ => WireId::Foreign,
    }
}

struct SentMeta {
    group: usize,
    member: usize,
    intended: Instant,
    ok: bool,
}

#[derive(Default)]
struct LastTracker {
    seen: HashSet<(u64, u32)>,
    per_msg: HashMap<u64, (usize, Instant)>,
}

impl LastTracker {
    fn record(&mut self, id: u64, slot: u32, at: Instant) {
        if !self.seen.insert((id, slot)) {
            return;
        }
        let e = self.per_msg.entry(id).or_insert((0, at));
        e.0 += 1;
        e.1 = e.1.max(at);
    }
}

pub async fn run_step(target: Arc<dyn Target>, cfg: &RunConfig, spec: &StepSpec, smoke: bool) -> StepOut {
    let started_at = chrono::Utc::now();
    let table = Arc::new(SendTable::new((spec.rate * cfg.step.measure_s) as usize + 16));
    let rec = Arc::new(StepRecorder::new(table.clone()));
    let metas: Arc<Mutex<Vec<SentMeta>>> = Arc::default();
    let last: Arc<Mutex<LastTracker>> = Arc::default();
    let open = Arc::new(AtomicBool::new(true));
    let mut conns = Vec::new();
    let mut tasks = Vec::new();

    for (g, group) in spec.groups.iter().enumerate() {
        for (m, user) in group.members.iter().enumerate() {
            let (tx, mut rx) = mpsc::unbounded_channel::<Delivery>();
            let conn = async {
                let mut c = target.connect(user, tx).await?;
                c.subscribe(&group.room).await?;
                anyhow::Ok(c)
            }
            .await;
            match conn {
                Ok(c) => conns.push(c),
                Err(e) => {
                    eprintln!("connection error for {}: {e:#}", user.id);
                    rec.on_error();
                }
            }
            let (rec, metas, last, open, track) =
                (rec.clone(), metas.clone(), last.clone(), open.clone(), spec.track_last);
            let step_index = spec.index;
            tasks.push(tokio::spawn(async move {
                while let Some(d) = rx.recv().await {
                    let id = match classify(step_index, d.msg_id) {
                        WireId::Warmup => continue,
                        WireId::Foreign => {
                            rec.on_delivery(0, FOREIGN_SLOT, d.at);
                            continue;
                        }
                        WireId::Mine(id) => id,
                    };
                    let slot = metas
                        .lock()
                        .unwrap()
                        .get(id as usize)
                        .filter(|s| s.group == g && s.member != m)
                        .map_or(FOREIGN_SLOT, |s| (m - usize::from(m > s.member)) as u32);
                    rec.on_delivery(id, slot, d.at);
                    if track && slot != FOREIGN_SLOT && open.load(Ordering::Relaxed) {
                        last.lock().unwrap().record(id, slot, d.at);
                    }
                }
            }));
        }
    }

    let step_index = spec.index;
    let cap = cfg.inflight_cap;
    let payload = cfg.payload_bytes;
    let who: Arc<Vec<Vec<(UserCred, String)>>> = Arc::new(
        spec.groups.iter().map(|g| g.members.iter().map(|u| (u.clone(), g.room.clone())).collect()).collect(),
    );

    if cfg.step.warmup_s > 0 {
        let (target, pick, who) = (target.clone(), spec.pick.clone(), who.clone());
        run_open_loop(spec.rate, Duration::from_secs(cfg.step.warmup_s), cap, move |i, _| {
            let (g, m) = pick(i);
            let (user, room) = who[g][m].clone();
            let target = target.clone();
            async move {
                let _ = timeout(SEND_TIMEOUT, target.send(&user, &room, body::encode(i, payload))).await;
            }
            .boxed()
        })
        .await;
    }

    let cpu = CpuMeter::start();
    let report = {
        let (target, table, rec, metas, pick) =
            (target.clone(), table.clone(), rec.clone(), metas.clone(), spec.pick.clone());
        let sizes: Vec<usize> = spec.groups.iter().map(|g| g.members.len()).collect();
        let who = who.clone();
        run_open_loop(spec.rate, Duration::from_secs(cfg.step.measure_s), cap, move |i, intended| {
            let (g, m) = pick(i);
            let (user, room) = who[g][m].clone();
            let intended = intended.into_std();
            let id = table.register(intended, (sizes[g] - 1) as u32);
            metas.lock().unwrap().push(SentMeta { group: g, member: m, intended, ok: false });
            let (target, table, rec, metas) = (target.clone(), table.clone(), rec.clone(), metas.clone());
            async move {
                let t0 = Instant::now();
                match timeout(SEND_TIMEOUT, target.send(&user, &room, body::encode(wire_id(step_index, id), payload))).await {
                    Ok(Ok(ack)) => {
                        table.mark_sent(id, t0);
                        table.mark_acked(id, ack.at);
                        metas.lock().unwrap()[id as usize].ok = true;
                    }
                    Ok(Err(e)) => {
                        eprintln!("send error: {e:#}");
                        rec.on_error();
                    }
                    Err(_) => {
                        eprintln!("send timed out");
                        rec.on_error();
                    }
                }
            }
            .boxed()
        })
        .await
    };
    let cpu = cpu.stop();

    tokio::time::sleep(Duration::from_secs(cfg.step.drain_s)).await;
    open.store(false, Ordering::Relaxed);
    rec.close_measurement();
    let stats = rec.finish();
    for t in tasks {
        t.abort();
    }
    for c in conns {
        c.close().await;
    }

    let lag = pct(&report.lag);
    let (invalid, reasons) = guards::evaluate(cpu, lag.p99, report.saturated);
    let mut extra = serde_json::Map::new();
    extra.insert("unsent".into(), (report.scheduled - report.started).into());
    extra.insert("foreign".into(), stats.foreign.into());
    if smoke {
        extra.insert("smoke".into(), true.into());
    }
    if spec.track_last {
        let mut h = new_hist();
        let mut incomplete = 0u64;
        let metas = metas.lock().unwrap();
        let last = last.lock().unwrap();
        for (id, s) in metas.iter().enumerate().filter(|(_, s)| s.ok) {
            let need = spec.groups[s.group].members.len() - 1;
            match last.per_msg.get(&(id as u64)) {
                Some((n, at)) if *n == need => {
                    h.saturating_record(at.saturating_duration_since(s.intended).as_micros() as u64)
                }
                _ => incomplete += 1,
            }
        }
        extra.insert("last_recipient_us".into(), serde_json::to_value(pct(&h)).unwrap());
        extra.insert("incomplete_messages".into(), incomplete.into());
    }

    let step = Step {
        index: spec.index,
        label: spec.label.clone(),
        params: spec.params.clone(),
        started_at,
        ended_at: chrono::Utc::now(),
        sent: stats.sent,
        expected: stats.expected,
        received: stats.received,
        lost: stats.lost,
        duplicates: stats.duplicates,
        errors: stats.errors,
        latency_us: pct(&stats.latency),
        service_us: pct(&stats.service),
        ack_us: pct(&stats.ack),
        sched_lag_us: lag,
        harness_cpu: cpu,
        saturated: report.saturated,
        invalid: invalid && !smoke,
        invalid_reasons: reasons,
        extra,
    };
    StepOut { step, latency: stats.latency }
}

pub fn finish_step(meta: &RunMeta, out: &StepOut) {
    if let Some(dir) = &meta.out {
        let path = dir.join(format!("{}-{}.hlog", out.step.index, out.step.label));
        if let Err(e) = std::fs::create_dir_all(dir).map_err(anyhow::Error::from).and_then(|_| results::write_hlog(&path, &out.latency)) {
            eprintln!("hlog write failed: {e:#}");
        }
    }
}

pub fn summary(meta: &RunMeta, steps: Vec<Step>, derived: Derived) -> Summary {
    Summary {
        schema: 1,
        target: meta.target.clone(),
        variant: meta.variant.clone(),
        scenario: meta.scenario.clone(),
        disk: meta.disk.clone(),
        rep: meta.rep,
        local: meta.local,
        smoke: meta.smoke,
        steps,
        derived,
    }
}

#[cfg(test)]
mod tests {
    use crate::adapter::Target;
    use crate::adapters::fake::FakeTarget;
    use crate::config::RunConfig;
    use crate::results::Fixtures;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{connections, fanout, history, latency_ladder, reconnect, setup_plan, RunMeta};

    fn cfg() -> RunConfig {
        let mut cfg = RunConfig::load(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/configs/smoke.toml"
        )))
        .unwrap();
        cfg.step.warmup_s = 1;
        cfg.step.measure_s = 1;
        cfg.step.drain_s = 1;
        cfg
    }

    fn meta(scenario: &str) -> RunMeta {
        RunMeta {
            target: "fake".into(),
            scenario: scenario.into(),
            variant: None,
            disk: "plain".into(),
            rep: 1,
            local: true,
            smoke: true,
            out: None,
        }
    }

    async fn fixtures(t: &FakeTarget, scenario: &str, cfg: &RunConfig) -> Fixtures {
        t.setup(&setup_plan(scenario, cfg).unwrap()).await.unwrap()
    }

    #[test]
    fn wire_ids_are_unique_per_step() {
        use super::{classify, wire_id, WireId};
        assert_eq!(wire_id(0, 7), super::WIRE_BASE + 7);
        assert_ne!(wire_id(0, 7), wire_id(1, 7));
        assert_eq!(classify(1, wire_id(1, 7)), WireId::Mine(7));
        assert_eq!(classify(1, wire_id(0, 7)), WireId::Foreign);
        assert_eq!(classify(0, wire_id(1, 7)), WireId::Foreign);
        assert_eq!(classify(1, 7), WireId::Warmup);
    }

    #[tokio::test]
    async fn ladder_happy() {
        let cfg = cfg();
        let t = Arc::new(FakeTarget::new(Duration::from_millis(2), None));
        let fx = fixtures(&t, "latency-ladder", &cfg).await;
        let s = latency_ladder::run(t, &fx, &cfg, &meta("latency-ladder")).await.unwrap();
        assert_eq!(s.steps.len(), 2);
        for st in &s.steps {
            assert_eq!(st.lost, 0);
            assert!(st.latency_us.p50 >= 2000, "p50 {}", st.latency_us.p50);
        }
        assert_eq!(s.derived.max_sustainable_rate, Some(5.0));
    }

    #[tokio::test]
    async fn ladder_loss_reported() {
        let cfg = cfg();
        let t = Arc::new(FakeTarget::new(Duration::from_millis(2), Some(4)));
        let fx = fixtures(&t, "latency-ladder", &cfg).await;
        let s = latency_ladder::run(t, &fx, &cfg, &meta("latency-ladder")).await.unwrap();
        assert!(s.steps.iter().map(|st| st.lost).sum::<u64>() > 0);
        assert_eq!(s.derived.max_sustainable_rate, None);
    }

    #[tokio::test]
    async fn ladder_stops_after_two_bad_steps() {
        let mut cfg = cfg();
        cfg.scenarios.latency_ladder.rates = vec![2, 5, 9, 12];
        cfg.inflight_cap = 0;
        let t = Arc::new(FakeTarget::new(Duration::from_millis(2), None));
        let fx = fixtures(&t, "latency-ladder", &cfg).await;
        let s = latency_ladder::run(t, &fx, &cfg, &meta("latency-ladder")).await.unwrap();
        assert_eq!(s.steps.len(), 2);
        assert!(s.steps.iter().all(|st| st.saturated));
    }

    #[tokio::test]
    async fn ladder_stops_when_sends_fail() {
        let mut cfg = cfg();
        cfg.scenarios.latency_ladder.rates = vec![2, 5, 9];
        let mut t = FakeTarget::new(Duration::from_millis(2), None);
        t.fail_sends = true;
        let t = Arc::new(t);
        let fx = fixtures(&t, "latency-ladder", &cfg).await;
        let s = latency_ladder::run(t, &fx, &cfg, &meta("latency-ladder")).await.unwrap();
        assert_eq!(s.steps.len(), 1);
        assert!(s.steps[0].errors > 0);
        assert_eq!(s.derived.max_sustainable_rate, None);
    }

    #[tokio::test]
    async fn fanout_last_recipient() {
        let mut cfg = cfg();
        cfg.scenarios.fanout.sizes = vec![5];
        let t = Arc::new(FakeTarget::new(Duration::from_millis(3), None));
        let fx = fixtures(&t, "fanout", &cfg).await;
        let s = fanout::run(t, &fx, &cfg, &meta("fanout")).await.unwrap();
        let st = &s.steps[0];
        let p50 = st.extra["last_recipient_us"]["p50"].as_u64().unwrap();
        assert!(p50 >= 3000, "p50 {p50}");
        assert_eq!(st.extra["incomplete_messages"], 0);
    }

    #[tokio::test]
    async fn connections_cumulative() {
        let mut cfg = cfg();
        cfg.scenarios.connections.hold_s = 0;
        cfg.scenarios.connections.connect_rate = 50;
        let t = Arc::new(FakeTarget::new(Duration::from_millis(1), None));
        let fx = fixtures(&t, "connections", &cfg).await;
        let s = connections::run(t, &fx, &cfg, &meta("connections")).await.unwrap();
        assert_eq!(s.steps.len(), 2);
        assert_eq!(s.steps[1].extra["connected"], 10);
        assert_eq!(s.steps[1].errors, 0);
        assert_eq!(s.steps[0].extra["level"], 5);
    }

    fn quick() -> reconnect::Timing {
        reconnect::Timing { settle: Duration::from_millis(300), ceiling: Duration::from_secs(20) }
    }

    #[tokio::test]
    async fn reconnect_recovers() {
        let cfg = cfg();
        let t = Arc::new(FakeTarget::new(Duration::from_millis(1), None));
        let fx = fixtures(&t, "reconnect-storm", &cfg).await;
        let restarter = t.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(450)).await;
            restarter.restart();
        });
        let s = reconnect::run_with(t, &fx, &cfg, &meta("reconnect-storm"), Some("true"), quick()).await.unwrap();
        let st = &s.steps[0];
        assert_eq!(st.extra["never_recovered"], 0);
        assert!(st.extra["t100_ms"].as_u64().unwrap() < 5000, "{:?}", st.extra);
        assert!(!st.invalid);
        assert!(st.extra["first_delivery_ms"].as_u64().is_some(), "{:?}", st.extra);
    }

    #[tokio::test]
    async fn reconnect_failed_cmd_invalid() {
        let cfg = cfg();
        let t = Arc::new(FakeTarget::new(Duration::from_millis(1), None));
        let fx = fixtures(&t, "reconnect-storm", &cfg).await;
        let timing = reconnect::Timing { settle: Duration::from_millis(100), ceiling: Duration::from_secs(60) };
        let began = std::time::Instant::now();
        let s = reconnect::run_with(t, &fx, &cfg, &meta("reconnect-storm"), Some("exit 3"), timing).await.unwrap();
        assert!(began.elapsed() < Duration::from_secs(5), "{:?}", began.elapsed());
        assert_eq!(s.steps[0].extra["never_recovered"], 6);
        assert!(s.steps[0].invalid);
        assert!(s.steps[0].invalid_reasons.contains(&"restart_cmd_failed".to_string()));
    }

    #[tokio::test]
    async fn reconnect_requires_cmd() {
        let cfg = cfg();
        let t = Arc::new(FakeTarget::new(Duration::from_millis(1), None));
        let fx = fixtures(&t, "reconnect-storm", &cfg).await;
        assert!(reconnect::run(t, &fx, &cfg, &meta("reconnect-storm"), None).await.is_err());
    }

    #[tokio::test]
    async fn history_short_pages() {
        let mut cfg = cfg();
        cfg.step.measure_s = 1;
        let mut t = FakeTarget::new(Duration::from_millis(1), None);
        t.history_returns = Some(10);
        let t = Arc::new(t);
        let fx = fixtures(&t, "history", &cfg).await;
        history::preload(t.clone(), &fx, &cfg).await.unwrap();
        let s = history::run(t, &fx, &cfg, &meta("history")).await.unwrap();
        let st = &s.steps[0];
        assert!(st.extra["short_pages"].as_u64().unwrap() > 0);
        assert_eq!(st.errors, st.extra["short_pages"].as_u64().unwrap());
    }

    #[tokio::test]
    async fn history_full_pages_ok() {
        let mut cfg = cfg();
        cfg.step.measure_s = 1;
        let t = Arc::new(FakeTarget::new(Duration::from_millis(1), None));
        let fx = fixtures(&t, "history", &cfg).await;
        let s = history::run(t, &fx, &cfg, &meta("history")).await.unwrap();
        assert_eq!(s.steps[0].errors, 0);
        assert!(s.steps[0].latency_us.count > 0);
    }
}
