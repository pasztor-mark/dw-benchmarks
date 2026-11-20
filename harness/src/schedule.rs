use futures_util::future::BoxFuture;
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::{sleep_until, Instant};

pub struct ScheduleReport {
    pub scheduled: u64,
    pub started: u64,
    pub saturated: bool,
    pub lag: Histogram<u64>,
}

pub async fn run_open_loop(
    rate_per_s: u64,
    duration: Duration,
    inflight_cap: usize,
    mut make_send: impl FnMut(u64, Instant) -> BoxFuture<'static, ()>,
) -> ScheduleReport {
    let total = (duration.as_secs_f64() * rate_per_s as f64).round() as u64;
    let sem = Arc::new(Semaphore::new(inflight_cap));
    let mut lag = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let (mut started, mut saturated) = (0, false);
    let start = Instant::now();
    for i in 0..total {
        let intended = start + Duration::from_nanos((i as u128 * 1_000_000_000 / rate_per_s as u128) as u64);
        sleep_until(intended).await;
        let Ok(permit) = sem.clone().try_acquire_owned() else {
            saturated = true;
            continue;
        };
        lag.saturating_record(Instant::now().saturating_duration_since(intended).as_micros() as u64);
        started += 1;
        let fut = make_send(i, intended);
        tokio::spawn(async move {
            fut.await;
            drop(permit);
        });
    }
    sleep_until(start + duration).await;
    ScheduleReport { scheduled: total, started, saturated, lag }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn rate_and_count() {
        let seen = Arc::new(Mutex::new(Vec::<Instant>::new()));
        let s = seen.clone();
        let report = run_open_loop(100, Duration::from_secs(1), 10, move |_, intended| {
            s.lock().unwrap().push(intended);
            async {}.boxed()
        })
        .await;
        let v = seen.lock().unwrap();
        assert_eq!(report.scheduled, 100);
        assert_eq!(v.len(), 100);
        for w in v.windows(2) {
            assert_eq!(w[1] - w[0], Duration::from_millis(10));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn saturates_without_blocking() {
        let t0 = Instant::now();
        let report = run_open_loop(100, Duration::from_secs(1), 2, |_, _| {
            futures_util::future::pending::<()>().boxed()
        })
        .await;
        assert!(report.saturated);
        assert_eq!(report.started, 2);
        assert_eq!(report.scheduled, 100);
        assert!(t0.elapsed() < Duration::from_millis(1100));
    }
}
