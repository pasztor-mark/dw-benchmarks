use std::time::Instant;

pub struct CpuMeter {
    wall: Instant,
    cpu: f64,
}

fn cpu_secs() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

impl CpuMeter {
    pub fn start() -> Self {
        Self { wall: Instant::now(), cpu: cpu_secs() }
    }

    pub fn stop(&self) -> f64 {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get()) as f64;
        let wall = self.wall.elapsed().as_secs_f64().max(1e-9);
        (cpu_secs() - self.cpu) / (wall * cores)
    }
}

pub fn evaluate(cpu: f64, lag_p99_us: u64, _saturated: bool) -> (bool, Vec<String>) {
    let mut reasons = Vec::new();
    if cpu > 0.70 {
        reasons.push("harness_cpu>0.70".to_string());
    }
    if lag_p99_us > 5000 {
        reasons.push("sched_lag_p99>5000us".to_string());
    }
    (!reasons.is_empty(), reasons)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guards_reasons() {
        let (bad, reasons) = evaluate(0.8, 6000, false);
        assert!(bad);
        assert_eq!(reasons, vec!["harness_cpu>0.70", "sched_lag_p99>5000us"]);
        assert_eq!(evaluate(0.1, 100, true), (false, vec![]));
    }
}
