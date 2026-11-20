use rand::RngExt;
use std::time::Duration;

const START: Duration = Duration::from_millis(100);
const CAP: Duration = Duration::from_secs(5);

pub struct Backoff {
    current: Duration,
}

impl Backoff {
    pub fn new() -> Self {
        Self { current: START }
    }

    pub fn next_delay(&mut self) -> Duration {
        let d = self.current;
        self.current = (d * 2).min(CAP);
        Duration::from_secs_f64(rand::rng().random_range(0.0..=d.as_secs_f64()))
    }

    pub fn reset(&mut self) {
        self.current = START;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn backoff_bounds() {
        let mut b = Backoff::new();
        assert!(b.next_delay() <= Duration::from_millis(100));
        for _ in 0..20 {
            assert!(b.next_delay() <= Duration::from_secs(5));
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }
}
