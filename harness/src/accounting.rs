use hdrhistogram::Histogram;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy)]
struct Entry {
    intended: Instant,
    expected_recipients: u32,
    sent: Option<Instant>,
    acked: Option<Instant>,
}

pub struct SendTable {
    entries: Mutex<Vec<Entry>>,
}

impl SendTable {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(Vec::with_capacity(capacity)),
        }
    }

    pub fn register(&self, intended: Instant, expected_recipients: u32) -> u64 {
        let mut entries = self.entries.lock().unwrap();
        entries.push(Entry {
            intended,
            expected_recipients,
            sent: None,
            acked: None,
        });
        (entries.len() - 1) as u64
    }

    pub fn mark_sent(&self, id: u64, actual: Instant) {
        if let Some(entry) = self.entries.lock().unwrap().get_mut(id as usize) {
            entry.sent = Some(actual);
        }
    }

    pub fn mark_acked(&self, id: u64, at: Instant) {
        if let Some(entry) = self.entries.lock().unwrap().get_mut(id as usize) {
            entry.acked = Some(at);
        }
    }

    fn get(&self, id: u64) -> Option<Entry> {
        self.entries.lock().unwrap().get(id as usize).copied()
    }

    fn for_each(&self, mut f: impl FnMut(&Entry)) {
        self.entries.lock().unwrap().iter().for_each(&mut f);
    }
}

pub struct StepStats {
    pub sent: u64,
    pub expected: u64,
    pub received: u64,
    pub lost: u64,
    pub duplicates: u64,
    pub errors: u64,
    pub foreign: u64,
    pub latency: Histogram<u64>,
    pub service: Histogram<u64>,
    pub ack: Histogram<u64>,
}

struct Tally {
    seen: HashSet<(u64, u32)>,
    received: u64,
    duplicates: u64,
    foreign: u64,
    errors: u64,
    closed: bool,
    latency: Histogram<u64>,
    service: Histogram<u64>,
}

pub struct StepRecorder {
    table: Arc<SendTable>,
    tally: Mutex<Tally>,
}

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

fn micros_between(later: Instant, earlier: Instant) -> u64 {
    u64::try_from(later.saturating_duration_since(earlier).as_micros()).unwrap_or(u64::MAX)
}

impl StepRecorder {
    pub fn new(table: Arc<SendTable>) -> Self {
        Self {
            table,
            tally: Mutex::new(Tally {
                seen: HashSet::new(),
                received: 0,
                duplicates: 0,
                foreign: 0,
                errors: 0,
                closed: false,
                latency: new_histogram(),
                service: new_histogram(),
            }),
        }
    }

    pub fn on_delivery(&self, msg_id: u64, recipient: u32, at: Instant) {
        let mut tally = self.tally.lock().unwrap();
        if tally.closed {
            return;
        }
        let entry = match self.table.get(msg_id) {
            Some(entry) if recipient < entry.expected_recipients => entry,
            _ => {
                tally.foreign += 1;
                return;
            }
        };
        if !tally.seen.insert((msg_id, recipient)) {
            tally.duplicates += 1;
            return;
        }
        tally.received += 1;
        tally.latency.saturating_record(micros_between(at, entry.intended));
        if let Some(sent) = entry.sent {
            tally.service.saturating_record(micros_between(at, sent));
        }
    }

    pub fn on_error(&self) {
        self.tally.lock().unwrap().errors += 1;
    }

    pub fn close_measurement(&self) {
        self.tally.lock().unwrap().closed = true;
    }

    pub fn finish(&self) -> StepStats {
        let mut sent = 0u64;
        let mut expected = 0u64;
        let mut ack = new_histogram();
        self.table.for_each(|entry| {
            if let Some(sent_at) = entry.sent {
                sent += 1;
                expected += u64::from(entry.expected_recipients);
                if let Some(acked_at) = entry.acked {
                    ack.saturating_record(micros_between(acked_at, sent_at));
                }
            }
        });
        let tally = self.tally.lock().unwrap();
        StepStats {
            sent,
            expected,
            received: tally.received,
            lost: expected.saturating_sub(tally.received),
            duplicates: tally.duplicates,
            errors: tally.errors,
            foreign: tally.foreign,
            latency: tally.latency.clone(),
            service: tally.service.clone(),
            ack,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn counts_loss_and_dupes() {
        let t0 = Instant::now();
        let table = Arc::new(SendTable::new(8));
        let ids: Vec<u64> = (0..3).map(|_| table.register(t0, 2)).collect();
        for id in &ids {
            table.mark_sent(*id, t0);
        }
        let rec = StepRecorder::new(table.clone());
        rec.on_delivery(0, 0, t0);
        rec.on_delivery(0, 0, t0);
        rec.on_delivery(0, 1, t0);
        rec.on_delivery(1, 0, t0);
        let s = rec.finish();
        assert_eq!(s.sent, 3);
        assert_eq!(s.expected, 6);
        assert_eq!(s.received, 3);
        assert_eq!(s.duplicates, 1);
        assert_eq!(s.lost, 3);
    }

    #[test]
    fn late_delivery_counts_as_lost() {
        let t0 = Instant::now();
        let table = Arc::new(SendTable::new(4));
        let id = table.register(t0, 1);
        table.mark_sent(id, t0);
        let rec = StepRecorder::new(table);
        rec.close_measurement();
        rec.on_delivery(id, 0, t0);
        let s = rec.finish();
        assert_eq!(s.expected, 1);
        assert_eq!(s.received, 0);
        assert_eq!(s.lost, 1);
        assert_eq!(s.duplicates, 0);
    }

    #[test]
    fn latency_from_intended() {
        let t0 = Instant::now();
        let table = Arc::new(SendTable::new(4));
        let id = table.register(t0, 1);
        table.mark_sent(id, t0 + Duration::from_millis(10));
        let rec = StepRecorder::new(table);
        rec.on_delivery(id, 0, t0 + Duration::from_millis(15));
        let s = rec.finish();
        let lat = s.latency.value_at_quantile(0.5) as f64;
        assert!((lat - 15_000.0).abs() <= 150.0, "latency p50 was {lat}");
        let svc = s.service.value_at_quantile(0.5) as f64;
        assert!((svc - 5_000.0).abs() <= 50.0, "service p50 was {svc}");
    }

    #[test]
    fn unknown_deliveries_count_as_foreign() {
        let t0 = Instant::now();
        let table = Arc::new(SendTable::new(4));
        let sent_id = table.register(t0, 1);
        table.mark_sent(sent_id, t0);
        let rec = StepRecorder::new(table);
        rec.on_delivery(999, 0, t0);
        rec.on_delivery(sent_id, 5, t0);
        let s = rec.finish();
        assert_eq!(s.foreign, 2);
        assert_eq!(s.received, 0);
        assert_eq!(s.expected, 1);
    }

    #[test]
    fn delivery_before_mark_sent_counts() {
        let t0 = Instant::now();
        let table = Arc::new(SendTable::new(4));
        let id = table.register(t0, 1);
        let rec = StepRecorder::new(table.clone());
        rec.on_delivery(id, 0, t0 + Duration::from_millis(3));
        table.mark_sent(id, t0 + Duration::from_millis(1));
        let s = rec.finish();
        assert_eq!(s.received, 1);
        assert_eq!(s.foreign, 0);
        assert_eq!(s.lost, 0);
        assert_eq!(s.expected, 1);
    }
}
