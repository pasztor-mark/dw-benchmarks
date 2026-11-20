use anyhow::Result;
use chrono::{DateTime, Utc};
use hdrhistogram::serialization::interval_log::IntervalLogWriterBuilder;
use hdrhistogram::serialization::V2DeflateSerializer;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Percentiles {
    pub count: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Step {
    pub index: usize,
    pub label: String,
    pub params: serde_json::Map<String, serde_json::Value>,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub sent: u64,
    pub expected: u64,
    pub received: u64,
    pub lost: u64,
    pub duplicates: u64,
    pub errors: u64,
    pub latency_us: Percentiles,
    pub service_us: Percentiles,
    pub ack_us: Percentiles,
    pub sched_lag_us: Percentiles,
    pub harness_cpu: f64,
    pub saturated: bool,
    pub invalid: bool,
    pub invalid_reasons: Vec<String>,
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Derived {
    pub max_sustainable_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Summary {
    pub schema: u32,
    pub target: String,
    pub variant: Option<String>,
    pub scenario: String,
    pub disk: String,
    pub rep: u32,
    pub local: bool,
    pub smoke: bool,
    pub steps: Vec<Step>,
    pub derived: Derived,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UserCred {
    pub id: String,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RoomFixture {
    pub id: String,
    pub members: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Fixtures {
    pub target: String,
    pub scenario: String,
    pub users: Vec<UserCred>,
    pub rooms: Vec<RoomFixture>,
    pub meta: serde_json::Map<String, serde_json::Value>,
}

pub fn write_hlog(path: &Path, hist: &Histogram<u64>) -> Result<()> {
    let mut file = std::fs::File::create(path)?;
    let mut serializer = V2DeflateSerializer::new();
    let mut writer = IntervalLogWriterBuilder::new()
        .begin_log_with(&mut file, &mut serializer)?;
    writer
        .write_histogram(hist, Duration::ZERO, Duration::ZERO, None)
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hdrhistogram::serialization::interval_log::{IntervalLogIterator, LogEntry};
    use hdrhistogram::serialization::Deserializer;

    #[test]
    fn summary_roundtrip() {
        let pct = Percentiles { count: 1, p50: 1, p95: 2, p99: 3, p999: 4, max: 5 };
        let step = Step {
            index: 0,
            label: "rate=100".into(),
            params: serde_json::json!({"rate": 100}).as_object().unwrap().clone(),
            started_at: chrono::Utc::now(),
            ended_at: chrono::Utc::now(),
            sent: 1,
            expected: 1,
            received: 1,
            lost: 0,
            duplicates: 0,
            errors: 0,
            latency_us: pct.clone(),
            service_us: pct.clone(),
            ack_us: pct.clone(),
            sched_lag_us: pct,
            harness_cpu: 0.0,
            saturated: false,
            invalid: false,
            invalid_reasons: vec![],
            extra: serde_json::Map::new(),
        };
        let summary = Summary {
            schema: 1,
            target: "dotwire".into(),
            variant: None,
            scenario: "latency-ladder".into(),
            disk: "plain".into(),
            rep: 1,
            local: true,
            smoke: true,
            steps: vec![step],
            derived: Derived { max_sustainable_rate: None },
        };
        let json: serde_json::Value = serde_json::to_value(&summary).unwrap();
        assert!(json["steps"][0]["latency_us"]["p999"].is_u64());
        assert!(json["steps"][0]["invalid_reasons"].is_array());
        assert!(json["derived"].get("max_sustainable_rate").unwrap().is_null());
        let back: Summary = serde_json::from_value(json).unwrap();
        assert_eq!(back, summary);
    }

    #[test]
    fn hlog_roundtrip() {
        let mut hist = Histogram::<u64>::new(3).unwrap();
        hist.record(1500).unwrap();
        let path = std::env::temp_dir().join(format!("dwbench-hlog-test-{}.hlog", std::process::id()));
        write_hlog(&path, &hist).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let mut found = None;
        for entry in IntervalLogIterator::new(&bytes) {
            if let LogEntry::Interval(h) = entry.unwrap() {
                let raw = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, h.encoded_histogram()).unwrap();
                let decoded: Histogram<u64> = Deserializer::new().deserialize(&mut raw.as_slice()).unwrap();
                found = Some(decoded);
            }
        }
        let decoded = found.expect("one interval written");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.max(), hist.max());
    }
}
