use crate::results::{Step, Summary};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const UNSTABLE_SPREAD: f64 = 0.15;

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct ContainerStats {
    pub cpu_pct_mean: f64,
    pub mem_max_bytes: u64,
    pub net_tx_bytes: u64,
    pub net_rx_bytes: u64,
    pub disk_write_bytes: u64,
}

#[derive(Debug, Default, Serialize, PartialEq)]
pub struct ServerStats {
    pub cpu_pct_mean: f64,
    pub mem_max_bytes: u64,
    pub net_tx_bytes: u64,
    pub net_rx_bytes: u64,
    pub disk_write_bytes: u64,
    pub containers: BTreeMap<String, ContainerStats>,
}

#[derive(Deserialize)]
struct Sample {
    t: i64,
    docker: Vec<DockerRow>,
}

#[derive(Deserialize)]
struct DockerRow {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "CPUPerc")]
    cpu: String,
    #[serde(rename = "MemUsage")]
    mem: String,
    #[serde(rename = "NetIO")]
    net: String,
    #[serde(rename = "BlockIO")]
    block: String,
}

pub fn parse_bytes(text: &str) -> Result<u64> {
    let text = text.trim();
    let split = text.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(text.len());
    let (number, unit) = text.split_at(split);
    let value: f64 = number.trim().parse().with_context(|| format!("size {text:?}"))?;
    let scale = match unit {
        "B" => 1f64,
        "kB" | "KB" => 1e3,
        "MB" => 1e6,
        "GB" => 1e9,
        "TB" => 1e12,
        "KiB" => 1024f64,
        "MiB" => 1024f64.powi(2),
        "GiB" => 1024f64.powi(3),
        "TiB" => 1024f64.powi(4),
        _ => anyhow::bail!("unknown size unit in {text:?}"),
    };
    Ok((value * scale).round() as u64)
}

fn parse_pair(text: &str) -> Result<(u64, u64)> {
    let (left, right) = text.split_once(" / ").with_context(|| format!("expected 'a / b' in {text:?}"))?;
    Ok((parse_bytes(left)?, parse_bytes(right)?))
}

fn parse_percent(text: &str) -> Result<f64> {
    text.trim().trim_end_matches('%').parse().with_context(|| format!("percent {text:?}"))
}

fn short_name(name: &str, prefix: &str) -> String {
    let base = name.strip_prefix(prefix).unwrap_or(name);
    match base.rsplit_once('-') {
        Some((head, tail)) if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) => head.to_string(),
        _ => base.to_string(),
    }
}

#[derive(Default)]
struct Acc {
    cpu_sum: f64,
    samples: u64,
    mem_max: u64,
    first: Option<[u64; 3]>,
    last: [u64; 3],
}

pub fn server_stats(path: &Path, stack: &str, window: (i64, i64)) -> Result<Option<ServerStats>> {
    if !path.is_file() {
        return Ok(None);
    }
    let prefix = format!("dwb-{stack}-");
    let mut accs: BTreeMap<String, Acc> = BTreeMap::new();
    let mut mem_max_total = 0u64;
    for line in std::fs::read_to_string(path)?.lines().filter(|l| !l.trim().is_empty()) {
        let sample: Sample = serde_json::from_str(line).with_context(|| format!("parsing {}", path.display()))?;
        if sample.t < window.0 || sample.t > window.1 {
            continue;
        }
        let mut mem_total = 0u64;
        for row in sample.docker.iter().filter(|r| r.name.starts_with(&prefix)) {
            let cpu = parse_percent(&row.cpu)?;
            let (mem, _) = parse_pair(&row.mem)?;
            let (rx, tx) = parse_pair(&row.net)?;
            let (_, disk_write) = parse_pair(&row.block)?;
            mem_total += mem;
            let acc = accs.entry(short_name(&row.name, &prefix)).or_default();
            acc.cpu_sum += cpu;
            acc.samples += 1;
            acc.mem_max = acc.mem_max.max(mem);
            let counters = [rx, tx, disk_write];
            acc.first.get_or_insert(counters);
            acc.last = counters;
        }
        mem_max_total = mem_max_total.max(mem_total);
    }
    if accs.is_empty() {
        return Ok(None);
    }
    let mut server = ServerStats { mem_max_bytes: mem_max_total, ..Default::default() };
    for (name, acc) in accs {
        let first = acc.first.unwrap_or(acc.last);
        let container = ContainerStats {
            cpu_pct_mean: acc.cpu_sum / acc.samples as f64,
            mem_max_bytes: acc.mem_max,
            net_rx_bytes: acc.last[0].saturating_sub(first[0]),
            net_tx_bytes: acc.last[1].saturating_sub(first[1]),
            disk_write_bytes: acc.last[2].saturating_sub(first[2]),
        };
        server.cpu_pct_mean += container.cpu_pct_mean;
        server.net_rx_bytes += container.net_rx_bytes;
        server.net_tx_bytes += container.net_tx_bytes;
        server.disk_write_bytes += container.disk_write_bytes;
        server.containers.insert(name, container);
    }
    Ok(Some(server))
}

fn last_valid_p99(summary: &Summary) -> Option<u64> {
    summary.steps.iter().rev().find(|s| !s.invalid).map(|s| s.latency_us.p99)
}

pub fn median_rep(summaries: &[Summary]) -> usize {
    let mut order: Vec<usize> = (0..summaries.len()).collect();
    order.sort_by_key(|&i| (last_valid_p99(&summaries[i]).unwrap_or(u64::MAX), i));
    order[(order.len() - 1) / 2]
}

fn spread(values: &[u64]) -> f64 {
    let min = values.iter().copied().min().unwrap_or(0);
    let max = values.iter().copied().max().unwrap_or(0);
    if max == 0 {
        0.0
    } else if min == 0 {
        f64::INFINITY
    } else {
        (max - min) as f64 / min as f64
    }
}

pub fn unstable_steps(summaries: &[Summary]) -> Vec<usize> {
    let mut by_index: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for summary in summaries {
        for step in &summary.steps {
            by_index.entry(step.index).or_default().push(step.latency_us.p99);
        }
    }
    by_index
        .into_iter()
        .filter(|(_, p99s)| p99s.len() > 1 && spread(p99s) > UNSTABLE_SPREAD)
        .map(|(index, _)| index)
        .collect()
}

fn window(step: &Step) -> (i64, i64) {
    (step.started_at.timestamp_millis(), step.ended_at.timestamp_millis())
}

fn run_value(run: &str, dirs: &[PathBuf], summaries: &[Summary]) -> Result<Value> {
    let median = median_rep(summaries);
    let first = &summaries[0];
    let stack = first.target.split('-').next().unwrap_or(&first.target).to_string();
    let mut reps = summaries.iter().map(serde_json::to_value).collect::<serde_json::Result<Vec<Value>>>()?;
    for i in 0..summaries[median].steps.len() {
        let server = server_stats(&dirs[median].join("stats-a.jsonl"), &stack, window(&summaries[median].steps[i]))?;
        reps[median]["steps"][i]["server"] = serde_json::to_value(server)?;
    }
    Ok(json!({
        "run": run,
        "target": first.target,
        "variant": first.variant,
        "scenario": first.scenario,
        "disk": first.disk,
        "reps": reps,
        "median_rep": median,
        "unstable_steps": unstable_steps(summaries),
    }))
}

fn subdirs(path: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let p = entry?.path();
        if p.is_dir() {
            dirs.push(p);
        }
    }
    dirs.sort();
    Ok(dirs)
}

pub fn aggregate_dir(results: &Path) -> Result<Value> {
    let campaign = results.file_name().map_or_else(|| "campaign".to_string(), |n| n.to_string_lossy().into_owned());
    let mut env = Map::new();
    let mut local = false;
    let mut runs = Vec::new();
    for run_dir in subdirs(results)? {
        let run_name = run_dir.file_name().unwrap_or_default().to_string_lossy().into_owned();
        let mut dirs = Vec::new();
        let mut summaries = Vec::new();
        for rep_dir in subdirs(&run_dir)? {
            let summary_path = rep_dir.join("summary.json");
            if !summary_path.is_file() {
                continue;
            }
            let summary: Summary = serde_json::from_slice(&std::fs::read(&summary_path)?)
                .with_context(|| format!("parsing {}", summary_path.display()))?;
            let env_path = rep_dir.join("env.json");
            if env_path.is_file() {
                let value: Value = serde_json::from_slice(&std::fs::read(&env_path)?)
                    .with_context(|| format!("parsing {}", env_path.display()))?;
                local |= value["local"].as_bool() == Some(true);
                env.entry(run_name.clone()).or_insert(value);
            }
            dirs.push(rep_dir);
            summaries.push(summary);
        }
        if summaries.is_empty() {
            continue;
        }
        runs.push(run_value(&run_name, &dirs, &summaries)?);
    }
    Ok(json!({"campaign": campaign, "local": local, "env": env, "runs": runs}))
}

pub fn write_results(results: &Path, out: &Path) -> Result<()> {
    let value = aggregate_dir(results)?;
    if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(out, serde_json::to_vec_pretty(&value)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_campaign() -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../report/fixtures/campaign"))
    }

    fn run<'a>(out: &'a Value, name: &str) -> &'a Value {
        out["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["run"] == name)
            .unwrap_or_else(|| panic!("run {name} missing"))
    }

    #[test]
    fn median_and_unstable() {
        let out = aggregate_dir(&fixture_campaign()).unwrap();
        let r = run(&out, "dotwire-latency-ladder-plain");
        assert_eq!(r["median_rep"], 2);
        assert_eq!(r["unstable_steps"], serde_json::json!([1]));
        let step = &r["reps"][2]["steps"][1];
        assert_eq!(step["server"]["cpu_pct_mean"].as_f64().unwrap(), 59.0);
        assert_eq!(step["server"]["mem_max_bytes"], 210_763_776u64);
        assert_eq!(step["server"]["containers"]["postgres"]["cpu_pct_mean"].as_f64().unwrap(), 7.0);
        assert_eq!(step["server"]["containers"]["dotwire"]["net_tx_bytes"], 7000);
        assert_eq!(step["server"]["containers"]["postgres"]["disk_write_bytes"], 2_000_000u64);
        assert!(step["server"]["containers"].get("redis").is_none());
    }

    #[test]
    fn sizes_parse() {
        assert_eq!(parse_bytes("1.5kB").unwrap(), 1500);
        assert_eq!(parse_bytes("100MiB").unwrap(), 104_857_600);
        assert_eq!(parse_bytes("0B").unwrap(), 0);
        assert_eq!(parse_bytes("7.6GiB").unwrap(), 8_160_437_862);
        assert!(parse_bytes("3 zz").is_err());
    }
}
