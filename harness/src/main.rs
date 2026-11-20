mod accounting;
mod aggregate;
mod adapter;
mod adapters;
mod scenarios;
mod body;
mod guards;
mod config;
mod results;
mod retry;
mod schedule;
mod sink;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dwbench", version, about = "Load generator for the dotwire benchmark suite")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Target {
    Dotwire,
    DotwireSse,
    DotwireHub,
    Tinode,
    Synapse,
    Fake,
}

#[derive(Clone, Copy, ValueEnum)]
enum Scenario {
    LatencyLadder,
    Fanout,
    Connections,
    ReconnectStorm,
    History,
}

#[derive(Clone, Copy, ValueEnum)]
enum Variant {
    Presend,
    Postsend,
    Ratelimits,
}

#[derive(Subcommand)]
enum Command {
    Setup {
        #[arg(long)]
        target: Target,
        #[arg(long)]
        scenario: Scenario,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        fixtures_out: PathBuf,
    },
    Run {
        #[arg(long)]
        target: Target,
        #[arg(long)]
        scenario: Scenario,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        fixtures: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        restart_cmd: Option<String>,
        #[arg(long)]
        smoke: bool,
        #[arg(long)]
        variant: Option<Variant>,
        #[arg(long, default_value = "plain")]
        disk: String,
        #[arg(long, default_value_t = 1)]
        rep: u32,
        #[arg(long)]
        local: bool,
    },
    Smoke {
        #[arg(long)]
        target: Target,
        #[arg(long)]
        config: PathBuf,
    },
    Sink {
        #[arg(long)]
        listen: String,
    },
    Aggregate {
        #[arg(long)]
        results: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
}

fn name_of<T: ValueEnum>(v: T) -> String {
    v.to_possible_value().expect("named variant").get_name().to_string()
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Setup { target, scenario, config, fixtures_out } => {
            let cfg = config::RunConfig::load(&config)?;
            let (target_name, scenario_name) = (name_of(target), name_of(scenario));
            let plan = scenarios::setup_plan(&scenario_name, &cfg)?;
            let adapter = adapter::build_target(&target_name, &cfg)?;
            let mut fixtures = adapter.setup(&plan).await?;
            if matches!(scenario, Scenario::History) {
                scenarios::history::preload(adapter, &fixtures, &cfg).await?;
            }
            fixtures.target = target_name;
            fixtures.scenario = scenario_name;
            std::fs::write(&fixtures_out, serde_json::to_vec_pretty(&fixtures)?)?;
            Ok(())
        }
        Command::Run { target, scenario, config, fixtures, out, restart_cmd, smoke, variant, disk, rep, local } => {
            if matches!(scenario, Scenario::ReconnectStorm) && restart_cmd.is_none() {
                anyhow::bail!("reconnect-storm requires --restart-cmd");
            }
            let cfg = config::RunConfig::load(&config)?;
            let (target_name, scenario_name) = (name_of(target), name_of(scenario));
            let fixtures: results::Fixtures = serde_json::from_slice(&std::fs::read(&fixtures)?)?;
            let adapter = adapter::build_target(&target_name, &cfg)?;
            std::fs::create_dir_all(&out)?;
            let meta = scenarios::RunMeta {
                target: target_name,
                scenario: scenario_name,
                variant: variant.map(name_of),
                disk,
                rep,
                local,
                smoke,
                out: Some(out.clone()),
            };
            let summary = match scenario {
                Scenario::LatencyLadder => scenarios::latency_ladder::run(adapter, &fixtures, &cfg, &meta).await?,
                Scenario::Fanout => scenarios::fanout::run(adapter, &fixtures, &cfg, &meta).await?,
                Scenario::Connections => scenarios::connections::run(adapter, &fixtures, &cfg, &meta).await?,
                Scenario::ReconnectStorm => {
                    scenarios::reconnect::run(adapter, &fixtures, &cfg, &meta, restart_cmd.as_deref()).await?
                }
                Scenario::History => scenarios::history::run(adapter, &fixtures, &cfg, &meta).await?,
            };
            std::fs::write(out.join("summary.json"), serde_json::to_vec_pretty(&summary)?)?;
            Ok(())
        }
        Command::Smoke { target, config } => {
            let cfg = config::RunConfig::load(&config)?;
            let name = name_of(target);
            match smoke(&name, &cfg).await {
                Ok(line) => {
                    println!("{line}");
                    Ok(())
                }
                Err((stage, e)) => {
                    eprintln!("SMOKE FAIL {name} stage={stage}: {e:#}");
                    std::process::exit(1);
                }
            }
        }
        Command::Sink { listen } => sink::run(&listen).await,
        Command::Aggregate { results, out } => aggregate::write_results(&results, &out),
    }
}

async fn smoke(name: &str, cfg: &config::RunConfig) -> std::result::Result<String, (&'static str, anyhow::Error)> {
    use std::time::{Duration, Instant};
    let plan = adapter::SetupPlan {
        rooms: vec![("r0".into(), vec!["a".into(), "b".into()])],
    };
    let target = adapter::build_target(name, cfg).map_err(|e| ("build", e))?;
    let fx = target.setup(&plan).await.map_err(|e| ("setup", e))?;
    let room = fx.rooms[0].id.clone();
    let (sender, receiver) = (fx.users[0].clone(), fx.users[1].clone());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut conn = target.connect(&receiver, tx).await.map_err(|e| ("connect", e))?;
    conn.subscribe(&room).await.map_err(|e| ("subscribe", e))?;
    let start = Instant::now();
    target.send(&sender, &room, body::encode(1, cfg.payload_bytes)).await.map_err(|e| ("send", e))?;
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .map_err(|_| ("deliver", anyhow::anyhow!("no delivery within 5s")))?
        .ok_or(("deliver", anyhow::anyhow!("connection closed")))?;
    let us = got.at.duration_since(start).as_micros();
    let n = target.history(&sender, &room, 50).await.map_err(|e| ("history", e))?;
    if n < 1 {
        return Err(("history", anyhow::anyhow!("history returned {n}")));
    }
    conn.close().await;
    Ok(format!("SMOKE OK {name} send→recv={us}us history={n}"))
}
