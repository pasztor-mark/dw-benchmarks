# dotwire benchmarks

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.24406317.svg)](https://doi.org/10.5281/zenodo.24406317)

Benchmark harness, raw data and case study comparing the [dotwire](https://dotwire.pasztormark.dev) realtime chat server with [Tinode](https://github.com/tinode/chat) on identical hardware. An exploratory, untuned Synapse attempt is kept as an appendix and excluded from every comparison. Results are also published at [dotwire.pasztormark.dev/docs/benchmarks](https://dotwire.pasztormark.dev/docs/benchmarks).

- `harness/`: `dwbench`, an open-loop load generator in Rust with an adapter per target.
- `targets/`: docker compose stacks for dotwire, Tinode and Synapse, plus the shared Postgres config.
- `boxes/`: setup and host sampler scripts for the benchmark servers.
- `bench`: the driver script.
- `report/`: the v0.4 case study notebook and the HTML report builder.
- `results/`: raw data of the v0.4 campaign.
- `METHODOLOGY.md`: how the numbers are produced and what they do not show.

## What is benchmarked

Five scenarios, each run against dotwire and Tinode:

| Scenario | Shape |
|---|---|
| Latency ladder | 100 rooms × 10 members, 100 → 500 → 1 000 → 2 000 → 5 000 → 10 000 msg/s |
| Fan-out | one sender, rooms of 10, 100, 1 000 and 5 000 members, 10 msg/s |
| Connections | 10 000, 25 000 and 50 000 users connecting at 500/s |
| Reconnect storm | 10 000 subscribed users, application container restarted |
| History | 1 000 rooms × 1 000 messages, read the latest 50 at 100-2 000 reads/s |

## Methodology

- **Open loop, no coordinated omission:** latency runs from the *intended* send time to delivery, so a sender that falls behind is charged for it. 256-byte payloads, 10 s send timeout.
- **Per step:** 30 s warmup, 120 s measured, 10 s drain. Latencies go into HdrHistograms (1 µs to 60 s, 3 significant digits).
- **Validity guards:** a step is invalid if the generator's CPU goes above 70% or its scheduling lag p99 above 5 ms.
- **Repetitions:** 3 per run, each restored from the same data snapshot. A step whose p99 varies more than 15% across reps is reported as unstable.
- **Topology:** box A runs one system under test at a time; box B runs the load generator over a private network.

`METHODOLOGY.md` has the full method, every deviation from upstream defaults, and the delivery accounting.

## Versions

| Component | Tag | Image ID (from `env.json`) |
|---|---|---|
| dotwire | `ghcr.io/pasztor-mark/dotwire:0.4.1` | `sha256:be796c0d13ce46ad6b1023c730a5434a44270d9f500be4ed7a7e1dde3c2b6caa` |
| dotwire (baseline) | `ghcr.io/pasztor-mark/dotwire:0.4.0` | `sha256:b0f7a7419e1099d5b7b6f47bb849994e3f87d828bb015d1806b754a1ae627253` |
| Tinode | `tinode/tinode-postgres:0.25.3` | `sha256:0df104ad2a96438b7581f71168518ffa103d0c9a5ec6ce2db6ef8649bd9e0ca5` |
| Postgres (all stacks) | `timescale/timescaledb:2.31.0-pg17` | `sha256:b56a3f51991df1f79d0c1a0841cad5c5fa8058fdd6a3ee6886394a6d4c174538` |
| NATS (dotwire) | `nats:2.15.1-alpine` | `sha256:7ae34c25dfd6fe2e01c6483eabef20d796690fdcac6c4d94ca780a6cf9877fde` |
| Synapse (appendix) | `matrixdotorg/synapse:v1.165.0` | `sha256:673b4223c81a7634fd07d832b939a58f97081cfdaae7cdd2d1664cdcf7611b62` |

The compose files pin tags; the IDs above are what actually ran, as recorded in every rep's `env.json`.

## Hardware

Two Hetzner Cloud CCX33 servers in Helsinki: 8 dedicated AMD EPYC-Milan vCPUs, 32 GB RAM, Ubuntu 26.04 LTS, kernel 7.0.0-30-generic, connected over a Hetzner private network. Benchmark data lives on one Hetzner network volume attached to box A, split into `plain` (ext4) and `crypt` (LUKS2 aes-xts + ext4) partitions. dotwire ran on `plain` (it encrypts payloads itself with AES-GCM), Tinode on `crypt`. Each rep's `env.json` and `results/v0.4-boxes/box-*/system.txt` record the kernel, sysctl, disk layout and `cryptsetup benchmark` actually in effect.

## Results

The full account is **[report/v0.4-case-study.ipynb](report/v0.4-case-study.ipynb)** (campaign of 16-17 Nov 2026). Summary:

| | Result |
|---|---|
| dotwire throughput | 5 000 msg/s sustained at 22.5-26.9 ms p99, zero loss, 3/3 reps |
| Tinode throughput | ceiling ~400-450 msg/s on this storage, bound by disk flushes |
| Latency ladder | dotwire ~5x lower p99 at 100 msg/s, ~10x higher sustained rate (**conditional**, see below) |
| Fan-out | dotwire ~3x faster in small rooms; tie at 5 000 members |
| Connect latency | Tinode wins (p99 7-20 ms vs 82-284 ms); dotwire had 0 failed connects, Tinode ~2% `503 locked` |
| History reads | Tinode ~20-30% lower p99 |
| Reconnect storm | dotwire recovers ~4x faster, direction only (1 and 2 reps) |
| 0.4.0 → 0.4.1 | 5k msg/s p99 ~110 → ~25 ms |

Caveats:

- **Durability is not matched.** dotwire acks after NATS JetStream accepts a message and writes Postgres in batches; Tinode acks after a Postgres commit with `synchronous_commit=on`. Throughput comparisons are conditional until a durability-matched rerun exists.
- Tinode ran on LUKS and dotwire on plain ext4; the Tinode plain-disk pass was not run. Storage is a network volume, so absolute latencies are higher than on local NVMe.
- The campaign was stopped early. Synapse was untuned, only its first rung ran, and it is excluded from every comparison.

## Regenerating the notebook

Every table and chart in the notebook is recomputed from `results/`. With Python 3.14, from a fresh clone:

```
python3 -m venv .venv
.venv/bin/pip install -r report/requirements.txt
.venv/bin/jupyter nbconvert --to notebook --execute --inplace report/v0.4-case-study.ipynb
```

Section 2 of the notebook prints a SHA-256 fingerprint of the archive and recomputes every step's percentiles from the raw `.hlog` files.

## Rerunning the benchmark

Needs two Ubuntu servers reachable over `ssh` as a user with passwordless sudo, and a spare data disk on box A.

```
cp bench.conf.example bench.conf   # fill in BOX_A, BOX_B, the private IPs and BENCH_DISK
./bench launch                     # sets up both boxes, then runs `bench smoke-all && bench all` on box B
./bench collect                    # copies results/ back
```

Everything uses `harness/configs/full.toml` except Synapse (`CONFIG=harness/configs/synapse.toml`). Single runs: `./bench run <target> <scenario> [--variant …] [--disk plain|crypt] [--reps N]`.

How the v0.4 campaign actually ran: `./bench launch` ran until the Synapse block, where `bench all` was stopped. The remaining runs were started by `results/v0.4-boxes/box-b/rest.sh`, and the campaign was stopped for good on 17 Nov 2026 before it finished (notebook section 10 lists what's missing). `results/v0.4-boxes/box-b/all.log` and `rest.log` are the logs of both.

## Provenance

- **Harness build:** `harness/src` is byte-identical to the build used by the last run of the campaign. Verify it with:

  ```
  git ls-files harness/src harness/Cargo.toml | LC_ALL=C sort | xargs cat | shasum -a 256
  # f3013f440a477dba519347441bc3ee005d41e56c57ce6f28581ab2b38c3af2b6
  ```

  `bench` records this same hash as `harness_hash` in every new `env.json`. The archived `env.json` files predate that definition: their hash also covered a local, uncommitted test key and used glibc sort order, so their values (`67bb8eb9…` for most runs, `4fadb92c…` for the Tinode latency ladder, `bf4e9b33…` for the `dotwire-sse` run) can't be recomputed from a clone. `bf4e9b33…` is this source tree; the two earlier builds differ only in the ladder stop rule and the history-preload concurrency option (notebook section 2).
- **Building:** `cargo build --release --locked` in `harness/` (tested with rustc 1.100.0). The unit tests need a throwaway RSA key, which is not committed: `openssl genrsa -out harness/src/adapters/test_key.pem 2048 && cargo test --locked`.
- **Not archived:** data snapshots, database volumes, and the boxes' system journals (they hold the operator's IP addresses).
- `results/v0.4-pre-redeploy/` is the earlier dotwire 0.4.0 campaign on the same boxes, and `results/v0.4-synapse-untuned/` the Synapse appendix.

## License

- **Code** (`harness/`, `targets/`, `boxes/`, `bench`, `report/build`, `report/index.html` and the notebook's code cells): [MIT](LICENSE).
- **Data and writing** (`results/`, `report/fixtures/`, the notebook's text and outputs, `METHODOLOGY.md` and this README): [CC BY 4.0](LICENSE-data).

## Citation

See [`CITATION.cff`](CITATION.cff). Archived on Zenodo: [10.5281/zenodo.24406317](https://doi.org/10.5281/zenodo.24406317) for the latest version, [10.5281/zenodo.24406318](https://doi.org/10.5281/zenodo.24406318) for `dotwire-0.4.1-vs-tinode-0.25.3`.
