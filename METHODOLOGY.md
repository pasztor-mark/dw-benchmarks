# dw-benchmarks methodology

This is the published method behind the report in `report/`. It covers how the numbers are produced, what is deliberately different from upstream defaults, and what the numbers do not show.

## Status of the numbers

Only a real run on the rented servers produces benchmark numbers. The page built from `results/20261113` is a local smoke run: Docker Desktop on an Apple M4, plain disk only, one repetition, every scenario at trivial scale (at most 10 users and 10 msg/s, 5 s per step). Its numbers are meaningless as performance data. Its validity guards are computed, but `invalid` is forced to false, and the page shows a red "LOCAL – NOT A BENCHMARK" banner for any campaign whose `env.json` has `local: true`.

## Hardware and topology

- Two Hetzner Cloud CCX33 servers in Helsinki (`hel1`, network zone `eu-central`): 8 dedicated AMD vCPUs, 32 GB RAM, local NVMe boot disk, Ubuntu 26.04 LTS. Dedicated vCPUs are not shared with other tenants; they are hardware threads, not full cores. Box-to-box traffic runs over a Hetzner Cloud private network.
- **Box A** is the system under test. Only one target stack runs at a time, unpinned, with the whole machine available to it. Its docker stats are sampled once per second.
- **Box B** is the load generator (`dwbench`, one process). It runs the webhook sink for the dotwire presend and postsend variants, and it has its own host sampler. It runs nothing else, so the generator never competes with the system for CPU.
- OS on the local NVMe boot disk. All benchmark data lives on one Hetzner Cloud Volume attached to Box A (network block storage, not local NVMe), split into two equal partitions. Every system pays the same network-storage write latency, so the comparison is fair, but absolute latencies, especially p99, are higher than on local disks:
  - `/data/plain`: ext4. Used by dotwire, and by the plain-disk pass of Tinode and Synapse.
  - `/data/crypt`: LUKS2 with `aes-xts-plain64`, a 512-bit key, `--perf-no_read_workqueue --perf-no_write_workqueue`, with ext4 on top. Used by Tinode and Synapse by default.
- Box A has an 8 GB swap file on the boot disk with `vm.swappiness=1`, as a guard against the kernel killing a process when memory runs out, added by hand (not in `boxes/setup.sh`). The kernel's swap-out counter (`pswpout` in `/proc/vmstat`) is checked at the end of the campaign: zero means no page was ever swapped and no result was affected.
- Host settings from `boxes/setup.sh`: CPU governor `performance` where the VM exposes one, `net.ipv4.ip_local_port_range=1024 65535`, `net.core.somaxconn=65535`, `net.ipv4.tcp_max_syn_backlog=65535`, `fs.file-max=2097152`. `env.json` records what was actually in effect, plus `cryptsetup benchmark` output for the crypt disk.
- Each target's compose bind-mounts its data under `${DATA_ROOT}/<target>/…`, and `bench` sets `DATA_ROOT` per pass. Disk parity means the same filesystem on the same partition type; the only difference is the LUKS layer.
- Images are pinned. `env.json` records the compose image IDs and tags (`image_digests`).

## Step timings and load shape

- Per step: 30 s warmup (sent, not recorded), 120 s measurement, 10 s drain.
- Open loop. Sends are scheduled at fixed intervals across senders and never wait for the previous ack. Sends are spread over the room members in a fixed pattern.
- In-flight sends are capped at 10 000. When the cap is hit the step is marked `saturated`, and the skipped sends are never registered.
- Payload: `dwb:<msg_id>:` padded to 256 bytes.
- Every send is wrapped in a 10 s timeout. A timeout counts as an error. The send is not expected, so it does not enter the loss count.
- Message ids are unique per step: the step index is encoded in the wire id, so a delivery from an earlier step cannot be counted in a later one. Warmup deliveries never reach the recorder.
- Scenarios (full scale): latency ladder 100 rooms × 10 members, 100 → 500 → 1 000 → 2 000 → 5 000 → 10 000 msg/s, stopping after two consecutive saturated or invalid steps, or right after a step where more sends failed than succeeded. Fanout with 10, 100, 1 000 and 5 000 members at 10 msg/s. Connections at 10 000, 25 000 and 50 000 users, connecting at 500/s, holding each level 60 s. Reconnect storm with 10 000 subscribed users. History with 1 000 rooms × 1 000 messages preloaded, reading the last 50 at 100, 500, 1 000 and 2 000 req/s.

## Latency: intended time, not actual time

- Each send gets an intended instant from the schedule. Latency is `received_at − intended_send_at`, so a sender that falls behind is charged for the delay (no coordinated omission).
- Service time (`received_at − actual_send_at`) is recorded alongside, and so is the ack time (`ack − actual send`). Scheduler lag (`actual start − intended`) is recorded too, as `sched_lag_us`.
- Percentiles come from an HdrHistogram at 1 µs to 60 s with 3 significant digits. Each step writes its `.hlog` and the percentiles (p50, p95, p99, p99.9, max) into `summary.json`, in microseconds.

## Delivery accounting

- Expected deliveries are, for every send that succeeded, the number of subscribed members of the room minus the sender. Failed sends are not expected.
- Received is the number of unique `(message, recipient)` pairs that arrived inside the measurement window or during the drain.
- Lost is expected minus received after the drain. Deliveries that arrive after the drain count as lost.
- Duplicates (the same pair twice) are counted separately and never added to received.
- Deliveries whose wire id belongs to another step, or whose recipient is not a member, count as foreign. They are reported in `extra.foreign`.
- Delivery ratio is received over expected, shown next to every latency.

## Validity guards

- Harness CPU is the `dwbench` process's CPU seconds over the measured window, divided by wall time times core count. It is the load generator's own CPU, not Box A's.
- A step is invalid when harness CPU is above 70% or scheduler lag p99 is above 5 ms. Any step where a reconnect restart command fails is also invalid.
- A saturated step is not invalid by itself, but it is excluded from max sustainable throughput (see below).
- On the local smoke run the guards are computed and recorded, and `invalid` is forced to false, because the Mac is not the load generator a real run would use.

## Repetitions, median and unstable steps

- Each run (target, variant, scenario, disk) is repeated 3 times. dotwire-sse and the dotwire variants run once: they measure dotwire's own feature costs, not the comparison, so they have no median or unstable-step check. Each repetition starts from the same snapshot: stop, replace the data directory, bring the stack up and wait for health.
- The median repetition is chosen per run by the p99 of its last valid step, so the choice is comparable across repetitions. With an even count the lower median is taken.
- The report shows the median repetition's numbers. The throughput table lists the value from all three repetitions.
- A step is unstable when its p99 differs by more than 15% between the minimum and maximum repetition: `(max − min) / min > 0.15`. Unstable steps are listed in `unstable_steps` and marked in the report.

## Server resource use

- `boxes/sampler.sh` writes one line per second of `docker stats` (plus host stats on Linux) to `stats-a.jsonl` on Box A, and to `stats-b.jsonl` on Box B.
- For each step, the samples whose timestamp is in `[started_at, ended_at]` are used. Only containers of the target's compose project are counted, which is `dwb-<stack>` (dotwire-sse and the dotwire variants all use the dotwire stack). Other containers on the host are ignored.
- Per container, names are normalised by stripping the project prefix and the replica suffix, so `dwb-dotwire-postgres-1` becomes `postgres`.
- Per container: `cpu_pct_mean` is the mean of docker's CPU percentage (100% = one core) over the window. `mem_max_bytes` is the maximum memory usage over the window. `net_tx_bytes`, `net_rx_bytes` and `disk_write_bytes` are the last minus the first counter value inside the window, clamped at zero.
- Summed (`server`) fields: `cpu_pct_mean` is the sum of the per-container means. `mem_max_bytes` is the maximum over time of the summed memory of the containers present in each sample (the peak of the sum, not the sum of the peaks). Net and disk fields are sums of the per-container deltas.
- Units are parsed from docker's strings. Decimal units (kB, MB, GB) use 1000 and binary units (KiB, MiB, GiB) use 1024.
- Only the median repetition's steps carry a `server` object. The other repetitions keep only the harness numbers.
- Limits: a counter that resets inside a window (a container restarted during a reconnect storm) is clamped to zero, so net and disk for that container in that step are lower bounds. The reconnect storm restarts the app containers on purpose, so read their net and disk with that in mind.

## Snapshot procedure

Setup and preload are not timed, so they run once per `(target, scenario, disk)`:

1. Run user and room creation (and, for history, the 1 000 × 1 000 preload through each system's own send API, untimed).
2. Stop the stack and copy its data directory to `${DATA_ROOT}/<target>/.snap-<scenario>` with `cp -a`, on the same filesystem.
3. Before each repetition: stop, replace the data directory with a fresh copy of the snapshot, bring the stack up and wait for health.

The snapshot is keyed by scenario, disk, smoke flag and a hash of the configuration. A snapshot whose key does not match the current configuration aborts the run instead of being reused. dotwire-sse and the dotwire variants share the dotwire snapshot for the same scenario and disk, because they run the same server state with a different receive path or a webhook. The snapshot is built with the baseline environment.

This keeps "clean state for each repetition" while skipping roughly 50 000 bcrypt registrations on Synapse and the history preload on every repetition. Re-runs skip finished `(target, variant, scenario, disk, rep)` directories, so a campaign can stop and resume.

## Disk and LUKS parity

- dotwire: plain disk, its own AES-GCM for message payloads (see below).
- Tinode and Synapse: LUKS by default, for every scenario. They also run one extra pass on plain disk for `latency-ladder` and `history`, so the report can show the LUKS cost on its own.
- The same ext4 on top in both cases. The only difference is the LUKS layer.
- The local smoke pass uses plain disk only. LUKS is not exercised locally (Docker Desktop's VM has no dm-crypt without privileged loop-device setup), so `boxes/setup.sh` is checked with ShellCheck and a dry run only.

## dotwire's encryption cannot be disabled

`EncryptionOptions` has no off switch, so dotwire always encrypts message payloads with AES-GCM, on its plain disk. Tinode and Synapse have no application-level at-rest encryption, so their data is protected by LUKS instead. The parity is "all three encrypted at rest, each the way it is realistically deployed", not identical mechanisms. dotwire's encryption cost is therefore included in every dotwire number, and there is no dotwire row without it in the at-rest section.

## Cross-system definitions

These are the definitions used side by side in the report. They are copied from the design spec.

| Term | dotwire | Tinode | Synapse |
|---|---|---|---|
| connected + subscribed | `Subscribe(roomId)` invocation completed | `{ctrl 200}` for `{sub}` | initial `/sync` returned with the room in `rooms.join` |
| connect latency | WS open → `Subscribe` completion | WS open → `{sub}` ctrl | first `/sync` request → response |
| idle connection | open WS | open WS | one `/sync` long-poll in flight (re-issued on return) |
| resubscribed after restart | `Subscribe` completes on a new WS | `{sub}` ctrl on a new WS | first successful `/sync` (resuming `since`) after the restart |
| "restart the app" | `dotwire` container | `tinode` container | main process + all workers (`sync1`, `sync2`, `client1`, `persister1`). Redis and nginx stay up, just as NATS stays up for dotwire |

Postgres is never restarted. Matrix has no persistent connection, so "connections" for Synapse mostly measures in-flight `/sync` cost. The report says so next to that chart.

## Competitor choice and licences

The competitors are self-hostable chat servers whose free edition has no user cap, so every scenario runs at full scale on all of them. Checked 2026-11-16 against the pinned versions:

| System | Version | Licence | Free self-hosted user cap |
|---|---|---|---|
| dotwire | 0.4.1 | Apache-2.0 | none |
| Tinode | 0.25.3 | GPL-3.0 | none |
| Synapse | v1.165.0 | AGPL-3.0 (or a commercial licence from Element) | none |

Not benchmarked, because their free editions cap users below the test scale (up to 50 000 connections and 10 000 users per scenario):

- Rocket.Chat: the free Community plan limits concurrent users. Rocket.Chat's own pages disagree on the number: [the plans documentation](https://docs.rocket.chat/our-plans) gives 100 and [the Community vs Starter sheet](https://content.rocket.chat/hubfs/Rocket.Chat-Community-vs-Starter-plan.pdf) gives 200. The free Starter plan caps the workspace at 50 users.
- Mattermost: [Team Edition is offered for under 250 activated users](https://docs.mattermost.com/product-overview/editions-and-offerings.html), and the default free Entry edition shows only the latest 10 000 channel messages.

These limits change often. The links above are the sources as of the check date.

## Deviations from upstream defaults

Every deviation is listed here, per target. Anything not listed runs at the upstream default.

### Shared Postgres (all three targets)

`targets/postgres.conf`, mounted by all three stacks, overrides the upstream tuning, sized for the 32 GB CCX33: `shared_buffers=8GB`, `effective_cache_size=24GB`, `work_mem=32MB`, `maintenance_work_mem=2GB`, `max_connections=500`, `wal_compression=on`, `synchronous_commit=on` (the default). This does not give durability parity: Tinode and Synapse acknowledge a message only after a Postgres commit, while dotwire acknowledges once NATS JetStream has accepted it (JetStream runs with its default periodic fsync, not `sync_interval: always`) and writes Postgres later in batches. See the fairness audit in `report/v0.4-case-study.ipynb`. dotwire's own compose uses 256 MB for shared buffers. The image is `timescale/timescaledb:2.31.0-pg17`, the same for all three (plain Postgres 17 plus an extension that only dotwire loads).

### dotwire (`targets/dotwire/bench.env`)

Each line below overrides dotwire's upstream compose defaults:

- `Dotwire__RateLimits__Enabled=false`. Upstream defaults are send 20 burst + 5/s and hub 30 + 10/s per user. The `dotwire-ratelimits` variant turns them back on, at a scaled-down ladder per user so the limits are not hit.
- `Auth__MaxTokenLifetimeMinutes=240`. The hub closes connections on token expiry (`CloseOnAuthenticationExpiration = true`), so the 60-minute default would drop connections mid-run.
- `Auth__Issuer=dwbench`
- `Auth__Audience=dotwire`
- `Dotwire__Encryption__ActiveKeyId=k1`
- `AllowedHosts=*`
- `Dotwire__Webhooks__AllowInsecureUrls=true`
- `Dotwire__HubSend__Enabled=true`. Inert on images without hub send.
- `DOTWIRE_READ_POOL_OPTS`. Appended verbatim after the read connection password. Empty by default, set only for improvement campaigns.

Other changes from upstream: ports bound to the private interface instead of `127.0.0.1`, `Cors` and `AllowedHosts` set for the private IP, and `DOTNET_gcServer=1` kept from upstream. The `DOTWIRE_IMAGE` override allows a local build for regression runs. Published numbers use the pinned `ghcr.io/pasztor-mark/dotwire:0.4.1`.

dotwire variants (`targets/dotwire/variants/`):

- `presend.env`: `Dotwire__Webhooks__Presend__Url=${SINK_URL}/presend`. The sink on Box B answers `{"allow":true}` immediately, so this measures the added round trip.
- `postsend.env`: `Dotwire__Webhooks__PostSend__Url=${SINK_URL}/postsend`. The sink receives the post-send event.
- `ratelimits.env`: `Dotwire__RateLimits__Enabled=true`.
- `dotwire-sse` is not a configuration variant. It receives through `GET /rooms/{id}/events` instead of SignalR, on the same dotwire server state.
- `dotwire-hub` is not a configuration variant. It sends through the hub `Send` method instead of REST, on the same server state.

Dropped on purpose: a `dotwire-multinode` variant. Its compose caps each node at 2 CPUs and 512 MB, adds a Postgres read replica and a 3-node NATS cluster. On one box that measures coordination overhead squeezed onto shared hardware, not scale-out, so it would be a misleading feature cost.

### Tinode

- `max_subscriber_count` is raised to 6000 in `targets/tinode/tinode.conf` (upstream default 128). The 1 000 and 5 000 member fanout steps need it. The image renders `/opt/tinode/config.template` from this file, so the setting takes effect at start-up. Setup fails loudly with `tinode join failed` when a room cannot hold all of its members.
- Postgres adapter (`tinode/tinode-postgres:0.25.3`, labelled beta upstream), with `POSTGRES_DSN` pointing at the shared Postgres, sample data disabled, and no SMTP, so email verification is off.
- Rate limiting at upstream defaults. Tinode has no per-user message rate limit to disable.
- Each sender uses the socket from its `connect()`. A user's first send may open a socket lazily. For users that were not reached during warmup, that connection cost lands in the measured window. The report does not subtract it.
- The image is amd64-only (`tinode/tinode-postgres:0.25.3`), so it runs under emulation on Apple silicon. This matters only for the local smoke run.

### Synapse

- `rc_*` rate limits are raised so they never trigger: `rc_message`, `rc_login` (address, account, failed_attempts), `rc_registration`, `rc_joins` (local, remote, per room) and `rc_invites` (per room, per user, per issuer), each at `per_second: 100000, burst_count: 100000`.
- `bcrypt_rounds: 4` (upstream default 12). This affects only registration and login, which both happen in untimed setup. Tinode and Synapse runs reuse stored auth tokens from setup, as dotwire does with its pre-minted JWTs.
- Presence is left on (`presence.enabled: true`, the upstream default), because dotwire also has presence.
- Federation is off (`federation_domain_whitelist: []`).
- Worker topology: main process, `sync1` and `sync2` (generic workers for `/sync`), `client1` (client `/send` and other client traffic), `persister1` (the event persister), plus `redis` (8.8.4) and `nginx` (1.31.8). The image is `matrixdotorg/synapse:v1.165.0`. Each worker is its own compose service running `synapse.app.generic_worker` with its own config. Upstream's single-container `docker/Dockerfile-workers` was not used, because it cannot restart the workers without also restarting Redis and nginx.
- `synapse.access` logging is at INFO (`targets/synapse/log.config`). That is more than the upstream default and adds overhead to every request.
- Later `/sync` requests use an inline filter with timeline limit 1000, `lazy_load_members`, and presence and account_data excluded. The long poll timeout is 30 s. The first sync after a (re)connect uses timeline limit 1, so messages sent during a disconnect in the reconnect storm are neither delivered nor counted.
- `limited` timelines (the server said events were cut off by the limit) are counted, and the count is printed once per connection close to stderr. They are not backfilled, so they show up as loss.
- Users are registered through the shared-secret admin API during setup. One user creates each room and the rest join.

## Known limits

- Local numbers are not performance data (see the first section). Only a real run counts.
- A single load generator and a single system per box. The private network's bandwidth is a hard ceiling for fanout and high-rate steps. Network bytes are recorded so that saturation shows up as network use, not as server slowness.
- The connection port ceiling on Box B is about 64 000 sockets from one source IP to one destination. 50 000 fits. Going higher needs extra source IPs and is out of scope.
- Max sustainable throughput is the highest latency-ladder step with p99 below 250 ms and loss below 0.1%, among steps that are neither saturated nor invalid, and where no more sends failed than succeeded. A saturated step is excluded because the in-flight cap skips sends that are never registered, so its loss is under-reported.
- dotwire's live fanout is a best-effort NATS core republish after the JetStream ack. Delivery ratio is therefore reported for every step, and 100% delivery is not assumed.
- Tinode and Synapse runs use stored auth tokens from setup. The authentication path is not measured.
- Only the dotwire-sse variant, and the presend, postsend and ratelimits variants, are run on the latency ladder and fanout. The other scenarios are run only on the baseline configuration.

## Reproduction

On a laptop with `ssh` access to both boxes, and `BOX_A` and `BOX_B` set to `user@host` (passwordless sudo on both):

```
./bench setup-boxes
./bench all
./bench collect
report/build results/<campaign>
report/build --png
```

- `./bench setup-boxes` installs Docker, sysctls, the CPU governor and ulimits, partitions the data disk (`BENCH_DISK`), sets up LUKS on `/data/crypt`, builds `dwbench` on Box B, and authorises Box B to restart Box A's containers.
- `./bench launch` does the setup, then starts `./bench smoke-all && ./bench all` on Box B in the background, so the laptop can disconnect. Progress is in `all.log` on Box B. The published campaign was started this way.
- `./bench all` runs every target and scenario at 3 repetitions and dotwire-sse and the dotwire variants once, with the snapshots described above. `--reps N` sets the count for all of them.
- Single pieces: `./bench run <target> <scenario> [--variant presend|postsend|ratelimits] [--disk plain|crypt] [--reps N]`, and `./bench up|down <target>`.
- `./bench smoke-all` runs one message through each target's adapter, and `LOCAL=1 ./bench all --smoke` runs every scenario at trivial scale on the laptop. Neither produces numbers to publish.
- `./bench collect` copies `results/` back from Box B to the laptop.
- `report/build results/<campaign>` runs `dwbench aggregate` and writes `report/dist/index.html` and `report/dist/results.json`. `report/build --png` writes the share images to `report/dist/png/`.
- The campaign folder name defaults to the UTC date. Every run directory holds `summary.json`, `env.json`, `stats-a.jsonl`, `stats-b.jsonl` and the latency histograms.
- The harness git SHA, that is its source hash, is in each `env.json` as `harness_hash`.
