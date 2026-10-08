# aws-bench-cluster — rationale and reference

Why [SKILL.md](SKILL.md) and `vsbench` work the way they do. The facts below
were checked against source code, AWS and Docker Hub in October 2026. Re-check
them when something stops matching.

## Contents

- [Code layout](#code-layout)
- [AWS access](#aws-access)
- [Tags, cleaners and the TTL](#tags-and-ttl)
- [Network and SSH](#network-and-ssh)
- [Node bootstrap](#node-bootstrap)
- [Scylla setup](#scylla-setup)
- [Vector Store builds](#vector-store-builds)
- [Monitoring](#monitoring)
- [Metrics](#metrics)
- [Datasets](#datasets)
- [Benchmark tool caveats](#benchmark-tool-caveats)
- [Measurement methodology](#measurement-methodology)
- [Detached jobs](#detached-jobs)
- [Upgrading pins](#upgrading-pins)
- [Troubleshooting](#troubleshooting)
- [Production data (Scylla Cloud clusters)](#production-data)
- [Follow-ups outside this skill](#follow-ups)

## Code layout

The operator side is stdlib Python 3.10+. Everything that runs on the nodes is
static bash, configured through environment variables with no templating, and
checked with shellcheck.

| Path | Role |
|---|---|
| `vsbench` | entry point |
| `vsbenchlib/cli.py` | argument parsing, dispatch, locking, history, signals, exit codes |
| `vsbenchlib/cli_cluster.py`, `cli_bench.py` | command handlers |
| `vsbenchlib/provision.py`, `teardown.py` | `doctor`/`up` (rollback, resume); `down`/`list`/`extend`/`refresh-ip` |
| `vsbenchlib/build.py` + `cross/` | source specs, version stamping, aarch64 cross build, build cache |
| `vsbenchlib/deploy.py`, `monitoring.py` | Scylla, Vector Store, benchmark binary, scylla-monitoring |
| `vsbenchlib/bench.py`, `bench_jobs.py` | datasets, load/index/search/ab, detached jobs and their finalization |
| `vsbenchlib/results.py`, `results_format.py`, `prom.py` | log parsing, result records, comparison; their text/markdown tables; Prometheus queries and window math |
| `vsbenchlib/remote.py`, `guard.py` | ssh, transfers, detached jobs; the `exec`/`ssh` power-off and TTL guard |
| `vsbenchlib/retro.py` | history, notes, retrospective digest |
| `vsbenchlib/state.py`, `config.py`, `proc.py`, `awsapi.py` | local state and locks, pins and defaults, process helpers, `aws` CLI wrapper |
| `node/` | user-data, Scylla/Vector Store/monitoring start scripts, job runner, dataset fetcher |
| `datasets.json` | dataset catalog (exact file sizes) |
| `tests/` | unit tests (`python3 -m unittest discover -s tests -t .`); no CI runs them, so run them and shellcheck (SKILL.md §6) before pushing |

## AWS access

- **Account `797456418907` is `rnd-core-lab`**, the R&D lab account that SCT
  also defaults to. `DeveloperAccessRole` can run EC2, security groups and key
  pairs there.
- **Login.** Confluence pages *Okta CLI Tools for AWS Access* (RND 447545413)
  and *Programatic access to AWS* (RND 42205809) describe it. Install:
  - AWS CLI v2: `curl -fsSL https://awscli.amazonaws.com/v2/install.sh | bash`,
    or the zip installer with `-i ~/.local/aws-cli -b ~/.local/bin`.
  - `gimme-aws-creds`: `uv tool install gimme-aws-creds --with keyrings.alt`.
    The extra package matters on headless or dbus-less machines.
- With `cred_profile = acc-role` in `~/.okta_aws_login_config`, the command
  `gimme-aws-creds --roles arn:aws:iam::797456418907:role/DeveloperAccessRole`
  writes the profile `797456418907-DeveloperAccessRole`. `--roles` skips the
  role picker; `--profile` selects a section of the Okta config, not an AWS
  profile.
- **MFA is interactive.** `vsbench login` runs the device flow without a
  terminal or a browser (`BROWSER=true`), prints only the activation URL and
  waits up to 10 minutes; the agent relays the URL and the user approves it.
- Credentials last 6 hours (`aws_default_duration = 21600`). `vsbench` reads
  `x_security_token_expires` from `~/.aws/credentials` offline to warn early.
  `up` and `down` refuse to start with less than 30 minutes left, because a
  half-finished create or delete is what leaves orphans behind.
- The owner (`Owner`/`RunByUser` tags, resource names) is the session name of
  the assumed-role ARN, `…/DeveloperAccessRole/<first.last>@scylladb.com`, up
  to the `@`.
- `~/.aws/config` may set `output = table`, so `vsbench` always passes
  `--output json`.

## Tags and TTL

Two automated cleaners run in rnd-core-lab:

- **SCT `hydra-cleanup-cloud`.**
  - Terminates running instances older than 14 h unless they are tagged
    `keep=alive` (or `keep=<hours>`).
  - Deletes unattached security groups, volumes and EIPs that lack `keep=alive`.
- **finops Lambdas.**
  - Attribute cost by `RunByUser`, then `Owner`.
  - Skip instances that have a `billing_project`.
  - Enforce budgets of $10/h per person and $20/h per project.
  - Skip `keep=alive` instances.

The user asked for `keep=alive` on instances, so that a long benchmark is never
killed by the janitor. That makes the **TTL on the nodes the only thing that
stops a forgotten cluster**, which is why it is built defensively:

- **It is installed first.** It is the first block of user-data and uses only
  bash and systemd, so a failed apt or download step later cannot prevent it.
- **The timer runs `/usr/local/sbin/vsbench-ttl` every minute.**
  - It reads `/etc/vsbench/expires_at` (an epoch).
  - On a missing or garbled file it falls back to the last good value. It
    never treats a parse error as "no expiry".
  - At expiry it runs `systemctl poweroff`.
  - Instances launch with `--instance-initiated-shutdown-behavior terminate`,
    so power-off means termination.
- **`extend` updates the nodes first, then the tag.**
  - The write is atomic and is read back.
  - It refuses times under 15 minutes ahead, and refuses shortening unless
    `--shorten` is passed.
  - A failed tag update is only a warning, so it still works with expired
    credentials.
- **The new expiry is capped.** `extend` moves it to at most 7 days ahead.
  Every week of a forgotten cluster is a deliberate step.
- **The tag can lag the nodes.** When `extend` cannot update the `ExpiresAt`
  tag (expired credentials), it stores `expires_tag_pending` and the next
  credentialed command re-applies it. `list` prefers a later local expiry and
  shows `tag stale` instead of OVERDUE.
- **`list` and `status` flag `OVERDUE` clusters**: running past their expiry,
  which means the TTL failed. Confirm on the nodes
  (`exec all -- cat /etc/vsbench/expires_at`) before offering `down`.
- **`exec` and `ssh` refuse TTL tampering**: stopping `vsbench-ttl.timer`, or
  writing to `/etc/vsbench/expires_at` or `ttl.lastgood`. Use `extend`.

Other tagging decisions:

- **Volumes, network interfaces and the security group have no `keep` tag**,
  so the janitor cleans them if they are ever orphaned.
- **Key pairs are never cleaned by the janitor**, so `down` deletes them.
  Each cluster has its own key, generated locally.
- **Approved `billing_project` values** come from `scylladb/finops`
  `billing_projects/projects.yaml`. The Vector Search ones include
  `Vector Search: Sharding`, `…: Filtering`, `…: DiskANN`, `…: Quantization`,
  `…: Full Text Search`, `…: Alternator API` and `…: Snapshotting`. New ones
  come from Cloud FinOps.

## Network and SSH

- **Layout.** One security group per cluster: all traffic inside the group,
  and TCP 22 from the operator's `/32` only. Prometheus and Grafana bind to
  127.0.0.1 on the client, because Grafana's anonymous access defaults to the
  Admin role. Use the tunnel line from `vsbench status`.
- **Draft security standard.** Confluence RND 332169220 is a draft, not
  enforced. It prefers StrongDM over direct SSH. SCT itself opens 22 to the
  world, so a `/32` is the pragmatic middle ground.
- **Tunnel.** The tunnel line printed by `vsbench status` uses `-S none`, so it
  does not attach to the multiplexing master. A forward attached to the master
  dies when the master's `ControlPersist` (10 min) ends. The command blocks
  until Ctrl-C, so it belongs in the user's own terminal.
- **ssh config.** `vsbench` writes `clusters/<name>/ssh/config` with
  `HostKeyAlias <instance-id>`, so recycled public IPs never trip host-key
  checks. It also enables ControlMaster multiplexing to speed up the many
  short commands.
- **docker group and multiplexing.** The readiness polls run without
  multiplexing. The first multiplexed master starts after user-data has added
  `ubuntu` to the docker group. Even so, `vsbench` always uses `sudo docker`.
- **Operator IP changes** (VPN, roaming) break SSH: `vsbench refresh-ip`.

## Node bootstrap

`node/userdata.sh` runs once at first boot. The ordering is deliberate:

1. TTL (above).
2. Disable `unattended-upgrades` and the apt timers. On Ubuntu 24.04 they fire
   at first boot and hold the dpkg lock. Later, a docker.io upgrade would
   restart docker in the middle of a benchmark, and needrestart could restart
   Vector Store, forcing a full index rebuild.
3. Docker with `live-restore`, so a dockerd restart does not stop containers.
4. apt with `DPkg::Lock::Timeout=600`.
5. Scylla-style sysctls:
   - `fs.aio-max-nr`, `fs.nr_open`, swappiness, `perf_event_paranoid`;
   - on Scylla nodes also `vfs_cache_pressure`, numa balancing, sched
     autogroup and `tcp_mem`.
   The Scylla image no longer ships kernel tuning.
6. node_exporter on the host, checked against a pinned sha256. It runs the
   ethtool collector for ENA allowance counters
   (`bw_in/out`, `pps`, `conntrack`, `linklocal`).
7. Scylla nodes: the instance-store NVMe, found by its model string, gets XFS
   (`-K -m rmapbt=0 -m reflink=0`, as `scylla_raid_setup` does) and is mounted
   at `/var/lib/scylla`. Multiple disks are combined with RAID0.
8. A ready marker. Any failing line is written to the failed marker via an
   ERR trap, and `up` prints the log tail and rolls back.

## Scylla setup

The old guide (RND 118358415) and `scripts/cluster-in-aws` ran Scylla in
developer mode on ext4 with bridge networking:

- the I/O scheduler was not configured (no iotune);
- `--overprovisioned` was implied;
- `nofile` was set to an invalid 4294967295.

`vsbench` runs it production-like:

- **Container flags:**
  - `--network host`;
  - `--developer-mode 0 --overprovisioned 0`;
  - `--cap-add SYS_NICE --cap-add PERFMON`, so mbind and perf work under
    Docker's seccomp profile;
  - `--ulimit nofile=1048576`;
  - `--restart unless-stopped`.
- **I/O properties.** The precomputed values from `scylla-machine-image`
  `aws_io_params.yaml` are used for single-disk i8g types and mounted read-only
  with `--io-setup 0`. Other types run iotune on every start (about 2 min).
- **Addresses and racks.** listen, rpc and broadcast addresses are the private
  IP. Each node is its own rack: `--dc datacenter1 --rack rack<i+1>`. The
  benchmark creates a numeric-RF `NetworkTopologyStrategy` keyspace, and vector
  indexes need tablets keyspaces, so **RF must be 1 or the node count**.
- **Vector Store URIs.** `--vector-store-primary-uri` lists every Vector Store
  node. Scylla picks one at random per request and fails over to the next.
  It can be changed live: `UPDATE system.config SET value='…' WHERE name='vector_store_primary_uri'`.
- **Bundled node_exporter.** Images up to 2026.2 bundle one on :9100. It is
  disabled by mounting `/dev/null` over its supervisord file, because the
  host's exporter owns that port.
- **Rack-valid keyspaces.** `--rf-rack-valid-keyspaces true` is passed, as the
  validator CI does. It is deprecated on master in favour of
  `enforce_rack_list`. If a future nightly drops the option, Scylla refuses
  to start: remove it from `node/scylla-start.sh`.
- **Image tags.** `nightly` is the digest of `scylla-nightly:latest`, as CI
  uses. That tag can lag the newest dated `-dev-` tag by a day.
  - Single-arch nightly tags end in `-aarch64`, not `-arm64`.
  - Releases are `scylladb/scylla:<ver>`. Vector search needs 2025.4 or later.
- **Wipe and restart.** `--wipe` stops every node, wipes every data dir, then
  starts the seed first: raft group0 state lives in the data dir. Without
  `--wipe`, an image change is a rolling restart.

## Vector Store builds

- **Release.** `release:latest` is resolved through the GitHub releases API.
  Docker Hub's `scylladb/vector-store:latest` tag is stale (April 2026), so it
  is not used. The node pulls `scylladb/vector-store:<ver>` and copies
  `/opt/vector-store/vector-store` out of it. Every source then runs the same
  way: a bare binary under systemd (`vector-store.service`) with
  `/opt/vector-store/.env`.
- **Configuration.** Vector Store reads `.env` from its working directory. It
  never overrides variables already in the process environment, but it does
  override them on SIGHUP. So the unit sets no `Environment=` at all;
  `RUST_LOG` lives in `.env` too.
- **Cross compilation (`cross/`).**
  - **Image.** `cross/Dockerfile` is `rust:<channel>-bookworm`, the release
    toolchain image, plus Debian's aarch64 cross gcc/g++ and qemu-user for
    smoke tests. No host qemu binfmt is needed.
  - **Native deps.** aws-lc-sys, ring, mimalloc, usearch (C++) and zstd all
    build with the cross compilers.
  - **Two cargo invocations.** `cross-build.sh` builds `vector-store` alone,
    then the benchmark. A combined build changes feature unification, and the
    Vector Store binary would differ from the release one.
  - **Version.** It is stamped the way `scripts/run-with-release-toolchain`
    does it: temporary `Cargo.toml` and `Cargo.lock` with `0.0.0-dev`
    replaced, bind-mounted over the originals.
  - **Keep in sync** with that script when release stamping changes.
- **`verify.sh` gates every build:**
  - aarch64 ELF;
  - GLIBC ≤ 2.34 and GLIBCXX ≤ 3.4.29, which is what ubi9-minimal and the
    release binary need;
  - the expected `--version` under qemu;
  - no `Failed to compile` in usearch's build output. usearch silently drops
    SIMD kernels it cannot compile, and Cargo hides registry build-script
    warnings, so a build could otherwise lose its SVE kernels unnoticed.
- **Build IDs.** A build ID is the version (with `+` replaced by `_`) plus the
  first 8 hex digits of the binary's sha256.
  - Dirty trees get `.d<hash of the diff>` in their build metadata, so two
    different uncommitted states never share an ID.
  - Builds are generic aarch64, like the release, so they compare
    like-for-like with Docker Hub. `-C target-cpu=neoverse-v2` would SIGILL on
    Graviton2/3 and only change Rust code; usearch already dispatches at
    runtime.
- **Older releases.** Before 1.11, `/api/v1/indexes` had no status or count.
  For those, readiness checks fall back to `/api/v1/indexes/<ks>/<index>/status`.
- **Activation is verified.** The upload is checked by sha256 and the symlink
  swap is atomic. After the restart, `vsbench` checks that the new MainPID's
  `/proc/<pid>/exe` is the expected binary, and that `/api/v1/info` reports
  the expected version.

## Monitoring

**Version.** scylla-monitoring 4.16.1 comes from the release tarball, which
ships prebuilt dashboards for `master` and 2026.x as well as `vector_1`. A
master checkout regenerates dashboards and needs Python YAML.

**Flags.** It is started with `--vector-search`, not the deprecated
`--vector-store` from the 4.11.1-era guide. That gives:
- a `vector_search` job, scraping `:6080/metrics` and adding a `ks_index` label;
- a `vector_search_os` job, scraping node_exporter on the VS hosts;
- the "Vector Search" dashboard.

Targets must be **bare IPs**: `IP:6080` would become `IP:6080:6080`.

**Target directory.** `--target-directory` with atomically replaced files
reloads live, in about 2 s. Empty manager files avoid permanently down
`manager_agent` targets.

**Health check.** `node/monitoring-start.sh` checks Prometheus targets with `jq`.
A component's jobs must be up once that component is deployed, and the
`node_exporter` job's scrape interval must be 10 s.

**Template patch.** The `node_exporter` job hardcodes `scrape_interval: 1m`,
and `--scrap` does not touch it. `vsbench` deletes exactly those two lines
from the template, so node metrics are scraped every 10 s like the rest. If
upstream rewords them, the deploy fails rather than silently falling back to
1-minute resolution.

**Client node.** The client's own node_exporter is scraped by an extra
`loadgen_os` job with cluster label `vsbench-loadgen`, so it does not pollute
the Scylla OS dashboards. Prometheus and Grafana are pinned to CPU 7 and the
benchmark runs under `taskset -c 0-6`, so they do not compete.

**arm64 caveat.** The bundled Scylla CQL Grafana datasource plugin is
amd64-only, so the CQL-datasource panels of the "Scylla CQL" dashboard do not
work on the arm64 client. Everything Prometheus-based works.

## Metrics

Vector Store (`/metrics` on :6080; no prefix; label sets disappear when an
index is dropped, so counters reset):

| Metric | Type | Labels |
|---|---|---|
| `request_latency_seconds` | histogram, buckets 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500 ms, 1, 2, 5, 10 s | keyspace, index_name |
| `index_size` | gauge (refreshed when scraped) | keyspace, index_name |
| `index_modified` | counter | keyspace, index_name, operation |
| `indexing_lag_seconds` | histogram (CDC only) | keyspace, index_name |
| `cdc_reader_up`, `cdc_handler_errors_total`, `cdc_reader_restarts_total`, `cdc_last_processed_timestamp_seconds` | | keyspace, index_name, reader (`wide`/`fine`) |

Vector Store exports no process metrics. Take CPU and memory from
node_exporter.

PromQL (`vsbench prom query '…'`):

```
sum by (instance)(rate(request_latency_seconds_count[1m]))                         # VS QPS
histogram_quantile(0.99, sum by (le)(rate(request_latency_seconds_bucket[1m])))    # VS p99 (bucket-interpolated)
sum(rate(request_latency_seconds_sum[1m])) / sum(rate(request_latency_seconds_count[1m]))   # VS mean
index_size ; deriv(index_size[1m])                                                 # index size / build rate
histogram_quantile(0.99, sum by (le)(rate(indexing_lag_seconds_bucket[5m])))       # CDC lag
time() - cdc_last_processed_timestamp_seconds                                      # CDC staleness
100*(1-avg by (instance)(rate(node_cpu_seconds_total{mode="idle"}[1m])))           # CPU %
node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes                        # memory used
rate(node_network_receive_bytes_total{device!="lo"}[1m])                           # network
increase(node_ethtool_bw_out_allowance_exceeded[5m])                               # ENA throttling
avg by (instance)(scylla_reactor_utilization)                                      # Scylla load
```

To discover names, run `vsbench prom api label/__name__/values 'match[]={job="vector_search"}'`; the single quotes keep the PromQL intact in bash.

### CDC baselines and capacities (default shape)

Measured on 2026-10-06 (VECTOR-951 reproduction; Scylla nightly
`2026.4.0~dev-0.20261005`, cohere-1m, index m=16, cb=128, F32):

- **Idle CDC lags are not zero.** With nothing to ingest,
  `time() - cdc_last_processed_timestamp_seconds` reads about **10 s for the
  fine reader and 46 s for the wide reader** (its safety 30 s plus sleep
  10 s). A reader is behind when the lag keeps growing by 1 s/s, not when it
  is above zero; an ingest stall shows as `index_size` not moving while rows
  are acknowledged (compare it with the base-table row count).
- **Insert capacity.** One `i8g.2xlarge` Scylla node takes single-row
  inserts of 768-d vectors at **~60K rows/s** (CL=ONE, 32 in flight). A
  20 s uncapped probe therefore added 1.2M rows, more than the 1M base:
  size insert probes by rows, not seconds.
- **Ingest capacity.** Vector Store on `r8g.4xlarge` ingests **~5.1K rows/s**
  through CDC at ~80% CPU with no concurrent searches. Under a steady
  search stream (`biased` select in `vs_index::recv`) it drops to
  ~650–800 rows/s at concurrency 64 and ~130 rows/s at concurrency 256
  (VECTOR-951); the backlog drains at 3–4K rows/s once the searches stop.
- **Index rebuilds.** The initial build of cohere-1m took 93 s; a rebuild
  after a Vector Store restart runs at ~6K rows/s (3.2M rows in 528 s).
- **Fresh readers start 10 minutes behind.** Every (re)created index starts
  both CDC readers from `now - 10 min` (`CHECKPOINT_TIMESTAMP_OFFSET`), so
  their lag begins near 600 s. Measured on a 100k-row table under a
  500 rows/s insert stream (2026-10-07, 1.11.0): the fine reader is within
  its idle lag in under a minute, the wide reader (10 s sleep between
  windows) in 2–3 minutes (375 s behind 75 s after a recreate). A lag right
  after `bench load`, `bench index` or a `deploy vs` restart is catch-up,
  not a stall; `status` says `behind` until it is gone. The catch-up
  depends on the rows written in those 10 minutes, not on the table size:
  on a production cluster with ~66M-row tables (CUSTOMER-765, four index
  creations) the wide reader was above the Scylla Cloud alert threshold
  (300 s) for at most one 5-minute sample, so the rules' `for 10m` absorbs
  it and a reader still 10 minutes behind after that is genuinely behind.
- **Dropped indexes.** Vector Store 1.9.0+ removes a dropped index's four
  CDC series and its index series within a second of `removed the index`
  (checked on 1.11.0: 11 drops, idle and under traffic, during bootstrap
  and drop+recreate back to back); the readers log `finished` at debug
  level (`--env RUST_LOG=info,vector_store::db_cdc=debug`).
Raw endpoints on the nodes:
- `127.0.0.1:9100/metrics` on every node;
- `<ip>:9180/metrics` on Scylla nodes;
- `127.0.0.1:6080/metrics` on VS nodes.

## Datasets

- **Source.** VectorDBBench public datasets at
  `https://assets.zilliz.com/benchmark/<dir>/`, an S3 bucket in us-west-2
  behind CloudFront.
- **Catalog.** Only datasets that work with the benchmark tool as they are:
  `cohere_small_100k`, `cohere_medium_1m`, `cohere_large_10m`,
  `openai_small_50k` and `openai_medium_500k`. All use COSINE.
- **Train file trap.** The tool loads every file whose name contains `train`,
  so `shuffle_train*` is never downloaded next to `train*`. That would upload
  every vector twice.
- **Not in the catalog yet:**
  - `openai_large_5m`, `bioasq_*` and the HF multimodal sets: their
    test/neighbors files use 32-bit `list` columns, which the tool rejects
    (`panicked … list array`). The follow-up is to make
    `crates/benchmark/src/data/parquet.rs` accept both offset widths.
  - big-ann fbin sets: they work with the tool, but need byte-range downloads
    and a header patch. Add them when a run needs them.
  - `gist`, `glove` and `sift`: they have no `neighbors.parquet`, so recall
    cannot be measured.
- **Memory.** cohere-10m is about 31 GB raw f32 and fits an r8g.4xlarge
  (128 GiB). LAION-100M does not fit without I8/B1 quantization.

## Benchmark tool caveats

`vector-search-benchmark` (`crates/benchmark`) behaviour that `vsbench` works around:

- **Histogram limits.**
  - The histogram covers 1–100 ms. Everything below is printed as `1.0ms`,
    everything above as `18446744073709551616.0s`.
  - Min and max are exact.
  - On a fast cluster p50 is often floored, so `vsbench` leads with the exact
    closed-loop mean and tags percentiles.
- **Panics.**
  - Any CQL or HTTP error in a search task aborts the run with exit 101 and
    no results.
  - `--bucket` on a global index panics.
  - A local index without `--bucket` fails.
  - `search-http` on a local index panics.
  - `vsbench` validates these combinations before starting.
- **Timeouts.** A CQL timeout (10 s) is logged and counted as recall 0.
  `vsbench` counts them.
- **`build-index` polls forever.** `vsbench` runs it under `timeout` and with a
  unique index name. Right after `drop-index`, a new index with the same name
  can look "ready" while the old one is still listed.
- **Similarity function.** It must match the ground-truth metric. A mismatch
  gives silently low recall (63% for EUCLIDEAN ground truth on a COSINE index).
- **No recall for `search-http`**, although the API returns primary keys.
- **ANSI colours** appear even when output is redirected; jobs set `NO_COLOR=1`.
- **Buckets.** `build-table` assigns bucket 255 when `buckets.bin` is missing.
  `--local-index` loads therefore run `build-buckets` first.

## Measurement methodology

- **Warmup.** The tool has no warmup, so `vsbench` runs a separate, discarded
  run with the same arguments before every measured run. That covers cold
  caches after a build switch, the Scylla row cache and the allocator. Client
  and server numbers then describe the same window.
- **Window.** The window is the measured run's own log timestamps, from
  `Starting search … tasks` to `Gathering measurements`. They come from the
  client node's clock, which also stamps Prometheus samples. Server metrics
  are deltas of raw samples inside the window, fetched one scrape interval
  after the end. Windows under 30 s get no server metrics.
- **Closed-loop mean.** With the tool's `--delay`, this quantity is the cycle
  time (request plus pause) and is recorded as `cycle_ms`, never as `mean_ms`.
  `mean_ms = concurrency × duration / queries` is exact
  and comparable across builds. Server mean is `Δsum/Δcount` of
  `request_latency_seconds`.
- **Repeats and order.** `--repeat` plus the ABBA order of `bench ab` turn
  noise and drift into something measurable. `results compare` reports
  median, min–max and CV, and calls a delta `within-noise` when the ranges
  overlap.
- **Index rebuilds.** Every Vector Store restart rebuilds its indexes from
  Scylla, and that time is a result in its own right. A usearch upgrade
  alone shifted it by about 35% (VECTOR-946).
- **Drift across repeats.** A series that drifts on one build and is flat
  on the next is inconclusive: it neither shows a build effect nor rules
  one out. On 2026-10-08 the CQL series of the baseline fell 19.1k → 17.8k
  → 16.8k QPS with nothing on either node to show for it (no compaction,
  no client network-allowance event, both CPUs falling), while the next
  build's series stayed flat and its first repeat matched the baseline's
  within 1%. Before attributing such a drift to either side, repeat the
  comparison with the same setup, workload, order (`bench ab`) and repeat
  count, and compare the repeats' medians, ranges and CV
  (`results compare`), not single runs.

## Detached jobs

- **How they run.** Load, search and fetch run on the client as
  `systemd-run --unit vsbench-job-<id>` jobs, with output in
  `/var/lib/vsbench/jobs/<id>/{log,exit_code,started_at,ended_at}`.
- **Steps.** A multi-step job is a step script; each step is bracketed by
  `=== VSBENCH STEP BEGIN/END` markers, which the finalizer splits on.
- **Finalizing.** Finalizing a job (parse, query Prometheus, append to
  `results.jsonl`) is idempotent and keyed by run ID. A finalize hours later
  works because Prometheus keeps 30 days.
- **Limits.** Jobs run with `LimitNOFILE=1048576`, because `search-http` opens
  one socket per in-flight request.
- **Churn (`bench churn`).** The insert stream is a job of kind `churn` that
  the command follows without holding the cluster lock, and `_ensure_idle`
  skips a running churn job, so `bench search` can start meanwhile; a second
  churn is refused while one runs (its ids continue from
  `2^40 + load.churn_rows`, so two at once would collide). The finalizer
  reads the tool's summary lines (rows issued/acked/failed, last id, insert
  rate) into the record and adds the acked rows to `load.churn_rows` unless
  the index changed under it; `status` and the `churned` search flag use
  that count.
- **Profiles (`--perf NODE`).** When the followed log shows a measured step
  beginning, `vsbench` starts `node/profile-step.sh` on each listed node as
  its own detached job. The script waits 4 s (job start latency plus the
  tool's 2 s start delay), then records `perf record -g` at 199 Hz and
  `pidstat` for the step's duration minus 10 s, so the capture stays inside
  the measured window, and renders `perf report` by symbol and by DSO. On a
  Scylla node the container's main process is profiled with `--namespaces`,
  so its binaries resolve through `/proc/<pid>/root`. At finalize the three
  text reports are pulled into `results/artifacts/profiles/<run_id>/<node>/`;
  a capture still rendering after 45 s is recorded as an error with the job
  id, so it can be pulled by hand. `perf` and `sysstat` come from user-data
  on scylla and vs nodes (best effort: a kernel without a `linux-tools`
  package only loses this feature).
- **Foreground limit.** It exits 75 just below the agent's 10-minute Bash
  limit (the default limit is 2 minutes, hence background runs).
  - The budget covers the whole command, including builds and uploads, and
    keeps about a minute for recording results.
  - `deploy vs` exits 75 the same way while indexes rebuild; continue with
    `vsbench wait-serving`.

## Upgrading pins

All pins live at the top of `vsbenchlib/config.py`. When bumping:

- **scylla-monitoring.**
  - Update the version and the tarball sha256
    (`curl -sL https://github.com/scylladb/scylla-monitoring/archive/refs/tags/<v>.tar.gz | sha256sum`).
  - Check that the two node_exporter interval lines still exist in
    `prometheus/prometheus.yml.template`.
  - Check that `--vector-search` and the two jobs it generates are unchanged,
    and that the Vector Search dashboard loads.
  - Check that all images still publish arm64.
- **node_exporter.** Update the version and the arm64 sha256 from the
  release's `sha256sums.txt`. Check the collector flags in
  `node/userdata.sh`.
- **Ubuntu AMI.** The SSM path selects the release. Moving to 26.04 needs a
  fresh check of the user-data: apt, docker.io and cloud-init exit codes.
- **Rust toolchain.** The cross image tag follows the built tree's
  `rust-toolchain.toml`, so nothing needs pinning. A new channel builds a new
  image on first use.
- **io_properties.** These come from `scylla-machine-image`
  `common/aws_io_params.yaml`. Only single-disk types are used.
- **Prices.** Approximate, for the estimate only.
- **packer.** `packer/files` installs node_exporter 1.9.1; aligning it is
  separate work.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| exit 3, `ExpiredToken` | credentials expired: start the Okta device flow with `vsbench login` (SKILL.md rule 6) |
| `login` exits 3 with `400 Client Error … /oauth2/v1/token` right after the approval | Okta refused gimme-aws-creds' web-SSO token exchange for the AWS app: `invalid_grant, "The application's assurance requirements are not met by the 'subject_token'"`. The device flow succeeded, but the approval carried only Okta Verify push + password (`amr=[swk,mfa,pwd]`) and the AWS app's Okta authentication policy wants the phishing-resistant factor. **Fix (2026-10-08):** the user signs in to `scylladb.okta.com` in the browser first, so that Okta asks for biometrics / Okta FastPass (the activate page alone never asks), then approves the next device code **in that browser**; the login then succeeds at once. Approving from a cold browser session fails every time, and new codes do not help, so stop after one such 400. Off/on VPN makes no difference. gimme-aws-creds does not print Okta's error body; a 20-line script that calls `OktaIdentityEngine.auth_session()` and `_web_sso_token_exchange()` and prints only `error`/`error_description` shows it. |
| exit 4 in `up` | no capacity in any AZ: other instance types or region |
| `up` fails with a userdata error | `vsbench` printed the log tail and rolled back. Typical causes: GitHub download outage, apt mirror trouble |
| SSH timeouts | operator IP changed: `vsbench refresh-ip` |
| `permission denied … docker.sock` | use `sudo docker` |
| Scylla does not start, "Bad I/O Scheduler configuration" | no io properties and iotune failed; check `vsbench logs scylla-0 --service scylla` |
| Vector Store restarts in a loop | `vsbench logs vs-0 --service vector-store`; look for `Memory usage above limit` |
| `bench search` refuses: index not SERVING | wait: `vsbench wait-serving`; after `deploy vs` the index is rebuilt |
| recall unexpectedly low | similarity function mismatch, or the index is still building |
| all percentiles `1.0ms` | tool floor; use `mean_ms` and server metrics |
| QPS identical for two builds | check `client_saturated` / `plateau`; compare below the knee |
| `deploy monitoring` fails "template changed" | upstream scylla-monitoring changed; see [Upgrading pins](#upgrading-pins) |

## Production data

When the skill is used to debug a **Scylla Cloud production cluster** (a
CUSTOMER ticket names one, e.g. `45228`), its Vector Store logs and metrics
are already collected centrally. Both stores answer plain HTTPS over the
ScyllaDB VPN, so the first step is a probe, not a reproduction:

```
curl -s -o /dev/null -w '%{http_code}' https://vmui.app.int.scylla.cloud/flags   # 200 = VPN is up
```

Everything below is internal data about customers: keep it in internal
Jira/Slack, never in a public issue, PR or chat.

### Logs: VictoriaLogs

- Endpoint: `https://vmui.app.int.scylla.cloud/select/logsql/query` with
  form fields `query`, `start`, `end` (RFC 3339 or Unix), `limit`; the
  response is one JSON object per line. `/select/logsql/field_names` lists
  the fields. Infra logs (OS, k8s): `https://vmui-infra.app.int.scylla.cloud`.
- Always filter by `cluster:<id>` (an unfiltered query is expensive). Useful
  fields: `_msg`, `_time`, `cluster`, `private_ip`, `hostname`, `level`,
  `service_name`, `node_type`, `systemd_unit`.
- LogsQL matches **whole tokens**: `_msg:"removed the index"` works, a
  substring of an index name does not; use the full
  `_msg:audio_vectors_without_isrc_v1_f16_embedding_idx`.
- Queries that answered CUSTOMER-765:
  - `cluster:45228 _msg:"removed the index"` — every DROP INDEX seen by the
    Vector Store nodes, with the node IP;
  - `cluster:45228 private_ip:10.0.128.244 _msg:"Starting vector-store"` —
    the running version (`Starting vector-store version 1.9.1`);
  - `cluster:45228 (_msg:"CDC handler error" OR _msg:"restarting after" OR
    _msg:"Session became None" OR _msg:"Session available" OR
    _msg:"Failed to create")` — the CDC readers' lifecycle;
  - `cluster:45228 private_ip:<ip> (level:warn OR level:error)` with a
    one-hour window around an alert's start.

### Metrics: Thanos

- Endpoint: `https://thanos.app.int.scylla.cloud/api/v1/query`,
  `/query_range` (`start`, `end`, `step`) and `/series` (`match[]`),
  the Prometheus HTTP API.
- Vector Store series carry `job="vector_search"`, `cluster="#45228"`
  (**with the hash**), `cluster_name`, `instance=<private ip>`, `keyspace`,
  `index_name`, `ks_index`, `reader` (`fine`/`wide`), `rack`, `serverId`.
  The metric names are the ones in [Metrics](#metrics).
- Queries that answered CUSTOMER-765:
  - `cdc_last_processed_timestamp_seconds{cluster="#45228",index_name="<dropped>"}`
    next to `cdc_reader_up{...}` for the same index — a watermark series
    without an `up` series is a ghost left by orphaned readers;
  - `time() - cdc_last_processed_timestamp_seconds{cluster="#45228"}` — the
    lag of every reader on every node (idle: fine ~15–30 s, wide ~55–70 s);
  - `max_over_time(cdc_reader_restarts_total{cluster="#45228"}[30d])` and
    the same for `cdc_handler_errors_total` — whether readers ever restarted;
  - `sum by (index_name,instance,operation)(rate(index_modified{cluster="#45228",keyspace="<ks>"}[1h]))`
    as a range query over an episode, and `index_size{...}` per `instance`:
    the Vector Store nodes index the same table independently, so a node
    whose rates and size diverge from the others has a stalled reader and a
    stale index — this is the freshness evidence.
- The Scylla Cloud alert rules for Vector Store (`VSCdcReaderDown`,
  `VSCdcReaderStalledFine` > 60 s, `VSCdcReaderStalledWide` > 300 s, all
  `for 10m`) are in `scylladb/siren`,
  `db/schema/00713_add_vs_cdc_reader_alerts.sql`.

### The TSE plugin

`scylla-customer-support@scylladb-technical-support` (Confluence page
431292442, "[TSE] scylla-customer-support Plugin — Installation Guide")
adds the Support team's workflows (ticket triage, log analysis, metrics
interpretation, `cx`/`sc` CLI, Alertmanager) and three MCP servers;
VictoriaLogs runs as two local stdio servers (`victorialogs_clusters`,
`victorialogs_infra`, binary `mcp-victorialogs`), and `cx-sre-tooling` /
`thanos-prod` come from the SRE-Tooling repo. Install it with
`claude plugin marketplace add https://github.com/scylladb/technical-support.git --sparse .claude-plugin plugins/scylla-customer-support`
and `claude plugin install scylla-customer-support@scylladb-technical-support`;
the skills load in the next session. Three things the guide got wrong on
2026-10-07: the mcp-victorialogs release asset is `*Linux_x86_64*`,
`go install …@latest` fails on the module's replace directives (use the
release tarball), and `claude mcp add` wants `<name>` before the `-e`
options. The `cx` CLI (StrongDM) is provisioned per engineer; without it the
`cx_*` tools answer "cx runtime not found", which is expected.

## Follow-ups

These belong in separate PRs, not in the skill:

- `benchmark:` HDR histogram (1 µs–60 s), `--warmup`, `--output-json`, recall
  for `search-http`, counting errors instead of panicking, a refusal of
  `--from` in the past, and accepting both list widths in parquet. `vsbench`
  would then read JSON instead of parsing logs.
- `scripts:` retire `scripts/cluster-in-aws` once this skill has replaced it
  in practice (VECTOR-237).
- VECTOR-433: promote `cross/cross-build.sh` to `scripts/` for general arm64
  cross builds.
- latte and VectorDBBench clients on the client node (VECTOR-694 moves the CQL
  benchmark path to latte).
