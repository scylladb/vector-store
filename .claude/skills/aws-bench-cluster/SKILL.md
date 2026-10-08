---
name: aws-bench-cluster
description: Provision and drive an AWS benchmark cluster for ScyllaDB Vector Store — Scylla nodes (i8g), Vector Store nodes (r8g) and a client node running scylla-monitoring and the repo's vector-search-benchmark — deploy Vector Store from a Docker Hub release, an upstream git ref or the local working tree (cross-compiled locally for arm64), load VectorDBBench datasets, run and compare benchmarks, query Prometheus and node_exporter, run commands on the nodes, and tear everything down. Use it whenever the user wants to benchmark or performance-test Vector Store or ScyllaDB vector search on AWS, compare Vector Store builds, branches or index options, check the performance impact of local changes, set up or tear down a benchmark cluster, or investigate performance on such a cluster — even if they do not name the skill.
---

# AWS benchmark cluster

Every step goes through one CLI, `vsbench`, which lives in this directory. It
keeps per-cluster state on the operator's machine, talks to AWS with the `aws`
CLI and to the nodes over SSH, and records every benchmark with the metadata
needed to compare it later.

**[reference.md](reference.md) carries the rationale**: why the cluster looks
the way it does, the metric catalog, benchmark-tool caveats and
troubleshooting. Read the matching section before overriding a rule.

## Configuration

| Setting | Value |
|---|---|
| CLI | `.claude/skills/aws-bench-cluster/vsbench` (run from the repo root; not on `PATH`) |
| AWS account / role | rnd-core-lab `797456418907` / `DeveloperAccessRole`, profile `797456418907-DeveloperAccessRole` |
| Region | `us-east-1`, all nodes in one AZ |
| Default shape | 1 × `i8g.2xlarge` Scylla, 1 × `r8g.4xlarge` Vector Store, 1 × `r8g.2xlarge` client: about $2.10/h |
| Tags | `Owner` and `RunByUser` = e-mail prefix, `keep=alive`, `billing_project` (default `Vector Search: Sharding`), `ExpiresAt`, `VsBench*` |
| TTL | default 24 h, at most 7 d per `up`/`extend`. It is enforced on the nodes themselves: at expiry they power off and AWS **terminates** them |
| Local state | `~/.local/state/vsbench/` (`VSBENCH_HOME`): `clusters/<name>/`, `builds/`, `history.jsonl`, `retrospectives/` |
| Cluster name | `-c NAME`, else `$VSBENCH_CLUSTER`, else `default` |

Below, `vsbench` means `.claude/skills/aws-bench-cluster/vsbench`.
`vsbench <command> --help` shows every flag with its default.

## Rules

1. **Ask before spending or destroying.**
   - Show the user the `up --dry-run` plan and get a yes before `up`. The plan
     covers profile, account, region, shape, cost per hour, TTL and billing
     project.
   - `extend` is spending too: agree on the new TTL first.
   - Never run `down --yes` unless the user agreed in this conversation.
2. **Never power off a node or disarm its TTL.**
   - `shutdown`, `poweroff`, `halt` and the like *terminate* the instance,
     losing the instance-store data. `reboot` is safe.
   - Stopping `vsbench-ttl.timer` or editing `/etc/vsbench/expires_at` leaves
     a cluster nothing will ever stop. Use `vsbench extend`.
   - `vsbench exec` and `ssh` refuse such commands.
3. **One mutating command at a time.**
   - `vsbench` takes a per-cluster lock and refuses a second mutating command.
   - `extend`, `refresh-ip`, `job cancel`, `status` and the read-only commands
     still work alongside a running command.
   - `bench churn` is the exception: it takes no lock and the other bench
     commands ignore its running job, so an insert stream can run under a
     search (see the Recipes).
4. **Use `sudo docker`** in ad-hoc commands on nodes.
5. **Do not open ports.** Only SSH from the operator's IP is allowed. Grafana and
   Prometheus are reachable through the SSH tunnel line that `vsbench status`
   prints. It blocks until Ctrl-C, so the user runs it in their own terminal.
6. **Credentials are the user's.** When `vsbench` exits with code 3, you
   cannot log in for the user, but you can start the Okta device flow for them.
   **The code expires about 2 minutes after it is issued**, so first ask the
   user (one question) whether they are ready to approve it, and only then:
   1. Run `vsbench login` with `run_in_background: true` (`--username` if the
      git `user.email` of the checkout is not the user's Okta login).
   2. Within seconds its stdout holds one line, the
      `https://scylladb.okta.com/activate?user_code=…` URL. Read **that
      attempt's** output file (an older attempt's code is dead) and give the
      user the URL as your very next message, with the deadline the command
      prints. Do nothing else first.
   3. It exits 0 once the user approves and reports the new expiry; exit 3
      means the code expired or the approval did not come in time: ask
      again whether the user is ready, then run it again.
   4. Exit 3 with `400 Client Error … /oauth2/v1/token` right after the
      approval is an Okta policy refusal, not a timing problem: do not issue
      another code. Ask the user to sign in to `scylladb.okta.com` in the
      browser first (Okta then asks for the biometric / FastPass factor that
      the activate page alone does not) and to approve the next code in that
      same browser (reference.md, troubleshooting).

   The `!` prefix does not run commands in every Claude Code front end (the VS
   Code extension sends it as a message), so do not rely on it.
   - `vsbench` finds the profile itself: the freshest
     `797456418907-…DeveloperAccessRole` section in `~/.aws/credentials`. Its
     name depends on the user's `include_path` setting.
   - Credentials last 6 hours.
   - Only AWS-touching commands need them: `up`, `down`, `list`, `refresh-ip`,
     `status --refresh` and the tag part of `extend`.
7. **Write notes as things happen.** Run `vsbench note --kind <workaround|surprise|user-correction|idea|time-sink> "<text>"`
   the moment it happens. The retrospective depends on these notes, and your
   memory of the session may be compacted.
8. **Compare like with like.** `vsbench results compare` refuses runs whose
   fairness fields differ:
   - dataset and load run;
   - index options, rf and local/global;
   - limit, duration, warmup, bucket and extra bench arguments;
   - bench build, Scylla image and Vector Store env;
   - instance types and **instance IDs** per role.

   So a comparison only works within one cluster and one load. The error names
   the differing fields. `--force` compares anyway and labels each group by the
   field that differs: use it for deliberate sweeps (index options, Scylla
   image) and say so in the report.
9. **Benchmark-tool edits are experiments.** Changes to `crates/benchmark`
   deployed with `deploy bench --source local` stay local. Anything worth
   keeping goes to its own `benchmark:` PR, never into a skill PR.

## Long-running commands and exit codes

The Bash tool's default timeout is 2 minutes, and `timeout: 600000` raises it
to the maximum of 10. Run every command below with `run_in_background: true`
and wait for the completion notification. Do not poll in the meantime.

| Command | Typical duration |
|---|---|
| `up`, `down` | 5–15 min each |
| `build`, `deploy vs`/`deploy bench` with `git:…`/`local` sources | 1–6 min per build (cross-compilation) |
| `deploy scylla`, `deploy monitoring`, `deploy all` | 3–15 min (image pulls, Scylla start per node, CQL and UN waits) |
| `deploy vs` while an index is loaded | until every node has rebuilt the index from Scylla (minutes for 1M, much longer for 10M) |
| `dataset fetch`, `bench load` | minutes (cohere-1m) to an hour (cohere-10m) |
| `bench search` sweeps, `bench index` | (warmup + duration) × points × repeats; the index build |
| `bench ab` | several builds, index rebuilds and searches: often an hour or more |
| `collect` | minutes (TSDB snapshot download) |

Benchmark work (`dataset fetch`, `bench load|index|search`) runs as a
**detached job** on the client node. It survives a dropped SSH session or a
killed `vsbench`. Commands keep their own foreground wait just under 10
minutes, then exit **75**.

| Exit | Meaning | What to do |
|---|---|---|
| 0 | done | |
| 1 | error | read `error:` and `hint:` |
| 2 | usage | fix the arguments (`--help`) |
| 3 | AWS credentials missing or expired | start the Okta device flow with `vsbench login` (rule 6) |
| 4 | AWS has no capacity | `up` already tried every AZ; offer other instance types or a region |
| 5 | precondition failed (busy cluster, index not serving, invalid flag combination, wrong account) | follow the hint |
| 75 | still running | a job: `vsbench job wait <id>` (records the results when it ends); `deploy vs`/`deploy all`/`wait-serving`: `vsbench wait-serving` |

## Procedure

### 0. Start

1. Record the session start: `date -u +%Y-%m-%dT%H:%M:%SZ`. The retrospective uses it.
2. `vsbench doctor`. Install missing tools as it suggests. With expired
   credentials, ask the user to log in (rule 6).
3. `vsbench list` shows the user's clusters in every region that has local
   state. Reuse a running cluster (`-c NAME`) when it fits.
   - **OVERDUE** means it is running past its expiry: the TTL failed. Before
     offering `down`, confirm with `vsbench -c NAME status` or
     `vsbench -c NAME exec all -- cat /etc/vsbench/expires_at`.
   - `tag stale` means a later `extend` could not update the AWS tag. The
     nodes have the newer expiry, so this is not a problem.

### 1. Plan and provision

1. Agree on the shape with the user and show the plan:
   `vsbench up --dry-run [--scylla-nodes N] [--vs-nodes M] [--scylla-type T] [--vs-type T] [--client-type T] [--ttl 24h] [--billing-project "…"]`.
   - It prints profile, account, region, nodes, AZ candidates, TTL and cost per
     hour.
   - It warns above the $10/h personal budget (counting the owner's other
     running clusters) and the $20/h project budget.
   - It warns when a price or the billing project is unknown.
   - It refuses an account other than rnd-core-lab unless `--profile` is
     passed explicitly.
2. After the user agrees, run `vsbench up …` with the same flags (background).
   - It creates a security group, a key pair and the instances.
   - It waits for each node's bootstrap: Docker, node_exporter, NVMe/XFS on
     Scylla nodes, and the TTL timer.
   - On any failure it terminates what it launched (`--keep-on-failure` keeps
     it for debugging). It falls back to the next AZ on capacity errors.
3. Multi-node: each Scylla node is its own rack, so RF must be 1 or the number
   of Scylla nodes.

### 2. Deploy

`vsbench deploy all` deploys whatever is not deployed yet, in order: Scylla,
monitoring, the benchmark binary, Vector Store. Vector Store goes last because
its SERVING wait may exit 75; then run `vsbench wait-serving`.
`deploy all --force` redeploys every component with its pinned source.

| Component | Default | Other choices |
|---|---|---|
| Scylla | `--image nightly` (digest of `scylla-nightly:latest`, as CI uses) | `release:<ver>` (`scylladb/scylla:<ver>`, 2025.4+), any Docker Hub image ref |
| Vector Store | `--source git:master` (upstream master, built locally) | `release:<ver>`, `release:latest` (Docker Hub; latest = newest GitHub release), `git:<ref>`, `local`, `local:+<label>`, `build:<id>` |
| Benchmark | `--source git:master` | `git:<ref>`, `local`, `build:<id>` |

Components deploy separately with their own flags:
- `deploy scylla [--image] [--wipe]`
- `deploy vs [--source] [--env K=V] [--unset K]`
- `deploy bench [--source]`
- `deploy monitoring`

Things to know:
- **Sources are pinned.** Without `--source` or `--image`, a redeploy reuses the
  resolved value (`git:<sha>`, `release:<ver>`, image digest), so nothing moves
  under a running comparison. Pass a floating ref explicitly, or `--refresh`, to
  move it. `deploy` prints every change and says `unchanged` otherwise.
- **Local builds** are cross-compiled for aarch64 in a Debian bookworm
  container that matches the release toolchain.
  - They are cached in `~/.local/state/vsbench/builds/<build_id>/` and
    verified: architecture, glibc, SIMD kernels, version.
  - The version comes from `git describe --dirty`. Uncommitted changes add a
    content hash; edits under `.claude/` do not count.
  - Add a label to tell experiments apart: `local:+prefetch`.
- **Builds stay on the nodes**, so switching back with `--source build:<id>`
  needs no upload.
- **A restart rebuilds every index from Scylla.** `deploy vs` then waits for
  SERVING and records the rebuild as an `index-build` result.
- `vsbench builds [--nodes]` lists builds locally and on the nodes.
- `--wipe` drops all Scylla data, and with it the loaded dataset.

Check with `vsbench status`. It shows:
- versions per node and the Scylla `nodetool` summary;
- Vector Store status and indexes;
- monitoring targets;
- `index (live)`: `index_size` against the rows vsbench put into the base
  table (`base_rows` = the load's rows plus churn), the rows `missing` from
  the index, the fine and wide CDC reader lags, and a verdict. Idle lags are
  about 10 s (fine) and 46 s (wide); `behind` means rows are missing or a
  lag is over three times its idle value, i.e. ingest has stalled or is
  catching up;
- TTL left;
- the SSH tunnel line for Grafana (`http://127.0.0.1:13000`, dashboard
  "Vector Search") and Prometheus (`http://127.0.0.1:19090`).

### 3. Load data

`vsbench bench load cohere-1m` (background).
1. It downloads the dataset to the client if needed.
2. It recreates the keyspace and uploads the vectors.
3. It builds an index with a unique name.
4. It checks that the effective index options match the requested ones, and
   records upload and index-build times.

- `vsbench dataset list` shows the catalog: `cohere-100k` (smoke test),
  `cohere-1m` (default), `cohere-10m`, `openai-50k`, `openai-500k`.
- Index options are a CQL map:
  `--index-options "{'similarity_function': 'COSINE', 'maximum_node_connections': 16, 'construction_beam_width': 128, 'search_beam_width': 64}"`.
  - The similarity function defaults to the dataset's ground-truth metric.
  - Vector Store silently falls back to defaults for invalid options. On a
    mismatch, `load` fails and searches are refused until `bench index`
    rebuilds with valid options.
- `--local-index` builds a local index on the `bucket` partition (filtering
  tests). Searches then need `--bucket N`.
- After an interruption, `vsbench bench load cohere-1m --resume` continues from
  the first incomplete phase.

### 4. Measure

`vsbench bench search {cql|http} [--concurrency 16,64,128] [--duration 60s]
[--warmup 30s] [--repeat 3] [--limit 10] [--label L] [-- EXTRA_ARGS]`.

- **Runs.** Every measured run is preceded by its own warmup run, with the
  same arguments, which is discarded. All runs of one invocation share a
  `series_id`.
- **Refusals.** `vsbench bench validate {cql|http} …` checks a search without
  running it. A search is refused when:
  - the index is not SERVING on every Vector Store node;
  - the flags do not fit the index (bucket vs local index);
  - the run would outlast the TTL.
- **cql vs http.** `cql` goes through Scylla and measures recall. `http` hits
  Vector Store directly and has **no recall**, so pair HTTP comparisons with a
  CQL run.
- **The client's network comes first.** CQL searches of 768-d vectors exceed
  the `r8g.2xlarge` client's ENA allowance at about 8–12K QPS
  (`net_allowance_exceeded`), while the Vector Store is at 55–65% CPU. For
  search-pressure experiments (saturating Vector Store on purpose) use
  `http`, a bigger `--client-type`, or a second client; a flagged run
  measures the client.

The summary has one row per run:
- `run_id`, `conc`, client `qps`;
- `mean_ms`, the closed-loop client mean;
- client `p50`/`p99`;
- `recall`;
- `vs_mean_ms`/`vs_p99`, server-side from Prometheus;
- `exit`, `flags`.

Tagged latencies read `<=1.00ms` (floored), `>100ms` (capped) or `~0.73ms`
(interpolated).

For details on specific runs use `vsbench results --series <id> --json` or
`results/<run_id>.log` in the cluster dir. Avoid a bare `results --json`,
which prints 20 full records.

### 5. Report

Use the team's Jira-comment format. The table comes from
`vsbench results compare … --format md`:

```markdown
**Scylla version:** <deployed.scylla.version> (`<image digest>`)
**Vector store version:** <version> (`<build_id>`, source `<source>`)
**Configuration:** <client type> client + <N>× <scylla type> Scylla + <M>× <vs type> Vector Store, RF=<rf>, <region/az>
**Parameters:** dataset <key>, index options <effective options>, k=<limit>, duration <d>, warmup <w>, concurrency <c>, repeats <n>

<table from results compare>

Notes: <flags and what they mean for the result; anything from the retrospective worth knowing>
```

### 6. Retrospective (mandatory, every session)

Do this at the end of **every** session that used this skill, whether the
cluster is kept or not. If you are going to tear down, do it **before** `down`.

1. Run `vsbench retro --since <session start>` (an ISO time, or e.g. `6h`). It
   prints a digest:
   - failures grouped by error;
   - commands that were killed or interrupted (no result recorded);
   - retries;
   - `exec`/`ssh` calls (each a candidate for a new subcommand);
   - the slowest steps;
   - result-quality flags;
   - your notes;
   - proposals from earlier retrospectives that recur.

   `vsbench history --since 6h [--failed]` lists the commands with their status:
   `ok|failed|interrupted|killed|running|detached`.
2. Write `~/.local/state/vsbench/retrospectives/<utc-ts>-<cluster>.md` with
   these sections:
   - `## What happened`: goal, outcome, wall time per phase.
   - `## What went wrong`: each failure with its root cause and evidence (a
     history line or note).
   - `## What we learned`: facts about the cluster, the tools or the system
     under test.
   - `## Proposals`: one `- ` bullet per proposal. `retro` finds earlier
     proposals by this exact format.
3. Keep a proposal only if it would have prevented a failure or saved time in
   a run with **different** parameters. It must cite its evidence and say
   whether an earlier retrospective already had it. Classify it as one of:
   - **Doc**: an edit to SKILL.md or reference.md. Show the diff.
   - **Code**: a change to `vsbench`, with a unit test. Run both of these
     before showing the diff:
     - `python3 -m unittest discover -s .claude/skills/aws-bench-cluster/tests -t .claude/skills/aws-bench-cluster`
     - `shellcheck .claude/skills/aws-bench-cluster/node/*.sh .claude/skills/aws-bench-cluster/cross/*.sh`
   - **Out of scope**: a benchmark-tool, Vector Store or Scylla problem. Draft
     a Jira issue or a follow-up note; this is not a skill edit. When the
     session involves a Scylla Cloud cluster, check the hypothesis against
     its production data first (VictoriaLogs and Thanos over the VPN,
     [reference.md](reference.md#production-data)) before filing a Jira
     issue or stating it on a customer ticket; a hypothesis the data does
     not support stays in the retrospective as a learning. (VECTOR-1032 was
     filed from a 100k-row test and refuted by one Thanos range query.)

   Run-specific workarounds go into the notes, not into proposals.
4. Present the proposals to the user. Apply nothing without approval. An
   approved improvement lands as its own small PR.

### 7. Keep or tear down

- **Keep.** `vsbench extend --ttl 12h` counts from now; `--until <ISO with Z>`
  sets an absolute time. The new expiry is capped at 7 days ahead, and the TTL
  is never shortened unless `--shorten` is passed. `extend` updates the nodes
  first, then the AWS tag; without credentials it updates only the nodes.
- **Collect.** Before teardown, `vsbench collect` (background) saves a
  Prometheus TSDB snapshot, the Scylla and Vector Store logs, and the job logs
  to `results/artifacts/`.
- **Tear down.** Ask the user, then run `vsbench down --yes` (background).
  - It terminates by tags and deletes the security group and the key pair.
  - It keeps local results; `--purge` deletes them.
  - History and retrospectives are never deleted.

## Recipes

**Find the saturation knee.**
`bench search cql --concurrency 1,8,16,32,64,128,256`. QPS stops growing at the
knee.
- `plateau` marks it.
- `client_saturated` means the client, not the cluster, is the bottleneck.

Compare builds at one or two concurrencies below the knee.

**Compare two Vector Store builds.**
`vsbench bench ab --a release:latest --b local --kind cql --concurrency 64 --repeat 2`
(background).
- Both sources are built or resolved once up front.
- The arms run in ABBA order. Each switch deploys the build, waits for SERVING
  (recording the rebuild time), warms up, then measures.
- All records share a `comparison_id`; compare them with
  `vsbench results compare <comparison_id> --format md`. That shows medians,
  min–max, CV and delta% with a `within-noise` verdict.
- `--bucket N` and `-- EXTRA_ARGS` work as in `bench search`. Pair HTTP
  comparisons with a CQL one for recall.
- **`ab` cannot be resumed.** After an exit 75 or a failure, the records made
  so far keep the comparison ID; start it again for a complete set.
- **It leaves the last arm deployed**: arm A with an even `--repeat`. Check
  `vsbench status` and redeploy the build you want before further searches.

**Sweep index options without reloading.**
`vsbench bench index --index-options "{…}"`:
- it drops the index and waits until it is gone from every node;
- it builds one with the new options, recording the build time and the
  effective options.

Then search as usual and compare with `results compare --force`. The groups
are labelled by the differing option.

**Test local changes.** Edit the code, then run
`vsbench deploy vs --source local:+<label>`. Switch back with
`--source build:<id>`, taking the ID from `vsbench builds --nodes`. Use
`bench ab` for the actual comparison.

**Measure ingest under query load (churn).**
`vsbench bench churn --rate 1250 --duration 3m` (background) inserts random
vectors into the loaded table at that rate (`--rate 0`: as fast as
`--concurrency` allows) as a detached job that holds no lock; start a
`bench search` in another background command while it runs. Then:
- `vsbench status` shows `index (live)`: the rows `missing` from the index
  and the CDC lags, so a stall is visible while it happens;
- the churn record (`vsbench results --last 3`) has the acked rows and the
  achieved rate; searches made meanwhile carry the `churned` flag;
- the acked rows accumulate in `state.load.churn_rows`, and churn ids start
  at 2^40, so they never collide with dataset ids.
It needs a benchmark build with `insert-rows` (VECTOR-1030): until it is on
master, `deploy bench --source git:<ref>` or `--source local`. On the default
shape Scylla takes ~60K such inserts/s and Vector Store ingests ~5.1K rows/s
with no searches running (reference.md, CDC baselines), so size runs by
rows: `--rate 0 --duration 20s` adds over a million rows.

**Change or debug the benchmark tool.**
1. Edit `crates/benchmark`.
2. Run `vsbench deploy bench --source local`.
3. Run `vsbench bench rerun <run_id>`. It repeats a recorded run's search
   parameters (kind, limit, duration, warmup, concurrency, bucket, extra
   arguments) on the **current** load and deployment, and warns when the
   dataset, load or index options differ.

The fairness key changes with the bench build, so do not mix the two in one
comparison.

**Investigate.**
- Prometheus:
  - `vsbench prom query '<promql>'`
  - `vsbench prom range '<promql>' --start now-15m --step 15s`: whole-range
    summary plus the last points; `--raw` prints everything.
  - `vsbench prom api targets state=active`
  - `vsbench prom api label/__name__/values 'match[]={job="vector_search"}'`
  - Write relative times as `now-15m`, never `-15m`.
  - The metric catalog and ready-made queries are in [reference.md](reference.md#metrics).
- CPU profiles: `vsbench bench search cql --concurrency 64 --perf vs-0`
  records `perf` (call graphs, 199 Hz) and `pidstat` on that node during
  every measured run and pulls the reports into
  `results/artifacts/profiles/<run_id>/<node>/` (`perf.txt`, `perf-dso.txt`,
  `pidstat.txt`); the record's `profile` field holds the paths. Scylla nodes
  work too (`--perf vs-0,scylla-0`), and so does `bench ab`. The capture is
  started when the command sees the step begin, so the command must stay
  attached to the job (runs after an exit 75 are not profiled). It needs
  `--duration` of 20 s or more.
- node_exporter directly: `vsbench exec vs-0 -- 'curl -s 127.0.0.1:9100/metrics | grep ^node_memory'`.
- Logs: `vsbench logs vs-0 --service vector-store --since 10m`,
  `vsbench logs scylla-0 --service scylla -n 200`.
- Anything else: `vsbench exec <all|scylla|vs|client|node> -- <cmd>`.
  - Output is capped by `--tail` (default 50 lines per node).
  - `--timeout` defaults to 9 min.
- Also: `vsbench ssh <node> [-- <cmd>]`, `vsbench push`/`pull`.
- Vector Store API on a node: `vsbench exec vs-0 -- curl -s 127.0.0.1:6080/api/v1/indexes`.
- Scylla: `vsbench exec scylla-0 -- sudo docker exec scylla nodetool status`.

**Debug a Scylla Cloud production cluster.** When the question comes from a
CUSTOMER ticket, the cluster's logs and metrics are already collected:
VictoriaLogs and Thanos are reachable over the ScyllaDB VPN with plain
HTTPS, no node access needed, and the TSE `scylla-customer-support` plugin
adds the support workflows. Field and label conventions, probe commands and
the queries that answered CUSTOMER-765 are in
[reference.md](reference.md#production-data). Reproduce on a benchmark
cluster only what the production data cannot show.

**Resume after a failure.**
- `vsbench job list` shows jobs and their state.
- `vsbench job wait <id>` re-attaches and records. It exits 1 for a failed job,
  even one already finalized.
- `vsbench job logs <id> --tail 100` shows a job's output.
- `vsbench job cancel <id>` stops a job, even while another command holds the
  lock; the next bench command or `job wait` records it.
- `bench load --resume` continues a load.

**Operator IP changed** (VPN, roaming): SSH times out and the hint says
`vsbench refresh-ip`, which needs credentials.

## Reading results

- **`client_metrics.mean_ms`** is concurrency × duration / queries. It is exact
  and the best single latency number for comparisons. It holds only when every
  task issues queries back to back: a run with the tool's `--delay` (passed
  after `--`) records that value as `cycle_ms` instead, with `mean_ms` empty
  and the `delayed` flag, because the pause is part of the cycle.
- **Percentile tags**:
  - `floored`: the tool reports everything under 1 ms as `1.0ms`.
  - `capped`: above 100 ms.
  - `bucket_interp`: server-side, interpolated inside a histogram bucket.
  Never draw conclusions from differences between tagged values. See
  [reference.md](reference.md#benchmark-tool-caveats).
- **Flags**:
  - `client_saturated`: client CPU > 80%. The result measures the client.
  - `net_allowance_exceeded`: AWS throttled the network.
  - `timeouts`: CQL queries timed out and count as recall 0.
  - `latency_floored` / `latency_capped`: see the percentile tags above.
  - `short_window`: under 30 s measured; no server metrics.
  - `plateau`: QPS stopped scaling while Vector Store CPU < 70%.
- **Noise**: `results compare` prints CV per group. CV above 10% means the run
  is noisy: add repeats or a longer duration before concluding anything.
