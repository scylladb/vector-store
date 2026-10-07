# Benchmarking scylla + vector-store

## Use the `aws-bench-cluster` skill (Claude Code)

The repository ships a Claude Code skill,
[`.claude/skills/aws-bench-cluster`](../.claude/skills/aws-bench-cluster/SKILL.md),
that runs the whole loop below for you: it provisions a benchmark cluster in
AWS (Scylla nodes, Vector Store nodes and a client node with
scylla-monitoring and `vector-search-benchmark`), deploys Vector Store from a
Docker Hub release, an upstream git ref or your local working tree
(cross-compiled for arm64), loads a VectorDBBench dataset, runs and compares
benchmarks, reads Prometheus and the node logs, runs commands on the nodes,
and tears everything down. For a Scylla Cloud production cluster it also
knows how to read the cluster's logs and metrics centrally
([reference.md, Production data](../.claude/skills/aws-bench-cluster/reference.md#production-data)).

### What you need

- Claude Code with this repository checked out; the skill is picked up
  automatically.
- AWS access to the rnd-core-lab account through `gimme-aws-creds` (the
  skill starts the Okta device flow and gives you the URL to approve; the
  code expires after about two minutes, so approve it when it appears).
- Docker, for the local arm64 cross builds.
- The ScyllaDB VPN when the task involves a production cluster.

### How to use it

Describe the task in plain words — benchmark a change, compare two builds or
index options, reproduce a performance issue — and name the Jira issue when
there is one. The skill asks before it spends money (it shows the cluster
plan with the cost per hour and the TTL), before it extends a cluster, and
before it tears one down; everything else runs unattended. A session takes
from half an hour to a few hours, so run it with `/loop`, which lets Claude
pace itself across the long-running steps and ends when the work is done.

Examples that were used in practice:

```
/loop Use /aws-bench-cluster to reproduce and debug https://scylladb.atlassian.net/browse/CUSTOMER-765. End the loop when you are done.
```

```
/loop Validate /aws-bench-cluster skill by investigating VECTOR-951 issue. Beware - the issue was detected on 4x larger instances so the rates needs adapting. And it needs reliable, repeatable reproduction method - I am not sure if the scenario described in the issue was not caused by coincidence. End the loop when you are done.
```

The second one shows what helps: say how the original observation differs
from what the skill will build (instance size, data size, rates) and what
would convince you (a repeatable method, a comparison of builds), and the
skill designs the experiment around that.

### What you get

- Results on the Jira issue in the team's comment format: the setup
  (versions, instance types, index options), a table of runs with QPS,
  latency, recall and the server-side metrics, and the interpretation.
- Every run recorded locally under `~/.local/state/vsbench/` (results,
  logs, Prometheus snapshots) so comparisons stay fair across sessions.
- A retrospective at the end of each session that proposes improvements to
  the skill itself; nothing is applied without your approval.

Every cluster carries a TTL that terminates it from inside the nodes (24 h by
default, 8 h or less for a reproduction), so a forgotten cluster does not
run forever. The details — rules, procedure, recipes, the metric catalog and
the measurement methodology — are in
[SKILL.md](../.claude/skills/aws-bench-cluster/SKILL.md) and
[reference.md](../.claude/skills/aws-bench-cluster/reference.md).

## Use `vector-search-benchmark` by hand

### Building

```bash
$ git clone git@github.com:scylladb/vector-store.git
$ cd vector-store
$ cargo build -r -p vector-search-benchmark
$ cp target/release/vector-search-benchmark path-to/vector-search-benchmark
```

### Usage

`vector-search-benchmark` must be used with a running scylla + vector-store
cluster. You should set up the cluster before running the benchmark. You can
use [cluster-in-aws](../scripts/cluster-in-aws/README.md) for creating the
cluster in AWS. You need ip address of one of the scylla nodes and ip addresses
of all vector-store nodes. You need also a dataset of vectors - currently only
VectorDBBench format (parquet) is supported - cli has a parameter for
path-to-directory-with-dataset.

```bash
$ path-to/vector-search-benchmark --help
Usage: vector-search-benchmark <COMMAND>

Commands:
  build-table
  build-index
  drop-table
  drop-index
  search-cql
  search-http
```

Each of the cli commands has its own help. Short description of each command:
- `build-table` - creates a keyspace and a table for storing vectors and
  populates it with vectors from dataset.
- `build-index` - creates a vector search index on the table and check when all
  vector-store nodes built it.
- `drop-index` - drops the vector search index.
- `drop-table` - drops the table and the keyspace.
- `search-cql` - runs ANN search queries from the dataset and measures qps,
  latency & recall. This search is using CQL over the scylla.
- `search-http` - runs ANN search queries from the dataset and measures qps &
  latency. This search is using HTTP over vector-store directly - it sends no
  requests to the scylla cluster.

