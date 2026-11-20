# Benchmarking scylla + vector-store

The `vector-search-benchmark` tool and the `aws-bench-cluster` Claude Code
skill live in
[scylladb/vector-store-bench](https://github.com/scylladb/vector-store-bench);
they moved there from this repository on 2026-10-08, with their history. Its
[README](https://github.com/scylladb/vector-store-bench#readme) explains how to
build and run the tool by hand, and how to use the skill to provision a
benchmark cluster in AWS, deploy Vector Store from a release, a git ref or a
local checkout of this repository, and run and compare benchmarks. To set up
the cluster by hand instead, see
[cluster-in-aws](../scripts/cluster-in-aws/README.md).

[latte](https://github.com/scylladb/latte) workloads that benchmark vector
search over CQL, recall included, stay in this repository:
[latte](../latte/README.md).

## In-process FTS indexing

`crates/vector-store/benches/fts_indexing.rs` feeds a synthetic corpus to the
full-text indexes through a mocked full scan, with no ScyllaDB cluster, and
prints one row per configuration: documents per second until all are
searchable, commits and operations per commit, commit latency, writer lock
wait, segments after each commit and memory at each commit. It runs every
combination of runtime threads (1, 2, 4, 8) and index counts (1, 100), each in
its own process:

```sh
cargo bench -p vector-store --features bench-hooks --bench fts_indexing
```

`FTS_BENCH_THREADS` and `FTS_BENCH_INDEXES` take comma-separated lists,
`FTS_BENCH_DOCS` sets the total number of documents and `FTS_BENCH_SEED` the
corpus seed. Run it on two builds with the same settings to compare them.
