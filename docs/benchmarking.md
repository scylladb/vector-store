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
[latte/vector-search](../latte/vector-search/README.md).
