# latte workloads

[latte](https://github.com/scylladb/latte) workload scripts that benchmark
ScyllaDB vector and full-text search over CQL. Run them from the repository
root, for example `latte schema latte/fts.rn <node>`.

| Workload | Measures | Documentation |
|---|---|---|
| `ann.rn` | ANN search over the whole dataset: throughput, latency, recall@k | [vector-search](vector-search/README.md) |
| `ann_buckets.rn` | ANN recall and throughput against index size, from one upload | [vector-search](vector-search/README.md) |
| `fts.rn` | BM25 index build, search latency and accuracy | [full-text-search](full-text-search/README.md) |

## Layout

```
latte/
  *.rn                # workload scripts
  common/             # modules the workloads share, and their smoke tests
  vector-search/      # vector-search documentation and fixtures
  full-text-search/   # full-text-search documentation, fixtures and dataset tools
```

rune resolves `mod` only beside the importing file or below it, so the
workload scripts sit here and declare the shared modules they use:

```rune
mod common {
    pub mod tsv_dataset;
}
use common::tsv_dataset;
```

Each script lists only its own modules: rune warns about every function it
compiles but never calls, so a `common/mod.rn` declaring all of them would
fill every run's output with warnings. A smoke test in `common/` imports its
siblings directly (`mod tsv_dataset;`).
