# latte vector-search workloads

[latte](https://github.com/scylladb/latte) workload scripts that benchmark
ScyllaDB vector search over the CQL path — measuring throughput, latency and
**recall@k**. Recall is exported into latte's JSON report via the custom-metrics
channel, so it can be compared and tracked like QPS and latency.

These are the CQL counterpart to the Rust `vector-search-benchmark`
(`crates/benchmark`); latte is the load engine, these `.rn` files are the
workload.

## Files

| File | Purpose |
|---|---|
| `text_dataset.rn` | dataset/query/ground-truth **text** loaders |
| `metrics.rn` | quality metrics — the `recall_at_k` definition |
| `recall.rn` | benchmark over the whole dataset (load → ANN search → recall/QPS/latency) |
| `recall_buckets.rn` | one upload, queried as the full dataset or per size-stratum, for a recall/QPS-vs-index-size curve |

## Requirements

- `scylladb/latte` **≥ 0.49.0-scylladb** (the custom-metrics support these scripts rely on).
- A running ScyllaDB + vector-store cluster.

## Quick start (`recall.rn`)

```sh
# string -P values are parsed as expressions, so quote paths
latte schema latte/vector-search/recall.rn <node>
latte run -f load   latte/vector-search/recall.rn <node> -d <dataset_size> \
    -P load_data=true -P 'vector_data_dir="<dir>/"' --threads 1 --concurrency 64
latte run -f search latte/vector-search/recall.rn <node> -d 60s --concurrency 32 \
    -P 'vector_data_dir="<dir>/"' --generate-report -o report.json
```

`load` preloads the dataset into memory and inserts one row per cycle (run it
single-threaded with high async concurrency); `search` loads only the small
query + ground-truth files. Key params: `keyspace`, `table`, `dimension`,
`ann_limit` (k), `index_options`, `vector_data_dir`, `dataset_file`,
`query_vectors_file`, `ground_truth_file`. See each script's header for the rest.

## Datasets

Inputs are plain text (format documented in `lib.rn`): a base-vector file, a
query-vector file, and a ground-truth file. They are prepared **offline** from
the source parquet/fbin datasets (VectorDBBench-style and big-ann formats).

Text is a stopgap: latte currently reads dataset files into memory and has no
binary reader, which does not scale to billion-vector datasets (e.g. deep1b).
Direct streaming reads of the binary formats are the planned enhancement.
