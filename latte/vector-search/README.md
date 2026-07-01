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
latte schema             latte/vector-search/recall.rn <node>
latte run -f load        latte/vector-search/recall.rn <node> -d <dataset_size> \
    -P load_data=true -P 'vector_data_dir="<dir>/"' --threads 1 --concurrency 64
latte run -f build_index latte/vector-search/recall.rn <node> -d 1 -P build_index=true
latte run -f search      latte/vector-search/recall.rn <node> -d 60s --concurrency 32 \
    -P 'vector_data_dir="<dir>/"' --generate-report -o report.json
```

`load` preloads the dataset into memory and inserts one row per cycle (run it
single-threaded with high async concurrency); `build_index` creates the index and
blocks until it is fully built; `search` loads only the small query +
ground-truth files. Key params: `keyspace`, `table`, `dimension`, `ann_limit`
(k), `index_options`, `index_before_load`, `vector_data_dir`, `dataset_file`,
`query_vectors_file`, `ground_truth_file`. See each script's header for the rest.

**Index build timing (`index_before_load`):** by default the index is created
*after* load, in the `build_index` phase, which blocks until the index is fully
built — so `search` measures a complete index and recall is deterministic. The
wait works because an ANN query on a not-yet-built index errors, and latte retries
it (during `build_index`'s warmup) until it succeeds; for a large dataset raise
`--retry-number` so the retry budget covers the build. Set
`-P index_before_load=true` to instead create the index in `schema` and skip
`build_index`; inserts then maintain the index during load (this measures the
index write path, but the index fills asynchronously, so searching too early sees
a partial index and you must wait an unbounded delay before `search`).

## Datasets

Inputs are plain text (format documented in `text_dataset.rn`): a base-vector
file, a query-vector file, and a ground-truth file. They are prepared **offline**
from the source parquet/fbin datasets (VectorDBBench-style and big-ann formats).

Default filenames — each overridable with the matching `-P` param:

- `dataset_file` — `dataset.txt` for `recall.rn`, `dataset_buckets.txt` for `recall_buckets.rn`
- `query_vectors_file` — `queries.txt`
- `ground_truth_file` — `ground_truth.txt`

The `search` phase does not read the dataset file, but it records `dataset_file`
as the `dataset` field in the report metadata — if you renamed the dataset file,
pass the same `-P dataset_file=...` to `search` too, or the report will record
the default name.

`recall_buckets.rn` additionally reads per-stratum files derived from the bucket
number (not params): `bucket/test_bucket<N>.txt`, `bucket/gt_bucket<N>.txt`.

Text is a stopgap: latte currently reads dataset files into memory and has no
binary reader, which does not scale to billion-vector datasets (e.g. deep1b).
Direct streaming reads of the binary formats are the planned enhancement.
