# ScyllaDB Full-Text Search (BM25) Performance Testing Suite

Performance testing framework for ScyllaDB Full-Text Search (BM25), built on
[Latte](https://github.com/scylladb/latte) (ScyllaDB's CQL load generator).

## Metrics

| Metric | Measurement |
|--------|-------------|
| Indexing throughput | Wall-clock timing of index build, reported as docs/sec |
| Query latency per category | Latte HDR histogram (request_latency.p50, .p99) |
| Search accuracy (optional) | recall@k, precision@k, MRR, nDCG@k via latte custom metrics |

## Prerequisites

- ScyllaDB cluster with vector-store and FTS enabled
  (`VECTOR_STORE_FULLTEXT_INDEXES=true`)
- Latte >= 0.49.0-scylladb
- Python 3.10+ (for the orchestrator and dataset preparation)

## Dataset Format

The benchmark expects TSV files in the data directory:

| File | Columns | Purpose |
|------|---------|---------|
| `documents.tsv` | `doc_id` , `body` | Corpus to index |
| `queries_<set>.tsv` | `query_id` , `text` | Query sets for search phase |
| `qrels_<set>.tsv` | `query_id` , `doc_id` , `relevance` | Relevance judgments for accuracy metrics (optional, per query set) |

Latte reads TSV files directly into CQL prepared statements — no intermediate
processing. This is the simplest portable format that works across any data
source (BEIR, custom, synthetic).

## Quick Start (testdata)

```bash
# Run the full benchmark with the bundled smoke-test fixture
# (requires ScyllaDB + vector-store — see docs/examples/docker/)
python run_benchmark.py --node 127.0.0.1 --data-dir testdata/ --out /tmp/test-results.json
```

## Manual Phase-by-Phase Execution

Each phase can be run independently with latte for debugging or experimentation.

### Schema

```bash
# Idempotent (safe to run multiple times) — CREATE TABLE IF NOT EXISTS
latte schema fts.rn 127.0.0.1 \
    -P keyspace="fts_bench" -P table="documents"

# Drop the FTS index before creating schema
latte schema fts.rn 127.0.0.1 \
    -P keyspace="fts_bench" -P table="documents" \
    -P drop_index=true

# Create the FTS index during schema (usually not needed — build_index handles this)
latte schema fts.rn 127.0.0.1 \
    -P keyspace="fts_bench" -P table="documents" \
    -P with_index=true

# Full reset — DROP TABLE first, then create
latte schema fts.rn 127.0.0.1 -P schema_cleanup=true
latte schema fts.rn 127.0.0.1 -P keyspace="fts_bench" -P table="documents"
```

Schema behavior is controlled by flags:

| Flag | Default | Effect |
|------|---------|--------|
| `schema_cleanup=true` | — | DROP TABLE only (full reset) |
| `drop_index=true` | false | Also DROP INDEX during schema |
| `with_index=true` | false | Also CREATE INDEX during schema |

### Load

```bash
latte load fts.rn 127.0.0.1 \
    -P fts_data_dir="testdata/" \
    --threads 1 --concurrency 64
```

Load is idempotent — it uses `CREATE TABLE IF NOT EXISTS`, so existing data is
preserved. To start fresh, run `latte schema -P schema_cleanup=true` first.

### Write Benchmark (optional)

```bash
latte run -f load fts.rn 127.0.0.1 -d <doc_count> \
    -P fts_data_dir="testdata/" \
    --threads 1 --concurrency 64
```

### Build Index

```bash
latte run -f build_index fts.rn 127.0.0.1 -d 1 \
    --retry-number 600 --retry-interval 1s \
    -P document_count=100000 \
    -P min_successful_probes=3 \
    --generate-report -o build_index.json
```

The command drops the index if it exists and creates a fresh one, then probes
with a BM25 query until serving. It waits for `min_successful_probes` (default 3)
consecutive successful probes before returning. The wall-clock time from sent
`CREATE INDEX` to the first successful probe is recorded as `index_ready_seconds`
in the report.

When `document_count` is provided (and > 0), the report also includes
`indexing_throughput_docs_per_sec` (`document_count / index_ready_seconds`).
Omit `document_count` (or set it to 0) to skip the throughput metric.

Both metrics appear under `custom_metrics` in the Latte report JSON:
- `custom_metrics.index_ready_seconds.distribution.mean`
- `custom_metrics.indexing_throughput_docs_per_sec.distribution.mean`

### Search

```bash
latte run -f search fts.rn 127.0.0.1 -d 60s \
    --concurrency 32 --warmup 10s \
    -P fts_data_dir="testdata/" \
    -P queries_file="queries_natural.tsv" \
    -P compute_accuracy=true \
    --generate-report -o report.json
```

The report contains request latency histograms and (if `compute_accuracy=true`)
custom metrics for recall, precision, MRR, and nDCG.

### Cleanup

```bash
latte schema fts.rn 127.0.0.1 -P drop_index=true \
    -P index_name="documents_fts_idx"

# Or drop the entire table (cascades to all indexes)
latte schema fts.rn 127.0.0.1 -P schema_cleanup=true
```

The orchestrator (`run_benchmark.py`) drops the FTS index after search by
default. Use `--no-cleanup` to skip this (user manages state).

## Workload Parameters

All workload parameters are passed via latte's `-P` flag.

| Parameter | Default | Description | Relevant commands |
|-----------|---------|-------------|-------------------|
| `keyspace` | `fts_bench` | CQL keyspace name | schema, load, build_index, search |
| `table` | `documents` | CQL table name | schema, load, build_index, search |
| `index_name` | `documents_fts_idx` | Full-text index name | schema, build_index |
| `replication_factor` | `1` | Keyspace RF | schema, load |
| `target_column` | `body` | Column to index (must be `text`) | schema, load, build_index, search |
| `index_options` | `""` | CQL `WITH OPTIONS` string for the index | schema, build_index |
| `with_index` | `false` | Also create full-text index during schema | schema |
| `drop_index` | `false` | Drop full-text index during schema | schema |
| `schema_cleanup` | `false` | DROP TABLE only — nothing else | schema |
| `min_successful_probes` | `3` | Consecutive successful probes before index is considered ready | build_index |
| `search_limit` | `5` | BM25 `LIMIT k` — documents returned per query | search |
| `fts_data_dir` | `./` | Directory containing TSV data files | load, search |
| `documents_file` | `documents.tsv` | Filename (relative to `fts_data_dir`) for doc corpus | load |
| `queries_file` | `queries_natural.tsv` | Filename for query phrases | search |
| `qrels_file` | `qrels_natural.tsv` | Filename for relevance judgments | search |
| `compute_accuracy` | `true` | Record recall/precision/MRR/nDCG metrics | search |

## Orchestrator

`run_benchmark.py` drives all phases via latte CLI calls:

```
python run_benchmark.py --node <host> --data-dir <dir> [options]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--node` | (required) | ScyllaDB host[:port] |
| `--data-dir` | (required) | Prepared dataset directory |
| `--out` | `<data-dir>/results.json` | Output summary path |
| `--keyspace` | `fts_bench` | Keyspace name |
| `--table` | `documents` | Table name |
| `--replication-factor` | `1` | RF |
| `--index-name` | `documents_fts_idx` | Full-text index name |
| `--index-options` | `""` | CQL WITH OPTIONS |
| `--load-threads` | `1` | Latte threads for load |
| `--load-concurrency` | `64` | Latte concurrency for load |
| `--retry-number` | `10` | Max retries for index probe |
| `--retry-interval` | `1s` | Latte retry interval for index probe (e.g. `500ms`, `5s`) |
| `--max-index-wait` | `600` | Max seconds to wait for index build |
| `--min-successful-probes` | `3` | Consecutive successful probes before index is ready |
| `--search-duration` | `60s` | Per-query-set run duration |
| `--search-concurrency` | `32` | Latte concurrency for search |
| `--limit` | `5` | CQL LIMIT for search queries |
| `--search-warmup` | `10s` | Passed to latte --warmup |
| `--doc-limit` | `(unlimited)` | Max documents to load/index (applied as min of total and this limit) |
| `--query-sets` | `all` | Comma-separated subset to run |
| `--skip-schema` | `false` | Skip keyspace/table creation |
| `--skip-load` | `false` | Skip schema + load (table pre-populated) |
| `--skip-search` | `false` | Skip search phase |
| `--skip-build-index` | `false` | Skip index build phase |
| `--drop-index` | `false` | Drop the FTS index before schema creation |
| `--no-cleanup` | `false` | Skip index cleanup after search (user manages state) |
| `--latte-bin` | `latte` | Path to latte binary |
| `--latte-workload` | `fts.rn` in script dir | Path to fts.rn |

### Running specific query sets

```bash
python run_benchmark.py --node 127.0.0.1 --data-dir prepared/scifact \
    --query-sets phrase,boolean_and,natural
```

## Output Format

The orchestrator writes a JSON summary:

```json
{
  "data_dir": "prepared/scifact",
  "manifest": { "documents": 5183 },
  "document_count": 5183,
  "cpu_count": 8,
  "phases": {
    "load": {
      "elapsed_seconds": 12.3,
      "throughput_docs_per_sec": 421.5
    },
    "build_index": {
      "elapsed_seconds": 15.7,
      "documents": 5183,
      "indexing_throughput_docs_per_sec": 330.1
    },
    "search": {
      "term_medium": {
        "qps": 15000.0,
        "latency_ms": {
          "min": 0.1,
          "p50": 1.2,
          "p90": 2.0,
          "p95": 2.5,
          "p99": 3.1,
          "p99.9": 8.0,
          "max": 15.0
        },
        "report": "reports/term_medium.json",
        "limit": 5,
        "metrics": {
          "result_count": { "mean": 5.0, "min": 3, "p50": 5, "p90": 5, "p95": 5, "p99": 5, "max": 5 },
          "recall": { "mean": 0.92, "p50": 0.95, "p99": 1.0 },
          "precision": { "mean": 0.15 },
          "mrr": { "mean": 0.96 },
          "ndcg": { "mean": 0.88 }
        }
      }
    }
  }
}
```

## Project Layout

```
fts.rn                  # Single Latte workload — all phase functions
metrics.rn              # IR accuracy metrics (recall, precision, MRR, nDCG)
prepare_dataset.py      # Offline data preparation utility
run_benchmark.py        # Benchmark orchestrator
testdata/               # Smoke-test fixture (10 docs, 8 queries, qrels)
prepared/               # Output for prepared datasets
  manifest.json
  documents.tsv
  queries_*.tsv
  qrels_<set>.tsv
  reports/
  shards/               # Only if --shards N >= 2
docker-compose.yml      # ScyllaDB + vector-store for local testing
```

## Notes

- **Data preparation is offline** — run `prepare_dataset.py` separately. The
  orchestrator never calls it.
- **Index build timing** uses external wall-clock measurement of the `latte
  run -f build_index` command, including index build + retry overhead (~1s per
  retry with default `--retry-interval`).
- **Accuracy metrics** require natural queries with matching qrels. Synthetic
  queries (term, phrase, boolean) have no qrels, so `compute_accuracy` is
  automatically set to `false` for them.
- **Scaling and accuracy** — `--scale-mode replicate` mirrors qrels to all
  copies for accuracy. `--scale-mode synthetic` produces no qrels (accuracy
  skipped). Measure accuracy on natural queries from BEIR.
- **Missing or empty query files** are skipped with a warning rather than
  causing a crash.
