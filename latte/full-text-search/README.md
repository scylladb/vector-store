# ScyllaDB Full-Text Search (BM25) Performance Testing

[Latte](https://github.com/scylladb/latte) workload scripts for measuring
ScyllaDB full-text search (BM25) indexing and search performance.

## Metrics

| Metric | Measurement |
|--------|-------------|
| Index build time | `index_ready_seconds` custom metric |
| Indexing throughput | `indexing_throughput_docs_per_sec` custom metric |
| Query latency | Latte request-latency HDR histogram |
| Search accuracy | recall@k, precision@k, MRR, and nDCG@k custom metrics |

## Requirements

- A ScyllaDB cluster with vector-store and full-text search enabled
  (`VECTOR_STORE_FULLTEXT_INDEXES=true`).
- [Latte](https://github.com/scylladb/latte) 0.49.0-scylladb or later.
- A prepared dataset directory containing the TSV files described below.

## Dataset Format

The workload reads these tab-separated files directly from `fts_data_dir`:

| File | Columns | Purpose |
|------|---------|---------|
| `documents.tsv` | `doc_id`, `body` | Corpus to index |
| `queries_<set>.tsv` | `query_id`, `text` | Query set for the search phase |
| `qrels_<set>.tsv` | `query_id`, `doc_id`, `relevance` | Relevance judgments for accuracy metrics |

The bundled `testdata/` directory is a small smoke-test fixture. Dataset
preparation is outside this workload; provide prepared TSV files for larger
benchmarks.

## Test Workflow

Run the commands from the repository root. Set the values for the target
cluster and dataset once:

```sh
WORKLOAD=latte/full-text-search/fts.rn
NODE=127.0.0.1
DATA_DIR=latte/full-text-search/testdata/
DOC_COUNT=10
```

Start from an empty table, then create the keyspace and table:

```sh
latte schema "$WORKLOAD" "$NODE" -P schema_cleanup=true
latte schema "$WORKLOAD" "$NODE"
```

Load the documents. Latte reads `documents.tsv` during preparation and runs one
load cycle per document:

```sh
latte load "$WORKLOAD" "$NODE" \
    -P "fts_data_dir=\"$DATA_DIR\"" \
    --threads 1 --concurrency 64
```

Build the index and wait until it is serving. The build phase drops a previous
index, creates a new one, and requires consecutive successful BM25 probes.
Set `max_index_wait_secs` for an index expected to take longer than the default
ten-minute readiness timeout.

```sh
latte run -f build_index "$WORKLOAD" "$NODE" -d 1 \
    -P "document_count=$DOC_COUNT" \
    -P max_index_wait_secs=600 \
    -P min_successful_probes=3 \
    --generate-report -o build_index.json
```

Run the search benchmark. With `compute_accuracy=true`, the workload reads the
matching qrels file and reports recall, precision, MRR, and nDCG in Latte's JSON
report.

```sh
latte run -f search "$WORKLOAD" "$NODE" -d 60s \
    --concurrency 32 --warmup 10s \
    -P "fts_data_dir=\"$DATA_DIR\"" \
    -P queries_file="queries_natural.tsv" \
    -P qrels_file="qrels_natural.tsv" \
    -P compute_accuracy=true \
    --generate-report -o search.json
```

Remove the index after the benchmark, or drop the complete table:

```sh
latte schema "$WORKLOAD" "$NODE" -P drop_index=true
# Or: latte schema "$WORKLOAD" "$NODE" -P schema_cleanup=true
```

## Individual Phases

The workload phases can also be run independently. `schema` is idempotent and
creates the keyspace and table if absent. `schema_cleanup=true` drops the table;
run `schema` again afterwards to recreate it. `drop_index=true` removes only the
full-text index, while `with_index=true` creates it during schema setup.

Use `latte load` for data ingestion. `latte run -f load -d <doc_count>` is also
available when measuring write throughput rather than loading the corpus once.

`build_index` records these metrics in its JSON report:

- `custom_metrics.index_ready_seconds.distribution.mean`
- `custom_metrics.indexing_throughput_docs_per_sec.distribution.mean`, when
  `document_count` is greater than zero

## Workload Parameters

All workload parameters are passed with Latte's `-P` flag.

| Parameter | Default | Description | Relevant commands |
|-----------|---------|-------------|-------------------|
| `keyspace` | `fts_bench` | CQL keyspace name | schema, load, build_index, search |
| `table` | `documents` | Table name | schema, load, build_index, search |
| `index_name` | `documents_fts_idx` | Full-text index name | schema, build_index |
| `replication_factor` | `1` | Keyspace replication factor | schema, load |
| `target_column` | `body` | Text column to index | schema, load, build_index, search |
| `index_options` | `""` | CQL `WITH OPTIONS` string for the index | schema, build_index |
| `with_index` | `false` | Create the index during schema setup | schema |
| `drop_index` | `false` | Drop the index during schema setup | schema |
| `schema_cleanup` | `false` | Drop the table | schema |
| `max_index_wait_secs` | `600` | Maximum time to wait for index readiness | build_index |
| `min_successful_probes` | `3` | Consecutive successful probes required for readiness | build_index |
| `document_count` | `0` | Documents indexed, used to calculate throughput | build_index |
| `search_limit` | `5` | BM25 result limit | search |
| `fts_data_dir` | `./` | Directory containing TSV data files | load, search |
| `documents_file` | `documents.tsv` | Document corpus filename | load |
| `queries_file` | `queries_natural.tsv` | Query filename | search |
| `qrels_file` | `qrels_natural.tsv` | Relevance-judgment filename | search |
| `compute_accuracy` | `true` | Record accuracy metrics; requires qrels | search |

## Project Layout

```
fts.rn              # Latte workload for schema, load, index build, and search
metrics.rn          # IR accuracy metrics
testdata/           # Smoke-test fixture
```
