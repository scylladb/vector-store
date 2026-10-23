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

- A ScyllaDB cluster with vector-store and full-text search enabled.
- [Latte](https://github.com/scylladb/latte) 0.50.0-scylladb or later - the
  loader needs the `prepare_worker` hook and seekable binary file handles.
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
WORKLOAD=latte/fts.rn
NODE=127.0.0.1
DATA_DIR=latte/full-text-search/testdata/
DOC_COUNT=10
THREADS=$(nproc)
```

Start from an empty table, then create the keyspace and table:

```sh
latte schema "$WORKLOAD" "$NODE" -P schema_cleanup=true
latte schema "$WORKLOAD" "$NODE"
```

Load the documents. Preparation scans `documents.tsv` once to record where each
line starts, then each load cycle reads one document from the file:

```sh
latte load "$WORKLOAD" "$NODE" \
    -P "fts_data_dir=\"$DATA_DIR\"" \
    --concurrency 64 \
    --threads "$THREADS"
```

Only the line offsets are held, so what the loader retains depends on the
document count rather than document size; like all workload state they are
copied into every worker thread. Raise `--threads` until the server, not the
client, is the limit.

Each cycle reads from disk, synchronously, so load-phase latency percentiles
measure the client too: read them as throughput, and keep the corpus on fast
local storage. `search` preloads the (small) query and qrels files and does no
file I/O.

A body containing a tab is stored whole. Earlier runs truncated it at the
second tab, so BM25 results for such a corpus will differ from older
baselines.

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

### Highlighting overhead

`highlight=true` selects a `BM25_HIGHLIGHT()` excerpt alongside each result.
Nothing else changes, so a search run with and one without it on the same
loaded corpus and query set measure what highlighting adds to BM25 query
latency:

```sh
latte run -f search "$WORKLOAD" "$NODE" -d 60s \
    --concurrency 32 --warmup 10s \
    -P "fts_data_dir=\"$DATA_DIR\"" \
    --generate-report -o search_plain.json
latte run -f search "$WORKLOAD" "$NODE" -d 60s \
    --concurrency 32 --warmup 10s \
    -P "fts_data_dir=\"$DATA_DIR\"" \
    -P highlight=true \
    --generate-report -o search_highlight.json
latte show search_highlight.json -b search_plain.json
```

Each report records `highlight` in its run metadata. The coordinator reads the
text of every matched row and sends it to the index node, which returns the
excerpt, so the overhead grows with document length and `search_limit` rather
than with corpus size; pick the corpus with the document sizes you care about.

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

## Hybrid search

`hybrid.rn` benchmarks a table with both a vector index and a full-text index,
queried by `RRF(ANN(..), BM25(..))`; the SEP target is a hybrid p99 under
50 ms on a single coordinator. Each leg alone is measured by `fts.rn` and
`ann.rn`.

The dataset is the full-text corpus above plus an embedding per document and
per query, in the big-ann fbin format:

| File | Content |
|------|---------|
| `documents.fbin` | record `i` is the embedding of line `i` of `documents.tsv` |
| `queries_<set>.fbin` | record `i` is the embedding of line `i` of `queries_<set>.tsv` |

`prepare` fails when a TSV and its fbin disagree on the count, or when the
`documents.fbin` dimension differs from `dimension`. The bundled `testdata/`
embeddings are random (`gen_hybrid_testdata.py`), so they exercise the
workload and not the ranking. Accuracy metrics are therefore off by default
(`compute_accuracy=false`); with real embeddings, turn them on to score the
fused ranking against the same qrels as `fts.rn`.

```sh
WORKLOAD=latte/hybrid.rn
DIM=4   # the fbin dimension

latte schema "$WORKLOAD" "$NODE" -P schema_cleanup=true
latte schema "$WORKLOAD" "$NODE" -P dimension=$DIM
latte load "$WORKLOAD" "$NODE" \
    -P "fts_data_dir=\"$DATA_DIR\"" -P dimension=$DIM \
    --threads 4 --concurrency 64
latte run -f build_index "$WORKLOAD" "$NODE" -d 1 -P dimension=$DIM
latte run -f search "$WORKLOAD" "$NODE" -d 60s \
    --concurrency 32 --warmup 10s \
    -P "fts_data_dir=\"$DATA_DIR\"" -P dimension=$DIM \
    --generate-report -o search.json
```

`build_index` creates both indexes if absent and waits until each answers,
so `search` runs against complete indexes. It does not drop them: to rebuild,
run `schema -P drop_index=true` first. It records no metric: each index's
build time is measured by `fts.rn` and `ann.rn`, and building several indexes
at once is out of scope here. `highlight=true` adds a `BM25_HIGHLIGHT()`
excerpt, as for `fts.rn`.

What a hybrid query supports, and how ScyllaDB runs it, is described in
ScyllaDB's [hybrid search query](https://github.com/scylladb/scylladb/blob/master/docs/cql/dml/select.rst#hybrid-search-queries)
documentation; its limits apply to `search`.

### Hybrid workload parameters

`hybrid.rn` shares `keyspace`, `table`, `replication_factor`, `schema_cleanup`,
`drop_index`, `max_index_wait_secs`, `min_successful_probes`, `search_limit`,
`fts_data_dir`, `documents_file`, `queries_file`, `qrels_file`,
`compute_accuracy` and `highlight` with `fts.rn` (`drop_index` drops both
indexes, and `compute_accuracy` defaults to `false`), and adds:

| Parameter | Default | Description | Relevant commands |
|-----------|---------|-------------|-------------------|
| `text_column` | `body` | Text column, full-text indexed | schema, load, build_index, search |
| `vector_column` | `embedding` | Vector column, ANN indexed | schema, load, build_index, search |
| `dimension` | `4` | Vector dimension; must match the fbin files | schema, load, build_index, search |
| `fts_index_name` | `documents_fts_idx` | Full-text index name | schema, build_index |
| `ann_index_name` | `documents_ann_idx` | Vector index name | schema, build_index |
| `fts_index_options` | `""` | CQL `WITH OPTIONS` string for the full-text index | build_index |
| `ann_index_options` | `""` | CQL `WITH OPTIONS` string for the vector index | build_index |
| `vectors_file` | `documents.fbin` | Document embeddings, aligned with `documents_file` | load |
| `query_vectors_file` | `queries_natural.fbin` | Query embeddings, aligned with `queries_file` | search |

## Workload Parameters

All workload parameters are passed with Latte's `-P` flag. This table is for
`fts.rn`; see above for what `hybrid.rn` adds.

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
| `highlight` | `false` | Also select a `BM25_HIGHLIGHT()` excerpt per result | search |

## Smoke test

`common/tsv_dataset_test.rn` checks the document loader against the fixtures
in `testdata/` (regenerate with `testdata/gen_tsv_testdata.py`). It needs a CQL
endpoint but no vector-store. From the repository root:

```sh
latte run -f smoke latte/common/tsv_dataset_test.rn <node> -d 1
```

## Project Layout

The workload scripts live in `latte/`, beside the `common/` modules they share
with the other workloads (see [../README.md](../README.md)):

```
latte/fts.rn                        # Workload for schema, load, index build, and search
latte/hybrid.rn                     # Hybrid (ANN + BM25) workload over the same corpus plus embeddings
latte/common/tsv_dataset.rn         # Document-corpus loader
latte/common/tsv_dataset_test.rn    # Smoke test for the loader
latte/common/fbin_dataset.rn        # Embedding loader, shared with the vector-search workloads
latte/common/relevance_metrics.rn   # Relevance metrics: recall, precision, MRR, nDCG
latte/common/search_queries.rn      # Query and qrels loaders
latte/common/index_probe.rn         # Index readiness probe for build_index
latte/full-text-search/testdata/    # Smoke-test fixtures; gen_hybrid_testdata.py regenerates the fbin files
```
