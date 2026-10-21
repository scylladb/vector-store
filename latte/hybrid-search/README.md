# ScyllaDB Hybrid Search Performance Testing

[Latte](https://github.com/scylladb/latte) workload script for measuring
ScyllaDB hybrid search: a table with both a vector index and a full-text index,
queried by `RRF(ANN(..), BM25(..))`. Each leg alone is measured by `fts.rn`
([full-text-search](../full-text-search/README.md)) and `ann.rn`
([vector-search](../vector-search/README.md)).

## Metrics

| Metric | Measurement |
|--------|-------------|
| Query latency | Latte request-latency HDR histogram |
| Search accuracy | recall@k, precision@k, MRR, and nDCG@k custom metrics, with `compute_accuracy=true` |

## Requirements

- A ScyllaDB cluster with hybrid search, and vector-store.
- [Latte](https://github.com/scylladb/latte) 0.50.0-scylladb or later.
- A prepared dataset directory containing the files described below.

## Dataset Format

The workload reads these files directly from `hybrid_data_dir`. The TSV files
have the same format as for `fts.rn`, so one dataset directory can serve both
workloads; the fbin files are in the big-ann format:

| File | Content |
|------|---------|
| `documents.tsv` | `doc_id`, `body`: corpus to index |
| `documents.fbin` | record `i` is the embedding of line `i` of `documents.tsv` |
| `queries_<set>.tsv` | `query_id`, `text`: query set for the search phase |
| `queries_<set>.fbin` | record `i` is the embedding of line `i` of `queries_<set>.tsv` |
| `qrels_<set>.tsv` | `query_id`, `doc_id`, `relevance`: relevance judgments for accuracy metrics |

`prepare` fails when a TSV and its fbin disagree on the count, or when the
`document_vectors_file` dimension differs from `dimension`.

The bundled `testdata/` directory is a small smoke-test fixture: the TSV files
are a copy of the full-text-search fixture, and the embeddings are random
(`testdata/gen_hybrid_testdata.py`), so they exercise the workload and not the
ranking. Accuracy metrics are therefore off by default
(`compute_accuracy=false`); with real embeddings, turn them on to score the
fused ranking against the qrels.

## Test Workflow

Run the commands from the repository root:

```sh
WORKLOAD=latte/hybrid.rn
NODE=127.0.0.1
DATA_DIR=latte/hybrid-search/testdata/
DIM=4   # the fbin dimension

latte schema "$WORKLOAD" "$NODE" -P schema_cleanup=true
latte schema "$WORKLOAD" "$NODE" -P dimension=$DIM
latte load "$WORKLOAD" "$NODE" \
    -P "hybrid_data_dir=\"$DATA_DIR\"" -P dimension=$DIM \
    --threads 4 --concurrency 64
latte run -f build_index "$WORKLOAD" "$NODE" -d 1 -P dimension=$DIM
latte run -f search "$WORKLOAD" "$NODE" -d 60s \
    --concurrency 32 --warmup 10s \
    -P "hybrid_data_dir=\"$DATA_DIR\"" -P dimension=$DIM \
    --generate-report -o search.json
```

`build_index` creates both indexes if absent and waits until each answers,
so `search` runs against complete indexes. It does not drop them: to rebuild,
run `schema -P drop_index=true` first. It records no metric: each index's
build time is measured by `fts.rn` and `ann.rn`, and building several indexes
at once is out of scope here. `highlight=true` adds a `BM25_HIGHLIGHT()`
excerpt, as for `fts.rn`.

What a hybrid query supports, and how ScyllaDB runs it, is described in
ScyllaDB's [hybrid search query](https://docs.scylladb.com/manual/master/cql/dml/select.html#hybrid-queries)
documentation; its limits apply to `search`.

## Workload Parameters

All workload parameters are passed with Latte's `-P` flag.

| Parameter | Default | Description | Relevant commands |
|-----------|---------|-------------|-------------------|
| `keyspace` | `hybrid_bench` | CQL keyspace name | schema, load, build_index, search |
| `table` | `documents` | Table name | schema, load, build_index, search |
| `text_column` | `body` | Text column, full-text indexed | schema, load, build_index, search |
| `vector_column` | `embedding` | Vector column, ANN indexed | schema, load, build_index, search |
| `dimension` | `4` | Vector dimension; must match the fbin files | schema, load, build_index, search |
| `replication_factor` | `1` | Keyspace replication factor | schema, load |
| `schema_cleanup` | `false` | Drop the table | schema |
| `drop_index` | `false` | Drop both indexes during schema setup | schema |
| `fts_index_name` | `documents_fts_idx` | Full-text index name | schema, build_index |
| `ann_index_name` | `documents_ann_idx` | Vector index name | schema, build_index |
| `fts_index_options` | `""` | CQL `WITH OPTIONS` string for the full-text index | build_index |
| `ann_index_options` | `""` | CQL `WITH OPTIONS` string for the vector index | build_index |
| `max_index_wait_secs` | `600` | Maximum time to wait for both indexes to be ready | build_index |
| `min_successful_probes` | `3` | Consecutive successful probes required for readiness | build_index |
| `search_limit` | `5` | RRF result limit | search |
| `hybrid_data_dir` | `./` | Directory containing the TSV and fbin files | load, search |
| `documents_file` | `documents.tsv` | Document corpus filename | load |
| `document_vectors_file` | `documents.fbin` | Document embeddings, aligned with `documents_file` | load |
| `query_texts_file` | `queries_natural.tsv` | Query text filename | search |
| `query_vectors_file` | `queries_natural.fbin` | Query embeddings, aligned with `query_texts_file` | search |
| `qrels_file` | `qrels_natural.tsv` | Relevance-judgment filename | search |
| `compute_accuracy` | `false` | Record accuracy metrics; requires qrels | search |
| `highlight` | `false` | Also select a `BM25_HIGHLIGHT()` excerpt per result | search |

## Project Layout

The workload script lives in `latte/`, beside the `common/` modules it shares
with the other workloads (see [../README.md](../README.md)):

```
latte/hybrid.rn                     # Workload for schema, load, index build, and search
latte/common/tsv_dataset.rn         # Document-corpus loader, shared with fts.rn
latte/common/fbin_dataset.rn        # Embedding loader, shared with the vector-search workloads
latte/common/search_queries.rn      # Query and qrels loaders, shared with fts.rn
latte/common/relevance_metrics.rn   # Relevance metrics: recall, precision, MRR, nDCG
latte/common/index_probe.rn         # Index readiness probe for build_index
latte/hybrid-search/testdata/       # Smoke-test fixtures; gen_hybrid_testdata.py regenerates the fbin files
```
