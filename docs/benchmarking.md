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

## In-process pipeline benchmarks

The `pipeline` benchmark runs Vector Store in process over an in-memory
database and feeds it rows through the full scan and CDC paths, so it needs
no cluster. The `ann-*` benchmarks cover vector indexes, the `fts-*` ones
cover full-text indexes backed by Tantivy. The parameter after the slash is
`BENCHES_CONCURRENCY` (default 1), the number of documents in flight or
parallel queries. `VECTOR_STORE_THREADS` sets the runtime threads and also
the indexing threads of every Tantivy writer, so report it with any result.

```sh
VECTOR_STORE_THREADS=8 BENCHES_CONCURRENCY=8 \
    cargo bench -p vector-store --features in-progress --bench pipeline -- fts
```

| Benchmark | One iteration | Reports |
| --- | --- | --- |
| `fts-fullscan-build-N` | 500,000 documents spread over N fresh indexes, until all are searchable | documents per second |
| `fts-search-N` | one BM25 query to one of N indexes of 1,000 documents | latency |
| `fts-search-while-inserting` | one BM25 query while CDC inserts stream into the same index | latency |
| `fts-search-while-other-building` | one BM25 query while another index takes a full-scan build | latency |

N is 1 and 10 by default; `BENCHES_FTS_INDEXES=1,10,100,1000` picks other
counts. Every Tantivy writer holds its own indexing and merge threads, 13 of
them with `VECTOR_STORE_THREADS=8`, so threads and memory grow with N. Run
large counts inside a memory-capped scope, so running out of memory kills the
benchmark and not the machine:

```sh
BENCHES_FTS_INDEXES=100,1000 systemd-run --user --scope -p MemoryMax=20G -p MemorySwapMax=0 \
    cargo bench -p vector-store --features in-progress --bench pipeline -- fts
```

The search queries rotate between a frequent word, a rare word and a pair of
words from a fixed Zipf corpus. Each FTS benchmark checks at the end that its
indexes hold every document sent, and prints the median, 99th percentile
and maximum latency of the search calls, with the number of calls over 50 ms
(`fts latency`), to stderr. The build and the idle
search benchmarks also print the thread count and memory of the process
(`fts footprint`), with the peak thread count sampled every 5 ms since the
benchmark started, so threads that live only while documents are indexed
are counted too.

Every document inserted during `fts-search-while-inserting` is a new row, so
the index grows for the whole run, and a version that ingests faster also
searches a bigger index. `BENCHES_FTS_INSERT_RATE` caps the documents sent per
second, so that two versions search indexes of a similar size; the scenario
prints the rate it achieved.

There is no FTS counterpart of `ann-fullscan-insert`: a document becomes
searchable only at the next commit, at most a second later, so its latency
would mostly measure that interval.

To compare an unmodified Tantivy with a patched one, save a baseline, point
the workspace at the patched checkout with a local `[patch.crates-io]`
section, which must not be committed, and run again against that baseline:

```sh
cargo bench -p vector-store --features in-progress --bench pipeline -- fts --save-baseline unmodified
# add to Cargo.toml: [patch.crates-io] tantivy = { path = "../tantivy" }
cargo bench -p vector-store --features in-progress --bench pipeline -- fts --baseline unmodified
```
