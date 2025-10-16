# Benchmarking scylla + vector-store

## Use `vector-search-benchmark`

### Building

```bash
$ git clone git@github.com:ewienik/vector-store.git
$ cd vector-store
$ cargo b -r -p vector-search-benchmark
$ cp target/release/vector-search-benchmark path-to/vector-search-benchmark
```

### Usage

`vector-search-benchmark` must be used with a running scylla + vector-store
cluster. You should set up the cluster before running the benchmark. You can
use [cluster-in-aws](../scripts/cluster-in-aws/README.md) for creating the
cluster in AWS. You need ip address of one of the scylla nodes and ip addresses
of all vector-store nodes. You need also a dataset of vectors - currently only
VectorDBBench format (parquet) is supported - cli has a parmater for
path-to-directory-with-dataset.

```bash
$ path-to/vector-search-benchmark --help
Usage: vector-search-benchmark <COMMAND>

Commands:
  build-table
  build-index
  drop-table
  drop-index
  search
```

Each of the cli commands has its own help. Short description of each command:
- `build-table` - creates a keyspace and a table for storing vectors and
  populates it with vectors from dataset.
- `build-index` - creates a vector search index on the table and check when all
  vector-store nodes built it.
- `drop-index` - drops the vector search index.
- `drop-table` - drops the table and the keyspace.
- `search` - runs ANN search queries from the dataset and measures qps &
  latency.  The basic search is using CQL over scylla. There is also an
  optional parameter for checking search directly on vector-store nodes.

