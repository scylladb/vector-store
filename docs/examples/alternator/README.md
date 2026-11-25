# Vector search with ScyllaDB Alternator (Python)

A minimal Python example that mirrors [`../quick-start.cql`](../quick-start.cql)
using the DynamoDB-compatible
[Alternator](https://docs.scylladb.com/manual/stable/alternator/alternator.html) API and plain
[boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

[`vector_search.py`](vector_search.py) creates a `comments` table, inserts
two rows, builds a vector index over them, then prints the comment whose
vector is most similar to the query vector, along with its cosine distance.

Alternator implements
[DynamoDB's Vector Search API](https://docs.scylladb.com/manual/branch-2026.4/alternator/vector-search.html)
(`VectorIndexes`, `SearchVectors`) starting with ScyllaDB 2026.4, so any
boto3 release that models that API (1.43.64 or later) works unmodified. As a
ScyllaDB extension, Alternator also accepts a more compact `FLOAT32VECTOR`
encoding for vectors, which boto3 does not model; see the linked
documentation for details.

## Run

1. Start single-node ScyllaDB (with Alternator) and Vector Store:

   ```bash
   docker compose -f ../docker/docker-compose-alternator.yml up -d
   ```

2. Create a virtual environment and install boto3:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. Run the example:

   ```bash
   python3 vector_search.py
   ```
