# Vector search with ScyllaDB Alternator (Python)

Minimal Python examples that mirror [`../quick-start.cql`](../quick-start.cql)
using the DynamoDB-compatible
[Alternator](https://docs.scylladb.com/stable/alternator/) API and the
[`alternator-client`](https://github.com/scylladb/alternator-client-python)
load-balancing driver.

Each script creates a `comments` table, inserts two rows, builds a vector
index over them, then prints the comment whose vector is most similar to the
query vector.

| Example | Vector data type | ScyllaDB image |
| --- | --- | --- |
| [`list_of_numbers.py`](list_of_numbers.py) | standard list of numbers | `scylladb/scylla:2026.2.0-rc3` |
| [`native_vector.py`](native_vector.py) | native `Vector` type | `scylladb/scylla-nightly:2026.3` |

ScyllaDB 2026.3 adds the native `Vector` type, which is more compact, along
with a configurable similarity function and similarity scores.

## Run

1. Start single-node ScyllaDB (with Alternator) and Vector Store. Set the
   `scylla` image in
   [`../docker/docker-compose-alternator.yml`](../docker/docker-compose-alternator.yml)
   to the version listed above for the example you want to run, then:

   ```bash
   docker compose -f ../docker/docker-compose-alternator.yml up -d
   ```

2. Create a virtual environment and install the driver:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. Run an example:

   ```bash
   python3 list_of_numbers.py
   # or
   python3 native_vector.py
   ```
