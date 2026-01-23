# Scylla Vector Store

This is an indexing service for ScyllaDB for vector searching functionality.

## Configuration

All configuration of the Vector Store is done using environment variables. The
service supports also `.env` files.

| Variable                                   | Description                                                                                                              | Default           |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ | ----------------- |
| `VECTOR_STORE_URI`                         | The bind address and a listening port of HTTP(S) API                                                                     | `127.0.0.1:6080`  |
| `VECTOR_STORE_TLS_CERT_PATH`               | Path to the TLS certificate file to enable HTTPS. Both certificate and key paths must be set.                            |                   |
| `VECTOR_STORE_TLS_KEY_PATH`                | Path to the TLS private key file to enable HTTPS. Both certificate and key paths must be set.                            |                   |
| `VECTOR_STORE_SCYLLADB_URI`                | The connection endpoint to ScyllaDB server.                                                                              | `127.0.0.1:9042`  |
| `VECTOR_STORE_SCYLLADB_USERNAME`           | The username for authenticating with ScyllaDB. If not set, authentication is disabled.                                   |                   |
| `VECTOR_STORE_SCYLLADB_PASSWORD_FILE`      | The path to a file containing the password for ScyllaDB authentication.                                                  |                   |
| `VECTOR_STORE_OPENSEARCH_URI`              | A connection endpoint to an OpenSearch instance HTTP API. If not set, the service uses the USearch library for indexing. |                   |
| `VECTOR_STORE_THREADS`                     | How many threads should be used for Vector Store indexing.                                                               | (number of cores) |
| `VECTOR_STORE_MEMORY_LIMIT`                | How much available memory (in bytes) could be in use to allow allocation more memory for the index.                      | 95% of avail mem  |
| `VECTOR_STORE_MEMORY_USAGE_CHECK_INTERVAL` | How frequently available memory should be checked. The value is in human readable value (ie. `100ms`)                    | `1s`              |
| `VECTOR_STORE_CQL_KEEPALIVE_INTERVAL`      | CQL Driver's keepalive interval. The value is in human readable value (ie. `30s`)                                        | (driver default)  |
| `VECTOR_STORE_CQL_KEEPALIVE_TIMEOUT`       | CQL Driver's keepalive timeout. The value is in human readable value (ie. `30s`)                                         | (driver default)  |
| `VECTOR_STORE_CQL_TCP_KEEPALIVE_INTERVAL`  | CQL Driver's TCP keepalive interval. The value is in human readable value (ie. `20s`)                                    | (driver default)  |
| `VECTOR_STORE_CQL_URI_TRANSLATION_MAP`     | For testing. Use specific translation map for cql cluster addresses. (`{"ip_src:port": "ip_dst:port"}`).                 |                   | 
| `VECTOR_STORE_CDC_SAFETY_INTERVAL`         | CDC Driver's safety interval. The value is in human readable value (ie. `30s`)                                           | (driver default)  |
| `VECTOR_STORE_CDC_SLEEP_INTERVAL`          | CDC Driver's sleep interval. The value is in human readable value (ie. `10s`)                                            | (driver default)  |
| `VECTOR_STORE_USEARCH_SIMULATOR`           | Enable simulator for USearch. Provides human readable delays for simulated operations (`search:add-remove:reserve`).     |                   | 

## Development builds

You need to install [Rust
environment](https://www.rust-lang.org/tools/install). To install all
components run `rustup install` in the main directory of the repository.

Development workflow is similar to the typical `Cargo` development in Rust.

```
$ cargo b [-r]
$ cargo r [-r]
```

To install all cargo tools used in the CI:

```
$ scripts/install-cargo-tools
```

## Subdirectories

- [scripts](./scripts/README.md) - helper scripts for development, release,
  deployment, and testing

```
$ docker build -t vector-store .
```

