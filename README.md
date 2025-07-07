# Scylla Vector Store

This is an indexing service for ScyllaDB for vector searching functionality.

## Configuration

All configuration of the Vector Store is done using environment variables. The
service supports also `.env` files. Supported parameters:

- `VECTOR_STORE_SCYLLADB_URI`: `ip:port`, a connection endpoint to ScyllaDB server, default
  `127.0.0.1:9042`
- `VECTOR_STORE_URI`: `ip:port`, a bind address and a listening port of HTTP API, default
  `127.0.0.1:6080`
- `VECTOR_STORE_OPENSEARCH_URI`: `ip:port`, a connection endpoint to OpenSearch instance HTTP API,
  if not set the service uses USearch library for indexing
- `VECTOR_STORE_THREADS`: `unsigned integer`, how many threads
  should be used for Vector Store indexing, default number of cores

## Development builds

You need to install [Rust
environment](https://www.rust-lang.org/tools/install). To install all
components run `rustup install` in the main directory of the repository.

Development workflow is similar to the typical `Cargo` development in Rust.

```
$ cargo b [-r]
$ cargo r [-r]
```

## Docker image

`Dockerfile` for the image is located in main directory of the repository. It
builds release version of the Vector Store. Sample command for building the
image:

```
$ docker build -t vector-store .
```

