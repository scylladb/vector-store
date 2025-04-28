# Scylla Vector Store

This is an indexing service for ScyllaDB for vector searching functionality.

## Configuration

All configuration of the Vector Store is done using environment variables. The
service supports also `.env` files. Supported parameters:

- `SCYLLADB_URI`: `ip:port`,  a listening port of ScyllaDB server, default
  `127.0.0.1:9042`
- `SCYLLA_USEARCH_URI`: `ip:port`,  a listening port of HTTP API, default
  `127.0.0.1:6080`
- `SCYLLA_USEARCH_BACKGROUND_THREADS`: `unsigned integer`, how many cores
  should be used for usearch indexing, default all cores

## Development builds

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

