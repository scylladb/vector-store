# Contributing to Vector Store

Thank you for your interest in contributing! To help us maintain a high-quality codebase, please follow the guidelines below.

## Pre-Review Checklist

Before submitting a pull request (PR), ensure your contribution meets these requirements:

- **Single Change:** PR should consist of a single business change. If implementation requires multiple logical changes, split them into separate PRs. Each PR can contain multiple commits, but they should all relate to the same single logical change.
- **Change Quality:** Each PR should introduce one logically coherent change. PR and commit messages must clearly describe what is being changed and why.
- **Issue Closing Reference** If a PR fixes/closes a Jira issue, add a `Fixes: VECTOR-XYZ` line at the end of the PR. If you need you can also add it to the commit message.
- **Issue References:** For related issues or discussions that are not directly fixed by the PR, add a `Refs: VECTOR-XYZ` line at the end of the PR message. If you need you can also add it to the commit message.
- **Testing:** All new features and bug fixes must include appropriate tests.
- **PR Checks:** Every PR must pass all CI checks, including compilation, test execution, formatting, and static code analysis.
- **PR Description:** Clearly explain the motivation and reasoning behind your changes in the PR description.

If you cannot meet any of these requirements, explain why in your PR description. Maintainers may grant exceptions if justified.

## Commit and PR Organization

Organize your changes into one or more small, self-contained commits. These conventions are inspired by the ScyllaDB [patch organization guidelines](https://github.com/scylladb/scylladb/blob/master/docs/dev/review-checklist.md), adapted to our workflow — note in particular that a Vector Store PR should introduce exactly one change (plus any adjustments it requires), as described in the Pre-Review Checklist above:

- **Small patches:** Keep each patch small and focused — a patch should do one thing and be self-contained.
- **Individually correct:** Every patch should compile and pass tests on its own. Order patches so the tree is always correct — for example, never place a regression test before the fix that makes it pass, or use a symbol before the patch that introduces it.
- **Descriptive commit log:** The subject line has the form `module: changes`, where `module` is the directory/component the change applies to. List multiple modules joined with `,`, and use the `tree:` prefix for tree-wide changes. The prefix may be omitted for files in the root directory. Modules commonly used in this repo include `vector-store`, `validator`, `scripts`, `README`, and `chore`, but these are examples rather than a closed set — use whichever module best describes the change. Make the summary specific: prefer `sstables: close file-descriptor on error` over a generic `sstables: fix the bug`. Try to keep commit-log lines within ~72 characters (the subject may be longer when unavoidable).
- **Self-contained description:** Write for a non-expert — include enough context to understand the change without being an expert on the matter, and do not refer to out-of-band discussions (e.g. "as agreed on the daily call").
- **Motivation:** Every patch must explain *why*, not just *what*. A good litmus test: if the inverse patch ("make X not do Y") would be just as compelling, the description is not explaining enough.
- **PR motivation:** Each PR must include an overall motivation. For a multi-patch series put it in the cover letter (PR description); for a single patch, the patch description is enough.

Reference the Jira issue at the end of the commit/PR message: `Fixes: VECTOR-<n>` if the change closes the issue, or `Refs: VECTOR-<n>` if it is only related.

## Review and Merging

After submitting a compliant PR, it will be reviewed by one or more maintainers. Once approved, a maintainer will merge your contribution.

**Current maintainers:**
- Paweł Pery (@ewienik)

## Rust Coding Conventions

For detailed Rust coding conventions and best practices used in this project, see [docs/rust_instructions.md](docs/rust_instructions.md).

## Static Checks

To ensure code quality, we use several static analysis tools. All static checks must pass before merging.

**Install required Rust components:**
```sh
rustup component add rustfmt clippy
```

**Verify installation:**
```sh
rustc --version
cargo fmt --version
cargo clippy --version
```

**Run static checks:**

- **Format Check:** Verify code formatting (without making changes):
  ```sh
  cargo fmt --all -- --check
  ```
- **Clippy Lint:** Run the Rust linter:
  ```sh
  cargo clippy --all-targets
  ```

Static checks run automatically in CI. All warnings are treated as errors (`RUSTFLAGS=-Dwarnings`). These checks must pass before merging.

## Testing

The project includes unit tests, integration tests, and an end-to-end validator harness.

### Unit and integration tests

**Run all tests with verbose output:**
```sh
cargo test --workspace --verbose
```
`--workspace` is required to cover every crate — the workspace's `default-members`
is only `crates/vector-store`, so a plain `cargo test` skips the other crates.

### Validator harness (end-to-end tests)

The validator (`crates/validator`) is an end-to-end test suite that spins up its
own ScyllaDB + Vector Store cluster and drives real CQL / Alternator workloads
against it. This is the suite CI runs (see `.github/workflows/validator.yml`).

Because the harness launches actual `scylla` and `vector-store` processes, it
needs two release binaries plus a ScyllaDB image.

**1. Build the binaries:**
```sh
cargo build --release --bin vector-store
cargo build --release --bin vector-search-validator -p vector-search-validator
```

A plain host build is enough: the harness runs the validator and Vector Store
on the host, and only ScyllaDB comes from the image. To run exactly what CI and
the release build produce, build with `run-with-release-toolchain` instead; it
builds inside the pinned toolchain image and writes to `target/<arch>/release/`,
where `<arch>` is the `TARGETARCH` it builds for (`amd64` by default), so pass
that path to the script below:
```sh
./scripts/run-with-release-toolchain cargo build --release --bin vector-store
./scripts/run-with-release-toolchain cargo build --release --bin vector-search-validator -p vector-search-validator
```

**2. Run the harness against a ScyllaDB image:**
```sh
./scripts/run-validator-with-scylla-docker \
    <scylla-image> \
    target/release/vector-store \
    target/release/vector-search-validator \
    [filters...] --verbose
```

- `<scylla-image>` is a ScyllaDB docker tag for the host's architecture, e.g.
  `scylladb/scylla-nightly:latest`. CI runs against `scylla-nightly`; some tests
  exercise features that are only available there. Any ScyllaDB image works, a
  release included, since nothing but ScyllaDB runs from it.
- The script keeps ScyllaDB in a container and runs the validator and Vector
  Store on the host, in the container's network namespace, as root: it prompts
  for `sudo` once, and kills whatever an interrupted run leaves in that network
  namespace (the comment at the top of the script explains how it works). It
  needs Linux, `nsenter` and `unshare` from util-linux, a docker daemon running
  as root on the host (not rootless, not a remote `DOCKER_HOST`), and no
  `scylla` user on the host. The run's files go to a directory under `/tmp`,
  shared with the container, that the script removes at the end; do not pass
  `--tmpdir`.
- Filters select test cases using `<group>::<test>` syntax. Partial matches work,
  and wrapping either side in double quotes forces an exact match. Omit filters to
  run everything. For example, `cdc_direct::` runs every group whose name contains
  `cdc_direct`; to match that group exactly, keep the double quotes intact through
  the shell with single quotes: `'"cdc_direct"::'`.

**List available test cases:**
```sh
./target/release/vector-search-validator list
```

**Using a locally built ScyllaDB:**

The docker image is convenient, but not required. To run the harness against a
locally built `scylla` binary instead of an image, use
`run-validator-with-scylla-unshare`:
```sh
./scripts/run-validator-with-scylla-unshare \
    <path/to/scylla> \
    target/<arch>/release/vector-store \
    target/<arch>/release/vector-search-validator \
    --scylla-default-conf <path/to/scylla.yaml> \
    [filters...] --verbose
```
The script runs the harness as root inside a private network namespace (via
`sudo unshare`), setting up the loopback addresses and DNS resolution it needs, so
you do not have to configure any networking or grant extra privileges yourself
(it will prompt for `sudo`). Point `--scylla-default-conf` at a ScyllaDB config
file — the ScyllaDB source tree ships one at `conf/scylla.yaml`. Running against a
local ScyllaDB build is especially useful when developing a feature that spans
both ScyllaDB and Vector Store, where you want to test your own ScyllaDB changes
rather than a published image.

### Manual testing with the example docker-compose stacks

For quick manual or functional testing — not a replacement for the automated
harness — the `docs/examples/docker/` directory ships ready-to-run single-node
stacks that pair ScyllaDB with Vector Store:

- `docker-compose.yml` — CQL / native vector API (ScyllaDB on `9042`, Vector Store on `6080`).
- `docker-compose-alternator.yml` — Alternator (DynamoDB-compatible) API on `8000`, with CQL still on `9042`.

**Bring up the CQL stack and exercise it with the quick-start schema:**
```sh
docker compose -f docs/examples/docker/docker-compose.yml up -d --wait
cqlsh -f docs/examples/quick-start.cql   # create a vector table + index and run an ANN query
```
`--wait` blocks until the ScyllaDB health check passes, so `cqlsh` does not race
the database's startup.

**Tear it down:**
```sh
docker compose -f docs/examples/docker/docker-compose.yml down
```

> These are single-node stacks intended for local development and exploration
> only. They are not for production, and they do not replace the validator
> harness that runs in CI. Create keyspaces with a replication factor of 1 on a
> single node (note that `quick-start.cql` uses `replication_factor: 3`, which a
> single node cannot fully satisfy).

## Continuous Integration (CI)

We use GitHub Actions for CI, configured in `rust.yml`.

**All checks must pass before a pull request can be merged.**

When a test or job fails in CI, file or update a VECTOR issue and set its
**Problem Symptom** field to `ci stability`. Use the field rather than a
label: its values are a fixed list, so every open CI failure can be found
with `"Problem Symptom" = "ci stability"`.

## OpenAPI Specification

Vector Store exposes an HTTP REST API, documented in the OpenAPI specification file at `api/openapi.json`.

**Important:** Do not manually edit `api/openapi.json`. This file is generated directly from the source code.
To update the OpenAPI specification based on your code changes, run the following command:

```sh
cargo openapi
```

The `api/openapi.json` file must always be synchronized with the actual API definition in the source code.
This synchronization is enforced by an integration test in our CI pipeline.

The benchmark in [scylladb/vector-store-bench](https://github.com/scylladb/vector-store-bench) uses the API types and client of this repository (`crates/httpapi`, `crates/httpclient`) as git dependencies, pinned to a commit in its `Cargo.lock`.
CI here does not build the benchmark, so a change to these crates that breaks it needs a matching change in vector-store-bench, which moves the pin with `cargo update -p httpapi -p httpclient`.

When modifying or extending the Vector Store REST API, please follow this convention:

- **Explicit Index References:** Always refer to an index by explicitly specifying both the keyspace name and the index name, rather than using a qualified index name.

  **Good:**
  ```json
  {
    "keyspace": "somekeyspace",
    "index": "someindex"
  }
  ```

  **Bad:**
  ```json
  {
    "index": "somekeyspace.someindex"
  }
  ```
