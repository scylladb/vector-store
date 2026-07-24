# Repository instructions

Vector Store is a Rust service that provides vector search for ScyllaDB.

Before making changes, follow the project's contributor and coding guidelines:

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — pre-review checklist, commit and PR
  organization (subject format, patch structure, Jira references), static checks,
  how to run the tests (unit/integration, the validator harness, and the example
  docker-compose stacks), CI expectations, and the OpenAPI workflow.
- **[docs/rust_instructions.md](docs/rust_instructions.md)** — Rust coding
  conventions and best practices used throughout the codebase.

## Quick reference

- Format and lint before committing, matching CI (warnings are errors):
  ```sh
  cargo fmt --all --check
  cargo clippy --workspace --all-targets -- -Dwarnings
  ```
- Run unit and integration tests: `cargo test --workspace`
  (`--workspace` is needed because `default-members` is only `crates/vector-store`).
- Run the end-to-end validator harness: see the Testing section of
  [CONTRIBUTING.md](CONTRIBUTING.md).
- Do not hand-edit `api/openapi.json`; regenerate it with `cargo openapi`.
- Organize commits and PRs per the Commit and PR Organization section of
  [CONTRIBUTING.md](CONTRIBUTING.md) (subject format `module: changes`, small
  self-contained patches, and `Fixes:`/`Refs: VECTOR-<n>` references).
