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

## Skills

Repository skills live in `.claude/skills/`:

- `daily-ci-triage` — triage failed runs of the Daily workflow (`daily.yml`):
  group failures by cause, run an initial investigation (history, bisect over
  vector-store commits and scylla-nightly builds, fixes in flight), check the
  VECTOR Jira project for duplicates, then comment on the existing issue or file
  a new one. Built to run unattended as a daily routine; `dry-run` writes
  nothing to Jira. `SKILL.md` holds the procedure; `reference.md` holds the
  reasoning and worked examples from past Daily failures.
- `aws-bench-cluster` — provision an AWS benchmark cluster (Scylla, Vector
  Store and a client with scylla-monitoring and `vector-search-benchmark`)
  through the `vsbench` CLI; deploy Vector Store from a Docker Hub release, an
  upstream git ref or the local working tree (cross-compiled for arm64), run
  and compare benchmarks, query Prometheus/node_exporter, run commands on the
  nodes, and tear down. Every session ends with a retrospective that proposes
  improvements to the skill. How to use it, with example prompts:
  [docs/benchmarking.md](docs/benchmarking.md). Unit tests:
  `python3 -m unittest discover -s .claude/skills/aws-bench-cluster/tests -t .claude/skills/aws-bench-cluster`.
