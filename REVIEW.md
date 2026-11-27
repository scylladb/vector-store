# REVIEW.md

Instructions for the code review agents (Qodo). Contributors and coding agents follow
[CONTRIBUTING.md](CONTRIBUTING.md) and [docs/rust_instructions.md](docs/rust_instructions.md).

## Coding conventions

Review Rust and C changes against [docs/rust_instructions.md](docs/rust_instructions.md), and raise a
finding for each item of its Quality Checklist that a change breaks. Its sections Public API, Unsafe
Code and FFI, Supported Targets, Code From Other Projects, and Testing and Documentation hold the
conventions that new code misses most.
