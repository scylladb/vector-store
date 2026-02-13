# Contributing to Vector Store

Thank you for your interest in contributing! To help us maintain a high-quality codebase, please follow the guidelines below.

## Pre-Review Checklist

Before submitting a pull request (PR), ensure your contribution meets these requirements:

- **Single Change:** PR should consist of a single business change. If implementation requires multiple logical changes, split them into separate PRs. Each PR can contain multiple commits, but they should all relate to the same single logical change.
- **Change Quality:** Each PR should introduce one logically coherent change. PR and commit messages must clearly describe what is being changed and why.
- **Issue Closing Reference** If PR fixes/closes an issue, add a `Fixes: #XYZ` line at the end of PR. If you need you can also add it to the commit message.
- **Issue References:** For related issues or discussions that are not directly fixed by the PR, add a `Reference: #XYZ` line at the end of PR message. If you need you can also add it to the commit message.
- **Testing:** All new features and bug fixes must include appropriate tests.
- **PR Checks:** Every PR must pass all CI checks, including compilation, test execution, formatting, and static code analysis.
- **PR Description:** Clearly explain the motivation and reasoning behind your changes in the PR description.

If you cannot meet any of these requirements, explain why in your PR description. Maintainers may grant exceptions if justified.

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

The project includes unit and integration tests to ensure correctness.

**Run all tests with verbose output:**
```sh
cargo test --verbose
```

## Continuous Integration (CI)

We use GitHub Actions for CI, configured in `rust.yml`.

**All checks must pass before a pull request can be merged.**

## OpenAPI Specification

Vector Store exposes an HTTP REST API, documented in the OpenAPI specification file at `api/openapi.json`.

**Important:** Do not manually edit `api/openapi.json`. This file is generated directly from the source code.
To update the OpenAPI specification based on your code changes, run the following command:

```sh
cargo openapi
```

The `api/openapi.json` file must always be synchronized with the actual API definition in the source code.
This synchronization is enforced by an integration test in our CI pipeline.

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
