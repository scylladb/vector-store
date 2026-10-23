# vector-store-release: reasoning and worked example

Why [SKILL.md](SKILL.md) says what it says. Each section explains one rule and
the release that taught it.

## The remote

`docs/releasing.md` pushes the tag with `git push origin X.Y.Z`. That is right
only in a clone of `scylladb/vector-store`. Most contributors work from a fork,
where `origin` is the fork and the scylladb repository is a second remote, so
the tag would land in the fork and no release could be published from it
(`gh release create --verify-tag` refuses a tag missing from the repository).
The skill therefore finds the remote by its URL, and reads tags from it with
`git ls-remote` rather than trusting local ones: a fork's checkout can hold a
local `1.7.0` that points at a different commit than upstream's, and
`git fetch --tags` then fails with `would clobber existing tag`.

## Helpers

Some sessions run git through a shell hook that compresses output for the
model. It can print nothing at all for a range such as `git log A..B`, which
was known before 1.14.0, and during 1.14.0 it did so for two `git diff`
commands; empty output reads like "no changes". The helpers
call git with `-c core.pager=cat` from inside a script, which no hook rewrites,
and write files that can be re-read. `collect-changes.sh` also fixes what is
read: every PR body in full, not a truncated excerpt, which is where the
measurements in the notes come from.

`slack-preview.sh` does not reimplement the `announce` job's parsing. It
extracts the job's "Compose the Slack announcement" step from the workflow and
runs it, with a stand-in `gh` that returns the notes file as the release body,
so a change to the job changes the preview too. A copy of the parsing could
drift silently, and a preview that diverged from the job would be worse than
none: the message goes out with `@here`, and correcting it means posting
again.

## What PR descriptions miss

PR descriptions explain a change to its reviewers, not its effect on a
release. Three examples from 1.14.0:

- **The amd64 compiler.** #650 ("frozen toolchain") replaced
  `scripts/release-toolchain.Dockerfile`, which installed clang 19 for arm64
  only and kept gcc 12 for amd64, with `scripts/Dockerfile.toolchain`, which
  makes clang 19 the C and C++ compiler for both. The PR described it as
  versioning the toolchain image. Only the diff of the release files against
  1.13.0 showed that usearch, its distance kernels, aws-lc and mimalloc are
  now compiled by a different compiler on x86. It went into the notes, and
  the user decided how to describe its performance impact.
- **Dependencies in the binary.** A first draft said that none of
  `aws-smithy-json`, `quinn-proto` and `serde_with` (#648) is built into the
  release binary. `cargo tree -p vector-store -e normal -i serde_with` showed
  `serde_with` comes in through the OpenSearch client. `cargo tree -i` also
  fails with "did not match any packages" when two versions of a crate are
  locked; name the version (`crate@x.y.z`) then.
- **A PR with no description.** #663 had an empty body; its title named two
  RUSTSEC ids. The lockfile diff gave the versions and the local advisory
  database gave what the advisories are about.

User-visible strings are the other blind spot: #651 renamed an internal enum
variant, and the user-facing change was the error text in `httproutes.rs`,
which clients may match on.

## Notes format

The format follows the 1.12.0 and 1.13.0 notes, which the team reviewed: one
bold headline per change, the problem and cause before the fix, measured
numbers with their setup, PR numbers in parentheses. Jira keys stay out
because the notes are public and the Jira project is not. Customer names stay
out for the same reason: 1.14.0's #661 was found on a customer cluster, and
the notes describe the trigger (deletes of rows the full scan has not read,
during rebuilds that overlap TTL expiry or bulk deletes) instead.

Performance statements need a source. For 1.14.0 the user asked the notes to
say that the release has no changes affecting vector search performance:
#652 and #657 each measured no difference beyond noise, and the compiler
change was the user's call. Without such a basis, say what was measured and
what was not.

The Slack block is the only part most people read. It is written for someone
deciding whether to upgrade: one line on the kind of release, the fixes that
matter, a `:warning:` per change they must act on, and a recommendation. The
`announce` job adds `@here`, turns `(#n)` into links and appends a link to the
notes.

Security-related dependency updates stay out of the block. The 1.14.0 block
had a `:lock:` line for two RUSTSEC advisories in `rustls-webpki` and
`webpki-roots`. After that release the user decided that such updates are
reported only in the notes' Security & CI section, not in the `@here`
announcement.

## Tag and publish

The tag is pushed alone (`refs/tags/X.Y.Z`): pushing all tags would also
push stale local ones. Publishing the release, not pushing the tag, starts
the workflow, so a tag can be pushed and checked before anything is
announced. `--verify-tag` makes `gh` fail rather than create a lightweight
tag at the default branch's head when the push did not land.

`--latest` marks the GitHub release as latest. The workflow decides Docker
Hub's `latest` separately, from the highest `X.Y.Z` tag, so a patch of an
older series must not be marked latest on GitHub either.

## Worked example: 1.14.0 (2026-10-22)

- Previous release 1.13.0; cut `29a90f3`, the head of master; 17 PRs.
  Rust, Validator and Daily green; CodeQL still running (not a gate).
- Changes read from 17 PR descriptions, `release-files.txt` (which found the
  amd64 compiler change) and the lock diff (usearch 2.26.4 → 2.26.5,
  `rustls-webpki`, `webpki-roots`, `serde_with`, `utoipa` 6).
- The user approved the notes after one change: replacing "x86 performance
  has not been benchmarked" with "no changes affecting vector search
  performance".
- Tag pushed to the scylladb remote, release published with `--latest`. The
  workflow ran 47 minutes (builds 7, validator groups 6, Docker archives and
  SBOMs 3, cloud images 30) and every job succeeded.
- Checks: the Slack post in `#release-announce` was identical to the preview;
  all six assets were attached; `scylladb/vector-store:1.14.0` listed
  `linux/amd64` and `linux/arm64`, and `latest` had the same digest; the siren
  PR (scylladb/siren#16552) was open, waiting for a merge by someone with
  siren rights.
- Retrospective: running step 8 as first written showed that
  `gh run view --job <id> --json steps` is not valid (`steps` is a field of
  `jobs`), and that the job summary holding the posted text cannot be read with
  `gh`; step 8.1 now reads the steps through `--json jobs` and the post itself
  through Slack.
