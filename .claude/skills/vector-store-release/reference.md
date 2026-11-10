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
commands; empty output reads like "no changes". In 1.15.0 a
`git diff … | grep` pipe printed nothing too, while the same `grep` on a saved
diff found the removed `VECTOR_STORE_FULLTEXT_INDEXES` variable and a changed
log line; step 3.4 therefore greps a file. The helpers
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

A dependency bump can be described as routine while changing what users see.
In 1.15.0, #681 described tantivy 0.26.1 → 0.27.0 in one line, as a fix for
RUSTSEC advisories. tantivy's changelog, checked against the code, gave three
changes for the notes: the query parser that parses `/bm25` query text no
longer panics or loops on malformed input; the stemming library moved from
rust-stemmers to frostem, which stems 165 of 73,604 English dictionary words
differently (measured by stemming the word list with both); and `lru` 0.18.5
fixed an advisory that `deny.toml` had been ignoring.

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
change was the user's call. For 1.15.0 the user chose the same sentence
again: usearch 2.26.6 was measured with no difference beyond noise (#675),
the driver 1.9.0 index build only fell within the spread of earlier builds
(#683), and full-text search on tantivy 0.27 was named as not measured.
Without such a basis, say what was measured and what was not.

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

`:warning:` lines are for what a reader can check or change before upgrading.
The 1.15.0 draft had a third one for tantivy 0.27's stemmer change; the user
moved it to the notes only, since no setting or client change follows from
it. The judgment calls (this one, and how strong the performance statement
is) were both changed at the gate, so step 5 now asks them as separate
questions.

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

## Worked example: 1.15.0 (2026-11-03)

- Previous release 1.14.0; cut `3fb43c1`, the head of master; 19 PRs. Rust
  and Validator (merge queue) and the last three Daily runs green. Open PRs
  mentioned, not waited for: #686 (Greek sigma in case-insensitive pattern
  indexes).
- Changes read from 19 PR descriptions, `lock-diff.txt` (numkong
  `7.8.5+git.23067331805a -> 7.8.6+git.2e43be7d77b5`, tantivy 0.27, scylla
  1.9, opensearch 2.4) and tantivy's changelog, which #681 did not describe.
  The OpenSearch client's move to reqwest 0.13 changed TLS verification to
  the system CA store; the release image (`redhat/ubi9-minimal`) has
  `ca-certificates`, so it was raised at the gate, not written into the notes.
- The user changed two calls at the gate: the performance statement became
  "no changes affecting vector search performance", and the stemmer
  `:warning:` line moved to the notes only. The preview was re-run and the
  final Slack text approved before tagging.
- The workflow ran 42 minutes, 81 jobs, all green. Checks: the Slack post was
  identical to the preview; six assets; `1.15.0` and `latest` with the same
  digest on `linux/amd64` and `linux/arm64`; the siren PR
  (scylladb/siren#16616) carried exactly the images `trigger-siren` selected.
- Retrospective: step 3.3 gained the changelog check for directly used
  crates, step 3.4 greps a saved diff, and step 5 asks the open judgment calls
  separately. The packer template names images with a 12-hour clock
  (`03` in its `isotime` layout), found while checking the siren PR.
