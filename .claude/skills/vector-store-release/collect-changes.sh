#!/bin/bash
#
# Everything the vector-store-release skill reads to write the release notes:
# the PRs merged between two revisions, each PR's description, and the changes
# that PR descriptions tend to leave out (release toolchain, workflows,
# dependency versions, API document).
#
# usage: collect-changes.sh <previous-sha> <cut-sha> <out-dir>
#
# Env: REPO (default scylladb/vector-store).
# Pass commits, not tag names: the skill fetches without tags, and a local tag
# can point elsewhere than upstream's (1.7.0 does). A release's commit is the
# `^{}` line of `git ls-remote <remote> 'refs/tags/X.Y.Z^{}'`. Both commits
# must exist locally: fetch them from the scylladb/vector-store remote first.
# Side effects: read-only `gh pr view` calls; writes only into <out-dir>.
# Output files in <out-dir>:
#   commits.tsv    first-parent commits, oldest first: sha, PR number ("-" if
#                  none), subject
#   pr-<n>.md      title, author, labels and description of PR <n>
#   lock-diff.txt  Cargo.lock packages added, removed or changed in version or
#                  in source; a version taken from git is marked with its
#                  commit, "+git.<12 hex digits>"
#   release-files.txt
#                  stat and diff of the files that decide how the release is
#                  built, validated and what it exposes, beyond the Rust
#                  sources
# A one-line summary of each file goes to stdout.

set -euo pipefail

# sort and join must agree on the order of crate names.
export LC_ALL=C

[[ $# -eq 3 ]] || {
    echo "usage: $0 <previous-sha> <cut-sha> <out-dir>" >&2
    exit 1
}

prev=$1
cut=$2
out=$3
repo=${REPO:-scylladb/vector-store}

# Plain git, no pager, no aliases: the output is parsed.
g() { git -c core.pager=cat -c color.ui=never "$@"; }

for rev in "${prev}" "${cut}"; do
    if g show-ref --verify --quiet "refs/tags/${rev}"; then
        echo "error: ${rev} is a tag name; pass the commit that git ls-remote prints for it on the ${repo} remote" >&2
        exit 1
    fi
    g rev-parse -q --verify "${rev}^{commit}" > /dev/null || {
        echo "error: ${rev} is not a local commit; fetch it from the ${repo} remote (in a shallow clone, with --unshallow)" >&2
        exit 1
    }
done
g merge-base --is-ancestor "${prev}" "${cut}" || {
    echo "error: ${prev} is not an ancestor of ${cut}" >&2
    exit 1
}

mkdir -p "${out}"
# Files of an earlier run on the same directory would read as part of this one.
rm -f "${out}"/pr-*.md

# A squash or merge commit of a PR ends its subject with "(#<n>)".
g log --first-parent --reverse --format='%h%x09%s' "${prev}..${cut}" \
    | awk -F'\t' '{
        pr = "-"
        if (match($2, /\(#[0-9]+\)$/)) pr = substr($2, RSTART + 2, RLENGTH - 3)
        print $1 "\t" pr "\t" $2
      }' > "${out}/commits.tsv"

prs=0
while IFS=$'\t' read -r _ pr _; do
    [[ ${pr} == - ]] && continue
    gh pr view "${pr}" -R "${repo}" --json title,author,labels,body \
        -q '"# " + .title + "\n\nAuthor: " + .author.login
            + "\nLabels: " + ([.labels[].name] | join(", "))
            + "\n\n" + .body' > "${out}/pr-${pr}.md"
    prs=$((prs + 1))
done < "${out}/commits.tsv"

# "name version" pairs of each lockfile; a package can appear in several
# versions. A version from git, e.g. through [patch.crates-io], is marked with
# the commit it is locked to, so a switch between crates.io and git, or to
# another commit, shows even when the version stays.
lock_versions() {
    g show "$1:Cargo.lock" | awk '
        function flush() { if (name != "") print name " " v (git != "" ? "+git." git : "") }
        /^\[\[package\]\]/ { flush(); name = ""; v = ""; git = "" }
        /^name = / { name = $3; gsub(/"/, "", name) }
        /^version = / { v = $3; gsub(/"/, "", v) }
        /^source = "git\+/ { git = $3; sub(/.*#/, "", git); gsub(/"/, "", git); git = substr(git, 1, 12) }
        END { flush() }' | sort -u
}
join -a1 -a2 -e - -o 0,1.2,2.2 \
    <(lock_versions "${prev}" | awk '{ a[$1] = a[$1] (a[$1] ? "," : "") $2 } END { for (n in a) print n, a[n] }' | sort) \
    <(lock_versions "${cut}" | awk '{ a[$1] = a[$1] (a[$1] ? "," : "") $2 } END { for (n in a) print n, a[n] }' | sort) \
    | awk '$2 != $3 { printf "%-40s %s -> %s\n", $1, $2, $3 }' > "${out}/lock-diff.txt"

release_paths=(
    rust-toolchain.toml
    .cargo/config.toml
    scripts/
    packer/
    .github/workflows/release.yaml
    .github/workflows/build-vector-store.yml
    .github/workflows/build-validator.yml
    .github/workflows/get-scylla-nightly-digest.yml
    .github/workflows/get-validator-test-list.yml
    .github/workflows/run-validator-tests.yml
    .github/workflows/release-toolchain.yaml
    api/openapi.json
    deny.toml
    Cargo.toml
    crates/vector-store/Cargo.toml
)
{
    g diff --stat "${prev}" "${cut}" -- "${release_paths[@]}"
    echo
    g diff "${prev}" "${cut}" -- "${release_paths[@]}"
} > "${out}/release-files.txt"

echo "commits.tsv: $(wc -l < "${out}/commits.tsv") first-parent commits, ${prs} with a PR"
awk -F'\t' '$2 == "-" { print "  without a PR: " $1 " " $3 }' "${out}/commits.tsv"
echo "lock-diff.txt: $(wc -l < "${out}/lock-diff.txt") packages changed"
echo "release-files.txt: $(g diff --name-only "${prev}" "${cut}" -- "${release_paths[@]}" | wc -l) files changed"
