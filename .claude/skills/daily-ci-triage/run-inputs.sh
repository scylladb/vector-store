#!/bin/bash
#
# The two inputs a Daily run tested, for bisecting between runs in the
# daily-ci-triage skill: the vector-store commit and the scylla-nightly build
# (digest and "Scylla version" banner, whose last field is the scylladb commit).
# Works on green runs too, which extract-failures.sh does not look at.
#
# usage: run-inputs.sh <run-id>
#
# Env: REPO (default scylladb/vector-store).
# Side effects: read-only `gh run view` calls; writes nothing.
# Output: one line of "key=value" pairs; "-" means the run has no such value,
# "?" means it could not be fetched (a warning says why, on stderr).

set -euo pipefail

[[ $# -eq 1 && $1 =~ ^[0-9]+$ ]] || {
    echo "usage: $0 <run-id>" >&2
    exit 1
}
run_id=$1
repo=${REPO:-scylladb/vector-store}

run=$(gh run view "$run_id" -R "$repo" --json createdAt,event,headSha,conclusion,jobs)

# job_log <job-id>: the whole log in a variable, so grep -m1 cannot SIGPIPE gh
# and a fetch failure is told apart from a missing value.
job_log() {
    gh run view -R "$repo" --job "$1" --log </dev/null 2>/dev/null
}

digest=-
digest_job=$(jq -r '[.jobs[] | select(.name | test("get-scylla-nightly-digest"))][0].databaseId // empty' <<<"$run")
if [[ -n $digest_job ]]; then
    if log=$(job_log "$digest_job"); then
        digest=$(grep -oE 'sha256:[0-9a-f]{64}' <<<"$log" | tail -n1 || true)
    else
        echo "warn: run $run_id: could not fetch the digest job log" >&2
        digest="?"
    fi
fi

# Any validator job that got as far as starting Scylla prints the banner; the
# logs are ~1 MB each, so try passed jobs first and stop at the first hit.
# There is no cap: only when most jobs failed before Scylla started does
# this fetch more than one log, and that is when the version matters most.
scylla_version=-
fetch_failed=0
for job in $(jq -r '[.jobs[] | select(.name | test("validator-tests"))]
        | sort_by(.conclusion != "success") | .[] | .databaseId' <<<"$run"); do
    if ! log=$(job_log "$job"); then
        fetch_failed=1
        continue
    fi
    scylla_version=$(grep -m1 -oE 'Scylla version [^ ]+' <<<"$log" | cut -d' ' -f3 || true)
    [[ -z $scylla_version ]] || break
    scylla_version=-
done
if [[ $scylla_version == - && $fetch_failed == 1 ]]; then
    echo "warn: run $run_id: could not fetch validator job logs" >&2
    scylla_version="?"
fi

jq -r --arg run_id "$run_id" --arg digest "${digest:--}" --arg version "$scylla_version" '
    "run=\($run_id) created=\(.createdAt) event=\(.event) conclusion=\(.conclusion) sha=\(.headSha)"
    + " digest=\($digest) scylla_version=\($version)"
    + " scylla_commit=\(if ($version | test("\\.")) then ($version | split(".") | last) else $version end)"
    + " digest_saved=\([.jobs[] | select(.name | test("save-scylla-nightly-digest")) | .conclusion][0] == "success")"
' <<<"$run"
