#!/bin/bash
#
# Pass/fail history of matching Daily jobs, for the daily-ci-triage skill:
# tells a persistent regression from a flaky test and finds the last green /
# first red boundary to bisect (vector-store SHA and scylla-nightly digest).
#
# usage: history.sh <job-name-regex> [runs]
#
#   history.sh 'alternator::'          # every alternator testcase, both arches
#   history.sh '\(reconnect,' 30       # one testcase, last 30 runs
#
# The regex is matched against full job names, e.g.
# "daily-run-validator-tests (alternator::put_item, ubuntu-24.04-arm) / run-validator-tests".
#
# Side effects: read-only `gh run list` / `gh run view` calls; writes nothing.
#
# Env: REPO (default scylladb/vector-store), WORKFLOW (default daily.yml),
#      DIGESTS=0 to skip the scylla-nightly digest lookup (one log fetch per run).
#
# Output: a header, then one tab-separated line per completed run, newest
# first. `matched` is failed/ran among the matching jobs that finished with a
# result; `skipped` counts matching jobs that never ran (e.g. a build failed
# upstream of them) and `cancelled` those cut off before finishing. Neither
# is a pass or a test failure: a cancellation is infra, unless its log shows
# a test that hung until the job was killed. `run` is the whole run's
# conclusion; `saved` says whether the run promoted its digest to
# valid-scylla-nightly-digest (which PR CI then tests against). A run whose
# jobs could not be fetched prints ERR in `matched`. Warnings go to stderr.
#
# Rows are per job, not per failure signature: a red row says the job
# failed, not why. Confirm the signature of the boundary runs with
# extract-failures.sh before calling a failure persistent.

set -euo pipefail

usage() {
    echo "usage: $0 <job-name-regex> [runs]" >&2
    exit 1
}

[[ $# -ge 1 && $# -le 2 ]] || usage
pattern=$1
runs=${2:-14}
[[ $runs =~ ^[0-9]+$ ]] || usage
repo=${REPO:-scylladb/vector-store}
workflow=${WORKFLOW:-daily.yml}
digests=${DIGESTS:-1}

jq -n --arg p "$pattern" '"" | test($p)' >/dev/null 2>&1 || {
    echo "error: invalid regex: $pattern" >&2
    exit 1
}

# Filter and sort locally: `gh run list --status completed` has been seen to
# return an unrelated, months-old page of runs now and then.
list=$(gh run list -R "$repo" --workflow "$workflow" --limit $((runs + 10)) \
    --json databaseId,createdAt,event,headSha,conclusion,status |
    jq -r --argjson n "$runs" '[.[] | select(.status == "completed")]
        | sort_by(.createdAt) | reverse | .[:$n][]
        | [.databaseId, .createdAt, .event, .headSha, .conclusion] | @tsv')

newest=$(head -n1 <<<"$list" | cut -f2)
if [[ -n $newest ]] && (($(date -u +%s) - $(date -u -d "$newest" +%s) > 3 * 86400)); then
    echo "warn: newest completed run is from $newest; the run list may be stale" >&2
fi

printf 'run_id\tcreated\tevent\tsha\tdigest\tmatched\tskipped\tcancelled\trun\tsaved\tfailed_jobs\n'
matched_any=0
while IFS=$'\t' read -r run_id created event sha conclusion; do
    [[ -n $run_id ]] || continue
    # gh reads from /dev/null: left on the loop's stdin, it could swallow the
    # remaining run list.
    if ! jobs=$(gh run view "$run_id" -R "$repo" --json jobs --jq '.jobs' </dev/null 2>/dev/null); then
        echo "warn: run $run_id: could not fetch jobs" >&2
        printf '%s\t%s\t%s\t%s\t-\tERR\t-\t-\t%s\t-\t\n' "$run_id" "$created" "$event" "${sha:0:10}" "$conclusion"
        continue
    fi
    digest="-"
    digest_job=$(jq -r '[.[] | select(.name | test("get-scylla-nightly-digest"))][0].databaseId // empty' <<<"$jobs")
    if [[ $digests != 0 && -n $digest_job ]]; then
        if log=$(gh run view -R "$repo" --job "$digest_job" --log </dev/null 2>/dev/null); then
            digest=$(grep -oE 'sha256:[0-9a-f]{64}' <<<"$log" | tail -n1 | cut -c8-19 || true)
        else
            digest="?"
        fi
    fi
    row=$(jq -r --arg p "$pattern" --arg run_id "$run_id" --arg created "$created" --arg event "$event" \
        --arg sha "${sha:0:10}" --arg digest "${digest:--}" --arg conclusion "$conclusion" '
        [.[] | select(.name | test($p))] as $m
        | [$m[] | select(.conclusion == "skipped")] as $s
        | [$m[] | select(.conclusion == "cancelled")] as $c
        | [$m[] | select(.conclusion != "skipped" and .conclusion != "cancelled")] as $ran
        | [$ran[] | select(.conclusion != "success")] as $f
        | ([.[] | select(.name | test("save-scylla-nightly-digest")) | .conclusion][0] == "success") as $saved
        | [$run_id, $created, $event, $sha, $digest, "\($f | length)/\($ran | length)", ($s | length), ($c | length), $conclusion,
           (if $saved then "yes" else "no" end),
           ([$f[] | .name | sub(" / run-validator-tests$"; "")] | join("; "))] | @tsv
    ' <<<"$jobs")
    [[ $(cut -f6 <<<"$row") == */0 && $(cut -f7 <<<"$row") == 0 && $(cut -f8 <<<"$row") == 0 ]] || matched_any=1
    echo "$row"
done <<<"$list"

if ((matched_any == 0)); then
    echo "warn: no job matched '$pattern' in any run; check the regex against the job names" >&2
fi
