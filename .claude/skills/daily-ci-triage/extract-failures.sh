#!/bin/bash
#
# Condense one GitHub Actions run into a JSON failure summary for the
# daily-ci-triage skill. Raw validator job logs are ~3k lines, almost all of
# them ScyllaDB and git noise; this keeps only what triage needs and saves the
# cleaned full logs next to the summary for deeper digging.
#
# usage: extract-failures.sh <run-id> [out-dir]
#
# Env: REPO (default scylladb/vector-store). Needs gh and jq.
# Side effects: read-only `gh run view` calls; writes and deletes only under
# <out-dir> (it recreates <out-dir>/jobs on every run).
# Writes <out-dir>/summary.json (per failed job: failing tests, panics, errors,
# log tail), <out-dir>/overview.txt (one line per failed job) and the cleaned
# logs under <out-dir>/logs/; prints the two file paths. A log that could not
# be fetched (expired, API error) is reported as log_fetch=failed and warned
# about on stderr, never passed off as an empty log.

set -euo pipefail

usage() {
    echo "usage: $0 <run-id> [out-dir]" >&2
    exit 1
}

[[ $# -ge 1 && $# -le 2 ]] || usage
run_id=$1
[[ $run_id =~ ^[0-9]+$ ]] || usage
repo=${REPO:-scylladb/vector-store}
out=${2:-${TMPDIR:-/tmp}/daily-ci-triage/$run_id}
rm -rf "$out/jobs"
mkdir -p "$out/logs" "$out/jobs"

warn() {
    echo "warn: $*" >&2
}

# grep, except that "no match" is a success, so pipelines survive pipefail.
grep_allow_empty() {
    grep "$@" || [[ $? -eq 1 ]]
}

# `gh run view --log` prefixes each line with "<job>\t<step>\t<timestamp> " and
# carries ANSI colours both as real ESC bytes and as literal "^[[..m" text.
clean_log() {
    cut -f3- |
        sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z ?//' |
        sed -E $'s/\x1b\\[[0-9;]*m//g; s/\\^\\[\\[[0-9;]*m//g'
}

# fetch_log <job-id> <--log|--log-failed> <file>: the raw log goes to a file
# first, so a fetch failure is visible and no reader can SIGPIPE gh.
fetch_log() {
    local err
    if ! err=$(gh run view -R "$repo" --job "$1" "$2" 2>&1 >"$3.raw" </dev/null); then
        warn "job $1: gh run view $2 failed: ${err//$'\n'/ }"
        rm -f "$3.raw"
        return 1
    fi
    clean_log <"$3.raw" >"$3"
    rm -f "$3.raw"
}

gh run view "$run_id" -R "$repo" \
    --json databaseId,url,createdAt,updatedAt,headSha,headBranch,event,conclusion,status,jobs \
    >"$out/run.json"

# The digest job prints the scylla-nightly digest this run tested against.
# Optional: without it, bisecting falls back to the Scylla version banner.
digest=""
digest_job=$(jq -r '[.jobs[] | select(.name | test("get-scylla-nightly-digest"))][0].databaseId // empty' "$out/run.json")
if [[ -n $digest_job ]] && fetch_log "$digest_job" --log "$out/logs/digest.log"; then
    digest=$(grep_allow_empty -oE 'sha256:[0-9a-f]{64}' "$out/logs/digest.log" | tail -n1)
fi

# Messages are normalized the same way in signatures and in the error lists,
# so per-run noise (node IPs, ports) does not split identical lines.
normalize_ips() {
    sed -E 's/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(:[0-9]+)?/<ip>/g'
}

while IFS=$'\t' read -r job_id job_name job_url conclusion; do
    log="$out/logs/$job_id.log"
    # --log-failed is empty for jobs that never reached a step (cancelled,
    # runner lost); fall back to the full log.
    log_fetch=ok
    if ! fetch_log "$job_id" --log-failed "$log" || [[ ! -s $log ]]; then
        fetch_log "$job_id" --log "$log" || { log_fetch=failed; : >"$log"; }
    fi
    log_lines=$(wc -l <"$log")

    # "daily-run-validator-tests (alternator::put_item, ubuntu-24.04-arm) / ...",
    # the older "daily-validator-tests (fts, ubuntu-latest)", and the PR
    # workflow's "validator-run-validator-tests (fts) / ..." without a runner.
    testcase=$(sed -nE 's/^[^(]*\(([^,)]+).*/\1/p' <<<"$job_name")
    runner=$(sed -nE 's/^[^(]*\([^,)]+, ([^)]+)\).*/\1/p' <<<"$job_name")

    # "Scylla version 2026.4.0~dev-0.20260928.173e616dc58c with build-id ..."
    scylla_version=$(grep_allow_empty -m1 -oE 'Scylla version [^ ]+' "$log" | cut -d' ' -f3)

    # "- validator::group::test" lines after the "Tests failed:" and
    # "Tests skipped by fixture errors:" headers of the validator summary.
    list_after() {
        awk -v hdr="$1" '
            index($0, hdr) { on = 1; next }
            on && /^- / { sub(/^- /, ""); print; next }
            on && NF { on = 0 }
        ' "$log"
    }
    failed_tests=$(list_after "Tests failed:" | jq -R . | jq -s .)
    fixture_errors=$(list_after "Tests skipped by fixture errors:" | jq -R . | jq -s .)

    # Pair each "panicked at <site>:" with the following
    # "<span>: failed: task N panicked with message \"...\"", and keep the
    # span-trace frames in our own crates: for a generic wait_for timeout the
    # site is common.rs and only the frames name the test that was waiting.
    # The message stays in its escaped form (\") so quotes inside the wrapped
    # error cannot end the capture early.
    panics=$(awk '
        function emit() {
            if (pending) print site "\t" span "\t" msg "\t" callers
            pending = 0; site = ""; callers = ""
        }
        pending && /╼ / {
            if (match($0, / at crates\/[^ ]+/)) {
                frame = substr($0, RSTART + 4, RLENGTH - 4)
                callers = callers (callers == "" ? "" : " < ") frame
            }
            next
        }
        pending { emit() }
        / ERROR .*panicked at / {
            site = $0; sub(/.*panicked at /, "", site); sub(/:$/, "", site); next
        }
        / ERROR .*: failed: task [0-9]+ panicked with message / {
            span = $0; sub(/^[^ ]+ +ERROR +/, "", span); sub(/: failed: task .*/, "", span)
            msg = $0; sub(/.*panicked with message "/, "", msg); sub(/"$/, "", msg)
            gsub(/\t/, " ", msg); sub(/, raw: Response \{.*/, "", msg)
            msg = substr(msg, 1, 4000); pending = 1
        }
        / ERROR / && !/panicked/ { site = "" }
        END { emit() }
    ' "$log" | jq -R 'split("\t") | {site: .[0], span: .[1], message: .[2], callers: (.[3] // "")}' |
        jq -s '
            # The innermost server/SDK error is what groups failures; the
            # call site and the wrapping expect() text vary per test.
            def inner:
                (capture("message: Some\\(\\\\\"(?<m>(?:[^\\\\]|\\\\[^\"])*)\\\\\"\\)") | .m)
                // (capture("DbError\\([A-Za-z]+, \\\\\"(?<m>(?:[^\\\\]|\\\\[^\"])*)\\\\\"\\)") | .m)
                // .;
            def norm:
                gsub("(?i)(alt-)?idx_[0-9]+"; "<idx>") | gsub("(?i)ksp_[0-9]+"; "<ksp>")
                | gsub("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"; "<uuid>")
                | gsub("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+(:[0-9]+)?"; "<ip>")
                | gsub("(?i)\\b[0-9a-f]{12,}\\b"; "<hex>") | gsub("[0-9]+"; "<n>");
            # One entry per signature, uncapped: a dropped signature is a
            # cause that never reaches triage. Messages are capped instead.
            map(. + {signature: (.message | inner | norm), message: (.message | gsub("\\\\\""; "\"") | .[:800])})
            | group_by(.signature) | map(.[0] + {count: length}) | sort_by(-.count)')

    stats=$(grep_allow_empty -m1 -oE 'test run failed: Statistics \{.*\}' "$log" | sed 's/^test run failed: //')

    # Both the validator and vector-store log "<ts>Z LEVEL ..."; ScyllaDB logs
    # "LEVEL  <date> ..." and is left out. vector-store lines carry a module
    # target ("WARN db: ..."); validator lines and span-prefixed vector-store
    # lines ("db:db-process:db_cdc{..}: ...") land in `errors`, in log order.
    # Lists are limited with `sed -n '1,Np'` rather than `head -n N`: sed
    # reads its whole input, while head exits early and, under pipefail, the
    # SIGPIPE it leaves upstream would abort the script on a long log.
    ts='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z +'
    target='[a-z_]+(::[a-z_]+)*: '
    errors=$(grep_allow_empty -E "${ts}ERROR " "$log" | grep_allow_empty -vE "${ts}ERROR ${target}" |
        grep_allow_empty -vE 'panicked (at|with message)|test run failed: Statistics|Tests (failed|skipped by fixture errors):$' |
        sed -E "s/${ts}ERROR //" | normalize_ips | cut -c1-400 |
        awk '!seen[$0]++' | sed -n '1,20p' | jq -R . | jq -s .)
    # Minus the warnings every healthy run prints while the cluster starts.
    benign='Received message during initialization|Database not yet initialized|TCP keepalive interval to low values'
    vs_errors=$(grep_allow_empty -E "${ts}(WARN|ERROR) ${target}" "$log" | grep_allow_empty -vE "$benign" | sed -E "s/${ts}//" |
        normalize_ips | cut -c1-300 | sort | uniq -c | sort -rn | sed -n '1,15p' | sed -E 's/^ +//' |
        jq -R . | jq -s .)

    # Last lines before the step's "##[error]" marker; the only evidence for
    # build, download and infrastructure failures, so kept only without panics.
    # A cancelled or interrupted job may have no marker: then take the end of
    # the whole log (`1,$`).
    error_line=$(grep_allow_empty -n -m1 '^##\[error\]' "$log" | cut -d: -f1)
    error_tail=$(sed -n "1,${error_line:-\$}p" "$log" | grep_allow_empty -vE '^\s*$' | cut -c1-400 |
        tail -n 25 | jq -R . | jq -s .)

    jq -n \
        --arg id "$job_id" --arg name "$job_name" --arg url "$job_url" \
        --arg conclusion "$conclusion" --arg testcase "$testcase" --arg runner "$runner" \
        --arg scylla_version "$scylla_version" --arg stats "$stats" --arg log "$log" \
        --arg log_fetch "$log_fetch" --arg log_lines "$log_lines" \
        --argjson failed_tests "$failed_tests" --argjson fixture_errors "$fixture_errors" \
        --argjson panics "$panics" --argjson errors "$errors" \
        --argjson vector_store_warnings "$vs_errors" --argjson error_tail "$error_tail" \
        '{id: ($id | tonumber), name: $name, url: $url, conclusion: $conclusion,
          testcase: $testcase, runner: $runner, scylla_version: $scylla_version,
          log_fetch: $log_fetch, log_lines: ($log_lines | tonumber),
          failed_tests: $failed_tests, fixture_errors: $fixture_errors, panics: $panics,
          stats: $stats, errors: $errors, vector_store_warnings: $vector_store_warnings,
          error_tail: (if ($panics | length) > 0 then [] else $error_tail end),
          log: $log}' >"$out/jobs/$job_id.json"
done < <(jq -r '.jobs[]
    | select(.conclusion != "success" and .conclusion != "skipped" and .conclusion != "neutral")
    | [.databaseId, .name, .url, (.conclusion // .status)] | @tsv' "$out/run.json")

if compgen -G "$out/jobs/*.json" >/dev/null; then
    jq -s . "$out"/jobs/*.json >"$out/jobs.json"
else
    echo '[]' >"$out/jobs.json"
fi

# A run can fail with no failed job at all: a startup_failure creates no jobs,
# and a run cancelled before scheduling leaves them all skipped. Say so
# explicitly, or the summary would read as "0 failed jobs" and nothing would
# reach triage.
jq --arg digest "$digest" --slurpfile jobs "$out/jobs.json" \
    '{run_id: .databaseId, url, created_at: .createdAt, updated_at: .updatedAt,
      head_sha: .headSha, head_branch: .headBranch, event, status, conclusion,
      scylla_nightly_digest: $digest,
      jobs_total: (.jobs | length), failed_jobs: $jobs[0],
      run_level_failure: (if (.conclusion // "success") != "success" and ($jobs[0] | length) == 0
          then {conclusion, reason: "the run failed but no job failed (no jobs created, or none ran)"}
          else null end)}' \
    "$out/run.json" >"$out/summary.json"

# One line per failed job, to read before opening summary.json (which runs to
# hundreds of KB when a whole group fails). The last column is the job's
# signatures, or for a job without panics the end of its log tail.
jq -r '"run \(.run_id) \(.event) \(.created_at) sha \(.head_sha[:10]) \(.conclusion) \(.failed_jobs | length)/\(.jobs_total) jobs failed scylla-nightly \(.scylla_nightly_digest[7:19])",
    (if .run_level_failure then "run-level failure: \(.run_level_failure.conclusion), \(.run_level_failure.reason)" else empty end),
    (.failed_jobs[]
        | ([.panics[] | "\(.signature) (x\(.count))"] | join(" | ")) as $sig
        | [.id, .conclusion, .testcase, .runner, .scylla_version,
            "failed=\(.failed_tests | length) fixture_err=\(.fixture_errors | length) log=\(.log_fetch)",
            (.panics[0].site // "-"),
            ((if $sig != "" then $sig else (.error_tail[-2] // .error_tail[-1] // "-") end)
                | gsub("\\s+"; " ") | .[:300])] | @tsv)' \
    "$out/summary.json" >"$out/overview.txt"

echo "$out/summary.json"
echo "$out/overview.txt"
