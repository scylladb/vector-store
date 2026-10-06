#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Helpers for the step scripts of benchmark jobs. bench.py writes each job as a
# steps.sh that sources this file and then calls `step` once per step; it runs
# on the client node under node/job-run.sh, so stdout is the job log.
#
#   step NAME [--timeout DURATION] -- CMD [ARGS...]
#
# prints "=== VSBENCH STEP BEGIN <name> <utc>" and "=== VSBENCH STEP END <name>
# <exit> <utc>" (each on a fresh line) around CMD; results.step_sections() splits
# the log on these markers. External commands run with NO_COLOR=1, under
# `timeout DURATION` when given, and under `taskset -c $VSBENCH_CPUSET` (all CPUs
# but 7 on nodes with 8+ CPUs: Prometheus and Grafana are pinned to CPU 7).
# Shell functions (e.g. wait_index_gone) run without timeout/taskset. A failing
# step ends the job with its exit code (timeout: 124).
#
# Optional env: VSBENCH_CPUSET overrides the CPU list ("" disables taskset).
set -euo pipefail

STEP_KILL_AFTER=60
STEP_NAME_RE='^[A-Za-z0-9][A-Za-z0-9_.-]*$'

utc_now() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

# The default CPU list: every CPU except 7 when there are at least 8.
default_cpuset() {
  local n
  n=$(nproc)
  if ((n == 8)); then
    echo "0-6"
  elif ((n > 8)); then
    echo "0-6,8-$((n - 1))"
  fi
}

# Start the next marker on a fresh line even when the output did not end in one.
fresh_line() {
  local out=/proc/$$/fd/1
  if [[ -f $out && -n $(tail -c 1 "$out" 2>/dev/null) ]]; then
    echo
  fi
}

step_usage() {
  echo "vsbench: error: usage: step NAME [--timeout DURATION] -- CMD [ARGS...]"
  exit 2
}

step() {
  local name=${1:-} limit="" rc=0
  shift || true
  if [[ ${1:-} == --timeout ]]; then
    limit=${2:-}
    shift 2 || step_usage
  fi
  if [[ ${1:-} == -- ]]; then
    shift
  fi
  if [[ ! $name =~ $STEP_NAME_RE || $# -eq 0 ]]; then
    step_usage
  fi
  local -a wrap=()
  if [[ $(type -t "$1") != function ]]; then
    if [[ -n $limit ]]; then
      wrap+=(timeout "--kill-after=$STEP_KILL_AFTER" "$limit")
    fi
    if [[ -n $VSBENCH_CPUSET ]]; then
      wrap+=(taskset -c "$VSBENCH_CPUSET")
    fi
  fi
  fresh_line
  echo "=== VSBENCH STEP BEGIN $name $(utc_now)"
  NO_COLOR=1 "${wrap[@]}" "$@" || rc=$?
  fresh_line
  echo "=== VSBENCH STEP END $name $rc $(utc_now)"
  if [[ $rc -ne 0 ]]; then
    exit "$rc"
  fi
}

# wait_index_gone KEYSPACE INDEX TIMEOUT_S URL...: poll GET <url>/api/v1/indexes
# on every Vector Store URL until KEYSPACE.INDEX is no longer listed (a new
# index built while VS still lists a dropped one can be reported ready at once).
wait_index_gone() {
  local keyspace=$1 index=$2 limit=$3 url body="" found
  shift 3
  local deadline=$((SECONDS + limit))
  if ! command -v jq >/dev/null; then
    echo "vsbench: error: jq is not installed"
    return 1
  fi
  for url in "$@"; do
    while true; do
      found=error
      if body=$(curl -fsS --max-time 10 "$url/api/v1/indexes" 2>&1); then
        found=$(jq -r --arg ks "$keyspace" --arg ix "$index" \
          'any(.[]; .keyspace == $ks and .index == $ix)' <<<"$body" 2>/dev/null) || found=error
      fi
      if [[ $found == false ]]; then
        echo "vsbench: index $keyspace.$index is gone from $url"
        break
      fi
      if ((SECONDS >= deadline)); then
        echo "vsbench: error: $url still lists $keyspace.$index after ${limit}s (last answer: ${body:0:200})"
        return 1
      fi
      sleep 2
    done
  done
}

VSBENCH_CPUSET=${VSBENCH_CPUSET-$(default_cpuset)}
fresh_line
echo "vsbench: cpuset=${VSBENCH_CPUSET:-all} nproc=$(nproc)"
