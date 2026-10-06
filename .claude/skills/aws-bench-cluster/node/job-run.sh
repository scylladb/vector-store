#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Detached job wrapper. vsbench starts it on a node with `systemd-run` (see
# remote.job_start), so the job survives the ssh session that started it.
#
# Usage: job-run.sh <job-id> [--script PATH] [-- ARGV...]
#
# Runs ARGV (or `bash PATH`) with NO_COLOR=1 and line-buffered stdout/stderr,
# both appended to $VSBENCH_JOBS_DIR/<job-id>/log. Files in that directory:
#   started_at  UTC ISO time the command started
#   command     the command line (shell-quoted)
#   log         combined output
#   ended_at    UTC ISO time the job ended     (written before exit_code)
#   exit_code   exit status; 128+N when stopped by signal N
# ended_at and exit_code are written atomically (tmp + mv) on success, on
# failure and when the job is stopped (SIGTERM/SIGINT/SIGHUP, e.g. by
# `systemctl stop`). Only SIGKILL leaves no exit_code behind; vsbench then
# reports the job as lost (or `job cancel` writes 143).
set -euo pipefail

: "${VSBENCH_JOBS_DIR:=/var/lib/vsbench/jobs}"

utc_now() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

write_atomic() {
  local tmp="$1.tmp.$$"
  printf '%s\n' "$2" >"$tmp"
  mv -f "$tmp" "$1"
}

fail_usage() {
  echo "job-run.sh: $1" >&2
  echo "usage: job-run.sh <job-id> [--script PATH] [-- ARGV...]" >&2
  exit 2
}

if [[ $# -lt 1 ]]; then
  fail_usage "missing job id"
fi
job_id=$1
shift
if [[ ! $job_id =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
  fail_usage "invalid job id '$job_id'"
fi
dir=$VSBENCH_JOBS_DIR/$job_id
mkdir -p "$dir"
if [[ -e $dir/exit_code ]]; then
  # Never clobber the record of an earlier run with the same id.
  fail_usage "job $job_id already ran (found $dir/exit_code)"
fi

exec >>"$dir/log" 2>&1

child=""
rc=""

# Start a marker line on a fresh line even when the output did not end with one.
# shellcheck disable=SC2329  # used by the trap handlers
fresh_line() {
  if [[ -n $(tail -c 1 "$dir/log" 2>/dev/null) ]]; then
    echo
  fi
}

# shellcheck disable=SC2329  # invoked by the EXIT trap
on_exit() {
  local status=$?
  trap '' TERM INT HUP
  if [[ -z $rc ]]; then
    rc=$status
  fi
  { fresh_line && echo "=== VSBENCH JOB END $job_id exit=$rc $(utc_now)"; } || true
  write_atomic "$dir/ended_at" "$(utc_now)"
  write_atomic "$dir/exit_code" "$rc"
}

# $1: received signal, $2: exit status to record. The child gets SIGTERM
# (background commands of a non-interactive shell ignore SIGINT).
# shellcheck disable=SC2329  # invoked by the signal traps
on_signal() {
  trap '' TERM INT HUP
  { fresh_line && echo "=== VSBENCH JOB SIGNAL $1 $(utc_now)"; } || true
  if [[ -n $child ]]; then
    kill -s TERM "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
  fi
  rc=$2
  exit "$2"
}

trap on_exit EXIT
trap 'on_signal TERM 143' TERM
trap 'on_signal INT 130' INT
trap 'on_signal HUP 129' HUP

script=""
if [[ ${1:-} == --script ]]; then
  if [[ $# -lt 2 ]]; then
    fail_usage "--script needs a path"
  fi
  script=$2
  shift 2
fi
if [[ ${1:-} == -- ]]; then
  shift
fi
if [[ -n $script ]]; then
  if [[ $# -gt 0 ]]; then
    fail_usage "--script and ARGV are mutually exclusive"
  fi
  if [[ ! -r $script ]]; then
    fail_usage "script not readable: $script"
  fi
  cmd=(bash "$script")
else
  if [[ $# -eq 0 ]]; then
    fail_usage "nothing to run"
  fi
  cmd=("$@")
fi

write_atomic "$dir/command" "$(printf '%q ' "${cmd[@]}")"
write_atomic "$dir/started_at" "$(utc_now)"
echo "=== VSBENCH JOB START $job_id $(utc_now)"

export NO_COLOR=1
stdbuf -oL -eL "${cmd[@]}" &
child=$!
status=0
wait "$child" || status=$?
child=""
rc=$status
exit "$rc"
