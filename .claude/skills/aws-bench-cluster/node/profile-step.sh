#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# One bounded CPU profile of a process during a benchmark step. `vsbench bench
# search --perf NODE` starts it on that node as a detached job (job-run.sh)
# when a measured step begins, so it survives the operator's ssh session.
#
# Inputs (environment):
#   OUT_DIR    where perf.txt, perf-dso.txt and pidstat.txt land (required)
#   RECORD_S   seconds to record (required)
#   DELAY_S    seconds to wait first, so the capture lands inside the measured
#              window after the job start latency and the tool's start delay (default 4)
#   PROCESS    name of the process to profile (default vector-store)
#   CONTAINER  profile the main process of this docker container instead (e.g. scylla)
#   FREQ       perf sampling frequency in Hz (default 199)
#
# Needs perf (linux-tools) and pidstat (sysstat); userdata.sh installs them on
# scylla and vs nodes. The perf.data is deleted after the reports are written.
set -euo pipefail
: "${OUT_DIR:?}" "${RECORD_S:?}"
DELAY_S=${DELAY_S:-4}
PROCESS=${PROCESS:-vector-store}
CONTAINER=${CONTAINER:-}
FREQ=${FREQ:-199}

command -v perf >/dev/null || { echo "profile: perf is not installed (linux-tools); nothing recorded"; exit 1; }
command -v pidstat >/dev/null || { echo "profile: pidstat is not installed (sysstat); nothing recorded"; exit 1; }
mkdir -p "$OUT_DIR"
sleep "$DELAY_S"

ns_opt=()
if [[ -n $CONTAINER ]]; then
    pid=$(sudo docker inspect -f '{{.State.Pid}}' "$CONTAINER" 2>/dev/null || echo 0)
    ns_opt=(--namespaces)  # resolve the container's binaries through /proc/<pid>/root
else
    pid=$(pidof -s "$PROCESS" || echo 0)
fi
[[ $pid =~ ^[0-9]+$ && $pid -gt 0 ]] || { echo "profile: no running process for ${CONTAINER:-$PROCESS}"; exit 1; }
echo "profile: pid $pid for ${RECORD_S}s from $(date -u +%FT%TZ)"

intervals=$((RECORD_S / 5))
((intervals >= 1)) || intervals=1
# The redirects below are done by this (ubuntu) shell on purpose: the reports stay owned by
# ubuntu, which is what the operator's scp needs; only the sampling itself needs root.
# shellcheck disable=SC2024
sudo pidstat -u -p "$pid" 5 "$intervals" >"$OUT_DIR/pidstat.txt" &
if ! sudo perf record "${ns_opt[@]}" -F "$FREQ" -g -p "$pid" -o "$OUT_DIR/perf.data" -- sleep "$RECORD_S" \
    2>"$OUT_DIR/perf-record.log"; then
    echo "profile: perf record failed:"
    tail -n 3 "$OUT_DIR/perf-record.log"
    wait
    exit 1
fi
wait
# shellcheck disable=SC2024
sudo perf report -i "$OUT_DIR/perf.data" --stdio --no-children --percent-limit 0.5 2>/dev/null >"$OUT_DIR/perf.txt"
# shellcheck disable=SC2024
sudo perf report -i "$OUT_DIR/perf.data" --stdio --no-children --sort dso --percent-limit 0.5 2>/dev/null \
    >"$OUT_DIR/perf-dso.txt"
sudo rm -f "$OUT_DIR/perf.data"  # tens of MB; the text reports are what gets pulled
echo "profile: done at $(date -u +%FT%TZ)"
