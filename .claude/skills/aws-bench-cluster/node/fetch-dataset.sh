#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Download one catalog dataset (datasets.json) into DATASET_DIR. vsbench runs it
# on the client node, as ubuntu, as the `fetch` step of a job (bench.py).
#
# Env:
#   DATASET_DIR  target directory, e.g. /var/lib/vsbench/datasets/cohere-1m
#   BASE_URL     URL of the dataset directory (files are BASE_URL/<name>)
#   FILES        newline-separated "<name> <bytes>" lines; bytes "-" means
#                "use the Content-Length of a HEAD request"
#   PARALLEL     concurrent downloads (default 4)
#
# Each file is downloaded to <name>.part (resumed with `curl -C -`) and renamed
# to <name> once its size is right. Files that are already complete are
# skipped, so a rerun only fetches what is missing. When every file is
# complete, DATASET_DIR/.complete lists them as "<name> <bytes>" lines.
set -euo pipefail

: "${DATASET_DIR:?}"
: "${BASE_URL:?}"
: "${FILES:?}"
PARALLEL=${PARALLEL:-4}
ATTEMPTS=3
RETRY_PAUSE_S=5
SPARE_BYTES=$((1024 * 1024 * 1024))
CURL_RETRY=(--retry 5 --retry-delay 5 --retry-all-errors)

log() {
  echo "vsbench: $*"
}

fail() {
  echo "vsbench: error: $*"
  exit 1
}

file_size() {
  stat -c %s -- "$1" 2>/dev/null || echo -1
}

remote_size() {
  local length
  length=$(curl -fsSIL --max-time 60 "${CURL_RETRY[@]}" "$1" |
    tr -d '\r' | awk 'tolower($1) == "content-length:" { n = $2 } END { print n }') || return 1
  if [[ ! $length =~ ^[0-9]+$ ]]; then
    return 1
  fi
  echo "$length"
}

# fetch_one NAME BYTES: download BASE_URL/NAME into NAME (via NAME.part). Failed
# transfers are retried (resuming); a completed transfer of the wrong size is not.
fetch_one() {
  local name=$1 expected=$2 url=$BASE_URL/$1 part=$1.part attempt size
  for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    size=$(file_size "$part")
    if ((size > expected)); then
      log "warning: $part is larger than $expected bytes; downloading it again"
      rm -f -- "$part"
      size=-1
    fi
    if ((size != expected)) && ! curl -fsSL "${CURL_RETRY[@]}" -C - -o "$part" "$url"; then
      log "warning: download of $name failed (attempt $attempt of $ATTEMPTS)"
      sleep "$RETRY_PAUSE_S"
      continue
    fi
    size=$(file_size "$part")
    if ((size == expected)); then
      mv -f -- "$part" "$name"
      log "fetched $name ($expected bytes)"
      return 0
    fi
    echo "vsbench: error: $url has $size bytes but the catalog expects $expected (update datasets.json)"
    return 1
  done
  echo "vsbench: error: could not fetch $url after $ATTEMPTS attempts"
  return 1
}

names=()
sizes=()
while IFS= read -r line; do
  if [[ -z ${line//[[:space:]]/} ]]; then
    continue
  fi
  if [[ ! $line =~ ^[[:space:]]*([A-Za-z0-9][A-Za-z0-9._-]*)[[:space:]]+([0-9]+|-)[[:space:]]*$ ]]; then
    fail "invalid FILES line: '$line' (expected '<name> <bytes>')"
  fi
  names+=("${BASH_REMATCH[1]}")
  sizes+=("${BASH_REMATCH[2]}")
done <<<"$FILES"
if [[ ${#names[@]} -eq 0 ]]; then
  fail "FILES lists no files"
fi
if [[ ! $PARALLEL =~ ^[1-9][0-9]*$ ]]; then
  fail "invalid PARALLEL '$PARALLEL'"
fi

mkdir -p -- "$DATASET_DIR"
cd -- "$DATASET_DIR"

# The benchmark loads every *.parquet whose name contains "train": a stray one
# (e.g. shuffle_train.parquet) would silently upload the data twice.
for path in *train*.parquet; do
  if [[ ! -e $path ]]; then
    continue
  fi
  listed=0
  for name in "${names[@]}"; do
    if [[ $name == "$path" ]]; then
      listed=1
    fi
  done
  if [[ $listed -eq 0 ]]; then
    fail "$DATASET_DIR/$path is not part of this dataset but would be loaded as train data; remove it"
  fi
done

for i in "${!names[@]}"; do
  if [[ ${sizes[i]} == - ]]; then
    sizes[i]=$(remote_size "$BASE_URL/${names[i]}") || fail "cannot get the size of $BASE_URL/${names[i]}"
  fi
done

missing=0
needed=0
total=0
for i in "${!names[@]}"; do
  total=$((total + sizes[i]))
  if [[ $(file_size "${names[i]}") != "${sizes[i]}" ]]; then
    missing=$((missing + 1))
    have=$(file_size "${names[i]}.part")
    if ((have < 0 || have > sizes[i])); then
      have=0
    fi
    needed=$((needed + sizes[i] - have))
  fi
done

if [[ $missing -gt 0 ]]; then
  rm -f -- .complete
  avail=$(df -B1 --output=avail . | tail -n 1 | tr -d ' ')
  if ((avail < needed + SPARE_BYTES)); then
    fail "not enough disk space in $DATASET_DIR: need $((needed / 1000000)) MB (+1 GiB spare), have $((avail / 1000000)) MB"
  fi
  log "fetching $missing of ${#names[@]} files ($((needed / 1000000)) MB) from $BASE_URL"
  running=0
  status=0
  for i in "${!names[@]}"; do
    if [[ $(file_size "${names[i]}") == "${sizes[i]}" ]]; then
      continue
    fi
    if ((running >= PARALLEL)); then
      wait -n || status=1
      running=$((running - 1))
    fi
    fetch_one "${names[i]}" "${sizes[i]}" &
    running=$((running + 1))
  done
  while ((running > 0)); do
    wait -n || status=1
    running=$((running - 1))
  done
  if [[ $status -ne 0 ]]; then
    fail "some files could not be downloaded; rerun to resume"
  fi
fi

for i in "${!names[@]}"; do
  if [[ $(file_size "${names[i]}") != "${sizes[i]}" ]]; then
    fail "${names[i]} is incomplete after the download"
  fi
done
for i in "${!names[@]}"; do
  printf '%s %s\n' "${names[i]}" "${sizes[i]}"
done >.complete.tmp
mv -f .complete.tmp .complete
log "dataset complete in $DATASET_DIR (${#names[@]} files, $((total / 1000000)) MB)"
