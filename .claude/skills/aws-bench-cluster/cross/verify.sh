#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Gate for cross-built aarch64 binaries (vector-store, vector-search-benchmark).
# Exits 1 when any check fails:
#   - a binary is missing or is not an ARM aarch64 ELF;
#   - it needs a GLIBC symbol version > MAX_GLIBC or a GLIBCXX one > MAX_GLIBCXX;
#   - `qemu-aarch64 <bin> --version` does not print "<bin> EXPECTED_VERSION";
#   - the usearch build script fell back from a SIMD backend ("Failed to compile"
#     in TARGET_DIR/.../build/usearch-*/output; Cargo hides that warning for
#     registry crates, so it would otherwise go unnoticed).
# It also prints NEEDED libs, the .comment toolchain strings and the number of
# simsimd SVE kernels (informational).
#
# Usage: verify.sh DIR EXPECTED_VERSION TARGET_DIR
#   DIR               directory with the two binaries (cross-build.sh -o)
#   EXPECTED_VERSION  version given to cross-build.sh -v
#   TARGET_DIR        cargo target dir given to cross-build.sh -t
# Env: CROSS_IMAGE (required, e.g. vs-bench-cross:1.97.1),
#      MAX_GLIBC (default 2.34), MAX_GLIBCXX (default 3.4.29).
# The binary checks run inside CROSS_IMAGE: the script mounts itself and
# re-runs with `--inside EXPECTED_VERSION` in the binaries directory.
#
set -euo pipefail

TARGET=aarch64-unknown-linux-gnu
BINARIES=(vector-store vector-search-benchmark)
failures=0

fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

# version_le A B: true when A <= B in version order.
version_le() {
    [[ $(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n 1) == "$1" ]]
}

# max_symbol_version BINARY PREFIX: the highest PREFIX_x.y.z the binary needs (empty if none).
max_symbol_version() {
    { aarch64-linux-gnu-objdump -T "$1" | grep -oE "${2}_[0-9]+(\.[0-9]+)*" || true; } \
        | sed "s/^${2}_//" | sort -V | tail -n 1
}

check_symbols() {
    local bin=$1 glibc glibcxx
    glibc=$(max_symbol_version "$bin" GLIBC)
    glibcxx=$(max_symbol_version "$bin" GLIBCXX)
    echo "max GLIBC: ${glibc:-none}, max GLIBCXX: ${glibcxx:-none}"
    if [[ -z $glibc ]]; then
        fail "$bin: no GLIBC symbol versions found"
    elif ! version_le "$glibc" "$MAX_GLIBC"; then
        fail "$bin: needs GLIBC_$glibc, newer than the allowed GLIBC_$MAX_GLIBC"
    fi
    if [[ -n $glibcxx ]] && ! version_le "$glibcxx" "$MAX_GLIBCXX"; then
        fail "$bin: needs GLIBCXX_$glibcxx, newer than the allowed GLIBCXX_$MAX_GLIBCXX"
    fi
}

check_version() {
    local bin=$1 expected=$2 printed
    if ! printed=$(qemu-aarch64 -L /usr/aarch64-linux-gnu "./$bin" --version 2>&1); then
        fail "$bin: qemu smoke run failed: $printed"
    elif [[ $printed != "$bin $expected" ]]; then
        fail "$bin: --version printed '$printed', expected '$bin $expected'"
    else
        echo "qemu --version: $printed"
    fi
}

check_binary() {
    local bin=$1 expected=$2 desc
    echo "===== $bin"
    if [[ ! -f $bin || ! -x $bin ]]; then
        fail "$bin: missing or not executable"
        return 0
    fi
    desc=$(file -b "$bin")
    echo "file: $desc"
    if [[ $desc != *"ARM aarch64"* ]]; then
        fail "$bin: not an ARM aarch64 binary"
        return 0
    fi
    echo "size bytes: $(stat -c %s "$bin")"
    aarch64-linux-gnu-readelf -d "$bin" | grep NEEDED || true
    check_symbols "$bin"
    aarch64-linux-gnu-readelf -p .comment "$bin" | grep -E 'GCC|rustc' || true
    if [[ $bin == vector-store ]]; then
        echo "simsimd SVE kernels: $(aarch64-linux-gnu-nm "$bin" | grep -c 'simsimd_.*_sve' || true)"
    fi
    check_version "$bin" "$expected"
}

# Runs inside the cross image, in the binaries directory.
inside() {
    local expected=$1 bin
    : "${MAX_GLIBC:?}" "${MAX_GLIBCXX:?}"
    for bin in "${BINARIES[@]}"; do
        check_binary "$bin" "$expected"
    done
    return $((failures > 0))
}

check_usearch() {
    local build_dir=$1/$TARGET/release/build outputs bad
    if [[ ! -d $build_dir ]]; then
        fail "no cargo build dir $build_dir (wrong TARGET_DIR?)"
        return 0
    fi
    shopt -s nullglob
    outputs=("$build_dir"/usearch-*/output)
    shopt -u nullglob
    if ((${#outputs[@]} == 0)); then
        echo "warning: no usearch build-script output in $build_dir; SIMD fallback check skipped" >&2
    elif bad=$(grep -l "Failed to compile" "${outputs[@]}"); then
        fail "usearch dropped SIMD backends ('Failed to compile') in: $bad (delete that usearch-* dir if it is stale)"
    else
        echo "usearch: all SIMD backends compiled (${#outputs[@]} build output(s) checked)"
    fi
}

if [[ ${1:-} == --inside ]]; then
    inside "${2:?expected version}"
    exit
fi

usage="usage: verify.sh DIR EXPECTED_VERSION TARGET_DIR"
dir=$(realpath "${1:?$usage}")
expected=${2:?$usage}
target_dir=$(realpath "${3:?$usage}")
: "${CROSS_IMAGE:?set CROSS_IMAGE to the cross image tag, e.g. vs-bench-cross:1.97.1}"
export MAX_GLIBC=${MAX_GLIBC:-2.34}
export MAX_GLIBCXX=${MAX_GLIBCXX:-3.4.29}

check_usearch "$target_dir"
if ! docker run --rm --init \
    --user "$(id -u):$(id -g)" \
    -e MAX_GLIBC -e MAX_GLIBCXX \
    -v "$(realpath "$0")":/verify.sh:ro \
    -v "$dir":/bins:ro \
    -w /bins \
    "$CROSS_IMAGE" \
    bash /verify.sh --inside "$expected"; then
    fail "binary checks failed (see above)"
fi

if ((failures > 0)); then
    echo "verify: $failures check(s) failed" >&2
    exit 1
fi
echo "verify: OK ($expected, max GLIBC $MAX_GLIBC, max GLIBCXX $MAX_GLIBCXX)"
