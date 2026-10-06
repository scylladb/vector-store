#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Cross-compile vector-store and vector-search-benchmark for
# aarch64-unknown-linux-gnu on an x86_64 host, inside the vs-bench-cross image
# (rust:<toolchain>-bookworm + Debian aarch64 cross gcc). No qemu binfmt needed.
#
# Version stamping mirrors scripts/run-with-release-toolchain: temp copies of
# Cargo.toml/Cargo.lock with 0.0.0-dev replaced are bind-mounted over the
# originals, ~/.cargo/{git,registry} are mounted and the build runs as the
# calling user. Keep the two in sync (see reference.md#upgrading-pins).
# Always generic aarch64 (no -C target-cpu), like the release build.
#
# Usage: cross-build.sh [-s SRC_DIR] [-v VERSION] [-j JOBS] [-t TARGET_DIR] [-o OUT_DIR]
#   -s  source tree to build (repo or an exported tree); default: git toplevel of $PWD
#   -v  version to embed (replaces 0.0.0-dev); default: `git describe --dirty` of SRC_DIR,
#       pass "none" to keep 0.0.0-dev
#   -j  CARGO_BUILD_JOBS; default 8
#   -t  cargo target dir (share it between source trees to reuse compiled deps);
#       default: $SRC_DIR/target/cross-aarch64
#   -o  directory to copy the two binaries to; default: $TARGET_DIR/dist
# Env: CROSS_IMAGE  cross image tag; default vs-bench-cross:<channel of SRC_DIR/rust-toolchain.toml>
#
set -euo pipefail

TARGET=aarch64-unknown-linux-gnu
src=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
version=""
jobs=8
out=""
target_dir=""

while getopts "s:v:j:t:o:" opt; do
    case $opt in
        s) src=$(realpath "$OPTARG") ;;
        v) version=$OPTARG ;;
        j) jobs=$OPTARG ;;
        t) target_dir=$OPTARG ;;
        o) out=$OPTARG ;;
        *) exit 2 ;;
    esac
done

[[ -f $src/Cargo.toml && -f $src/Cargo.lock ]] || { echo "error: $src is not the vector-store workspace" >&2; exit 1; }

if [[ -n ${CROSS_IMAGE:-} ]]; then
    IMAGE=$CROSS_IMAGE
else
    channel=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$src/rust-toolchain.toml" 2>/dev/null || true)
    [[ -n $channel ]] || { echo "error: no channel in $src/rust-toolchain.toml; set CROSS_IMAGE" >&2; exit 1; }
    IMAGE=vs-bench-cross:$channel
fi

if [[ -z $version ]]; then
    version=$(git -C "$src" describe --dirty) || { echo "error: no annotated tag found in $src" >&2; exit 1; }
fi

# cargo requires a SemVer version; keep sed safe too (no '/', '|', '&')
semver='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$'
if [[ $version != none && ! $version =~ $semver ]]; then
    echo "error: version '$version' is not valid SemVer (allowed: X.Y.Z[-pre][+build], [0-9A-Za-z.-])" >&2
    exit 1
fi

target_dir=$(realpath -m "${target_dir:-$src/target/cross-aarch64}")
out=${out:-$target_dir/dist}

cargo_home=${CARGO_HOME:-$HOME/.cargo}
mkdir -p "$cargo_home/git" "$cargo_home/registry" "$target_dir" "$out"

tmp_toml=$(mktemp "${TMPDIR:-/tmp}/Cargo_toml.XXXXXX")
tmp_lock=$(mktemp "${TMPDIR:-/tmp}/Cargo_lock.XXXXXX")
trap 'rm -f "$tmp_toml" "$tmp_lock"' EXIT
chmod 644 "$tmp_toml" "$tmp_lock"

if [[ $version == none ]]; then
    cp "$src/Cargo.toml" "$tmp_toml"
    cp "$src/Cargo.lock" "$tmp_lock"
else
    sed -e "s|version = \"0.0.0-dev\"|version = \"$version\"|" "$src/Cargo.toml" > "$tmp_toml"
    sed -e "s|version = \"0.0.0-dev\"|version = \"$version\"|" "$src/Cargo.lock" > "$tmp_lock"
fi

echo "building $src (version ${version}, jobs ${jobs}, image ${IMAGE}) -> $target_dir"

# Two separate cargo invocations on purpose: building vector-store alone gives
# exactly the same feature unification as the release CI
# (`cargo build --release --bin vector-store -p vector-store`).
# --init: a SIGTERM to `docker run` (forwarded by the CLI) stops the build.
docker run --rm --init \
    --user "$(id -u):$(id -g)" \
    -e CARGO_TARGET_DIR="$target_dir" \
    -e CARGO_BUILD_JOBS="$jobs" \
    -v "$src":"$src" \
    -v "$target_dir":"$target_dir" \
    -v "$tmp_toml":"$src/Cargo.toml" \
    -v "$tmp_lock":"$src/Cargo.lock" \
    -v "$cargo_home/registry":/usr/local/cargo/registry \
    -v "$cargo_home/git":/usr/local/cargo/git \
    -w "$src" \
    "$IMAGE" \
    sh -euc "
        cargo build --release --target $TARGET -p vector-store --bin vector-store
        cargo build --release --target $TARGET -p vector-search-benchmark --bin vector-search-benchmark
        echo \"container memory.peak: \$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo n/a)\"
    "

for bin in vector-store vector-search-benchmark; do
    cp "$target_dir/$TARGET/release/$bin" "$out/$bin"
done
echo "$version" > "$out/VERSION"
ls -l "$out"
