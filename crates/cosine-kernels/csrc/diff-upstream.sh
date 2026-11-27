#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Prints how the NumKong functions of cosine.c differ from NumKong's own: each NK_INTERNAL or
# NK_PUBLIC function of cosine.c against the function of the same name in include/numkong of a
# NumKong checkout at a ref.
#
#   csrc/diff-upstream.sh NUMKONG_CHECKOUT [REF]    (REF defaults to HEAD, e.g. v7.8.6)
#
# A function under `#if NK_ACCUMULATE_F32_IN_F64` is compared with its whole #if/#else/#endif block,
# so against upstream NumKong the diff shows ScyllaDB's #else branch as an addition, and a change to
# NumKong's branch is upstream's to port. Against the scylladb/NumKong fork the kernels came from
# (2e43be7d) there is no difference. A function NumKong does not have is listed as ScyllaDB's.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: $0 NUMKONG_CHECKOUT [REF]" >&2
    exit 2
fi
repo=$1
ref=${2:-HEAD}
ours=$(dirname "$0")/cosine.c

# The definition of function $1 read from stdin: from its NK_INTERNAL/NK_PUBLIC line (or the
# `#if NK_ACCUMULATE_F32_IN_F64` before it) to its closing `}` (or the guard's `#endif`).
definition() {
    awk -v name="$1" '
        $0 ~ "^NK_(INTERNAL|PUBLIC)[^(]* " name "\\(" && !/;$/ {
            if (prev ~ /^#if NK_ACCUMULATE_F32_IN_F64/) { print prev; guarded = 1 }
            p = 1
        }
        p { print }
        p && /^}/ && !guarded { exit }
        p && /^#endif \/\/ NK_ACCUMULATE_F32_IN_F64/ { exit }
        { prev = $0 }'
}

mapfile -t names < <(sed -n -E 's/^NK_(INTERNAL|PUBLIC)[^(]* ([a-z0-9_]+)\(.*\{$/\2/p' "$ours" | sort -u)
same=0
for name in "${names[@]}"; do
    file=$(git -C "$repo" grep -l -E "^NK_(INTERNAL|PUBLIC)[^(]* $name\(.*\{$" "$ref" -- include | head -1 || true)
    if [[ -z $file ]]; then
        echo "== $name: not in NumKong $ref (ScyllaDB's)"
        continue
    fi
    file=${file#"$ref":}
    if diff -u --label "NumKong $ref:$file" --label "cosine.c" \
        <(git -C "$repo" show "$ref:$file" | definition "$name") <(definition "$name" < "$ours"); then
        same=$((same + 1))
    fi
done
echo "== $same of ${#names[@]} functions are as in NumKong $ref"
