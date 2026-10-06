#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Vector Store builds on a vsbench vs node. vsbench (deploy.py) runs it as
# root with remote.run_script; every input comes from the environment:
#
#   ACTION=activate (default)  BUILD_ID ENV_B64 UNIT_SRC VS_PORT
#       Switch the service to /opt/vector-store/builds/<BUILD_ID>/vector-store:
#       write /opt/vector-store/.env (base64 of ENV_B64) atomically, install the
#       unit from UNIT_SRC when it changed, swap the `current` symlink atomically
#       (ln -s + mv -T), restart vector-store.service, wait until a NEW MainPID
#       runs exactly that binary (readlink -f /proc/<pid>/exe) and answers
#       GET /api/v1/info on VS_PORT.
#       stdout: pid=, old_pid=, exe=, restarted_at= (epoch seconds, node clock),
#       info= (the /api/v1/info JSON)
#   ACTION=extract  BUILD_ID VS_IMAGE VERSION
#       Official release: copy /opt/vector-store/vector-store out of the arm64
#       Docker Hub image into builds/<BUILD_ID>/ (skipped when its recorded
#       sha256 still matches), check `--version`, record builds/<BUILD_ID>/sha256.
#       stdout: sha256=
set -euo pipefail

VS_DIR=/opt/vector-store
UNIT=vector-store.service
UNIT_FILE=/etc/systemd/system/$UNIT
RUN_AS=ubuntu
START_TIMEOUT_S=90
INFO_TIMEOUT_S=120
STABLE_S=2
CLEANUP=""
NEW_PID=""
NEW_EXE=""

log() {
    echo "vs-activate: $*" >&2
}

fail() {
    log "ERROR: $*"
    exit 1
}

# shellcheck disable=SC2329  # invoked by the EXIT trap
cleanup() {
    if [[ -n $CLEANUP ]]; then
        rm -rf -- "$CLEANUP"
    fi
}
trap cleanup EXIT

check_build_id() {
    local re='^[0-9A-Za-z][0-9A-Za-z._-]*$'
    [[ $BUILD_ID =~ $re && $BUILD_ID != *..* ]] || fail "invalid BUILD_ID '$BUILD_ID'"
}

file_sha256() {
    sha256sum <"$1" | cut -d' ' -f1
}

extract_release() {
    : "${BUILD_ID:?}" "${VS_IMAGE:?}" "${VERSION:?}"
    check_build_id
    local dir=$VS_DIR/builds/$BUILD_ID tmp cid got sha
    if [[ -f $dir/sha256 && -f $dir/vector-store && $(file_sha256 "$dir/vector-store") == "$(cat "$dir/sha256")" ]]; then
        log "$BUILD_ID already present"
        echo "sha256=$(cat "$dir/sha256")"
        return 0
    fi
    install -d -m 0755 "$VS_DIR/builds"
    log "pulling $VS_IMAGE (linux/arm64)"
    docker pull --quiet --platform linux/arm64 "$VS_IMAGE" >/dev/null
    tmp=$(mktemp -d "$VS_DIR/builds/.extract-XXXXXX")
    CLEANUP=$tmp
    cid=$(docker create --platform linux/arm64 "$VS_IMAGE")
    if ! docker cp "$cid:/opt/vector-store/vector-store" "$tmp/vector-store"; then
        docker rm "$cid" >/dev/null || true
        fail "no /opt/vector-store/vector-store in $VS_IMAGE"
    fi
    docker rm "$cid" >/dev/null
    chmod 0755 "$tmp" "$tmp/vector-store"
    got=$("$tmp/vector-store" --version)
    [[ $got == "vector-store $VERSION" ]] || fail "the binary reports '$got', expected 'vector-store $VERSION'"
    sha=$(file_sha256 "$tmp/vector-store")
    printf '%s\n' "$sha" >"$tmp/sha256"
    rm -rf -- "$dir"
    mv -T "$tmp" "$dir"
    CLEANUP=""
    echo "sha256=$sha"
}

write_env() {
    local tmp
    tmp=$(mktemp "$VS_DIR/.env.XXXXXX")
    CLEANUP=$tmp
    printf '%s' "$ENV_B64" | base64 -d >"$tmp" || fail "ENV_B64 is not valid base64"
    chmod 0644 "$tmp"
    mv -f "$tmp" "$VS_DIR/.env"
    CLEANUP=""
}

install_unit() {
    if ! cmp -s "$UNIT_SRC" "$UNIT_FILE"; then
        install -m 0644 "$UNIT_SRC" "$UNIT_FILE"
        systemctl daemon-reload
        log "installed $UNIT_FILE"
    fi
    systemctl enable --quiet "$UNIT"
}

swap_current() {
    rm -f "$VS_DIR/current.tmp"
    ln -s "builds/$BUILD_ID" "$VS_DIR/current.tmp"
    mv -T "$VS_DIR/current.tmp" "$VS_DIR/current"
}

main_pid() {
    systemctl show -p MainPID --value "$UNIT" 2>/dev/null || echo 0
}

# Sets NEW_PID/NEW_EXE once a MainPID other than $1 has run $2 for STABLE_S seconds.
wait_new_pid() {
    local old=$1 want=$2 pid exe="" deadline=$((SECONDS + START_TIMEOUT_S))
    while ((SECONDS < deadline)); do
        pid=$(main_pid)
        if [[ $pid =~ ^[0-9]+$ && $pid != 0 && $pid != "$old" ]]; then
            exe=$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)
            if [[ $exe == "$want" ]]; then
                sleep "$STABLE_S"
                if [[ $(main_pid) == "$pid" ]] && systemctl is-active --quiet "$UNIT"; then
                    NEW_PID=$pid
                    NEW_EXE=$exe
                    return 0
                fi
            fi
        fi
        sleep 1
    done
    journalctl -u "$UNIT" -n 40 --no-pager >&2 || true
    fail "vector-store did not start from $want within ${START_TIMEOUT_S}s (MainPID $(main_pid), was $old, exe '$exe')"
}

# Prints the /api/v1/info JSON (one line) once the new process answers it.
wait_info() {
    local url=http://127.0.0.1:$VS_PORT/api/v1/info info deadline=$((SECONDS + INFO_TIMEOUT_S))
    while ((SECONDS < deadline)); do
        if info=$(curl -fsS --max-time 5 "$url" 2>/dev/null); then
            printf '%s\n' "${info//$'\n'/}"
            return 0
        fi
        sleep 2
    done
    journalctl -u "$UNIT" -n 40 --no-pager >&2 || true
    fail "$url did not answer within ${INFO_TIMEOUT_S}s"
}

activate() {
    : "${BUILD_ID:?}" "${ENV_B64:?}" "${UNIT_SRC:?}" "${VS_PORT:?}"
    check_build_id
    [[ $VS_PORT =~ ^[0-9]+$ ]] || fail "invalid VS_PORT '$VS_PORT'"
    local exe=$VS_DIR/builds/$BUILD_ID/vector-store old_pid restarted_at info
    [[ -x $exe ]] || fail "$exe is missing (deploy uploads it first)"
    [[ -f $UNIT_SRC ]] || fail "unit file $UNIT_SRC is missing"
    install -d -o "$RUN_AS" -g "$RUN_AS" -m 0755 "$VS_DIR"
    write_env
    install_unit
    swap_current
    old_pid=$(main_pid)
    restarted_at=$(date +%s.%N)
    log "restarting $UNIT on $BUILD_ID"
    systemctl restart "$UNIT"
    wait_new_pid "$old_pid" "$exe"
    info=$(wait_info)
    echo "info=$info"
    echo "pid=$NEW_PID"
    echo "old_pid=$old_pid"
    echo "exe=$NEW_EXE"
    echo "restarted_at=$restarted_at"
}

main() {
    case ${ACTION:-activate} in
        activate) activate ;;
        extract) extract_release ;;
        *) fail "unknown ACTION '${ACTION:-}' (activate or extract)" ;;
    esac
}

if [[ ${BASH_SOURCE[0]:-$0} == "$0" ]]; then
    main
fi
