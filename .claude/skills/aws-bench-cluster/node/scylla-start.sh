#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# ScyllaDB in Docker on a vsbench scylla node. vsbench (deploy.py) runs it as
# root with remote.run_script; every input comes from the environment:
#
#   ACTION=pull   IMAGE           pull the image (done on all nodes in parallel first)
#   ACTION=stop                   nodetool drain, docker stop -t 300, docker rm
#   ACTION=wipe                   stop, then delete everything under /var/lib/scylla
#   ACTION=start  IMAGE IP SEED RACK CLUSTER_NAME DC [VS_PRIMARY]
#                 [IO_READ_BW IO_READ_IOPS IO_WRITE_BW IO_WRITE_IOPS]
#                 pull, stop the current container (if any), start a new one and
#                 wait until it answers CQL (fails fast when the container dies).
#                 Without the four IO_* values Scylla runs iotune on every start.
#   ACTION=wait-un  EXPECT_IPS    wait until `nodetool status` shows exactly these
#                                 (comma-separated) addresses, all UN
#
# Production-like settings (research scylla-docker §2, §6): --developer-mode 0,
# --overprovisioned 0, host network, explicit addresses, precomputed
# io_properties, all CPUs (no --cpuset), --restart unless-stopped.
# Progress goes to stderr; stdout carries `key=value` results only.
set -euo pipefail

NAME=scylla
DATA_DIR=/var/lib/scylla
CONF_DIR=/etc/scylla-bench
DRAIN_TIMEOUT_S=120
STOP_TIMEOUT_S=300
CQL_TIMEOUT_S=600
UN_TIMEOUT_S=300
PULL_ATTEMPTS=3
RUN_ARGS=()

log() {
    echo "scylla-start: $*" >&2
}

fail() {
    log "ERROR: $*"
    exit 1
}

check_ip() {
    local re='^([0-9]{1,3}[.]){3}[0-9]{1,3}$'
    [[ $2 =~ $re ]] || fail "$1 is not an IPv4 address: '$2'"
}

check_word() {
    local re='^[A-Za-z0-9][A-Za-z0-9_.-]*$'
    [[ $2 =~ $re ]] || fail "invalid $1: '$2'"
}

check_ip_list() {
    local re='^([0-9]{1,3}[.]){3}[0-9]{1,3}(,([0-9]{1,3}[.]){3}[0-9]{1,3})*$'
    [[ $2 =~ $re ]] || fail "$1 must be comma-separated IPv4 addresses, got '$2'"
}

check_image() {
    local re='^[a-z0-9][a-z0-9._/-]*(:[A-Za-z0-9_][A-Za-z0-9._-]*)?(@sha256:[0-9a-f]{64})?$'
    [[ $IMAGE =~ $re ]] || fail "invalid IMAGE '$IMAGE'"
}

validate_start_env() {
    : "${IMAGE:?}" "${IP:?}" "${SEED:?}" "${RACK:?}" "${CLUSTER_NAME:?}" "${DC:?}"
    check_image
    check_ip IP "$IP"
    check_ip SEED "$SEED"
    check_word RACK "$RACK"
    check_word CLUSTER_NAME "$CLUSTER_NAME"
    check_word DC "$DC"
    local uri_re='^http://[0-9.]+:[0-9]+(,http://[0-9.]+:[0-9]+)*$'
    if [[ -n ${VS_PRIMARY:-} && ! $VS_PRIMARY =~ $uri_re ]]; then
        fail "invalid VS_PRIMARY '$VS_PRIMARY' (comma-separated http://IP:PORT)"
    fi
    local value count=0
    for value in "${IO_READ_BW:-}" "${IO_READ_IOPS:-}" "${IO_WRITE_BW:-}" "${IO_WRITE_IOPS:-}"; do
        [[ -z $value ]] && continue
        [[ $value =~ ^[0-9]+$ ]] || fail "IO_* values must be integers, got '$value'"
        count=$((count + 1))
    done
    ((count == 0 || count == 4)) || fail "set all four IO_* values or none of them"
}

require_data_mount() {
    mountpoint -q "$DATA_DIR" ||
        fail "$DATA_DIR is not a mount point (instance store missing after a stop/start?); recreate the cluster"
}

pull_image() {
    local attempt
    log "pulling $IMAGE"
    for ((attempt = 1; attempt <= PULL_ATTEMPTS; attempt++)); do
        if docker pull --quiet "$IMAGE" >/dev/null; then
            return 0
        fi
        log "docker pull failed (attempt $attempt/$PULL_ATTEMPTS)"
        sleep $((attempt * 5))
    done
    fail "cannot pull $IMAGE"
}

stop_scylla() {
    if ! docker container inspect "$NAME" >/dev/null 2>&1; then
        log "no $NAME container to stop"
        return 0
    fi
    if [[ $(docker inspect -f '{{.State.Running}}' "$NAME") == true ]]; then
        log "draining and stopping $NAME (up to ${STOP_TIMEOUT_S}s)"
        timeout "$DRAIN_TIMEOUT_S" docker exec "$NAME" nodetool drain >&2 ||
            log "nodetool drain failed; stopping anyway"
        docker stop -t "$STOP_TIMEOUT_S" "$NAME" >/dev/null
    fi
    docker rm "$NAME" >/dev/null
}

wipe_data() {
    require_data_mount
    log "wiping $DATA_DIR"
    find "$DATA_DIR" -mindepth 1 -delete
}

own_data_dir() {
    local uid gid
    uid=$(docker run --rm --entrypoint id "$IMAGE" -u)
    gid=$(docker run --rm --entrypoint id "$IMAGE" -g)
    [[ $uid =~ ^[0-9]+$ && $gid =~ ^[0-9]+$ ]] || fail "cannot read the scylla user/group ids of $IMAGE"
    if [[ $(stat -c %u:%g "$DATA_DIR") != "$uid:$gid" ]]; then
        chown -R "$uid:$gid" "$DATA_DIR"
    fi
}

write_io_files() {
    install -d -m 0755 "$CONF_DIR"
    printf 'disks:\n- mountpoint: %s\n  read_bandwidth: %s\n  read_iops: %s\n  write_bandwidth: %s\n  write_iops: %s\n' \
        "$DATA_DIR" "$IO_READ_BW" "$IO_READ_IOPS" "$IO_WRITE_BW" "$IO_WRITE_IOPS" >"$CONF_DIR/io_properties.yaml.tmp"
    mv -f "$CONF_DIR/io_properties.yaml.tmp" "$CONF_DIR/io_properties.yaml"
    echo 'SEASTAR_IO="--io-properties-file=/etc/scylla.d/io_properties.yaml"' >"$CONF_DIR/io.conf.tmp"
    mv -f "$CONF_DIR/io.conf.tmp" "$CONF_DIR/io.conf"
}

# Fills RUN_ARGS with the `docker run` arguments (kept separate so tests can check them).
build_run_args() {
    RUN_ARGS=(run -d --name "$NAME" --network host --restart unless-stopped
        --cap-add SYS_NICE --cap-add PERFMON --ulimit nofile=1048576:1048576
        --stop-timeout "$STOP_TIMEOUT_S" --log-opt max-size=200m --log-opt max-file=5
        -v "$DATA_DIR:$DATA_DIR"
        -v /dev/null:/etc/supervisord.conf.d/scylla-node-exporter.conf:ro)
    local io_setup=1
    if [[ -n ${IO_READ_BW:-} ]]; then
        RUN_ARGS+=(-v "$CONF_DIR/io_properties.yaml:/etc/scylla.d/io_properties.yaml:ro"
            -v "$CONF_DIR/io.conf:/etc/scylla.d/io.conf:ro")
        io_setup=0
    fi
    RUN_ARGS+=("$IMAGE" --developer-mode 0 --io-setup "$io_setup" --overprovisioned 0
        --listen-address "$IP" --rpc-address "$IP" --broadcast-address "$IP" --broadcast-rpc-address "$IP"
        --seeds "$SEED" --cluster-name "$CLUSTER_NAME" --dc "$DC" --rack "$RACK")
    if [[ -n ${VS_PRIMARY:-} ]]; then
        RUN_ARGS+=(--vector-store-primary-uri "$VS_PRIMARY")
    fi
    RUN_ARGS+=(--rf-rack-valid-keyspaces true --tablets-mode-for-new-keyspaces enabled
        --batch-size-warn-threshold-in-kb 1024)
}

wait_cql() {
    local deadline=$((SECONDS + CQL_TIMEOUT_S)) status
    log "waiting for CQL on $IP (up to ${CQL_TIMEOUT_S}s)"
    while ((SECONDS < deadline)); do
        status=$(docker inspect -f '{{.State.Status}} {{.RestartCount}}' "$NAME" 2>/dev/null || echo missing)
        if [[ $status != "running 0" ]]; then
            docker logs --tail 40 "$NAME" >&2 || true
            fail "the $NAME container is '$status' (it crashed while starting?)"
        fi
        if docker exec "$NAME" cqlsh "$IP" -e 'SELECT now() FROM system.local' >/dev/null 2>&1; then
            return 0
        fi
        sleep 5
    done
    docker logs --tail 40 "$NAME" >&2 || true
    fail "CQL is not answering on $IP after ${CQL_TIMEOUT_S}s"
}

# stdin: `nodetool status`; stdout: sorted "STATE ADDRESS" lines, one per node.
node_states() {
    awk '$1 ~ /^[UD][NLJM]$/ {print $1, $2}' | sort
}

wait_un() {
    : "${EXPECT_IPS:?}"
    check_ip_list EXPECT_IPS "$EXPECT_IPS"
    local want got out="" deadline=$((SECONDS + UN_TIMEOUT_S))
    want=$(tr ',' '\n' <<<"$EXPECT_IPS" | sed 's/^/UN /' | sort)
    while ((SECONDS < deadline)); do
        out=$(docker exec "$NAME" nodetool status 2>&1 || true)
        got=$(node_states <<<"$out")
        if [[ $got == "$want" ]]; then
            log "all nodes UN: $EXPECT_IPS"
            return 0
        fi
        sleep 5
    done
    echo "$out" >&2
    fail "expected exactly $EXPECT_IPS, all UN (a node of an older cluster? redeploy with --wipe)"
}

start_scylla() {
    validate_start_env
    require_data_mount
    pull_image
    stop_scylla
    own_data_dir
    if [[ -n ${IO_READ_BW:-} ]]; then
        write_io_files
    fi
    build_run_args
    log "starting $IMAGE (ip $IP, seed $SEED, $DC/$RACK)"
    docker "${RUN_ARGS[@]}" >/dev/null
    wait_cql
    echo "container=$(docker inspect -f '{{.Id}}' "$NAME")"
}

main() {
    : "${ACTION:?}"
    case $ACTION in
        pull)
            : "${IMAGE:?}"
            check_image
            pull_image
            ;;
        stop) stop_scylla ;;
        wipe)
            stop_scylla
            wipe_data
            ;;
        start) start_scylla ;;
        wait-un) wait_un ;;
        *) fail "unknown ACTION '$ACTION' (pull, stop, wipe, start or wait-un)" ;;
    esac
}

# Run main unless sourced (tests source this file to check build_run_args).
if [[ ${BASH_SOURCE[0]:-$0} == "$0" ]]; then
    main
fi
