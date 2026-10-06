#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# scylla-monitoring (Prometheus + Grafana) on the vsbench client node. vsbench
# (deploy.py) runs it as root with remote.run_script; inputs come from the
# environment (IP lists are comma-separated private IPs, possibly empty):
#
#   ACTION=targets  SCYLLA_IPS VS_IPS CLUSTER_LABEL DC
#       Rewrite the Prometheus target files atomically (live reload, no restart).
#   ACTION=check    EXPECT_JOBS SCRAPE_S
#       stdout: ready=0|1, then (when ready) the target report: one
#       `job=<job> <up> <down> <scrape interval>` line per job, one `problem=` line
#       per unmet expectation and `fatal=` when node_exporter ignores SCRAPE_S.
#       EXPECT_JOBS = comma-separated job:count:must_be_up (1|0).
#   ACTION=start    targets + check inputs + SM_VERSION SM_SHA256 CLIENT_IP [SCYLLA_VERSION]
#       Install the pinned release tarball (sha256-checked) under /opt/vsbench,
#       patch the template (node_exporter follows --scrap), write the targets,
#       (re)start the stack as ubuntu unless it already runs with the same
#       settings, then wait until the target report has no problem. Dashboards:
#       ver_X.Y of SCYLLA_VERSION if the tarball has them, else master.
#       stdout: the target report, dashboards=, cpuset=, restarted=0|1
#
# See research monitoring.md §3-5, §10 and reference.md#upgrading-pins.
set -euo pipefail

OPT_DIR=/opt/vsbench
MON_DIR=/var/lib/vsbench/monitoring
RUN_AS=ubuntu # owner of the files (`chown ubuntu:` = its login group) and of the stack
PROM_URL=http://127.0.0.1:9090
GRAFANA_URL=http://127.0.0.1:3000
READY_TIMEOUT_S=180
TARGETS_TIMEOUT_S=180
SM_URL_BASE=https://github.com/scylladb/scylla-monitoring/archive/refs/tags
PIN_HINT="upstream changed, see reference.md#upgrading-pins"
SM_DIR=""
DASHBOARDS=""
CPUSET=""
START_ARGS=()

log() {
    echo "monitoring-start: $*" >&2
}

fail() {
    log "ERROR: $*"
    exit 1
}

check_ip_list() {
    local re='^([0-9]{1,3}[.]){3}[0-9]{1,3}(,([0-9]{1,3}[.]){3}[0-9]{1,3})*$'
    if [[ -n $2 && ! $2 =~ $re ]]; then
        fail "$1 must be comma-separated IPv4 addresses, got '$2'"
    fi
}

check_word() {
    local re='^[A-Za-z0-9][A-Za-z0-9_.-]*$'
    [[ $2 =~ $re ]] || fail "invalid $1: '$2'"
}

# Writes stdin to $1 atomically (hidden temp file in the same directory, then mv).
write_file() {
    local target=$1 tmp
    tmp=$(mktemp "$(dirname "$target")/.$(basename "$target").XXXXXX")
    cat >"$tmp"
    chmod 0644 "$tmp"
    chown "$RUN_AS:" "$tmp"
    mv -f "$tmp" "$target"
}

# file_sd YAML for a comma-separated IP list (bare IPs: the jobs add the ports).
targets_yaml() {
    if [[ -z $1 ]]; then
        echo "[]"
        return 0
    fi
    printf -- '- targets: [%s]\n  labels: {cluster: %s, dc: %s}\n' "${1//,/, }" "$CLUSTER_LABEL" "$DC"
}

write_targets() {
    local dir=$MON_DIR/targets
    install -d -m 0755 "$MON_DIR" "$dir"
    chown "$RUN_AS:" "$MON_DIR" "$dir"
    targets_yaml "$SCYLLA_IPS" | write_file "$dir/scylla_servers.yml"
    targets_yaml "$SCYLLA_IPS" | write_file "$dir/node_exporter_servers.yml"
    targets_yaml "$VS_IPS" | write_file "$dir/vector_search_servers.yml"
    : | write_file "$dir/scylla_manager_servers.yml"
    echo "[]" | write_file "$dir/scylla_manager_agents.yml"
    log "targets: scylla [$SCYLLA_IPS], vector search [$VS_IPS]"
}

# The client's own node_exporter, as a job that does not match node_exporter.*
# (keeps it out of the Scylla OS dashboards).
write_extra_jobs() {
    printf -- '- job_name: loadgen_os\n  static_configs:\n    - targets: ['"'"'%s:9100'"'"']\n      labels: {cluster: %s-loadgen, role: loadgen}\n' \
        "$CLIENT_IP" "$CLUSTER_LABEL" | write_file "$MON_DIR/extra_scrape_jobs.yml"
}

# The node_exporter job hardcodes a 1m interval / 20s timeout that --scrap does
# not touch; delete exactly those two lines so it inherits the global interval.
patch_template() {
    local template=$1 pattern='^  scrape_(interval: 1m # By default|timeout: 20s # Timeout)' count
    count=$(grep -cE "$pattern" "$template" || true)
    if [[ $count != 2 ]]; then
        fail "expected 2 hardcoded node_exporter scrape lines in $(basename "$template"), found $count: $PIN_HINT"
    fi
    sed -i -E "/$pattern/d" "$template"
}

download_tarball() {
    local tarball=$1
    if echo "$SM_SHA256  $tarball" | sha256sum -c --status - 2>/dev/null; then
        return 0
    fi
    log "downloading scylla-monitoring $SM_VERSION"
    curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors -o "$tarball.part" "$SM_URL_BASE/$SM_VERSION.tar.gz"
    mv -f "$tarball.part" "$tarball"
    if ! echo "$SM_SHA256  $tarball" | sha256sum -c --status -; then
        rm -f "$tarball"
        fail "sha256 mismatch for scylla-monitoring $SM_VERSION: $PIN_HINT"
    fi
}

install_monitoring() {
    SM_DIR=$OPT_DIR/scylla-monitoring-$SM_VERSION
    if [[ -f $SM_DIR/.vsbench-ready ]]; then
        return 0
    fi
    install -d -m 0755 "$OPT_DIR"
    chown "$RUN_AS:" "$OPT_DIR"
    rm -rf -- "$OPT_DIR"/.sm-*
    local tarball=$OPT_DIR/scylla-monitoring-$SM_VERSION.tar.gz tmp
    download_tarball "$tarball"
    tmp=$(mktemp -d "$OPT_DIR/.sm-XXXXXX")
    tar -xzf "$tarball" -C "$tmp"
    [[ -d $tmp/scylla-monitoring-$SM_VERSION ]] || fail "the tarball has no scylla-monitoring-$SM_VERSION/: $PIN_HINT"
    patch_template "$tmp/scylla-monitoring-$SM_VERSION/prometheus/prometheus.yml.template"
    touch "$tmp/scylla-monitoring-$SM_VERSION/.vsbench-ready"
    chown -R "$RUN_AS:" "$tmp"
    rm -rf -- "$SM_DIR"
    mv -T "$tmp/scylla-monitoring-$SM_VERSION" "$SM_DIR"
    rmdir "$tmp"
    log "installed $SM_DIR"
}

choose_dashboards() {
    local version=${SCYLLA_VERSION:-} re='^([0-9]{4}[.][0-9]+)[.]'
    DASHBOARDS=master
    if [[ $version =~ $re && $version != *dev* ]] && [[ -d $SM_DIR/grafana/build/ver_${BASH_REMATCH[1]} ]]; then
        DASHBOARDS=${BASH_REMATCH[1]}
    fi
}

build_start_args() {
    START_ARGS=(-v "$DASHBOARDS" -d "$MON_DIR/prometheus-data"
        --target-directory "$MON_DIR/targets" --vector-search vector_search_servers.yml
        -T "$MON_DIR/extra_scrape_jobs.yml" --scrap "$SCRAPE_S"
        -b --storage.tsdb.retention.time=30d -b --web.enable-admin-api
        -A 127.0.0.1 --no-loki --no-renderer --auto-restart)
    CPUSET=""
    if (($(nproc) >= 8)); then
        # The benchmark runs under taskset -c 0-6; keep monitoring off those CPUs.
        CPUSET=7
        START_ARGS+=(--limit "prometheus,--cpuset-cpus=$CPUSET" --limit "grafana,--cpuset-cpus=$CPUSET")
    fi
}

wait_ready() {
    local deadline=$((SECONDS + READY_TIMEOUT_S))
    while ((SECONDS < deadline)); do
        if curl -fsS -o /dev/null --max-time 5 "$PROM_URL/-/ready" &&
            curl -fsS -o /dev/null --max-time 5 "$GRAFANA_URL/api/health"; then
            return 0
        fi
        sleep 2
    done
    docker logs --tail 30 aprom >&2 || true
    fail "Prometheus/Grafana not ready after ${READY_TIMEOUT_S}s"
}

# stdin: Prometheus /api/v1/targets JSON; stdout: the target report (see ACTION=check).
target_report() {
    # shellcheck disable=SC2016  # jq variables, not shell expansions
    jq -r --arg expect "$EXPECT_JOBS" --arg want "${SCRAPE_S}s" --arg hint "$PIN_HINT" '
        (.data.activeTargets // []) as $all
        | ($all | group_by(.labels.job)[]
            | "job=\(.[0].labels.job) \(map(select(.health == "up")) | length)"
              + " \(map(select(.health != "up")) | length) \(.[0].scrapeInterval // "?")"),
          ($expect | split(",")[] | split(":") as [$job, $count, $must_be_up]
            | [$all[] | select(.labels.job == $job)] as $members
            | if ($members | length) != ($count | tonumber) then
                "problem=\($job): \($members | length) targets, expected \($count)"
              elif $must_be_up == "1" then
                ($members[] | select(.health != "up")
                  | "problem=\($job) \(.scrapeUrl): \(.health) \(.lastError // "")")
              else empty end),
          ([$all[] | select(.labels.job == "node_exporter" and .scrapeInterval != null
              and .scrapeInterval != $want)][0] // empty
            | "fatal=node_exporter is scraped every \(.scrapeInterval), expected \($want): \($hint)")'
}

targets_report() {
    curl -fsS --max-time 10 "$PROM_URL/api/v1/targets?state=active" | target_report ||
        echo "problem=cannot read $PROM_URL/api/v1/targets"
}

wait_targets() {
    local report="" deadline=$((SECONDS + TARGETS_TIMEOUT_S))
    while ((SECONDS < deadline)); do
        report=$(targets_report)
        if grep -q '^fatal=' <<<"$report"; then
            fail "$(grep -m1 '^fatal=' <<<"$report" | cut -d= -f2-)"
        fi
        if ! grep -q '^problem=' <<<"$report"; then
            echo "$report"
            return 0
        fi
        sleep 5
    done
    grep '^problem=' <<<"$report" | head -20 >&2
    fail "Prometheus targets are not healthy after ${TARGETS_TIMEOUT_S}s (node_exporter listens on :9100 of every node)"
}

start_stack() {
    local args_file=$MON_DIR/start-args.txt want
    build_start_args
    want=$(printf '%s\n' "$SM_DIR" "${START_ARGS[@]}")
    if [[ -f $args_file && $(cat "$args_file") == "$want" ]] &&
        curl -fsS -o /dev/null --max-time 5 "$PROM_URL/-/ready"; then
        log "already running with the same settings"
        echo "restarted=0"
        return 0
    fi
    cd "$SM_DIR"
    log "starting scylla-monitoring $SM_VERSION (dashboards $DASHBOARDS)"
    sudo -u "$RUN_AS" -H ./kill-all.sh >&2 || true
    sudo -u "$RUN_AS" -H ./start-all.sh "${START_ARGS[@]}" >&2
    wait_ready
    printf '%s\n' "$want" | write_file "$args_file"
    echo "restarted=1"
}

validate_common() {
    : "${CLUSTER_LABEL:?}" "${DC:?}" "${SCYLLA_IPS?}" "${VS_IPS?}"
    check_word CLUSTER_LABEL "$CLUSTER_LABEL"
    check_word DC "$DC"
    check_ip_list SCYLLA_IPS "$SCYLLA_IPS"
    check_ip_list VS_IPS "$VS_IPS"
}

validate_check() {
    : "${EXPECT_JOBS:?}" "${SCRAPE_S:?}"
    local re='^[a-z_]+:[0-9]+:[01](,[a-z_]+:[0-9]+:[01])*$'
    [[ $EXPECT_JOBS =~ $re ]] || fail "invalid EXPECT_JOBS '$EXPECT_JOBS' (job:count:0|1,...)"
}

validate_start() {
    : "${SM_VERSION:?}" "${SM_SHA256:?}" "${CLIENT_IP:?}" "${SCRAPE_S:?}"
    validate_check
    [[ $SM_VERSION =~ ^[0-9]+[.][0-9]+[.][0-9]+$ ]] || fail "invalid SM_VERSION '$SM_VERSION'"
    [[ $SM_SHA256 =~ ^[0-9a-f]{64}$ ]] || fail "invalid SM_SHA256"
    if ! [[ $SCRAPE_S =~ ^[0-9]+$ ]] || ((SCRAPE_S < 6)); then
        fail "invalid SCRAPE_S '$SCRAPE_S' (seconds, >= 6)"
    fi
    check_ip_list CLIENT_IP "$CLIENT_IP"
}

main() {
    : "${ACTION:?}"
    validate_common
    case $ACTION in
        targets) write_targets ;;
        check)
            validate_check
            if curl -fsS -o /dev/null --max-time 5 "$PROM_URL/-/ready"; then
                echo "ready=1"
                targets_report
            else
                echo "ready=0"
            fi
            ;;
        start)
            validate_start
            install_monitoring
            choose_dashboards
            write_targets
            write_extra_jobs
            start_stack
            wait_targets
            echo "dashboards=$DASHBOARDS"
            echo "cpuset=$CPUSET"
            ;;
        *) fail "unknown ACTION '$ACTION' (start, targets or check)" ;;
    esac
}

if [[ ${BASH_SOURCE[0]:-$0} == "$0" ]]; then
    main
fi
