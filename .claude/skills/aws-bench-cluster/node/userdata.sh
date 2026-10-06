#!/usr/bin/env bash
# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# EC2 user-data of every vsbench node; cloud-init runs it once, as root, on
# first boot. Static on purpose: provision.userdata_for() only inserts
# shell-quoted assignments of ROLE, NODE_NAME, CLUSTER, EXPIRES_AT_EPOCH,
# NODE_EXPORTER_VERSION and NODE_EXPORTER_SHA256 right after the shebang.
#
# ORDER MATTERS: instances are tagged keep=alive, so the on-node TTL is the
# only thing that terminates them. It is armed first, using bash and systemd
# only, so no later failure (apt, network, disks) can leave a node running.
# Ends with NODE_READY_MARKER, or NODE_FAILED_MARKER ("line N: command").
set -Eeuo pipefail

VSB_HOME=/var/lib/vsbench
READY_MARKER=$VSB_HOME/ready
FAILED_MARKER=$VSB_HOME/failed
EXPIRES_FILE=/etc/vsbench/expires_at
LOG_FILE=/var/log/vsbench-userdata.log

mkdir -p "$VSB_HOME" /etc/vsbench
exec > >(tee -a "$LOG_FILE") 2>&1

on_failure() {
    echo "$1" >"$FAILED_MARKER"
    echo "vsbench: bootstrap FAILED: $1" >&2
    # Safety net when the failure happened before the TTL timer was armed.
    if ! systemctl is-active --quiet vsbench-ttl.timer; then
        echo "vsbench: TTL timer is not active; powering off in 60 min" >&2
        shutdown -h +60 "vsbench: bootstrap failed before the TTL was armed" || true
    fi
}
fail() {
    on_failure "$*"
    exit 1
}
trap 'on_failure "line $LINENO: $BASH_COMMAND"' ERR

: "${ROLE:?}" "${NODE_NAME:?}" "${CLUSTER:?}" "${EXPIRES_AT_EPOCH:?}"
: "${NODE_EXPORTER_VERSION:?}" "${NODE_EXPORTER_SHA256:?}"
case $ROLE in
    scylla | vs | client) ;;
    *) fail "unknown ROLE '$ROLE'" ;;
esac
[[ $EXPIRES_AT_EPOCH =~ ^[0-9]{10}$ ]] || fail "EXPIRES_AT_EPOCH is not a 10-digit epoch: '$EXPIRES_AT_EPOCH'"
echo "vsbench: bootstrapping $NODE_NAME ($ROLE) of cluster $CLUSTER at $(date -u +%FT%TZ)"

# --- 1. TTL: power off (= terminate) at EXPIRES_FILE; bash + systemd only ---
printf '%s\n' "$EXPIRES_AT_EPOCH" >"$EXPIRES_FILE.tmp"
mv "$EXPIRES_FILE.tmp" "$EXPIRES_FILE"
printf '%s\n' "$EXPIRES_AT_EPOCH" >"$VSB_HOME/ttl.lastgood"
cat >/usr/local/sbin/vsbench-ttl <<'TTL_SCRIPT'
#!/usr/bin/env bash
# vsbench TTL check (run every minute by vsbench-ttl.timer). Powers the node
# off once the epoch in /etc/vsbench/expires_at has passed; the instance is
# launched with shutdown behaviour "terminate", so power-off terminates it.
# A missing, partial or invalid file (anything but a 10-digit epoch) falls
# back to the last good value; it is never treated as "no expiry".
set -uo pipefail
EXPIRES_FILE=${VSBENCH_TTL_EXPIRES_FILE:-/etc/vsbench/expires_at}
STATE_DIR=${VSBENCH_TTL_STATE_DIR:-/var/lib/vsbench}
LASTGOOD=$STATE_DIR/ttl.lastgood
WARNED=$STATE_DIR/ttl.warned

note() {
    logger -t vsbench-ttl "$1" || true
    echo "vsbench-ttl: $1"
}

raw=$(tr -d '[:space:]' <"$EXPIRES_FILE" 2>/dev/null || true)
if [[ $raw =~ ^[0-9]{10}$ ]]; then
    expires=$raw
    if [[ $(tr -d '[:space:]' <"$LASTGOOD" 2>/dev/null || true) != "$expires" ]]; then
        printf '%s\n' "$expires" >"$LASTGOOD.tmp" && mv "$LASTGOOD.tmp" "$LASTGOOD"
    fi
else
    expires=$(tr -d '[:space:]' <"$LASTGOOD" 2>/dev/null || true)
    note "invalid or missing $EXPIRES_FILE ('$raw'); using the last good expiry '$expires'"
    if ! [[ $expires =~ ^[0-9]{10}$ ]]; then
        note "no valid expiry at all; powering off for safety"
        expires=0
    fi
fi

left=$((expires - $(date +%s)))
if ((left <= 0)); then
    note "expired at $expires: powering off now (the instance terminates)"
    wall "vsbench: TTL expired, powering off now (the instance terminates)" || true
    if [[ -n ${VSBENCH_TTL_DRY_RUN:-} ]]; then
        echo "vsbench-ttl: DRY-RUN poweroff"
    else
        systemctl poweroff
    fi
    exit 0
fi
for minutes in 5 10 30; do
    if ((left <= minutes * 60)); then
        if [[ $(cat "$WARNED" 2>/dev/null || true) != "$expires:$minutes" ]]; then
            printf '%s\n' "$expires:$minutes" >"$WARNED"
            msg="vsbench: this node powers off (terminates) in $((left / 60 + 1)) min; extend with: vsbench extend"
            note "$msg"
            wall "$msg" || true
        fi
        break
    fi
done
exit 0
TTL_SCRIPT
chmod 0755 /usr/local/sbin/vsbench-ttl
cat >/etc/systemd/system/vsbench-ttl.service <<'UNIT'
[Unit]
Description=vsbench TTL: power off (terminate) after /etc/vsbench/expires_at

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/vsbench-ttl
UNIT
cat >/etc/systemd/system/vsbench-ttl.timer <<'UNIT'
[Unit]
Description=vsbench TTL check every minute

[Timer]
OnBootSec=1min
OnUnitActiveSec=1min
AccuracySec=10s

[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable --now vsbench-ttl.timer
systemctl is-active --quiet vsbench-ttl.timer || fail "vsbench-ttl.timer is not active"

# --- 2. No background apt: first-boot timers race our apt-get, and later
# upgrades would restart docker or vector-store in the middle of a benchmark.
systemctl disable --now unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer || true
systemctl stop apt-daily.service apt-daily-upgrade.service || true

# --- 3. docker: keep containers running across a dockerd restart ---
mkdir -p /etc/docker
cat >/etc/docker/daemon.json <<'JSON'
{"live-restore": true, "log-driver": "json-file", "log-opts": {"max-size": "200m", "max-file": "5"}}
JSON

# --- 4. packages ---
apt_get() {
    DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=l \
        apt-get -o DPkg::Lock::Timeout=600 -o Acquire::Retries=5 -y "$@"
}
# Repairs a dpkg run interrupted by stopping apt-daily-upgrade above.
DEBIAN_FRONTEND=noninteractive dpkg --configure -a || true
packages=(docker.io jq zstd curl ca-certificates)
if [[ $ROLE == scylla ]]; then
    packages+=(xfsprogs mdadm nvme-cli)
fi
apt_get update
apt_get install "${packages[@]}"
usermod -aG docker ubuntu

# --- 5. sysctls (scylla-kernel-conf equivalents, see research scylla-docker §3) ---
{
    echo "fs.aio-max-nr = 30000000"
    echo "fs.file-max = 9223372036854775807"
    echo "fs.nr_open = 1073741816"
    echo "fs.inotify.max_user_instances = 1200"
    echo "vm.swappiness = 1"
    echo "kernel.perf_event_paranoid = 1"
    if [[ $ROLE == scylla ]]; then
        mem_bytes=$(($(awk '/^MemTotal:/ {print $2}' /proc/meminfo) * 1024))
        page=$(getconf PAGESIZE)
        tcp=$((mem_bytes * 3 / 100))
        echo "vm.vfs_cache_pressure = 2000"
        echo "kernel.numa_balancing = 0"
        echo "kernel.sched_autogroup_enabled = 0"
        echo "net.ipv4.tcp_mem = $((tcp / 2 / page)) $((tcp * 2 / 3 / page)) $((tcp / page))"
    fi
} >/etc/sysctl.d/99-vsbench.conf
sysctl -e -p /etc/sysctl.d/99-vsbench.conf

# --- 6. node_exporter (pinned release, sha256-verified) ---
ne_name="node_exporter-${NODE_EXPORTER_VERSION}.linux-arm64"
ne_url="https://github.com/prometheus/node_exporter/releases/download/v${NODE_EXPORTER_VERSION}/${ne_name}.tar.gz"
ne_tgz=$(mktemp /tmp/node_exporter.XXXXXX.tar.gz)
curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors -o "$ne_tgz" "$ne_url"
echo "${NODE_EXPORTER_SHA256}  ${ne_tgz}" | sha256sum -c - || fail "node_exporter sha256 mismatch for $ne_url"
tar -xzf "$ne_tgz" -C /usr/local/bin --strip-components=1 "${ne_name}/node_exporter"
rm -f "$ne_tgz"
cat >/etc/systemd/system/node-exporter.service <<'UNIT'
[Unit]
Description=Prometheus node_exporter (vsbench)
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/local/bin/node_exporter --web.listen-address=:9100 \
    --collector.ethtool \
    "--collector.ethtool.metrics-include=(bw_in_allowance_exceeded|bw_out_allowance_exceeded|pps_allowance_exceeded|conntrack_allowance_exceeded|conntrack_allowance_available|linklocal_allowance_exceeded)" \
    --collector.interrupts \
    --collector.systemd "--collector.systemd.unit-include=^(vector-store|docker)[.]service$$" \
    --no-collector.hwmon --no-collector.thermal_zone --no-collector.rapl
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now node-exporter.service
for _ in $(seq 1 30); do
    curl -fsS -o /dev/null http://127.0.0.1:9100/metrics && break
    sleep 1
done
curl -fsS -o /dev/null http://127.0.0.1:9100/metrics || fail "node_exporter does not serve :9100/metrics"

# --- 7. role specific ---
setup_scylla_disks() {
    local mnt=/var/lib/scylla dev queue
    local -a devs
    # Device names are not stable; select instance-store NVMe by model.
    mapfile -t devs < <(lsblk -d -n -p -o NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')
    ((${#devs[@]} > 0)) || fail "no instance-store NVMe found (scylla needs an instance type with local NVMe)"
    if ! mountpoint -q "$mnt"; then
        if ((${#devs[@]} > 1)); then
            mdadm --create /dev/md0 --verbose --force --run --level=0 -c1024 --raid-devices="${#devs[@]}" "${devs[@]}"
            dev=/dev/md0
        else
            dev=${devs[0]}
        fi
        mkfs.xfs -f -K -m rmapbt=0 -m reflink=0 "$dev"
        mkdir -p "$mnt"
        mount -o noatime,discard,lazytime "$dev" "$mnt"
        echo "UUID=$(blkid -s UUID -o value "$dev") $mnt xfs noatime,discard,lazytime,nofail 0 0" >>/etc/fstab
    fi
    # scylla_blocktune equivalent (not persistent across reboots).
    for dev in "${devs[@]}" /dev/md0; do
        [[ -b $dev ]] || continue
        queue=/sys/block/$(basename "$dev")/queue
        echo none >"$queue/scheduler" 2>/dev/null || true
        echo 2 >"$queue/nomerges" 2>/dev/null || true
    done
}

disable_deep_cstates() {
    # Best effort: keep only state0 for lower wake-up latency.
    local state
    for state in /sys/devices/system/cpu/cpu*/cpuidle/state[1-9]*/disable; do
        [[ -e $state ]] || continue
        echo 1 >"$state" 2>/dev/null || true
    done
}

case $ROLE in
    scylla) setup_scylla_disks ;;
    vs) disable_deep_cstates ;;
    client) ;;
esac

# --- 8. working directories ---
for dir in scripts jobs bench datasets monitoring; do
    install -d -o ubuntu -g ubuntu -m 0755 "$VSB_HOME/$dir"
done

# --- 9. done ---
touch "$READY_MARKER"
echo "vsbench: $NODE_NAME ready at $(date -u +%FT%TZ)"
