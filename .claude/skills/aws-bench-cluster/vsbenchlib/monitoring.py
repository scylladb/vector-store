# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""scylla-monitoring on the client node: deploy, target updates and status.

node/monitoring-start.sh does the work on the client (ACTION=start|targets|check) and
reports `key=value` lines plus `job=<job> <up> <down> <interval>` / `problem=` / `fatal=`
lines. deploy.py re-exports everything here, so `deploy.deploy_monitoring` keeps working.
"""

from __future__ import annotations

from typing import Any

from . import config, proc, remote
from . import state as st
from .proc import VsbenchError
from .state import State

MONITORING_SCRIPT = "monitoring-start.sh"
LOADGEN_JOB = "loadgen_os"
MONITORING_TIMEOUT_S = 1500  # downloads, image pulls, 180 s ready + 180 s targets waits
CHECK_TIMEOUT_S = 90


def script_fields(text: str) -> dict[str, str]:
    """`key=value` lines of a node script's stdout (the last value of a key wins)."""
    pairs: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip().isidentifier():
            pairs[key.strip()] = value.strip()
    return pairs


def _deployed(current: State, component: str) -> dict[str, Any] | None:
    return (current.get("deployed") or {}).get(component)


def _target_env(current: State) -> dict[str, str]:
    ips = {role: ",".join(n["private_ip"] for n in st.nodes(current, role)) for role in ("scylla", "vs")}
    env = {"SCYLLA_IPS": ips["scylla"], "VS_IPS": ips["vs"], "CLUSTER_LABEL": config.SCYLLA_CLUSTER_NAME}
    return env | {"DC": config.SCYLLA_DC}


def _check_env(current: State) -> dict[str, str]:
    """EXPECT_JOBS (job:count:must_be_up; app jobs must be up once deployed) + SCRAPE_S + targets."""
    scylla, vs = len(st.nodes(current, "scylla")), len(st.nodes(current, "vs"))
    up = {component: int(bool(_deployed(current, component))) for component in ("scylla", "vector_store")}
    jobs = [("scylla", scylla, up["scylla"]), ("node_exporter", scylla, 1), ("vector_search", vs, up["vector_store"])]
    jobs += [("vector_search_os", vs, 1), (LOADGEN_JOB, 1, 1)]
    expect = ",".join(f"{job}:{count}:{must}" for job, count, must in jobs)
    return {"EXPECT_JOBS": expect, "SCRAPE_S": str(config.SCRAPE_INTERVAL_S), **_target_env(current)}


def parse_target_report(text: str) -> dict[str, Any]:
    """monitoring-start.sh's report: {jobs: {job: {up, down, scrape_interval}}, problems}."""
    jobs: dict[str, dict[str, Any]] = {}
    problems = []
    for line in text.splitlines():
        key, _, value = line.partition("=")
        if key == "job" and len(value.split()) == 4:
            job, up, down, interval = value.split()
            jobs[job] = {"up": int(up), "down": int(down), "scrape_interval": interval}
        elif key in ("problem", "fatal"):
            problems.append(value)
    return {"jobs": jobs, "problems": problems}


def deploy_monitoring(cluster: str) -> State:
    """scylla-monitoring on the client; node/monitoring-start.sh waits until every target is up."""
    current = st.require(cluster)
    client = st.client(current)
    scylla = _deployed(current, "scylla") or {}
    env = {"ACTION": "start", "SM_VERSION": config.SCYLLA_MONITORING_VERSION, "CLIENT_IP": client["private_ip"]}
    env |= {"SM_SHA256": config.SCYLLA_MONITORING_SHA256, "SCYLLA_VERSION": scylla.get("version") or ""}
    proc.log(f"monitoring: scylla-monitoring {config.SCYLLA_MONITORING_VERSION} on the client")
    env |= _check_env(current)
    stdout = remote.run_script(cluster, "client", MONITORING_SCRIPT, env, timeout=MONITORING_TIMEOUT_S).stdout
    out, report = script_fields(stdout), parse_target_report(stdout)
    if out.get("restarted") == "0":
        proc.log("monitoring: already running with the same settings")
    info = {"version": config.SCYLLA_MONITORING_VERSION, "dashboards": out.get("dashboards") or None}
    info |= {"cpuset": out.get("cpuset") or None, "deployed_at": proc.iso(proc.utcnow())}
    up = sum(job["up"] for job in report["jobs"].values())
    ports = f"127.0.0.1:{config.PORT_GRAFANA} (Grafana) and :{config.PORT_PROMETHEUS} (Prometheus)"
    proc.log(f"monitoring: {up} targets up, dashboards {info['dashboards']}; the client serves {ports} (ssh -L)")
    return st.update(cluster, lambda s: st.with_deployed(s, "monitoring", info))


def update_monitoring_targets(cluster: str) -> None:
    """Rewrite the target files on the client atomically (Prometheus reloads them live)."""
    current = st.require(cluster)
    if not _deployed(current, "monitoring"):
        proc.debug("monitoring is not deployed; no targets to update")
        return
    env = {"ACTION": "targets", **_target_env(current)}
    remote.run_script(cluster, "client", MONITORING_SCRIPT, env, timeout=CHECK_TIMEOUT_S)
    proc.log("monitoring: targets updated")


def monitoring_status(cluster: str) -> dict[str, Any]:
    """{deployed, version, dashboards, ready, jobs: {job: {up, down, scrape_interval}}, problems, error}."""
    current = st.require(cluster)
    info = _deployed(current, "monitoring") or {}
    status: dict[str, Any] = {"deployed": bool(info), "version": info.get("version"), "jobs": {}, "problems": []}
    status |= {"dashboards": info.get("dashboards"), "ready": False, "error": None}
    if not info:
        return status
    try:
        env = {"ACTION": "check", **_check_env(current)}
        result = remote.run_script(cluster, "client", MONITORING_SCRIPT, env, check=False, timeout=CHECK_TIMEOUT_S)
    except VsbenchError as err:  # local timeout
        return status | {"error": str(err)}
    if result.returncode != 0:
        return status | {"error": proc.tail((result.stderr or result.stdout).strip(), 5)}
    return status | {"ready": script_fields(result.stdout).get("ready") == "1"} | parse_target_report(result.stdout)
