# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Deploy ScyllaDB, Vector Store, the benchmark binary and monitoring onto a cluster.

Pins: without --image/--source a deploy reuses the resolved spec in state.pins (repo@sha256:...,
git:<sha>, release:<ver>, build:<id>); the floating spec it came from (`<key>_spec`, e.g.
"nightly", "git:master") is resolved again only with --refresh. Explicit specs are resolved and
pinned. A component already running its resolved spec everywhere is skipped ("unchanged").
Scylla, Vector Store and bench deploys refuse to run while a benchmark job runs.
The monitoring part lives in monitoring.py and is re-exported here.
"""

from __future__ import annotations

import base64
import concurrent.futures
import datetime
import json
import re
import shlex
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any, TypeVar

from . import build, config, proc, remote, results
from . import state as st
from .monitoring import (  # noqa: F401  (re-exported: callers use deploy.<name>)
    LOADGEN_JOB,
    MONITORING_SCRIPT,
    MONITORING_TIMEOUT_S,
    _check_env,
    _target_env,
    deploy_monitoring,
    monitoring_status,
    parse_target_report,
    update_monitoring_targets,
)
from .monitoring import script_fields as _kv
from .proc import PreconditionError, StillRunning, VsbenchError
from .state import Node, State

DEFAULT_SCYLLA_IMAGE = "nightly"
DEFAULT_VS_SOURCE = "git:master"
DEFAULT_BENCH_SOURCE = "git:master"
VS_SOURCE_KINDS = {"release", "git", "local", "build"}
BENCH_SOURCE_KINDS = {"git", "local", "build"}
SCYLLA_SCRIPT = "scylla-start.sh"
VS_SCRIPT = "vs-activate.sh"
VS_UNIT = "vector-store.service"
BENCH_BIN = "vector-search-benchmark"
MIN_SCYLLA_RELEASE = (2025, 4)  # first release with vector_store_primary_uri
SAFE_JOB_KINDS = ("fetch",)  # job kinds that touch neither Scylla nor Vector Store
MIN_COUNT_RATIO = 0.99  # an index counts as built at >= 99% of the loaded rows

PULL_TIMEOUT_S = 900
START_TIMEOUT_S = 1500  # drain 120 s + stop 300 s + pull + CQL wait 600 s (scylla-start.sh)
UN_TIMEOUT_S = 400  # scylla-start.sh waits 300 s
EXTRACT_TIMEOUT_S = 900
ACTIVATE_TIMEOUT_S = 300  # vs-activate.sh: 90 s for the new process + 120 s for /api/v1/info
HUB_TIMEOUT_S = 20
SSH_TIMEOUT_S = 90
POLL_S = 3.0
PROGRESS_EVERY_S = 30.0
MAX_STRIKES = 3

_IMAGE_RE = re.compile(
    r"^(?P<repo>[a-z0-9]+(?:[._-][a-z0-9]+)*/[a-z0-9]+(?:[._-][a-z0-9]+)*)"
    r"(?::(?P<tag>[A-Za-z0-9_][A-Za-z0-9._-]{0,127}))?(?:@(?P<digest>sha256:[0-9a-f]{64}))?$"
)
_SCYLLA_RELEASE_RE = re.compile(r"^(latest|(?P<year>[0-9]{4})\.(?P<minor>[0-9]+)(\.[0-9]+)?(-rc[0-9]+)?)$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_NODETOOL_RE = re.compile(r"^(?P<state>[UD][NLJM])\s+(?P<address>\S+)\s+(?P<rest>.*)$")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_VS_EXE_RE = re.compile(rf"^{re.escape(config.NODE_VS_DIR)}/builds/([^/]+)/vector-store$")
_BUILD_LINK_RE = re.compile(r"(?:^|/)builds/([^/]+)")
_CQL_NAME_RE = re.compile(r"^[A-Za-z0-9_]{1,64}$")
_ACTIVE_JOBS = (
    "systemctl list-units --plain --no-legend --all --state=active,activating,deactivating,reloading "
    f"'{remote.JOB_UNIT_PREFIX}*' 2>/dev/null | awk '{{print $1}}'"
)
_VS_URL = f"http://127.0.0.1:{config.PORT_VS}/api/v1"
_VS_PROBE = (
    'p=$(systemctl show -p MainPID --value vector-store.service 2>/dev/null); echo "now=$(date +%s.%N)"; '
    'echo "active=$(systemctl is-active vector-store.service 2>/dev/null)"; echo "pid=${p:-0}"; '
    'if [ "${p:-0}" != 0 ]; then echo "exe=$(readlink -f /proc/$p/exe 2>/dev/null || sudo -n readlink -f '
    '/proc/$p/exe)"; fi; for e in info status indexes; do '
    f"echo \"$e=$(curl -fsS --max-time 5 {_VS_URL}/$e 2>&1 | tr -d '\\n')\"; done"
)
_JOURNAL_SERVING = (
    "journalctl -u vector-store.service --since @{since} -o short-unix --no-pager 2>/dev/null"
    " | grep -m1 'Service is running' | cut -d' ' -f1"
)

_sleep = time.sleep
_monotonic = time.monotonic
T = TypeVar("T")


# --- small helpers ------------------------------------------------------------------
def _float_or_none(text: Any) -> float | None:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _deployed(current: State, component: str) -> dict[str, Any] | None:
    return (current.get("deployed") or {}).get(component)


def _remaining(budget_s: int, started: float) -> int:
    """What is left of a foreground budget of `budget_s` seconds counted from `started` (monotonic)."""
    return max(0, int(budget_s - (_monotonic() - started)))


def _role_nodes(current: State, role: str) -> list[Node]:
    members = st.nodes(current, role)
    if not members:
        raise PreconditionError(f"the cluster has no {role} nodes", "create a cluster with them: vsbench up")
    return members


def _parallel(names: list[str], work: Callable[[str], T]) -> tuple[dict[str, T], dict[str, VsbenchError]]:
    """Run work(name) for every node in parallel: (results, errors), both in input order."""
    done: dict[str, T] = {}
    failed: dict[str, VsbenchError] = {}
    if names:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(names), remote.MAX_PARALLEL)) as pool:
            futures = {pool.submit(work, name): name for name in names}
            for future in concurrent.futures.as_completed(futures):
                try:
                    done[futures[future]] = future.result()
                except VsbenchError as err:
                    failed[futures[future]] = err
    return {n: done[n] for n in names if n in done}, {n: failed[n] for n in names if n in failed}


def _on_all(names: list[str], work: Callable[[str], T], what: str) -> dict[str, T]:
    """_parallel that raises one error naming every node that failed."""
    done, failed = _parallel(names, work)
    if failed:
        lines = [f"{what} failed on {', '.join(failed)}:"]
        lines += [f"  {name}: " + str(err).strip().replace("\n", "\n    ") for name, err in failed.items()]
        raise VsbenchError("\n".join(lines), next((err.hint for err in failed.values() if err.hint), None))
    return done


def _pick_spec(explicit: str | None, pins: dict[str, Any], key: str, default: str, refresh: bool) -> tuple[str, str]:
    """(spec to resolve, floating spec to remember as pins[<key>_spec])."""
    if explicit:
        return explicit, explicit
    origin = pins.get(f"{key}_spec") or pins.get(key) or default
    if refresh or not pins.get(key):
        return origin, origin
    return pins[key], origin


def _with_pins(current: State, pins: dict[str, str]) -> State:
    return {**current, "pins": {**(current.get("pins") or {}), **pins}}


def require_no_bench_job(cluster: str, current: State) -> None:
    """PreconditionError while a job other than a dataset fetch runs on the client
    (job ids are `<utc>-<kind>-<hex>`; the unit list on the client is authoritative)."""
    output = remote.run(cluster, "client", _ACTIVE_JOBS, timeout=SSH_TIMEOUT_S).stdout
    prefix, suffix = remote.JOB_UNIT_PREFIX, ".service"
    active = [w[len(prefix) : -len(suffix)] for w in output.split() if w.startswith(prefix) and w.endswith(suffix)]
    busy = [job for job in active if "-".join(job.split("-")[1:-1]) not in SAFE_JOB_KINDS]
    if busy:
        hint = f"wait for it (vsbench -c {cluster} job wait {busy[0]}) or cancel it (job cancel {busy[0]})"
        raise PreconditionError(f"a benchmark job is running on the client: {', '.join(busy)}", hint)
    stale = [j for j, job in (current.get("jobs") or {}).items() if job.get("status") == "running" and j not in active]
    if stale:
        proc.warn(f"job(s) {', '.join(stale)} ended on the client; record them: vsbench -c {cluster} job wait <id>")


# --- Scylla image -----------------------------------------------------------------
def _fetch_json(url: str) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": "vsbench", "Accept": "application/json"})
    hint = "or pass an image pinned by digest: --image <repo>@sha256:<digest>"
    try:
        with urllib.request.urlopen(request, timeout=HUB_TIMEOUT_S) as response:
            return json.load(response)
    except urllib.error.HTTPError as err:
        raise VsbenchError(f"Docker Hub request failed: HTTP {err.code} for {url}", "check the tag, " + hint) from err
    except (OSError, ValueError) as err:
        raise VsbenchError(f"Docker Hub request failed for {url}: {err}", "check the network, " + hint) from err


def _hub_digest(repo: str, tag: str) -> str:
    """The (multi-arch) digest of a Docker Hub tag; it must include linux/arm64."""
    namespace, name = repo.split("/", 1)
    url = f"https://hub.docker.com/v2/namespaces/{namespace}/repositories/{name}/tags/{tag}"
    if (repo, tag) == (config.SCYLLA_NIGHTLY_REPO, "latest"):
        url = config.DOCKER_HUB_NIGHTLY_LATEST
    data = _fetch_json(url)
    digest = str(data.get("digest") or "") if isinstance(data, dict) else ""
    if not _DIGEST_RE.match(digest):
        raise VsbenchError(f"Docker Hub returned no digest for {repo}:{tag}")
    archs = {str(i.get("architecture")) for i in data.get("images") or [] if isinstance(i, dict)}
    if archs and "arm64" not in archs:
        hint = "use a multi-arch tag (single-arch Scylla nightly tags end in -aarch64)"
        raise VsbenchError(f"{repo}:{tag} has no linux/arm64 image ({', '.join(sorted(archs))})", hint)
    return digest


def resolve_scylla_image(spec: str) -> tuple[str, str]:
    """(image pinned by digest, display text) for `nightly`, `release:<ver>` or a Docker Hub image."""
    text, digest = spec.strip(), None
    if text == "nightly":
        repo, tag = config.SCYLLA_NIGHTLY_REPO, "latest"
    elif text.startswith("release:"):
        repo, tag = config.SCYLLA_RELEASE_REPO, text.split(":", 1)[1]
        release = _SCYLLA_RELEASE_RE.match(tag)
        if not release:
            raise VsbenchError(f"invalid Scylla release '{tag}'", "use release:<YYYY.N[.P]> or release:latest")
        if release.group("year") and (int(release.group("year")), int(release.group("minor"))) < MIN_SCYLLA_RELEASE:
            raise VsbenchError(f"Scylla {tag} has no vector search", "use release 2025.4 or newer")
    else:
        match = _IMAGE_RE.match(text)
        if not match:
            hint = "use nightly, release:<version> or a Docker Hub image (scylladb/scylla-nightly@sha256:<digest>)"
            raise VsbenchError(f"invalid Scylla image '{spec}'", hint)
        repo, tag, digest = match.group("repo"), match.group("tag"), match.group("digest")
    if digest:
        return f"{repo}@{digest}", f"{repo}{':' + tag if tag else ''}@{digest[:19]}"
    tag = tag or "latest"
    digest = _hub_digest(repo, tag)
    proc.log(f"{repo}:{tag} resolves to {digest[:19]}")
    return f"{repo}@{digest}", f"{repo}:{tag} ({digest[:19]})"


# --- Scylla --------------------------------------------------------------------------
def _vs_primary(current: State) -> str:
    return ",".join(f"http://{n['private_ip']}:{config.PORT_VS}" for n in st.nodes(current, "vs"))


def _scylla_env(node: Node, image: str, seed_ip: str, vs_primary: str) -> dict[str, str]:
    io = config.IO_PROPERTIES.get(str(node.get("instance_type")))
    io_values = [str(v) for v in io] if io else ["", "", "", ""]
    io_keys = ("IO_READ_BW", "IO_READ_IOPS", "IO_WRITE_BW", "IO_WRITE_IOPS")
    env = {"ACTION": "start", "IMAGE": image, "IP": node["private_ip"], "SEED": seed_ip}
    env |= {"RACK": f"rack{node['index'] + 1}", "VS_PRIMARY": vs_primary, "DC": config.SCYLLA_DC}
    return env | {"CLUSTER_NAME": config.SCYLLA_CLUSTER_NAME} | dict(zip(io_keys, io_values, strict=True))


def parse_nodetool_status(text: str) -> list[dict[str, str | None]]:
    """Rows of `nodetool status`: {state (UN, DN, UJ...), address, host_id, rack}."""
    rows: list[dict[str, str | None]] = []
    for line in text.splitlines():
        match = _NODETOOL_RE.match(line.strip())
        if match:
            rest, host = match.group("rest").split(), _UUID_RE.search(match.group("rest"))
            row = {"state": match.group("state"), "address": match.group("address")}
            rows.append(row | {"host_id": host.group(0) if host else None, "rack": rest[-1] if rest else None})
    return rows


def _scylla_rollout(cluster: str, current: State, nodes: list[Node], image: str, fresh: bool) -> None:
    """Pull everywhere; when fresh, stop + wipe every node first; then start the seed and each
    other node in turn (a rolling restart otherwise; the script waits for CQL); then all UN."""
    names = [n["name"] for n in nodes]

    def script(name: str, env: dict[str, str], timeout: float) -> Any:
        return remote.run_script(cluster, name, SCYLLA_SCRIPT, env, timeout=timeout)

    proc.log(f"scylla: pulling {image.rsplit('@', 1)[-1][:19]} on {len(names)} node(s)")
    _on_all(names, lambda n: script(n, {"ACTION": "pull", "IMAGE": image}, PULL_TIMEOUT_S), "docker pull")
    if fresh:
        proc.log("scylla: stopping and wiping every node (fresh cluster)")
        _on_all(names, lambda n: script(n, {"ACTION": "wipe"}, START_TIMEOUT_S), "scylla wipe")
    seed_ip, vs_primary = nodes[0]["private_ip"], _vs_primary(current)
    for position, node in enumerate(nodes):
        what = "seed" if position == 0 else f"{position + 1}/{len(nodes)}"
        proc.log(f"scylla: {'starting' if fresh else 'restarting'} {node['name']} ({what}), waiting for CQL")
        script(node["name"], _scylla_env(node, image, seed_ip, vs_primary), START_TIMEOUT_S)
    expect = ",".join(n["private_ip"] for n in nodes)
    script(names[0], {"ACTION": "wait-un", "EXPECT_IPS": expect}, UN_TIMEOUT_S)


def _scylla_version(cluster: str, names: list[str]) -> str:
    out = remote.run_many(cluster, names, "docker exec scylla scylla --version", sudo=True, timeout=SSH_TIMEOUT_S)
    versions = {name: (result.stdout.strip().splitlines() or [""])[-1] for name, result in out.items()}
    if len(set(versions.values())) != 1:
        raise VsbenchError(f"scylla nodes report different versions: {versions}")
    return next(iter(versions.values()))


def _scylla_unchanged(cluster: str, old: dict[str, Any], image: str, vs_primary: str, names: list[str]) -> bool:
    if old.get("image") != image or old.get("vs_primary") != vs_primary:
        return False
    command = "docker inspect -f '{{.Config.Image}} {{.State.Running}}' scylla 2>/dev/null || true"
    out = remote.run_many(cluster, names, command, sudo=True, check=False, timeout=SSH_TIMEOUT_S)
    return all(result.stdout.strip() == f"{image} true" for result in out.values())


def _with_scylla(current: State, info: dict[str, Any], pins: dict[str, str], fresh: bool) -> State:
    updated = _with_pins(st.with_deployed(current, "scylla", info), pins)
    if fresh and updated.get("load"):
        proc.warn("the Scylla data was wiped: the loaded dataset and its index are gone (run `vsbench bench load`)")
        updated["load"] = None
    return updated


def deploy_scylla(cluster: str, image_spec: str | None, wipe: bool, refresh: bool) -> State:
    """Deploy (or rolling-upgrade) Scylla; `wipe` (implied on first deploy) starts a fresh cluster."""
    current = st.require(cluster)
    nodes = _role_nodes(current, "scylla")
    names = [n["name"] for n in nodes]
    require_no_bench_job(cluster, current)
    spec, origin = _pick_spec(image_spec, current.get("pins") or {}, "scylla_image", DEFAULT_SCYLLA_IMAGE, refresh)
    image, display = resolve_scylla_image(spec)
    old, vs_primary = _deployed(current, "scylla") or {}, _vs_primary(current)
    pins = {"scylla_image": image, "scylla_image_spec": origin}
    if not wipe and _scylla_unchanged(cluster, old, image, vs_primary, names):
        proc.log(f"scylla: unchanged ({image.rsplit('@', 1)[-1][:19]})")
        return st.update(cluster, lambda s: _with_pins(s, pins))
    fresh = wipe or not old
    proc.log(f"scylla: {old.get('display') or old.get('image')} -> {display}" if old else f"scylla: {display}")
    _scylla_rollout(cluster, current, nodes, image, fresh)
    info = {"image": image, "digest": image.rsplit("@", 1)[-1], "version": _scylla_version(cluster, names)}
    info |= {"display": display, "spec": origin, "vs_primary": vs_primary, "deployed_at": proc.iso(proc.utcnow())}
    info["racks"] = {n["name"]: f"rack{n['index'] + 1}" for n in nodes}
    proc.log(f"scylla: {info['version']} on {len(nodes)} node(s), all UN")
    updated = st.update(cluster, lambda s: _with_scylla(s, info, pins, fresh))
    if _deployed(updated, "monitoring"):
        update_monitoring_targets(cluster)
    return updated


def scylla_status(cluster: str) -> dict[str, Any]:
    """{deployed, image, version, expected, un, ok, nodes: nodetool rows (+ node name), error}."""
    current = st.require(cluster)
    nodes, info = st.nodes(current, "scylla"), _deployed(current, "scylla") or {}
    status: dict[str, Any] = {"deployed": bool(info), "image": info.get("image"), "version": info.get("version")}
    status |= {"expected": len(nodes), "un": 0, "ok": False, "nodes": [], "error": None}
    if not info or not nodes:
        return status
    errors = []
    for node in nodes:  # the first node that answers
        try:
            command = "docker exec scylla nodetool status"
            result = remote.run(cluster, node["name"], command, sudo=True, check=False, timeout=SSH_TIMEOUT_S)
        except VsbenchError as err:  # local timeout
            errors.append(f"{node['name']}: {err}")
            continue
        if result.returncode == 0:
            names = {n["private_ip"]: n["name"] for n in nodes}
            rows = [row | {"node": names.get(str(row["address"]))} for row in parse_nodetool_status(result.stdout)]
            un = sum(1 for row in rows if row["state"] == "UN")
            return status | {"nodes": rows, "un": un, "ok": un == len(nodes) == len(rows)}
        errors.append(f"{node['name']}: {proc.tail((result.stderr or result.stdout).strip(), 3)}")
    return status | {"error": "; ".join(errors)}


# --- Vector Store ------------------------------------------------------------------
def env_file_text(env: dict[str, str]) -> str:
    """The .env text: `KEY='value'` lines (sorted); single quotes and newlines are rejected."""
    lines = []
    for key in sorted(env):
        value = str(env[key])
        if not _ENV_KEY_RE.match(key):
            raise VsbenchError(f"invalid environment variable name '{key}'", "use [A-Za-z_][A-Za-z0-9_]*")
        if any(c in value for c in "'\n\r\0"):
            hint = "every value is single-quoted (taken literally) in /opt/vector-store/.env"
            raise VsbenchError(f"unsupported value for {key}: no single quotes or newlines", hint)
        lines.append(f"{key}='{value}'\n")
    return "".join(lines)


def _merge_env(previous: dict[str, str], env_set: dict[str, str], env_unset: list[str]) -> dict[str, str]:
    """Overrides kept in state: previous ones, minus --unset, plus --env."""
    merged = dict(previous)
    for key in env_unset:
        if key not in merged and key not in env_set:
            proc.warn(f"--unset {key}: no --env sets it (built-in defaults stay)")
        merged.pop(key, None)
    merged.update({key: str(value) for key, value in env_set.items()})
    env_file_text(merged)
    return merged


def _node_env(current: State, node: Node, overrides: dict[str, str]) -> dict[str, str]:
    """Defaults (VS node j talks to Scylla node j % N), then the overrides."""
    scylla = _role_nodes(current, "scylla")
    contact = scylla[node["index"] % len(scylla)]["private_ip"]
    defaults = {"RUST_LOG": "info", "VECTOR_STORE_URI": f"0.0.0.0:{config.PORT_VS}"}
    return defaults | {"VECTOR_STORE_SCYLLADB_URI": f"{contact}:{config.PORT_CQL}"} | overrides


def _ensure_vs_binary(cluster: str, node: str, record: dict[str, Any]) -> str:
    """Put builds/<id>/vector-store on the node (sha256-verified); returns its sha256."""
    build_id = record["build_id"]
    if record.get("kind") == "release":
        image = f"{config.VS_DOCKER_REPO}:{record['version']}"
        env = {"ACTION": "extract", "BUILD_ID": build_id, "VS_IMAGE": image, "VERSION": record["version"]}
        sha = _kv(remote.run_script(cluster, node, VS_SCRIPT, env, timeout=EXTRACT_TIMEOUT_S).stdout).get("sha256", "")
        if not _SHA256_RE.match(sha):
            raise VsbenchError(f"{node}: extracting {image} reported no sha256")
        return sha
    expected = (record.get("sha256") or {}).get("vector-store")
    if not expected:
        raise VsbenchError(f"build {build_id} has no vector-store sha256", "rebuild it: vsbench build")
    path = f"{config.NODE_VS_DIR}/builds/{build_id}/vector-store"
    if remote.remote_sha256(cluster, node, path) != expected:
        remote.upload(cluster, node, build.build_dir(build_id) / "vector-store", path, sudo=True, mode="0755")
    return expected


def _activate_vs(cluster: str, current: State, name: str, record: dict[str, Any], env: dict[str, str]) -> dict:
    """vs-activate.sh on one node; checks the version /api/v1/info reports."""
    text = env_file_text(_node_env(current, st.node(current, name), env))
    unit = remote.ensure_script(cluster, name, VS_UNIT)
    script_env = {"ACTION": "activate", "BUILD_ID": record["build_id"], "UNIT_SRC": unit}
    script_env |= {"VS_PORT": str(config.PORT_VS), "ENV_B64": base64.b64encode(text.encode()).decode()}
    out = _kv(remote.run_script(cluster, name, VS_SCRIPT, script_env, timeout=ACTIVATE_TIMEOUT_S).stdout)
    try:
        info = json.loads(out.get("info") or "")
    except ValueError:
        info = {}
    if not isinstance(info, dict) or info.get("version") != record["version"]:
        hint = f"see its log: vsbench -c {cluster} logs {name} --service vector-store"
        raise VsbenchError(f"{name}: /api/v1/info is {out.get('info')!r}, expected {record['version']}", hint)
    proc.log(f"{name}: running {record['build_id']} (pid {out.get('pid')}, was {out.get('old_pid')})")
    return {"restarted_at": _float_or_none(out.get("restarted_at")), "engine": info.get("engine")}


def _vs_unchanged(cluster: str, old: dict, record: dict[str, Any], env: dict[str, str], names: list[str]) -> bool:
    if old.get("build_id") != record["build_id"] or (old.get("env") or {}) != env:
        return False
    probes = _probe_vs(cluster, names)
    return all(p["build_id"] == record["build_id"] and p["active"] == "active" for p in probes.values())


def _vs_info(current: State, record: dict[str, Any], env: dict[str, str], sha: str, done: dict, extra: dict) -> dict:
    """deployed.vector_store; with a built index it carries pending_index_build for wait_serving."""
    old, pending = _deployed(current, "vector_store") or {}, None
    if st.built_index(current):
        restarted = {name: d["restarted_at"] for name, d in done.items() if d.get("restarted_at")}
        pending = {"build_id": record["build_id"], "previous_build_id": old.get("build_id"), "trigger": "deploy-vs"}
        pending |= {"restarted_at": restarted, "extra": dict(extra)}
    info = {key: record.get(key) for key in ("build_id", "version", "source", "commit", "dirty", "kind")}
    info |= {"env": env, "engine": next(iter(done.values())).get("engine"), "sha256": sha}
    info |= {"deployed_at": proc.iso(proc.utcnow()), "nodes": {name: record["build_id"] for name in done}}
    return info | {"pending_index_build": pending}


def deploy_vs(
    cluster: str,
    source: str | None,
    env_set: dict[str, str],
    env_unset: list[str],
    refresh: bool,
    timeout_s: int,
    *,
    record_extra: dict[str, Any] | None = None,
) -> State:
    """Build/resolve, upload, switch every VS node, then wait_serving; `timeout_s` is the budget of the
    whole call (StillRunning once it is used up). With a built index, the rebuild is recorded as an
    index-build result tagged with `record_extra` (e.g. comparison_id, arm, label, series_id, trigger)."""
    started = _monotonic()
    current = st.require(cluster)
    names = [n["name"] for n in _role_nodes(current, "vs")]
    if not _deployed(current, "scylla"):
        raise PreconditionError("Scylla is not deployed", f"deploy it first: vsbench -c {cluster} deploy scylla")
    require_no_bench_job(cluster, current)
    spec, origin = _pick_spec(source, current.get("pins") or {}, "vs_source", DEFAULT_VS_SOURCE, refresh)
    record = build.build(build.parse_source(spec, VS_SOURCE_KINDS))
    old = _deployed(current, "vector_store") or {}
    env = _merge_env(old.get("env") or {}, env_set, env_unset)
    pins = {"vs_source": record.get("pin") or spec, "vs_source_spec": origin}
    if _vs_unchanged(cluster, old, record, env, names):
        proc.log(f"vs: unchanged ({record['build_id']})")
        return st.update(cluster, lambda s: _with_pins(s, pins))
    proc.log(f"vs: {old['build_id']} -> {record['build_id']}" if old.get("build_id") else f"vs: {record['build_id']}")
    previous = old.get("env") or {}
    changes = [f"{k}={v}" for k, v in env.items() if previous.get(k) != v] + [f"-{k}" for k in previous if k not in env]
    if changes:
        proc.log("vs env: " + ", ".join(changes))
    shas = _on_all(names, lambda n: _ensure_vs_binary(cluster, n, record), "vector-store upload")
    if len(set(shas.values())) != 1:
        raise VsbenchError(f"vs nodes got different vector-store binaries: {shas}", "deploy again")
    done = _on_all(names, lambda n: _activate_vs(cluster, current, n, record, env), "vector-store activation")
    info = _vs_info(current, record, env, next(iter(shas.values())), done, record_extra or {})
    st.update(cluster, lambda s: _with_pins(st.with_deployed(s, "vector_store", info), pins))
    wait_serving(cluster, _remaining(timeout_s, started))
    return st.require(cluster)


# --- Vector Store: status and readiness ---------------------------------------------
def _parse_probe(text: str) -> dict[str, Any]:
    fields = _kv(text)
    pid, exe = fields.get("pid", ""), fields.get("exe") or None
    match = _VS_EXE_RE.match(exe or "")
    probe: dict[str, Any] = {"active": fields.get("active") or None, "pid": int(pid) if pid.isdigit() else 0}
    probe |= {"exe": exe, "build_id": match.group(1) if match else None, "now": _float_or_none(fields.get("now"))}
    probe |= {"info": None, "status": None, "indexes": None, "errors": {}, "error": None}
    for key in ("info", "status", "indexes"):
        try:
            probe[key] = json.loads(fields.get(key, ""))
        except ValueError:
            probe["errors"][key] = fields.get(key, "")[:200] or "no answer"
    if isinstance(probe["indexes"], list):
        probe["indexes"] = _with_index_status(probe["indexes"], fields)
    return probe


def _vs_probe(expect: dict[str, Any] | None) -> str:
    """_VS_PROBE; with an expected index it also reads /indexes/<ks>/<idx>/status, because
    Vector Store < 1.11 reports an index's status and count only there, not in /indexes."""
    keyspace, index = str((expect or {}).get("keyspace") or ""), str((expect or {}).get("index") or "")
    if not (_CQL_NAME_RE.match(keyspace) and _CQL_NAME_RE.match(index)):
        return _VS_PROBE
    url = shlex.quote(f"{_VS_URL}/indexes/{keyspace}/{index}/status")
    status = f"curl -fsS --max-time 5 {url} 2>&1 | tr -d '\\n'"
    return f'{_VS_PROBE}; echo "index_ref={keyspace}/{index}"; echo "index_status=$({status})"'


def _with_index_status(entries: list[Any], fields: dict[str, str]) -> list[Any]:
    """/indexes entries; the probed index gets status/count/build_progress it lacks (VS < 1.11)."""
    keyspace, _, index = fields.get("index_ref", "").partition("/")
    try:
        extra = json.loads(fields.get("index_status", ""))
    except ValueError:
        return entries
    if not isinstance(extra, dict):
        return entries
    added = {key: extra[key] for key in ("status", "count", "build_progress") if key in extra}

    def merged(entry: Any) -> Any:
        if not isinstance(entry, dict) or (entry.get("keyspace"), entry.get("index")) != (keyspace, index):
            return entry
        return {**added, **{key: value for key, value in entry.items() if value is not None}}

    return [merged(entry) for entry in entries]


def _probe_vs(cluster: str, names: list[str], expect: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    out = remote.run_many(cluster, names, _vs_probe(expect), check=False, timeout=SSH_TIMEOUT_S)
    probes = {}
    for name, result in out.items():
        failed = remote.is_ssh_failure(result)
        probes[name] = _parse_probe("" if failed else result.stdout)
        if failed:
            probes[name]["error"] = proc.tail((result.stderr or "").strip(), 3) or "ssh failed"
    return probes


def _expected_index(current: State) -> dict[str, Any] | None:
    """{keyspace, index, min_count} of the built index (st.built_index); None without one."""
    built = st.built_index(current)
    if not built:
        return None
    min_count = int((built.get("rows") or 0) * MIN_COUNT_RATIO)
    return {"keyspace": built.get("keyspace"), "index": built["index"], "min_count": min_count}


def vs_status(cluster: str) -> dict[str, dict[str, Any]]:
    """Per VS node: active, pid, exe, build_id (running), now, info, status, indexes, errors, error.
    The built index's entry in `indexes` always has status and count (also on VS < 1.11)."""
    current = st.require(cluster)
    return _probe_vs(cluster, [n["name"] for n in st.nodes(current, "vs")], _expected_index(current))


def _index_entry(probe: dict[str, Any], expect: dict[str, Any] | None) -> dict[str, Any] | None:
    wanted = (expect or {}).get("keyspace"), (expect or {}).get("index")
    entries = [e for e in probe.get("indexes") or [] if isinstance(e, dict)]
    return next((e for e in entries if expect and (e.get("keyspace"), e.get("index")) == wanted), None)


def _is_serving(probe: dict[str, Any], expect: dict[str, Any] | None) -> bool:
    entry = _index_entry(probe, expect) or {}
    if probe.get("status") != "SERVING" or not expect:
        return probe.get("status") == "SERVING"
    return entry.get("status") == "SERVING" and (entry.get("count") or 0) >= expect.get("min_count", 0)


def _describe(probe: dict[str, Any], expect: dict[str, Any] | None) -> str:
    text = str(probe.get("status") or probe.get("error") or next(iter(probe["errors"].values()), "?"))
    entry = _index_entry(probe, expect)
    if entry and expect:
        text += f", {expect['index']} {entry.get('status')} {entry.get('build_progress', '?')}% ({entry.get('count')})"
    elif expect and isinstance(probe.get("indexes"), list):
        text += f", index {expect['index']} not listed"
    return text


def _not_serving(cluster: str, waiting: dict[str, dict], expect: dict | None) -> StillRunning:
    """StillRunning for the nodes still waiting; the hint depends on whether the index exists on them."""
    text = "; ".join(f"{name}: {_describe(probe, expect)}" for name, probe in waiting.items())
    missing = [n for n, p in waiting.items() if expect and p.get("status") == "SERVING" and not _index_entry(p, expect)]
    hint = f"it keeps building on the nodes; wait again with: vsbench -c {cluster} wait-serving"
    if missing and expect:
        hint = (
            f"index {expect['index']} is not listed on {', '.join(missing)} (dropped or never created?); "
            f"rebuild it: vsbench -c {cluster} bench index (details: vsbench -c {cluster} status)"
        )
    return StillRunning(f"vector store is not SERVING yet ({text})", hint)


def _check_alive(cluster: str, name: str, probe: dict[str, Any], pids: dict[str, int], strikes: dict) -> None:
    """Fail on a crash (MainPID changed) or after MAX_STRIKES polls with the service down."""
    hint = f"see its log: vsbench -c {cluster} logs {name} --service vector-store"
    pid = probe.get("pid") or 0
    if pids.get(name) and pid and pid != pids[name]:
        raise VsbenchError(f"{name}: vector-store restarted while building (pid {pids[name]} -> {pid}): crash?", hint)
    if pid:
        pids[name] = pid
    healthy = probe.get("active") in ("active", "reloading") and not probe.get("error")
    strikes[name] = 0 if healthy else strikes.get(name, 0) + 1
    if strikes[name] >= MAX_STRIKES:
        raise VsbenchError(f"{name}: vector-store is {probe.get('active') or probe.get('error') or 'down'}", hint)


def _poll_serving(cluster: str, names: list[str], expect: dict | None, deadline: float) -> dict[str, dict]:
    serving: dict[str, dict] = {}
    pids: dict[str, int] = {}
    strikes: dict[str, int] = {}
    reported = _monotonic()
    while True:
        probes = _probe_vs(cluster, [n for n in names if n not in serving], expect)
        for name, probe in probes.items():
            if _is_serving(probe, expect):
                serving[name] = probe
            else:
                _check_alive(cluster, name, probe, pids, strikes)
        if len(serving) == len(names):
            return serving
        if _monotonic() >= deadline:
            raise _not_serving(cluster, {n: probes[n] for n in names if n not in serving}, expect)
        if _monotonic() - reported >= PROGRESS_EVERY_S:
            waiting = "; ".join(f"{n}: {_describe(probes[n], expect)}" for n in names if n not in serving)
            proc.log(f"vs: waiting for SERVING: {waiting}")
            reported = _monotonic()
        _sleep(POLL_S)


def _index_build_info(cluster: str, pending: dict[str, Any], serving: dict[str, dict]) -> dict[str, Any]:
    """restart -> SERVING seconds per node: SERVING time from the first `Service is running`
    journal line after the restart (node clock), else the poll that saw it (approximate)."""
    restarted = {n: float(t) for n, t in (pending.get("restarted_at") or {}).items() if t is not None}
    exact: dict[str, float] = {}
    if restarted:
        command = _JOURNAL_SERVING.format(since=int(min(restarted.values())))
        out = remote.run_many(cluster, sorted(restarted), command, sudo=True, check=False, timeout=SSH_TIMEOUT_S)
        found = {n: _float_or_none(r.stdout.strip()) for n, r in out.items()}
        exact = {n: t for n, t in found.items() if t is not None and t >= restarted[n]}
    ends = {n: exact.get(n) or (serving.get(n) or {}).get("now") or start for n, start in restarted.items()}
    info: dict[str, Any] = {"trigger": pending.get("trigger"), "build_id": pending.get("build_id")}
    info |= {"previous_build_id": pending.get("previous_build_id"), **(pending.get("extra") or {})}
    info["seconds"] = {n: round(ends[n] - start, 1) for n, start in restarted.items()}
    window = (min(restarted.values()), max(ends.values())) if restarted else None
    if window:
        info["window"] = tuple(datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc) for t in window)
    if any(n not in exact for n in restarted):
        info["approximate_nodes"] = sorted(n for n in restarted if n not in exact)
    return info


def _finalize_index_build(cluster: str, serving: dict[str, dict]) -> dict[str, Any] | None:
    """Record the index-build result of the last `deploy vs` (once, only while the index is built),
    then clear the marker (a stale one never becomes a record of a later, unrelated build)."""
    current = st.load(cluster) or {}
    pending = (_deployed(current, "vector_store") or {}).get("pending_index_build")
    if not pending:
        return None
    record = None
    if st.built_index(current):
        record = results.record_index_build(cluster, _index_build_info(cluster, pending, serving))
        seconds = (record.get("index_build") or {}).get("seconds") or {}
        proc.log("vs: index rebuilt after the restart in " + ", ".join(f"{n} {s}s" for n, s in seconds.items()))

    def clear(state: State) -> State:
        info = _deployed(state, "vector_store")
        return st.with_deployed(state, "vector_store", {**info, "pending_index_build": None}) if info else state

    st.update(cluster, clear)
    return record


def _log_unbuilt_index(cluster: str, current: State) -> None:
    """Say why wait_serving does not wait for load.index: that index is not built (st.built_index)."""
    load = current.get("load") or {}
    if not load.get("index"):
        return
    job = load.get("pending_job")
    why = f"job {job} is not recorded as finished" if job else "its load/index job failed or was cancelled"
    proc.log(f"vs: index {load['index']} is not built ({why}); waiting for node-level SERVING only")
    todo = f"job wait {job}" if job else f"bench load {load.get('dataset')} --resume (or bench index)"
    proc.log(f"vs: to get the index: vsbench -c {cluster} {todo}")


def wait_serving(cluster: str, timeout_s: int, expect_index: dict | None = None) -> dict[str, Any]:
    """Wait until every VS node is SERVING and, by default, the built index (st.built_index) is
    SERVING with >= 99% of its rows (expect_index {keyspace, index, min_count}; {} skips the index
    check; without a built index only node-level SERVING counts). Raises StillRunning after
    timeout_s. Records the pending index-build of the last deploy vs."""
    current = st.require(cluster)
    if not _deployed(current, "vector_store"):
        raise PreconditionError("the vector store is not deployed", f"run: vsbench -c {cluster} deploy vs")
    names = [n["name"] for n in _role_nodes(current, "vs")]
    expect = _expected_index(current) if expect_index is None else expect_index
    if expect_index is None and not expect:
        _log_unbuilt_index(cluster, current)
    started = _monotonic()
    serving = _poll_serving(cluster, names, expect, started + max(0, timeout_s))
    record = _finalize_index_build(cluster, serving)
    proc.log(f"vs: {len(names)} node(s) SERVING" + (f" {expect['index']}" if expect else ""))
    counts = {n: (_index_entry(p, expect) or {}).get("count") for n, p in serving.items()}
    nodes = {n: {"build_id": p["build_id"], "count": counts[n]} for n, p in serving.items()}
    return {"serving": True, "waited_s": round(_monotonic() - started, 1), "nodes": nodes, "index_build": record}


# --- bench binary ----------------------------------------------------------------------
def deploy_bench(cluster: str, source: str | None, refresh: bool) -> State:
    """Upload vector-search-benchmark to the client and point NODE_BENCH_DIR's symlink at it."""
    current = st.require(cluster)
    st.client(current)
    require_no_bench_job(cluster, current)
    spec, origin = _pick_spec(source, current.get("pins") or {}, "bench_source", DEFAULT_BENCH_SOURCE, refresh)
    record = build.build(build.parse_source(spec, BENCH_SOURCE_KINDS))
    build_id, sha = record["build_id"], (record.get("sha256") or {}).get(BENCH_BIN)
    if not sha:
        raise VsbenchError(f"build {build_id} has no {BENCH_BIN}", "use --source git:<ref>, local or build:<id>")
    link = f"{config.NODE_BENCH_DIR}/{BENCH_BIN}"
    pins = {"bench_source": record.get("pin") or spec, "bench_source_spec": origin}
    old = _deployed(current, "bench") or {}
    if old.get("build_id") == build_id and remote.remote_sha256(cluster, "client", link) == sha:
        proc.log(f"bench: unchanged ({build_id})")
        return st.update(cluster, lambda s: _with_pins(s, pins))
    proc.log(f"bench: {old['build_id']} -> {build_id}" if old.get("build_id") else f"bench: deploying {build_id}")
    target = f"{config.NODE_BENCH_DIR}/builds/{build_id}/{BENCH_BIN}"
    if remote.remote_sha256(cluster, "client", target) != sha:
        remote.upload(cluster, "client", build.build_dir(build_id) / BENCH_BIN, target, mode="0755")
    q, tmp = shlex.quote, f"{link}.tmp"
    swap = f"ln -sfn {q(f'builds/{build_id}/{BENCH_BIN}')} {q(tmp)} && mv -T {q(tmp)} {q(link)} && {q(link)} --version"
    lines = remote.run(cluster, "client", swap, timeout=SSH_TIMEOUT_S).stdout.strip().splitlines()
    if (lines or [""])[-1] != f"{BENCH_BIN} {record['version']}":
        raise VsbenchError(f"client: {link} --version printed {lines!r}, expected '{BENCH_BIN} {record['version']}'")
    info = {key: record.get(key) for key in ("build_id", "version", "source", "commit", "dirty")}
    info |= {"sha256": sha, "deployed_at": proc.iso(proc.utcnow())}
    return st.update(cluster, lambda s: _with_pins(st.with_deployed(s, "bench", info), pins))


# --- all / inventory -------------------------------------------------------------------
def deploy_all(cluster: str, force: bool, timeout_s: int = config.DEFAULT_FOREGROUND_SECONDS) -> State:
    """Deploy the components not deployed yet (all of them, with pinned specs, when force).
    `timeout_s` is the budget of the whole call: the Vector Store wait gets what is left of it."""
    started = _monotonic()
    current = st.require(cluster)
    steps: list[tuple[str, Callable[[], State]]] = [
        ("scylla", lambda: deploy_scylla(cluster, None, False, False)),
        ("monitoring", lambda: deploy_monitoring(cluster)),
        ("bench", lambda: deploy_bench(cluster, None, False)),
        ("vector_store", lambda: deploy_vs(cluster, None, {}, [], False, _remaining(timeout_s, started))),
    ]
    todo = [(component, run) for component, run in steps if force or not _deployed(current, component)]
    if not todo:
        proc.log("deploy all: everything is deployed already (--force redeploys the pinned versions)")
    for component, run in todo:
        proc.log(f"deploy all: {component}")
        current = run()
    return current


def builds_on_nodes(cluster: str) -> dict[str, Any]:
    """{vector_store: {node: {builds, current, error}}, bench: {client: {...}}}: build dirs on the nodes."""
    current = st.require(cluster)

    def listing(directory: str, link: str) -> str:
        return f'ls -1 {shlex.quote(directory)} 2>/dev/null; echo "current=$(readlink {shlex.quote(link)})"'

    def parse(result: Any) -> dict[str, Any]:
        if remote.is_ssh_failure(result):
            return {"builds": [], "current": None, "error": proc.tail((result.stderr or "").strip(), 3)}
        match = _BUILD_LINK_RE.search(_kv(result.stdout).get("current", ""))
        names = sorted(line.strip() for line in result.stdout.splitlines() if line.strip() and "=" not in line)
        return {"builds": names, "current": match.group(1) if match else None, "error": None}

    vs_dir, bench_dir = config.NODE_VS_DIR, config.NODE_BENCH_DIR
    names = [n["name"] for n in st.nodes(current, "vs")]
    vs = remote.run_many(cluster, names, listing(f"{vs_dir}/builds", f"{vs_dir}/current"), check=False, timeout=60)
    command = listing(f"{bench_dir}/builds", f"{bench_dir}/{BENCH_BIN}")
    bench = remote.run(cluster, "client", command, check=False, timeout=SSH_TIMEOUT_S)
    return {"vector_store": {n: parse(r) for n, r in vs.items()}, "bench": {"client": parse(bench)}}
