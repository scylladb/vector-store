# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""vsbench command handlers: cluster lifecycle, builds, deploy, node access and Prometheus.

Split from cli.py (which parses, dispatches and records history) to keep files small.
Each handler takes (Context, argparse.Namespace), prints its result on stdout and
returns the exit code; failures are raised as VsbenchError subclasses.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime
import json
import shlex
import stat
import sys
from pathlib import Path
from typing import Any

from . import awsapi, config, guard, proc
from . import state as st
from .awsapi import Aws
from .cli import Context, UsageError, load_module, out, print_doc, short, table, tree
from .proc import PreconditionError, VsbenchError

OVERDUE_GRACE_S = 15 * 60  # same as provision.OVERDUE_GRACE_S (not imported: status must work without it)
MAX_OUTPUT_BYTES = 20_000
TUNNEL_PORTS = ((13000, config.PORT_GRAFANA), (19090, config.PORT_PROMETHEUS))
# Not through the ControlMaster (-S none): a multiplexed forward returns at once and dies with
# the master's ControlPersist. This one stays in the foreground until Ctrl-C.
TUNNEL_OPTIONS = "-S none -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -N"
# tag stale: the expiry is this machine's state, newer than the ExpiresAt tag (re-synced just now)
LIST_COLUMNS = (
    "cluster",
    "region",
    "owner",
    "nodes",
    "states",
    "types",
    "cost/h",
    "expires_in",
    "tag stale",
    "OVERDUE",
)
MAX_LIST_WORKERS = 8
DEFAULT_LOG_SERVICE = {"scylla": "scylla", "vs": "vector-store", "client": "monitoring"}
MONITORING_CONTAINERS = ("aprom", "agraf")  # scylla-monitoring's Prometheus and Grafana containers
NODE_KEYS = ("name", "role", "instance_type", "instance_id", "public_ip", "private_ip", "aws_state")
DEPLOYED_KEYS = {"scylla": ["scylla"], "vs": ["vector_store"], "bench": ["bench"], "monitoring": ["monitoring"]}
DEPLOYED_KEYS["all"] = ["scylla", "vector_store", "bench", "monitoring"]
I_MEAN_IT_HINT = "Pass --i-mean-it only if the user asked for exactly this"


def _print_deployed(state: Any, keys: list[str]) -> None:
    deployed = (state or {}).get("deployed") or {} if isinstance(state, dict) else {}
    for key in keys:
        info = deployed.get(key)
        fields = ("version", "build_id", "image", "source", "dashboards")
        brief = " ".join(f"{f}={info[f]}" for f in fields if isinstance(info, dict) and info.get(f))
        out(f"{key}: {brief or ('deployed' if info else 'not deployed')}")


# --- cluster lifecycle -------------------------------------------------------------------
def cmd_doctor(ctx: Context, args: argparse.Namespace) -> int:
    report = load_module("provision").doctor(ctx.aws())
    if args.json:
        proc.print_json(report)
    else:
        out(table(report["checks"], ("check", "status", "detail", "hint")))
    failed = {c["check"] for c in report["checks"] if c["status"] == "fail"}
    if failed & {"credentials", "identity"}:
        return proc.EXIT_AUTH
    return proc.EXIT_ERROR if failed else 0


def cmd_login(ctx: Context, args: argparse.Namespace) -> int:
    login = load_module("login")

    def on_url(url: str) -> None:
        out(url)  # stdout: the only thing printed there, so an agent can relay it as it is
        sys.stdout.flush()  # stdout is a pipe when an agent runs this: the URL must not wait for the exit
        until = proc.utcnow() + datetime.timedelta(seconds=login.OKTA_APPROVAL_S)
        proc.log(f"approve this URL in a browser (Okta, with MFA) within about {login.OKTA_APPROVAL_S // 60} minutes,")
        proc.log(f"by {until.strftime('%H:%M:%S')}Z; then the code expires and the command exits 3")

    result = login.login(args.username, args.timeout_s, on_url=on_url)
    expires = result.get("expires_at") or "unknown"
    proc.log(f"logged in as {result['username']}: profile {result['profile']}, credentials expire at {expires}")
    return 0


def up_options(provision: Any, args: argparse.Namespace) -> Any:
    """provision.UpOptions from the flags; explicit_profile: --profile was given on the command line
    (then up does not insist on config.EXPECTED_ACCOUNT)."""
    fields = {f.name for f in dataclasses.fields(provision.UpOptions)}
    values = {n: getattr(args, n) for n in fields if hasattr(args, n)}
    if "explicit_profile" in fields:
        values["explicit_profile"] = args.profile is not None
    return provision.UpOptions(**values)


def dry_run_lines(ctx: Context, result: dict[str, Any]) -> list[str]:
    """The `up --dry-run` plan: AWS identity, cost (and why it is incomplete), adoption and resume."""
    cost, unpriced = result.get("cost_per_hour"), result.get("unpriced_types") or []
    azs = ", ".join(p.get("az", "?") for p in result.get("az_candidates") or []) or "none"
    aws = {k: result.get(k) or default for k, default in (("profile", ctx.profile), ("account", "?"))}
    lines = [f"dry run, nothing created: {len(result.get('nodes') or [])} nodes"]
    lines.append(
        f"AWS: profile {aws['profile']}, account {aws['account']}, region {result.get('region') or ctx.region}"
    )
    lines.append(
        f"cost: {'?' if cost is None else f'${cost:.2f}'}/h; TTL {proc.format_duration(result['ttl_seconds'])}"
    )
    if unpriced:
        lines.append(f"  no price for {', '.join(unpriced)}: budget check incomplete")
    if result.get("other_clusters_cost_per_hour"):
        lines.append(f"  your other running clusters here: ${result['other_clusters_cost_per_hour']:.2f}/h")
    lines.append(f"AZ candidates: {azs}")
    if result.get("would_adopt"):
        lines.append(f"would adopt the instances of the interrupted up: {', '.join(result['would_adopt'])}")
    if result.get("kept_expires_at"):
        lines.append(f"resume keeps the expiry {result['kept_expires_at']}")
    if result.get("resume_note"):
        lines.append(f"  {result['resume_note']}")
    return lines


def cmd_up(ctx: Context, args: argparse.Namespace) -> int:
    provision = load_module("provision")
    result = provision.up(ctx.cluster, ctx.aws(), up_options(provision, args))
    lines = dry_run_lines(ctx, result) if result.get("dry_run") else provision.summary_lines(result)
    out("\n".join(lines))
    return 0


def cmd_down(ctx: Context, args: argparse.Namespace) -> int:
    result = load_module("provision").down(ctx.cluster, ctx.aws(), args.yes, args.purge)
    out(f"cluster '{ctx.cluster}' down: terminated {', '.join(result['terminated']) or 'no instances'}")
    out(
        f"deleted security groups: {', '.join(result['security_groups']) or '-'}; key pair: {result['key_pair'] or '-'}"
    )
    if result.get("purged"):
        out(f"deleted {st.paths(ctx.cluster).root}")
    return 0


def state_regions() -> list[str]:
    """Regions recorded in $VSBENCH_HOME/clusters/*/state.json (unreadable files are skipped)."""
    found = set()
    for path in (st.home() / "clusters").glob("*/state.json"):
        try:
            region = json.loads(path.read_text()).get("region")
        except (OSError, ValueError, AttributeError):
            continue
        if isinstance(region, str) and region:
            found.add(region)
    return sorted(found)


def list_regions(ctx: Context, owner: str | None) -> tuple[list[str], list[dict[str, Any]]]:
    """provision.list_clusters in ctx.region (its failure fails the command) and in every region
    with local state (a failure there is a warning); (regions scanned, rows with a region)."""
    provision = load_module("provision")
    regions = [ctx.region, *(r for r in state_regions() if r != ctx.region)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(regions), MAX_LIST_WORKERS)) as pool:
        futures = {r: pool.submit(provision.list_clusters, Aws(ctx.profile, r), owner) for r in regions}
    scanned, rows = [], []
    for region, future in futures.items():
        try:
            found = future.result()
        except VsbenchError as err:
            if region == ctx.region:
                raise
            proc.warn(f"cannot list region {region}: {(str(err).splitlines() or [type(err).__name__])[0]}")
            continue
        scanned.append(region)
        rows += [row | {"region": region} for row in found]
    return scanned, rows


def _list_row(row: dict[str, Any]) -> dict[str, Any]:
    left = row.get("expires_in_s")
    expires = "?" if left is None else proc.format_duration(left)
    states = ",".join(f"{k}:{v}" for k, v in sorted((row.get("states") or {}).items()))
    cost = row.get("cost_per_hour")
    extra = {"states": states, "expires_in": expires, "cost/h": "?" if cost is None else f"${cost:.2f}"}
    extra |= {"types": ",".join(row.get("types") or []), "tag stale": row.get("tag_stale")}
    return row | extra | {"OVERDUE": row.get("overdue")}


def cmd_list(ctx: Context, args: argparse.Namespace) -> int:
    owner = None if args.all_owners else awsapi.identity(ctx.aws()).owner
    scanned, rows = list_regions(ctx, owner)
    if args.json:
        proc.log(f"regions scanned: {', '.join(scanned)}")
        proc.print_json(rows)
        return 0
    out(f"regions scanned: {', '.join(scanned)}")
    out(table([_list_row(row) for row in rows], LIST_COLUMNS))
    return 0


def tunnel_command(cluster: str) -> str:
    """The Grafana/Prometheus tunnel; it blocks until Ctrl-C (the user runs it in a terminal)."""
    forwards = " ".join(f"-L {local}:127.0.0.1:{port}" for local, port in TUNNEL_PORTS)
    return f"ssh -F {shlex.quote(str(st.paths(cluster).ssh_config))} {TUNNEL_OPTIONS} {forwards} client"


def _live_status(cluster: str, state: st.State) -> dict[str, Any]:
    """deploy.scylla_status / vs_status / monitoring_status and prom.index_status (index rows vs the
    base table, CDC lags) in parallel; failures become {error, hint}."""
    live: dict[str, Any] = {}
    try:
        deploy = load_module("deploy")
        calls = {"scylla": deploy.scylla_status, "vector_store": deploy.vs_status}
        calls["monitoring"] = deploy.monitoring_status
    except VsbenchError as err:
        calls, live = {}, dict.fromkeys(("scylla", "vector_store", "monitoring"), {"error": str(err)})
    calls["index"] = lambda c: load_module("prom").index_status(c, state)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as pool:
        futures = {key: pool.submit(func, cluster) for key, func in calls.items()}
    for key, future in futures.items():
        try:
            live[key] = future.result()
        except VsbenchError as err:
            live[key] = {"error": str(err).splitlines()[0] if str(err) else type(err).__name__, "hint": err.hint}
    return live


def status_doc(cluster: str, state: st.State, live: dict[str, Any], now: datetime.datetime) -> dict[str, Any]:
    expires = state.get("expires_at")
    try:
        left = int((proc.parse_iso(expires) - now).total_seconds()) if expires else None
    except ValueError:
        left = None
    jobs = {i: j for i, j in (state.get("jobs") or {}).items() if isinstance(j, dict) and j.get("status") == "running"}
    doc = {k: state.get(k) for k in ("cluster", "owner", "profile", "region", "az", "created_at", "expires_at")}
    doc |= {"expires_in_s": left, "expires_in": proc.format_duration(left) if left is not None else None}
    doc |= {"overdue": left is not None and left < -OVERDUE_GRACE_S, "terminated_at": state.get("terminated_at")}
    doc |= {"nodes": [{k: n.get(k) for k in NODE_KEYS if k in n} for n in st.nodes(state)]}
    doc |= {k: state.get(k) for k in ("deployed", "pins", "load")} | {"running_jobs": jobs, "live": live}
    return doc | {"tunnel": tunnel_command(cluster), "grafana": "http://127.0.0.1:13000"}


def format_status(doc: dict[str, Any]) -> str:
    flag = "  ** OVERDUE: the on-node TTL failed; offer `down` **" if doc["overdue"] else ""
    lines = [f"cluster {doc['cluster']}: owner {doc['owner']}, {doc['region']} ({doc['az']}), profile {doc['profile']}"]
    lines.append(f"expires {doc['expires_at']} (in {doc['expires_in']}){flag}")
    lines += ["nodes:", *("  " + line for line in table(doc["nodes"], NODE_KEYS[:6]).splitlines())]
    lines += ["deployed:", *tree(doc.get("deployed") or {}, "  ", 2)]
    lines += ["pins:", *tree(doc.get("pins") or {}, "  ", 1)]
    lines += ["load:", *tree(doc.get("load") or "none", "  ", 2)]
    jobs = [
        f"  {i}: {j.get('kind')} on {j.get('node')} since {j.get('started_at')}" for i, j in doc["running_jobs"].items()
    ]
    lines += ["running jobs:", *(jobs or ["  (none)"])]
    for key, value in doc["live"].items():
        lines += [f"{key} (live):", *tree(value, "  ", 3)]
    lines.append(f"grafana/prometheus tunnel (blocks until Ctrl-C; run it in your own terminal): {doc['tunnel']}")
    lines.append(f"  then open {doc['grafana']} (Prometheus: http://127.0.0.1:19090)")
    return "\n".join(lines)


def cmd_status(ctx: Context, args: argparse.Namespace) -> int:
    if args.refresh:
        state = load_module("provision").refresh_status(ctx.cluster, ctx.aws())
    else:
        state = st.require(ctx.cluster)
    doc = status_doc(ctx.cluster, state, _live_status(ctx.cluster, state), proc.utcnow())
    if args.json:
        proc.print_json(doc)
    else:
        out(format_status(doc))
    return 0


def _aws_if_credentials(ctx: Context) -> Aws | None:
    """None when the profile's credentials are known to be expired (extend then skips the tag)."""
    expiry = awsapi.credentials_expiry(ctx.profile)
    return None if expiry is not None and expiry <= proc.utcnow() else ctx.aws()


def cmd_extend(ctx: Context, args: argparse.Namespace) -> int:
    provision = load_module("provision")
    until = provision.parse_until(args.until) if args.until else None
    state = provision.extend(ctx.cluster, _aws_if_credentials(ctx), args.ttl, until, args.shorten)
    left = (proc.parse_iso(state["expires_at"]) - proc.utcnow()).total_seconds()
    out(f"cluster '{ctx.cluster}' expires at {state['expires_at']} (in {proc.format_duration(left)})")
    return 0


def cmd_refresh_ip(ctx: Context, args: argparse.Namespace) -> int:
    state = load_module("provision").refresh_ip(ctx.cluster, ctx.aws())
    out(f"ssh allowed from {(state.get('aws') or {}).get('operator_cidr')}")
    return 0


def cmd_build(ctx: Context, args: argparse.Namespace) -> int:
    build = load_module("build")
    record = build.build(build.parse_source(args.source, {"local", "git"}), jobs=args.jobs)
    out(record["build_id"])
    return 0


def cmd_builds(ctx: Context, args: argparse.Namespace) -> int:
    local = load_module("build").cached_builds()
    nodes = load_module("deploy").builds_on_nodes(ctx.cluster) if args.nodes else None
    if args.json:
        proc.print_json({"local": local, "nodes": nodes})
        return 0
    out(table(local, ("build_id", "kind", "version", "source", "dirty", "built_at")))
    if args.nodes:
        out("on the nodes:")
        out("\n".join(tree(nodes, "  ", 3)))
    return 0


# --- deploy ---------------------------------------------------------------------------
def cmd_deploy(ctx: Context, args: argparse.Namespace) -> int:
    deploy, component = load_module("deploy"), args.component
    if component == "scylla":
        state = deploy.deploy_scylla(ctx.cluster, args.image, args.wipe, args.refresh)
    elif component == "vs":
        state = deploy.deploy_vs(
            ctx.cluster, args.source, dict(args.env), list(args.unset), args.refresh, args.timeout_s
        )
    elif component == "bench":
        state = deploy.deploy_bench(ctx.cluster, args.source, args.refresh)
    elif component == "monitoring":
        state = deploy.deploy_monitoring(ctx.cluster)
    else:
        state = deploy.deploy_all(ctx.cluster, args.force, args.timeout_s)
    _print_deployed(state, DEPLOYED_KEYS[component])
    return 0


def cmd_wait_serving(ctx: Context, args: argparse.Namespace) -> int:
    print_doc(load_module("deploy").wait_serving(ctx.cluster, args.timeout_s))
    return 0


# --- nodes ----------------------------------------------------------------------------------
def _refuse_dangerous(command: str, allowed: bool) -> None:
    """exec/ssh refuse power-off and TTL-disarming commands (guard.refusal_reason) without --i-mean-it."""
    reason = None if allowed else guard.refusal_reason(command)
    if reason:
        raise PreconditionError(f"refusing to run: {short(command, 200)}", f"{reason}. {I_MEAN_IT_HINT}")


def tail_text(text: str, lines: int) -> tuple[str, int]:
    """The last `lines` lines (0: all; then no byte cap), at most MAX_OUTPUT_BYTES; (text, omitted lines)."""
    every = text.splitlines()
    kept = every[-lines:] if lines else every
    out = "\n".join(kept)
    if lines and len(out.encode()) > MAX_OUTPUT_BYTES:
        out = out.encode()[-MAX_OUTPUT_BYTES:].decode("utf-8", "replace").split("\n", 1)[-1]
        kept = out.splitlines()
    return out, len(every) - len(kept)


def _exec_entry(result: Any, lines: int) -> dict[str, Any]:
    stdout, out_omitted = tail_text(result.stdout or "", lines)
    stderr, err_omitted = tail_text(result.stderr or "", lines)
    return {"exit": result.returncode, "stdout": stdout, "stderr": stderr, "omitted_lines": out_omitted + err_omitted}


def _print_exec(doc: dict[str, dict[str, Any]]) -> None:
    single = len(doc) == 1
    for name, entry in doc.items():
        if not single:
            out(f"=== {name} (exit {entry['exit']})")
        if entry["omitted_lines"]:
            out(f"[... {entry['omitted_lines']} earlier lines omitted; use --tail N, 0 for all]")
        if entry["stdout"]:
            out(entry["stdout"])
        if entry["stderr"] and single:
            sys.stderr.write(entry["stderr"] + "\n")
        elif entry["stderr"]:
            out("--- stderr\n" + entry["stderr"])


def cmd_exec(ctx: Context, args: argparse.Namespace) -> int:
    command = " ".join([*args.words, *args.extra])
    if not command.strip():
        raise UsageError("exec needs a command", "e.g. vsbench exec vs-0 -- 'uptime; free -g'")
    _refuse_dangerous(command, args.i_mean_it)
    names = [n["name"] for n in st.resolve_targets(st.require(ctx.cluster), args.target)]
    results = load_module("remote").run_many(ctx.cluster, names, command, check=False, timeout=args.timeout_s)
    doc = {name: _exec_entry(result, args.tail) for name, result in results.items()}
    if args.json:
        proc.print_json(doc)
    else:
        _print_exec(doc)
    failed = [f"{name} (exit {entry['exit']})" for name, entry in doc.items() if entry["exit"] != 0]
    if failed:
        raise VsbenchError(f"the command failed on {', '.join(failed)}")
    return 0


def cmd_ssh(ctx: Context, args: argparse.Namespace) -> int:
    words = [*args.words, *args.extra]
    if words:
        _refuse_dangerous(" ".join(words), args.i_mean_it)
    st.node(st.require(ctx.cluster), args.node)
    return int(load_module("remote").interactive(ctx.cluster, args.node, words or None))


def _remote_spec(ctx: Context, text: str) -> tuple[str, str]:
    node, sep, path = text.partition(":")
    if not sep or not node or not path:
        raise UsageError(f"expected NODE:PATH, got '{text}'", "e.g. vs-0:/tmp/file")
    st.node(st.require(ctx.cluster), node)
    return node, path


def cmd_push(ctx: Context, args: argparse.Namespace) -> int:
    node, path = _remote_spec(ctx, args.dest)
    local = Path(args.local)
    if not local.is_file():
        raise VsbenchError(f"local file not found: {local}")
    path = path + local.name if path.endswith("/") else path
    mode = format(stat.S_IMODE(local.stat().st_mode), "04o")
    load_module("remote").upload(ctx.cluster, node, local, path, mode=mode)
    out(f"{local} -> {node}:{path}")
    return 0


def cmd_pull(ctx: Context, args: argparse.Namespace) -> int:
    node, path = _remote_spec(ctx, args.source)
    local = Path(args.local)
    target = local / Path(path).name if local.is_dir() else local
    load_module("remote").download(ctx.cluster, node, path, target)
    out(f"{node}:{path} -> {target}")
    return 0


def logs_command(service: str, lines: int, since_s: int | None) -> str:
    since = f" --since={since_s}s" if since_s else ""
    if service == "scylla":
        return f"sudo docker logs --timestamps --tail {lines}{since} scylla 2>&1"
    if service == "vector-store":
        journal_since = f" --since=-{since_s}s" if since_s else ""
        return f"sudo journalctl -u vector-store --no-pager -o short-iso-precise -n {lines}{journal_since}"
    if service == "userdata":
        return f"sudo tail -n {lines} /var/log/vsbench-userdata.log"
    names = " ".join(MONITORING_CONTAINERS)
    return f'for c in {names}; do echo "=== $c"; sudo docker logs --timestamps --tail {lines}{since} "$c" 2>&1; done'


def cmd_logs(ctx: Context, args: argparse.Namespace) -> int:
    node = st.node(st.require(ctx.cluster), args.node)
    service = args.service or DEFAULT_LOG_SERVICE.get(node["role"], "userdata")
    if service == "userdata" and args.since:
        proc.warn("--since is ignored for the userdata log")
    result = load_module("remote").run(
        ctx.cluster, node["name"], logs_command(service, args.lines, args.since), timeout=120
    )
    sys.stdout.write(result.stdout)
    return 0


def cmd_prom(ctx: Context, args: argparse.Namespace) -> int:
    prom = load_module("prom")
    if args.prom_cmd == "api":
        proc.print_json(prom.api(ctx.cluster, args.path, list(args.params)))
        return 0
    if args.prom_cmd == "query":
        result = prom.query(ctx.cluster, args.promql, args.time)
    else:
        result = prom.query_range(ctx.cluster, args.promql, args.start, args.end, args.step)
    if args.raw:
        proc.print_json(result)
    else:
        out(prom.format_vector(result))
    return 0
