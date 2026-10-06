# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""vsbench command handlers: datasets, benchmarks, jobs, results, collect and the retrospective.

Split from cli.py (which parses, dispatches and records history) to keep files small.
"""

from __future__ import annotations

import argparse
import dataclasses
import re
import shlex
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from . import config, proc, retro
from . import state as st
from .cli import RESULTS_LAST, Context, UsageError, load_module, out, print_doc, print_records, short, table
from .proc import VsbenchError

STATUS_OFFSET = 1 << 50  # a job_poll offset past any log: status fields only
JOB_TAIL_LINES = 10
COLLECT_TIMEOUT_S = 600
COLLECT_LOG_LINES = 1_000_000
COLLECT_UNITS = ("vector-store", "vsbench-ttl", "node-exporter")
SNAPSHOT_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
MISSING = dataclasses.MISSING
OPTION_CLASSES = {"load": "LoadOptions", "index": "IndexOptions", "search": "SearchOptions", "ab": "AbOptions"}
OPTION_CLASSES["validate"] = "SearchOptions"
OPTION_FIELDS = {
    "load": ("dataset", "index_options", "rf", "concurrency", "local_index", "resume", "index_timeout_s", "timeout_s"),
    "index": ("index_options", "index_timeout_s", "timeout_s"),
    "search": ("kind", "limit", "duration_s", "warmup_s", "concurrency", "repeat", "bucket", "label", "timeout_s"),
}
OPTION_FIELDS["validate"] = OPTION_FIELDS["search"]
OPTION_FIELDS["ab"] = ("a", "b", *OPTION_FIELDS["search"])  # bucket, timeout_s and extra args too
EXTRA_ARGS_COMMANDS = ("search", "validate", "ab")  # words after `--` go to the benchmark tool
MISMATCH_HINT = "cli_bench.py and bench.py disagree on the options (design-v2 §8); fix one of them"
CATALOG_COLUMNS = ("dataset", "rows", "dim", "similarity", "download_gb", "tags", "description")
HISTORY_COLUMNS = ("ts", "status", "exit", "duration", "cluster", "command", "error")


# --- datasets ---------------------------------------------------------------------------
def catalog_rows(bench: Any) -> list[dict[str, Any]]:
    """`dataset list` rows: bench.dataset_rows() when bench has it, else from bench.catalog()."""
    if callable(getattr(bench, "dataset_rows", None)):
        rows = bench.dataset_rows()
    else:
        rows = [{"dataset": key, **item} for key, item in bench.catalog().items()]
        rows = [r | {"tags": [tag for tag in ("default", "smoke") if r.get(tag)]} for r in rows]
    return [r | {"description": short(str(r.get("description") or ""), 60)} for r in rows]


def dataset_status_command() -> str:
    directory = shlex.quote(config.NODE_DATASETS_DIR)
    return (
        f"cd {directory} 2>/dev/null || exit 0; "
        'for d in */; do d=${d%/}; [ -d "$d" ] || continue; c=0; [ -e "$d/.complete" ] && c=1; '
        'echo "$d $c $(du -sb -- "$d" | cut -f1)"; done'
    )


def parse_dataset_status(text: str, catalog: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 3 or not parts[2].isdigit():
            continue
        name, complete, size = parts
        keys = ",".join(k for k, item in catalog.items() if item.get("dir") == name or k == name) or None
        rows.append({"dir": name, "keys": keys, "complete": complete == "1", "size_gb": round(int(size) / 1e9, 2)})
    return rows


def cmd_dataset(ctx: Context, args: argparse.Namespace) -> int:
    bench = load_module("bench")
    if args.dataset_cmd == "list":
        if args.json:
            proc.print_json(bench.catalog())
        else:
            out(table(catalog_rows(bench), CATALOG_COLUMNS))
    elif args.dataset_cmd == "fetch":
        print_doc(bench.fetch(ctx.cluster, args.key, args.timeout_s))
    elif callable(getattr(bench, "dataset_status", None)):
        status = bench.dataset_status(ctx.cluster)
        out(f"{status.get('dir')} on the client, {status.get('free_gb')} GB free:")
        out(table(status.get("datasets") or [], ("dataset", "complete", "gb", "expected_gb")))
    else:
        st.require(ctx.cluster)
        result = load_module("remote").run(ctx.cluster, "client", dataset_status_command(), timeout=120)
        out(table(parse_dataset_status(result.stdout, bench.catalog()), ("dir", "keys", "complete", "size_gb")))
    return 0


# --- bench ---------------------------------------------------------------------------------
def bench_options(bench: Any, command: str, values: dict[str, Any]) -> Any:
    """bench.<Load|Index|Search|Ab>Options from the parsed flags; only the dataclass's fields are
    passed, a missing required field or a dropped non-default value is reported. A None value
    (a flag not given) leaves the dataclass default, so bench decides it."""
    name = OPTION_CLASSES[command]
    cls = getattr(bench, name, None)
    if cls is None or not dataclasses.is_dataclass(cls):
        raise VsbenchError(f"bench.{name} is missing", MISMATCH_HINT)
    fields = {f.name: f for f in dataclasses.fields(cls)}
    for key, value in values.items():
        if key not in fields and value not in (None, False, [], ""):
            proc.warn(f"bench.{name} has no field '{key}'; ignoring {key}={value!r}")
    optional = {n for n, f in fields.items() if f.default is not MISSING or f.default_factory is not MISSING}
    missing = [n for n in fields if n not in values and n not in optional]
    if missing:
        raise VsbenchError(f"cannot build bench.{name}: no value for {', '.join(missing)}", MISMATCH_HINT)
    given = {k: v for k, v in values.items() if k in fields and (v is not None or k not in optional)}
    return cls(**{k: tuple(v) if isinstance(v, list) else v for k, v in given.items()})


def cmd_bench(ctx: Context, args: argparse.Namespace) -> int:
    bench, command = load_module("bench"), args.bench_cmd
    if command == "raw":
        code = bench.raw(ctx.cluster, list(args.extra), args.timeout_s)
        if code:
            hint = f"see the output above; jobs and their logs: vsbench -c {ctx.cluster} job list"
            raise VsbenchError(f"vector-search-benchmark exited with {code}", hint)
        return 0
    if command == "rerun":
        print_bench_result(bench, bench.rerun(ctx.cluster, args.run_id, args.timeout_s))
        return 0
    values = {name: getattr(args, name) for name in OPTION_FIELDS[command]}
    if command in EXTRA_ARGS_COMMANDS:
        values["extra_args"] = list(args.extra)
    if command == "validate":
        print_doc(bench.validate(ctx.cluster, bench_options(bench, command, values)))
        return 0
    runner = {"load": bench.load, "index": bench.index, "search": bench.search, "ab": bench.ab}[command]
    print_bench_result(bench, runner(ctx.cluster, bench_options(bench, command, values)))
    return 0


def print_bench_result(bench: Any, result: Any) -> None:
    """A bench result: its scalar fields, then bench.format_summary of its records (the
    results table when bench has no format_summary)."""
    records = result if isinstance(result, list) else None
    if isinstance(result, dict):
        records = result.get("records") if isinstance(result.get("records"), list) else None
        print_doc({k: v for k, v in result.items() if k != "records"})
    if records is None:
        return
    summarize = getattr(bench, "format_summary", None)
    if records and callable(summarize):
        out(summarize(records))
    else:
        print_records(records)


# --- jobs ------------------------------------------------------------------------------------
def _live_job_state(ctx: Context, node: str, job_id: str) -> str:
    remote = load_module("remote")
    try:
        progress = remote.job_poll(ctx.cluster, node, job_id, STATUS_OFFSET, max_bytes=1)
    except remote.JobNotFound:
        return "not found"
    except VsbenchError as err:
        return f"unknown ({short(str(err), 80)})"
    if progress.exit_code is not None:
        return f"exited {progress.exit_code}"
    return "running" if progress.running else "lost (no exit code)"


def _job_list(ctx: Context, state: st.State, as_json: bool) -> None:
    jobs = {i: j for i, j in (state.get("jobs") or {}).items() if isinstance(j, dict)}
    rows = []
    for job_id, job in sorted(jobs.items(), key=lambda item: str(item[1].get("started_at"))):
        row = {"id": job_id, **{k: job.get(k) for k in ("kind", "node", "started_at", "status")}}
        if job.get("status") == "running":
            row["live"] = _live_job_state(ctx, str(job.get("node") or "client"), job_id)
        rows.append(row)
    if as_json:
        proc.print_json(rows)
    else:
        out(table(rows, ("id", "kind", "node", "started_at", "status", "live")))


def _job_status(ctx: Context, job_id: str, job: dict[str, Any], node: str) -> None:
    live = _live_job_state(ctx, node, job_id)
    started = job.get("started_at") or "?"
    out(
        f"job {job_id}: {job.get('kind') or '?'} on {node}, started {started}, recorded {job.get('status')}; now {live}"
    )
    if live == "not found":
        return
    try:
        text = load_module("remote").job_log(ctx.cluster, node, job_id, JOB_TAIL_LINES)
    except VsbenchError as err:
        out(f"(no log: {short(str(err), 80)})")
        return
    out(f"last {JOB_TAIL_LINES} log lines:")
    sys.stdout.write(text if text.endswith("\n") or not text else text + "\n")


def _job_wait(ctx: Context, args: argparse.Namespace, job: dict[str, Any], node: str) -> None:
    if job:  # a job vsbench started: bench finalizes it and records its results
        bench = load_module("bench")
        print_bench_result(bench, bench.job_wait(ctx.cluster, args.job_id, args.timeout_s))
        return
    code = load_module("remote").job_follow(ctx.cluster, node, args.job_id, args.timeout_s, out)
    if code:
        hint = f"its log: vsbench -c {ctx.cluster} job logs {args.job_id}"
        raise VsbenchError(f"job {args.job_id} exited with {code}", hint)
    out(f"job {args.job_id} finished")


def cmd_job(ctx: Context, args: argparse.Namespace) -> int:
    state = st.require(ctx.cluster)
    if args.job_cmd == "list":
        _job_list(ctx, state, args.json)
        return 0
    job = (state.get("jobs") or {}).get(args.job_id) or {}
    node = str(job.get("node") or "client")
    remote = load_module("remote")
    if args.job_cmd == "status":
        _job_status(ctx, args.job_id, job, node)
    elif args.job_cmd == "logs":
        sys.stdout.write(remote.job_log(ctx.cluster, node, args.job_id, args.tail or None))
    elif args.job_cmd == "cancel":
        remote.job_cancel(ctx.cluster, node, args.job_id)
    else:
        _job_wait(ctx, args, job, node)
    return 0


# --- results --------------------------------------------------------------------------------
def select_records(records: list[dict[str, Any]], ids: list[str]) -> list[dict[str, Any]]:
    """Records whose run_id, series_id or comparison_id is one of `ids`; unknown ids fail."""
    wanted = set(ids)
    chosen = [r for r in records if wanted & {r.get("run_id"), r.get("series_id"), r.get("comparison_id")}]
    found = {value for r in chosen for value in (r.get("run_id"), r.get("series_id"), r.get("comparison_id"))}
    missing = sorted(wanted - found)
    if missing:
        raise VsbenchError(f"no results with the id {', '.join(missing)}", "list run and series ids: vsbench results")
    return chosen


def cmd_results(ctx: Context, args: argparse.Namespace) -> int:
    records = load_module("results").load_records(ctx.cluster)
    if args.kind:
        records = [r for r in records if str(r.get("kind", "")).startswith(args.kind)]
    if args.series:
        records = [r for r in records if r.get("series_id") == args.series]
    if args.comparison:
        records = [r for r in records if r.get("comparison_id") == args.comparison]
    records = records[-args.last :] if args.last else records
    if args.json:
        proc.print_json(records)
    else:
        print_records(records, args.format)
    return 0


def cmd_results_compare(ctx: Context, args: argparse.Namespace) -> int:
    filters = {"--kind": args.kind, "--series": args.series, "--comparison": args.comparison}
    ignored = [flag for flag, value in filters.items() if value] + (["--last"] if args.last != RESULTS_LAST else [])
    if ignored:
        hint = "compare selects by id: vsbench results compare <run|series|comparison id>... [--json|--format md]"
        raise UsageError(f"{', '.join(ignored)} cannot be used with `results compare`", hint)
    results = load_module("results")
    result = results.compare(select_records(results.load_records(ctx.cluster), args.ids), args.force)
    for warning in result.get("warnings") or []:
        proc.warn(warning)
    if args.json:
        proc.print_json(result)
        return 0
    display = load_module("results_format")
    render = display.format_markdown if args.format == "md" else display.format_table
    out(render(display.compare_rows(result), display.COMPARE_COLUMNS))
    return 0


# --- collect --------------------------------------------------------------------------------
def snapshot_command(name: str, archive: str) -> str:
    """Shell on the client: tar the TSDB snapshot (+ scylla.txt) into `archive`, drop the snapshot."""
    q = shlex.quote
    return "; ".join(
        [
            "set -e",
            f"snap=$(sudo find {q(config.NODE_MONITORING_DIR)} -maxdepth 4 -type d -path {q('*/snapshots/' + name)}"
            " | head -n 1)",
            f'if [ -z "$snap" ]; then echo "snapshot {name} not found in {config.NODE_MONITORING_DIR}" >&2; exit 4; fi',
            'data=$(dirname "$(dirname "$snap")")',
            'extra=""; if [ -e "$data/scylla.txt" ]; then extra=scylla.txt; fi',
            f'sudo tar -C "$data" -czf {q(archive)} "snapshots/{name}" $extra',
            f'sudo chown "$(id -u):$(id -g)" {q(archive)}',
            'sudo rm -rf -- "$snap"',
        ]
    )


def node_logs_command(archive: str) -> str:
    """Shell on a node: docker logs of every container, vsbench journals, kernel log, user-data
    log and (where present) the jobs directory, tarred into `archive`."""
    q, lines = shlex.quote, COLLECT_LOG_LINES
    return "; ".join(
        [
            "t=$(mktemp -d /tmp/vsbench-collect.XXXXXX)",
            "for c in $(sudo docker ps -a --format '{{.Names}}' 2>/dev/null); do "
            f'sudo docker logs --timestamps --tail {lines} "$c" >"$t/docker-$c.log" 2>&1; done',
            f'for u in {" ".join(COLLECT_UNITS)}; do if systemctl cat "$u.service" >/dev/null 2>&1; then '
            f'sudo journalctl -u "$u" --no-pager -o short-iso-precise -n {lines} >"$t/journal-$u.log" 2>&1; fi; done',
            'sudo journalctl -k --no-pager -o short-iso-precise >"$t/kernel.log" 2>&1',
            'sudo cp /var/log/vsbench-userdata.log "$t/" 2>/dev/null',
            f'if [ -d {q(config.NODE_JOBS)} ]; then sudo tar -C {q(config.NODE_HOME)} -cf "$t/jobs.tar" jobs; fi',
            f'sudo tar -C "$t" -czf {q(archive)} . && sudo chown "$(id -u):$(id -g)" {q(archive)}',
            "rc=$?",
            'sudo rm -rf -- "$t"',
            'exit "$rc"',
        ]
    )


def _attempt(failures: list[str], what: str, action: Callable[[], None]) -> None:
    try:
        action()
    except VsbenchError as err:
        message = f"{what}: {str(err).splitlines()[0] if str(err) else type(err).__name__}"
        proc.warn(message)
        failures.append(message)


def _fetch(cluster: str, node: str, archive: str, local: Path) -> None:
    remote = load_module("remote")
    try:
        remote.download(cluster, node, archive, local)
    finally:
        try:
            remote.run(cluster, node, f"rm -f -- {shlex.quote(archive)}", check=False, timeout=60)
        except VsbenchError as err:
            proc.debug(f"{node}: cannot remove {archive}: {err}")


def _collect_snapshot(cluster: str, stamp: str, dest: Path) -> None:
    proc.log("taking a Prometheus TSDB snapshot")
    data = load_module("prom").api(cluster, "admin/tsdb/snapshot", post=True, timeout=300)
    name = str(data.get("name") or "") if isinstance(data, dict) else ""
    if not SNAPSHOT_NAME_RE.match(name):
        raise VsbenchError(f"unexpected snapshot answer from Prometheus: {data!r}")
    archive = f"/tmp/vsbench-collect-{stamp}-prometheus.tar.gz"
    load_module("remote").run(cluster, "client", snapshot_command(name, archive), timeout=COLLECT_TIMEOUT_S)
    _fetch(cluster, "client", archive, dest / "prometheus-snapshot.tar.gz")


def _collect_node_logs(cluster: str, names: list[str], stamp: str, dest: Path) -> list[str]:
    archive = f"/tmp/vsbench-collect-{stamp}-logs.tar.gz"
    proc.log(f"collecting logs from {', '.join(names)}")
    remote = load_module("remote")
    results = remote.run_many(cluster, names, node_logs_command(archive), check=False, timeout=COLLECT_TIMEOUT_S)
    failures: list[str] = []
    for name, result in results.items():
        if result.returncode != 0:
            detail = proc.tail((result.stderr or "").strip(), 3)
            failures.append(f"{name} logs: exit {result.returncode}: {detail}")
            continue
        local = dest / f"{name}-logs.tar.gz"
        _attempt(failures, f"{name} logs", lambda name=name, local=local: _fetch(cluster, name, archive, local))
    return failures


def cmd_collect(ctx: Context, args: argparse.Namespace) -> int:
    state = st.require(ctx.cluster)
    stamp = proc.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dest = st.paths(ctx.cluster).results_dir / "artifacts" / stamp
    dest.mkdir(parents=True, exist_ok=True)
    proc.atomic_write(dest / "state.json", proc.dump_json(state) + "\n", mode=0o600)
    failures: list[str] = []
    if (state.get("deployed") or {}).get("monitoring"):
        _attempt(failures, "prometheus snapshot", lambda: _collect_snapshot(ctx.cluster, stamp, dest))
    else:
        proc.warn("monitoring is not deployed: no Prometheus snapshot")
    failures += _collect_node_logs(ctx.cluster, [n["name"] for n in st.nodes(state)], stamp, dest)
    out(f"saved to {dest}:")
    for path in sorted(p for p in dest.iterdir() if p.is_file()):
        out(f"  {path.name}  {path.stat().st_size / 1e6:.1f} MB")
    if failures:
        hint = "the files listed above were saved; retry, or fetch the rest with vsbench pull"
        raise VsbenchError("collect is incomplete:\n  " + "\n  ".join(failures), hint)
    return 0


# --- notes, retrospective, history ------------------------------------------------------------
def cmd_note(ctx: Context, args: argparse.Namespace) -> int:
    retro.note(ctx.cluster, args.kind, " ".join(args.text))
    proc.log(f"noted ({args.kind})")
    return 0


def cmd_retro(ctx: Context, args: argparse.Namespace) -> int:
    digest = retro.digest(args.since)
    if args.json:
        proc.print_json(digest)
    else:
        out(retro.format_digest(digest))
    return 0


def cmd_history(ctx: Context, args: argparse.Namespace) -> int:
    rows = retro.history_rows(args.since, args.failed, args.last or None)
    if args.json:
        proc.print_json(rows)
        return 0
    shown = []
    for row in rows:
        seconds = row.get("duration_s")
        duration = retro.format_seconds(seconds) if isinstance(seconds, (int, float)) else None
        error = (str(row.get("error") or "").strip().splitlines() or [""])[0]
        extra = {"duration": duration, "command": short(row["text"], 70), "error": short(error, 70) or None}
        shown.append(row | extra)
    out(table(shown, HISTORY_COLUMNS))
    return 0
