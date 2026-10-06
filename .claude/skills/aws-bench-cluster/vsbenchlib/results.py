# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Benchmark results: log parsing, records (results.jsonl), aggregation, comparison.

Every latency is {"value": ms, "tag": t}: exact (client min/max, client percentiles in
(1, 100] ms); floored (client percentile histogram starts at 1 ms: `1.0ms` = <= ~1.05 ms);
capped (client > 100 ms, value null; server: above the highest finite bucket);
bucket_interp (server histogram, interpolated inside "bucket": [lo, hi] ms).
Comparisons use exact metrics only (QPS, closed-loop client mean, server mean, recall).
"""

from __future__ import annotations

import datetime
import fcntl
import hashlib
import json
import re
import secrets
import statistics
from collections.abc import Iterable, Sequence
from typing import Any

from . import config
from . import state as state_mod
from .proc import PreconditionError, VsbenchError, atomic_write, debug, utcnow, warn

MIN_WINDOW_S = 30  # server metrics need >= 3 scrapes inside the window
CLIENT_SATURATED_PCT = 80.0
PLATEAU_QPS_GAIN = 0.05
PLATEAU_VS_CPU_PCT = 70.0
HIGH_CV = 0.10
MAX_ERRORS = 20
TIMEOUT_TEXT = "Search query timed out"
METRICS = ("qps", "client_mean_ms", "server_mean_ms", "recall_avg", "build_s")
Differences = dict[str, dict[str, list[Any]]]  # {kind: {fairness field: [distinct values]}}

_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)\s+(?P<level>TRACE|DEBUG|INFO|WARN|ERROR)\s+(?P<msg>.*?)\s*$"
)
_MEASURE_RE = re.compile(
    r"^(?P<key>duration|queries|QPS|latency (?:min|max|P\d+)|recall (?:min|avg|max))"
    r"(?: for (?P<node>\d+))?: (?P<value>\S+)$"
)
_TOOK_RE = re.compile(r"^(?P<what>Build table|Build Index|Drop Index|Drop Table|Delete rows) took (?P<value>\S+)$")
_STARTING_RE = re.compile(r"^Starting search (?P<kind>cql|http) tasks$")
_DIMENSION_RE = re.compile(r"^Found dimension (?P<dim>\d+) for dataset")
_DURATION_RE = re.compile(r"^(?P<num>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(?P<unit>ns|\u00b5s|\u03bcs|us|ms|s)$")
_UNIT_MS = {"ns": 1e-6, "\u00b5s": 1e-3, "\u03bcs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1000.0}
_CAPPED_MS = 1e12  # the Duration::MAX sentinel prints as 18446744073709551616.0s
_FLOOR_MS = 1.0
_STEP_BEGIN_RE = re.compile(r"^=== VSBENCH STEP BEGIN (?P<name>\S+) (?P<ts>\S+)\s*$")
_STEP_END_RE = re.compile(r"^=== VSBENCH STEP END (?P<name>\S+) (?P<exit>-?\d+) (?P<ts>\S+)\s*$")
_TS_RE = re.compile(r"^(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:?\d\d)?$")


# --- Timestamps -------------------------------------------------------------------
def parse_timestamp(text: str) -> datetime.datetime:
    """Parse ISO 8601 (`Z`, `±HH:MM` or no zone = UTC, any fraction digits) to aware UTC."""
    match = _TS_RE.match(text.strip())
    if not match:
        raise VsbenchError(f"invalid timestamp '{text}'", "use ISO 8601 UTC, e.g. 2026-10-06T10:04:05Z")
    date, clock, fraction, zone = match.groups()
    micros = (fraction or "")[:6].ljust(6, "0")
    zone = zone or "Z"
    offset = "+00:00" if zone == "Z" else (zone if ":" in zone else f"{zone[:3]}:{zone[3:]}")
    return datetime.datetime.fromisoformat(f"{date}T{clock}.{micros}{offset}").astimezone(datetime.timezone.utc)


def _as_datetime(value: datetime.datetime | str) -> datetime.datetime:
    if isinstance(value, str):
        return parse_timestamp(value)
    return value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)


def iso_ms(moment: datetime.datetime) -> str:
    """UTC ISO 8601 with milliseconds, e.g. 2026-10-06T10:04:05.970Z."""
    utc = _as_datetime(moment).astimezone(datetime.timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


# --- Log parsing -----------------------------------------------------------------
def strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def parse_duration_ms(text: str) -> tuple[float | None, str]:
    """Parse a Rust `{:.1?}` Duration into ms with a precision tag.

    "354.4µs" -> (0.3544, "exact"); "1.0ms" -> (1.0, "floored");
    "18446744073709551616.0s" -> (None, "capped"); garbage -> (None, "invalid").
    """
    match = _DURATION_RE.match(text.strip())
    if not match:
        return None, "invalid"
    value = float(match.group("num")) * _UNIT_MS[match.group("unit")]
    if value >= _CAPPED_MS:
        return None, "capped"
    if match.group("unit") == "ms" and value == _FLOOR_MS:
        return value, "floored"
    return round(value, 6), "exact"


def _empty_measure() -> dict[str, Any]:
    return {"qps": None, "queries": None, "latency": {}, "recall": None}


def parse_bench_log(text: str) -> dict[str, Any]:
    """Parse vector-search-benchmark output (one step; the last measurement block wins).

    Returns {qps, queries, duration_s, latency:{min,p1,p10,p25,p50,p75,p90,p99,max} (tagged ms),
    recall:{min,avg,max}|None (percent), per_node:{"<i>": {qps, queries, latency, recall}},
    timeouts, errors (unique, at most MAX_ERRORS), error_count, panicked,
    took:{build_table_s, build_index_s, drop_index_s, drop_table_s, delete_rows_s},
    started_at, gathering_at (log timestamps, ISO), search_kind (cql|http), dimension}.
    """
    result = {**_empty_measure(), **dict.fromkeys(("duration_s", "started_at", "gathering_at", "search_kind"))}
    result.update(dimension=None, timeouts=0, error_count=0, panicked=False, per_node={}, errors=[], took={})
    lines = strip_ansi(text).splitlines()
    for number, raw in enumerate(lines):
        match = _LINE_RE.match(raw)
        if match:
            _parse_line(result, match.group("ts"), match.group("level"), match.group("msg"))
        elif "panicked at" in raw:
            following = lines[number + 1].strip() if number + 1 < len(lines) else ""
            if following.startswith("note:") or _LINE_RE.match(following):
                following = ""
            _add_error(result, f"panic: {raw.strip()} {following}".strip(), panic=True)
    return result


def _parse_line(result: dict[str, Any], ts: str, level: str, msg: str) -> None:
    if level == "ERROR" and TIMEOUT_TEXT in msg:
        result["timeouts"] += 1
    elif level == "ERROR" or "panicked at" in msg:
        _add_error(result, msg if level == "ERROR" else f"panic: {msg}", panic=level != "ERROR")
    elif measure := _MEASURE_RE.match(msg):
        _store_measure(result, measure.group("key"), measure.group("node"), measure.group("value"))
    elif took := _TOOK_RE.match(msg):
        value, _ = parse_duration_ms(took.group("value"))
        key = took.group("what").lower().replace(" ", "_") + "_s"
        result["took"][key] = round(value / 1000, 6) if value is not None else None
    elif starting := _STARTING_RE.match(msg):
        result["started_at"] = ts
        result["search_kind"] = starting.group("kind")
    elif msg == "Gathering measurements":
        result["gathering_at"] = ts
    elif dimension := _DIMENSION_RE.match(msg):
        result["dimension"] = int(dimension.group("dim"))


def _store_measure(result: dict[str, Any], key: str, node: str | None, value: str) -> None:
    target = result if node is None else result["per_node"].setdefault(node, _empty_measure())
    try:
        if key == "duration":
            millis, _ = parse_duration_ms(value)
            target["duration_s"] = round(millis / 1000, 6) if millis is not None else None
        elif key == "queries":
            target["queries"] = int(value)
        elif key == "QPS":
            target["qps"] = float(value)
        elif key.startswith("latency "):
            name = key.split()[1].lower()
            millis, tag = parse_duration_ms(value)
            exact = name in ("min", "max") and tag == "floored"  # min and max are exact in the tool
            target["latency"][name] = {"value": millis, "tag": "exact" if exact else tag}
        else:
            target["recall"] = {**(target["recall"] or {}), key.split()[1]: float(value)}
    except ValueError:
        debug(f"unparsable benchmark value: {key}: {value}")


def _add_error(result: dict[str, Any], message: str, panic: bool = False) -> None:
    result["error_count"] += 1
    result["panicked"] = result["panicked"] or panic
    text = message.strip()[:300]
    if text not in result["errors"] and len(result["errors"]) < MAX_ERRORS:
        result["errors"].append(text)


def step_sections(log: str) -> list[dict[str, Any]]:
    """Split a job log by `=== VSBENCH STEP BEGIN|END` markers.

    Returns [{name, begin, end, exit, text}] in log order; a step without an END
    marker (killed or still running) has end=None and exit=None.
    """
    sections: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for raw in log.splitlines():
        line = strip_ansi(raw)
        begin = _STEP_BEGIN_RE.match(line)
        end = None if begin else _STEP_END_RE.match(line)
        if begin:
            if current is not None:
                sections.append(_close_section(current, None, None))
            current = {"name": begin.group("name"), "begin": begin.group("ts"), "lines": []}
        elif end and current is not None:
            if end.group("name") != current["name"]:
                debug(f"step END {end.group('name')} closes step {current['name']}")
            sections.append(_close_section(current, end.group("ts"), int(end.group("exit"))))
            current = None
        elif current is not None:
            current["lines"].append(raw)
    if current is not None:
        sections.append(_close_section(current, None, None))
    return sections


def _close_section(section: dict[str, Any], end: str | None, exit_code: int | None) -> dict[str, Any]:
    text = "\n".join(section["lines"])
    return {"name": section["name"], "begin": section["begin"], "end": end, "exit": exit_code, "text": text}


# --- Records -----------------------------------------------------------------------
def new_run_id(kind: str) -> str:
    return f"{utcnow():%Y%m%dT%H%M%SZ}-{kind}-{secrets.token_hex(2)}"


def make_record(
    kind: str,
    cluster_state: dict[str, Any],
    params: dict[str, Any] | None,
    client_metrics: dict[str, Any] | None,
    server_metrics: dict[str, Any] | None,
    window: Sequence[datetime.datetime | str] | dict[str, Any] | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one results.jsonl record (schema in design §9).

    `client_metrics` is parse_bench_log() output. `window` is (start, end) as datetimes or
    ISO strings. Recognised `extra` keys: run_id, series_id, comparison_id, arm, label,
    repeat_index, started_at, ended_at, exit, log, error_tail, server_metrics_error,
    index_name, index_options (effective), vs_engine, previous (record of the previous
    concurrency in the series, for the plateau flag). Other keys are stored verbatim.
    """
    rest = dict(extra or {})
    previous = rest.pop("previous", None)
    run_id = rest.pop("run_id", None) or new_run_id(kind)
    params = dict(params or {})
    load = cluster_state.get("load") or {}
    record = {
        "run_id": run_id,
        "kind": kind,
        **{key: rest.pop(key, None) for key in ("series_id", "comparison_id", "arm", "label")},
        "repeat_index": rest.pop("repeat_index", 1),
        **{key: rest.pop(key, None) for key in ("started_at", "ended_at")},
        "window": _window_block(window),
        "params": params,
        "dataset": load.get("dataset"),
        "load_run_id": load.get("run_id"),
        "index": _index_block(load, rest.pop("index_name", None), rest.pop("index_options", None)),
        "cluster": _cluster_block(cluster_state),
        "versions": _versions_block(cluster_state, rest.pop("vs_engine", None)),
        "client_metrics": _client_block(client_metrics, params),
        "server_metrics": server_metrics,
        "server_metrics_error": rest.pop("server_metrics_error", None),
        "flags": [],
        "fairness_key": "",
        "exit": rest.pop("exit", 0),
        "log": rest.pop("log", None) or f"results/{run_id}.log",
        "error_tail": rest.pop("error_tail", None),
    }
    record.update(rest)
    record["flags"] = flags_for(record, previous)
    record["fairness_key"] = fairness_key(record)
    return record


def _window_block(window: Sequence[datetime.datetime | str] | dict[str, Any] | None) -> dict[str, Any] | None:
    if window is None:
        return None
    start, end = (window.get("start"), window.get("end")) if isinstance(window, dict) else tuple(window)
    if start is None or end is None:
        return None
    start_dt, end_dt = _as_datetime(start), _as_datetime(end)
    seconds = round((end_dt - start_dt).total_seconds(), 3)
    return {"start": iso_ms(start_dt), "end": iso_ms(end_dt), "seconds": seconds}


def _index_block(load: dict[str, Any], name: str | None, options: Any) -> dict[str, Any]:
    options = options if options is not None else load.get("index_options")
    return {
        "name": name or load.get("index"),
        "options": options,
        "rf": load.get("rf"),
        "local": bool(load.get("local_index")),
    }


def _cluster_block(cluster_state: dict[str, Any]) -> dict[str, Any]:
    block: dict[str, Any] = {}
    for role in config.ROLES:
        members = state_mod.nodes(cluster_state, role)
        types = sorted({n["instance_type"] for n in members if n.get("instance_type")})
        ids = [n.get("instance_id") for n in members]
        block[role] = {"count": len(members), "type": ",".join(types) or None, "instance_ids": ids}
    return {**block, "region": cluster_state.get("region"), "az": cluster_state.get("az")}


def _versions_block(cluster_state: dict[str, Any], vs_engine: str | None) -> dict[str, Any]:
    deployed = cluster_state.get("deployed") or {}
    scylla, bench = deployed.get("scylla") or {}, deployed.get("bench") or {}
    vector_store = deployed.get("vector_store") or {}
    return {
        "scylla": scylla.get("version"),
        "scylla_image": scylla.get("image"),
        "vector_store": {k: vector_store.get(k) for k in ("build_id", "version", "source", "commit", "dirty")},
        "vs_engine": vs_engine or vector_store.get("engine"),
        "vs_env": dict(vector_store.get("env") or {}),
        "bench": {k: bench.get(k) for k in ("build_id", "version", "source", "commit")},
    }


def query_delay(extra_args: Any) -> str | None:
    """The tool's `--delay` value in the extra bench args (`--delay 9ms` or `--delay=9ms`), else None."""
    args = [str(a) for a in (extra_args or [])]
    for index, arg in enumerate(args):
        if arg == "--delay":
            return args[index + 1] if index + 1 < len(args) else ""
        if arg.startswith("--delay="):
            return arg.split("=", 1)[1]
    return None


def _client_block(parsed: dict[str, Any] | None, params: dict[str, Any]) -> dict[str, Any] | None:
    if parsed is None:
        return None
    if "latency_ms" in parsed:  # already a record block (e.g. replayed)
        return dict(parsed)
    concurrency, duration, queries = params.get("concurrency"), parsed.get("duration_s"), parsed.get("queries")
    closed_loop = round(concurrency * duration * 1000 / queries, 4) if concurrency and duration and queries else None
    # concurrency x duration / queries is the mean latency only when every task issues queries back to
    # back; with the tool's --delay (a pause between queries) it is the cycle time, which includes the pause.
    delay = query_delay(params.get("extra_args"))
    block = {"qps": parsed.get("qps"), "queries": queries, "duration_s": duration}
    block["mean_ms"] = None if delay is not None else closed_loop
    if delay is not None:
        block.update(cycle_ms=closed_loop, delay=delay)
    block.update(latency_ms=dict(parsed.get("latency") or {}), recall=parsed.get("recall"))
    block.update(timeouts=parsed.get("timeouts", 0), errors=list(parsed.get("errors") or []))
    return {**block, "per_node": dict(parsed.get("per_node") or {})}


def _dig(data: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _numbers(values: Iterable[Any]) -> list[float]:
    return [v for v in values if _is_number(v)]


def flags_for(record: dict[str, Any], previous: dict[str, Any] | None = None) -> list[str]:
    """Interpretation flags; `previous` = the record of the previous concurrency in the series."""
    client = record.get("client_metrics") or {}
    server = record.get("server_metrics") or {}
    flags = []
    if any(v > CLIENT_SATURATED_PCT for v in _numbers([_dig(server, "cpu_pct", "client")])):
        flags.append("client_saturated")
    if any(v > 0 for v in _numbers((server.get("net_allowance_exceeded") or {}).values())):
        flags.append("net_allowance_exceeded")
    if (client.get("timeouts") or 0) > 0:
        flags.append("timeouts")
    if client.get("delay") is not None:
        flags.append("delayed")
    tags = {e.get("tag") for e in (client.get("latency_ms") or {}).values() if isinstance(e, dict)}
    flags += [f"latency_{tag}" for tag in ("floored", "capped") if tag in tags]
    if any(v < MIN_WINDOW_S for v in _numbers([_dig(record, "window", "seconds")])):
        flags.append("short_window")
    if _is_plateau(record, previous):
        flags.append("plateau")
    return flags


def _is_plateau(record: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    if not previous:
        return False
    conc, prev_conc = _dig(record, "params", "concurrency"), _dig(previous, "params", "concurrency")
    if conc is not None and prev_conc is not None and prev_conc >= conc:
        return False
    qps, prev_qps = _dig(record, "client_metrics", "qps"), _dig(previous, "client_metrics", "qps")
    cpu = _dig(record, "server_metrics", "cpu_pct") or {}
    vs_cpu = _numbers(v for k, v in cpu.items() if k.startswith("vs-"))
    if not qps or not prev_qps or not vs_cpu:
        return False
    return (qps - prev_qps) / prev_qps < PLATEAU_QPS_GAIN and max(vs_cpu) < PLATEAU_VS_CPU_PCT


def fairness_fields(record: dict[str, Any]) -> dict[str, Any]:
    """What must be equal for two runs to be comparable (VS build, kind and concurrency are not)."""
    params, index = record.get("params") or {}, record.get("index") or {}
    versions, cluster = record.get("versions") or {}, record.get("cluster") or {}
    nodes = {}
    for role in config.ROLES:
        ids = sorted(i for i in (_dig(cluster, role, "instance_ids") or []) if i)
        nodes[role] = {"type": _dig(cluster, role, "type"), "instance_ids": ids}
    return {
        **{key: record.get(key) for key in ("dataset", "load_run_id")},
        **{f"index_{key}": index.get(key) for key in ("options", "rf", "local")},
        **{key: params.get(key) for key in ("limit", "duration_s", "warmup_s", "bucket")},
        "extra_args": list(params.get("extra_args") or []),
        "bench_build_id": _dig(versions, "bench", "build_id"),
        "scylla_image": versions.get("scylla_image"),
        "vs_env": versions.get("vs_env") or {},
        "nodes": nodes,
    }


def _canonical(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)


def fairness_key(record: dict[str, Any]) -> str:
    """First 16 hex chars of sha256 over the canonical JSON of fairness_fields()."""
    return hashlib.sha256(_canonical(fairness_fields(record)).encode()).hexdigest()[:16]


# --- Storage --------------------------------------------------------------------
def append(cluster: str, record: dict[str, Any]) -> None:
    """Append to results.jsonl unless the run_id is already there (idempotent).

    Adds the `plateau` flag when the series has a lower-concurrency record.
    """
    target = state_mod.paths(cluster).results_file
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        handle.seek(0)
        existing = _parse_lines(handle.read(), str(target))
        if any(r.get("run_id") == record.get("run_id") for r in existing):
            debug(f"result {record.get('run_id')} already recorded")
            return
        previous = _previous_in_series(existing, record)
        flags = list(record.get("flags") or [])
        flags += [f for f in flags_for(record, previous) if f not in flags]
        handle.write(json.dumps({**record, "flags": flags}) + "\n")
        handle.flush()


def _previous_in_series(existing: list[dict[str, Any]], record: dict[str, Any]) -> dict[str, Any] | None:
    series, conc = record.get("series_id"), _dig(record, "params", "concurrency")
    if not series or conc is None:
        return None
    same = ("series_id", "kind", "repeat_index")
    candidates = [r for r in existing if all(r.get(k) == record.get(k) for k in same)]
    lower = [
        r for r in candidates if _is_number(_dig(r, "params", "concurrency")) and r["params"]["concurrency"] < conc
    ]
    return max(lower, key=lambda r: r["params"]["concurrency"]) if lower else None


def _parse_lines(text: str, source: str) -> list[dict[str, Any]]:
    records = []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            warn(f"{source}:{number}: skipping a malformed result line")
    return records


def load_records(cluster: str) -> list[dict[str, Any]]:
    target = state_mod.paths(cluster).results_file
    return _parse_lines(target.read_text(), str(target)) if target.exists() else []


def save_log(cluster: str, run_id: str, text: str) -> str:
    """Store a run's log next to results.jsonl; returns the record's relative `log` path."""
    atomic_write(state_mod.paths(cluster).results_dir / f"{run_id}.log", text)
    return f"results/{run_id}.log"


_ROUTING_KEYS = ("run_id", "series_id", "comparison_id", "arm", "label", "started_at", "ended_at", "exit", "log")


def record_index_build(cluster: str, info: dict[str, Any]) -> dict[str, Any]:
    """Record a kind "index-build" result and return it.

    `info` keys: seconds ({node: restart->SERVING s} or a number), build_index_s
    ("Build Index took"), build_id, trigger, window (start, end) and the routing keys
    run_id/series_id/comparison_id/arm/label/started_at/ended_at/exit/log; any other key
    is kept in record["index_build"].
    """
    details = dict(info)
    routing = {key: details.pop(key) for key in _ROUTING_KEYS if key in details}
    window = details.pop("window", None)
    current = state_mod.load(cluster) or {}
    record = make_record("index-build", current, {}, None, None, window, {**routing, "index_build": details})
    build_id = details.get("build_id")
    if build_id and build_id != _dig(record, "versions", "vector_store", "build_id"):
        vector_store = {**record["versions"]["vector_store"], "build_id": build_id}
        record = {**record, "versions": {**record["versions"], "vector_store": vector_store}}
    append(cluster, record)
    return record


# --- Aggregation and comparison --------------------------------------------------
def index_build_seconds(record: dict[str, Any]) -> float | None:
    """Index build time: "Build Index took", else the slowest node's restart->SERVING."""
    block = record.get("index_build") or {}
    for value in (block.get("build_index_s"), block.get("build_s"), _dig(record, "load", "index_s")):
        if _is_number(value):
            return float(value)
    seconds = block.get("seconds", block.get("per_node"))
    values = _numbers(seconds.values() if isinstance(seconds, dict) else [seconds])
    return float(max(values)) if values else None


_METRIC_PATHS = {
    "qps": ("client_metrics", "qps"),
    "client_mean_ms": ("client_metrics", "mean_ms"),
    "server_mean_ms": ("server_metrics", "vs_mean_ms"),
    "recall_avg": ("client_metrics", "recall", "avg"),
}


def metric_value(record: dict[str, Any], metric: str) -> float | None:
    """One of METRICS for a record (build_s = index_build_seconds)."""
    return index_build_seconds(record) if metric == "build_s" else _dig(record, *_METRIC_PATHS[metric])


def _stats(values: list[Any]) -> dict[str, Any] | None:
    numbers = _numbers(values)
    if not numbers:
        return None
    mean, median = statistics.fmean(numbers), round(statistics.median(numbers), 4)
    cv = round(statistics.stdev(numbers) / mean, 4) if len(numbers) > 1 and mean else None
    return {"n": len(numbers), "median": median, "min": min(numbers), "max": max(numbers), "cv": cv}


def aggregate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group by (VS build_id, fairness_key, kind, concurrency) in first-seen order.

    The fairness key covers the whole setup (VS env, index options, Scylla image, bench
    build, ...), so runs of different setups (a forced compare) are never merged.
    Each row: build_id, version, vs_env, fairness_key, kind, concurrency, n, run_ids, arms,
    flags and, per metric in METRICS, {n, median, min, max, cv (stdev/mean, a ratio)} or None.
    """
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for record in records:
        build_id = _dig(record, "versions", "vector_store", "build_id")
        key = (build_id, fairness_key(record), record.get("kind"), _dig(record, "params", "concurrency"))
        groups.setdefault(key, []).append(record)
    rows = []
    for (build_id, setup_key, kind, concurrency), members in groups.items():
        first = members[0]
        row = {"build_id": build_id, "version": _dig(first, "versions", "vector_store", "version")}
        row.update(vs_env=_dig(first, "versions", "vs_env") or {}, fairness_key=setup_key, kind=kind)
        row.update(concurrency=concurrency, n=len(members), run_ids=[m.get("run_id") for m in members])
        row["arms"] = sorted({m["arm"] for m in members if m.get("arm")})
        row["flags"] = sorted({f for m in members for f in (m.get("flags") or [])})
        rows.append({**row, **{metric: _stats([metric_value(m, metric) for m in members]) for metric in METRICS}})
    return rows


def compare(records: list[dict[str, Any]], force: bool = False) -> dict[str, Any]:
    """Aggregate and compare groups; refuse unfair comparisons unless `force`.

    Fairness is checked per kind. Deltas are relative to the first group with the same
    (kind, concurrency); within_noise = the [min, max] ranges overlap (None if n < 2).
    Each group's `setup` holds its values of the fields that differ within its kind
    ({} unless forced). Returns {fairness_ok, differences: {kind: {field: [values]}},
    groups, warnings}.
    """
    if not records:
        raise VsbenchError("nothing to compare", "pass run or series ids listed by `vsbench results`")
    differences = _fairness_differences(records)
    warnings = []
    if differences:
        summary = " ".join(
            f"[{kind}] "
            + "; ".join(f"{field}: {' vs '.join(map(_canonical, values))}" for field, values in diff.items())
            for kind, diff in differences.items()
        )
        if not force:
            raise PreconditionError(
                f"runs are not comparable: {summary}",
                "compare runs with identical setups, or pass --force and mention the difference in the report",
            )
        warnings.append(f"--force: comparing runs that differ in {summary}")
    setups = {fairness_key(r): fairness_fields(r) for r in records}
    groups = [
        {**g, "setup": {f: setups[g["fairness_key"]].get(f) for f in differences.get(str(g["kind"]), {})}}
        for g in aggregate(records)
    ]
    rows = _with_deltas(groups)
    warnings += _compare_warnings(records, rows)
    return {"fairness_ok": not differences, "differences": differences, "groups": rows, "warnings": warnings}


def _fairness_differences(records: list[dict[str, Any]]) -> Differences:
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_kind.setdefault(str(record.get("kind")), []).append(fairness_fields(record))
    differences: Differences = {}
    for kind, fields_list in by_kind.items():
        diff = {}
        for field in fields_list[0]:
            distinct = {_canonical(f.get(field)): f.get(field) for f in fields_list}
            if len(distinct) > 1:
                diff[field] = list(distinct.values())
        if diff:
            differences[kind] = diff
    return differences


def _with_deltas(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baselines: dict[tuple[Any, Any], dict[str, Any]] = {}
    rows = []
    for group in groups:
        base = baselines.setdefault((group["kind"], group["concurrency"]), group)
        deltas = {m: None if base is group else _delta(group.get(m), base.get(m)) for m in METRICS}
        rows.append({**group, "baseline": base is group, "delta": deltas})
    return rows


def _delta(stats: dict[str, Any] | None, base: dict[str, Any] | None) -> dict[str, Any] | None:
    if not stats or not base or not base["median"]:
        return None
    pct = (stats["median"] - base["median"]) / base["median"] * 100
    noise = None
    if stats["n"] >= 2 and base["n"] >= 2:
        noise = stats["min"] <= base["max"] and base["min"] <= stats["max"]
    return {"pct": round(pct, 2), "within_noise": noise}


_FLAG_WARNINGS = {
    "client_saturated": "the client may be the bottleneck; lower --concurrency",
    "timeouts": "queries timed out",
    "net_allowance_exceeded": "the EC2 network allowance was exceeded",
    "short_window": "the window is too short for server metrics",
}


def _compare_warnings(records: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[str]:
    warnings = []
    for flag, meaning in _FLAG_WARNINGS.items():
        hit = [str(r.get("run_id")) for r in records if flag in (r.get("flags") or [])]
        if hit:
            warnings.append(f"{flag} in {', '.join(hit)}: {meaning}")
    if any(row["n"] < 2 for row in rows):
        warnings.append("some groups have a single run; use --repeat >= 2 to judge noise")
    noisy = [f"{row['build_id']}@{row['concurrency']}" for row in rows if (_dig(row, "qps", "cv") or 0) > HIGH_CV]
    if noisy:
        warnings.append(f"QPS CV > {HIGH_CV:.0%} in {', '.join(noisy)}: results are noisy")
    return warnings
