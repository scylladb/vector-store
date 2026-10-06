# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Prometheus HTTP API on the client node (ssh + curl) and benchmark-window math.

Server metrics of a run are computed locally from raw samples: a range selector
`metric[R s]` evaluated at `end + scrape interval` returns every sample around the
window [start, end]; deltas use the first sample >= start and the last sample <= end
(counter resets handled) over the actual time between those two samples. This avoids
rate() extrapolation and the up-to-one-scrape lag of querying right at `end`.
"""

from __future__ import annotations

import datetime
import itertools
import json
import math
import re
import shlex
import statistics
import subprocess
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from . import config
from .proc import VsbenchError, log, tail, utcnow, warn
from .results import MIN_WINDOW_S, iso_ms, parse_timestamp

PROM_HINT = "check monitoring with `vsbench status`; (re)deploy it with `vsbench deploy monitoring`"
TIME_HINT = "use now, now-15m (s/m/h/d), an ISO 8601 UTC time (2026-10-06T10:04:05Z) or a Unix epoch"
GIB = 1024**3
_API_BASE = f"http://127.0.0.1:{config.PORT_PROMETHEUS}/api/v1/"
_SETTLE_S = 5  # after the last scrape: scrape timeout + TSDB commit
_MAX_SCRAPE_WAIT_S = 120
_RELATIVE_RE = re.compile(r"^(?:now)?\s*([+-])\s*(\d+(?:\.\d+)?)\s*(s|m|h|d)$")
_EPOCH_RE = re.compile(r"^\d{9,}(?:\.\d+)?$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_PATH_RE = re.compile(r"^[A-Za-z0-9_./\[\]-]+$")
TAIL_POINTS = 12  # a longer range series prints a whole-range summary plus its last TAIL_POINTS values


# --- HTTP API ---------------------------------------------------------------------


def _now() -> datetime.datetime:
    return utcnow()


def _run_on_client(cluster: str, command: str, timeout: float) -> subprocess.CompletedProcess[str]:
    """Run on the client; ssh failures raise, curl failures are returned for _decode."""
    from . import remote  # imported lazily: remote pulls in the ssh plumbing

    result = remote.run(cluster, "client", command, check=False, timeout=timeout)
    if remote.is_ssh_failure(result):
        detail = tail((result.stderr or "").strip(), 5)
        raise VsbenchError(f"ssh to client failed: {detail}", remote.ssh_failure_hint(cluster, result.stderr or ""))
    return result


def _param_pairs(params: dict[str, str] | Sequence[tuple[str, str]] | None) -> list[tuple[str, str]]:
    if params is None:
        return []
    items = params.items() if isinstance(params, dict) else params
    return [(str(k), str(v)) for k, v in items]


def api(
    cluster: str,
    path: str,
    params: dict[str, str] | Sequence[tuple[str, str]] | None = None,
    *,
    timeout: float = 60.0,
    post: bool = False,
) -> Any:
    """GET (or POST, e.g. admin/tsdb/snapshot) /api/v1/<path> on the client's Prometheus.

    Returns the `data` member; raises VsbenchError when curl fails, the body is not JSON
    or status != success.
    """
    clean = path.strip().lstrip("/")
    if clean.startswith("api/v1/"):
        clean = clean[len("api/v1/") :]
    if not _PATH_RE.match(clean) or ".." in clean:
        raise VsbenchError(f"invalid Prometheus API path '{path}'", "e.g. query, targets, label/__name__/values")
    method = ["-X", "POST"] if post else ["-G"]
    argv = ["curl", "-sS", "-g", *method, "--max-time", str(int(timeout)), _API_BASE + clean]
    for key, value in _param_pairs(params):
        argv += ["--data-urlencode", f"{key}={value}"]
    result = _run_on_client(cluster, " ".join(shlex.quote(a) for a in argv), timeout + 15)
    return _decode(result, clean)


def _decode(result: subprocess.CompletedProcess[str], path: str) -> Any:
    if result.returncode != 0:
        detail = tail((result.stderr or result.stdout or "").strip(), 5)
        raise VsbenchError(f"Prometheus request '{path}' failed (exit {result.returncode}): {detail}", PROM_HINT)
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as err:
        snippet = (result.stdout or "").strip()[:200]
        raise VsbenchError(f"Prometheus returned non-JSON for '{path}': {snippet}", PROM_HINT) from err
    if not isinstance(payload, dict) or payload.get("status") != "success":
        details = payload if not isinstance(payload, dict) else f"{payload.get('errorType')}: {payload.get('error')}"
        raise VsbenchError(f"Prometheus '{path}' failed: {details}", "check the PromQL / parameters")
    for message in payload.get("warnings") or []:
        warn(f"prometheus: {message}")
    return payload.get("data")


def _result_list(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    result = data.get("result")
    if data.get("resultType") in ("scalar", "string"):
        return [{"metric": {}, "value": result}]
    return list(result or [])


def parse_time(text: str, now: datetime.datetime) -> float:
    """`now`, `now-15m` (s/m/h/d), ISO 8601 or Unix epoch -> epoch seconds.

    `-15m` works too, but argparse before Python 3.14 reads `--start -15m` as two options:
    document and suggest `now-15m`.
    """
    value = text.strip()
    if value == "now":
        return now.timestamp()
    relative = _RELATIVE_RE.match(value)
    if relative:
        sign = 1 if relative.group(1) == "+" else -1
        return now.timestamp() + sign * float(relative.group(2)) * _UNITS[relative.group(3)]
    if _EPOCH_RE.match(value):
        return float(value)
    try:
        return parse_timestamp(value).timestamp()
    except VsbenchError as err:
        raise VsbenchError(f"invalid time '{text}'", TIME_HINT) from err


def _epoch(value: str | float | datetime.datetime, now: datetime.datetime) -> float:
    if isinstance(value, datetime.datetime):
        return value.timestamp() if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc).timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    return parse_time(value, now)


def query(cluster: str, promql: str, time: str | float | datetime.datetime | None = None) -> list[dict[str, Any]]:
    """Instant query; scalar/string results come back as [{"metric": {}, "value": [t, v]}]."""
    params = [("query", promql)]
    if time is not None:
        params.append(("time", f"{_epoch(time, _now()):.3f}"))
    return _result_list(api(cluster, "query", params))


def query_range(
    cluster: str,
    promql: str,
    start: str | float | datetime.datetime,
    end: str | float | datetime.datetime,
    step: str,
) -> list[dict[str, Any]]:
    """Range query; start/end accept everything parse_time() does."""
    now = _now()
    start_s, end_s = _epoch(start, now), _epoch(end, now)
    if start_s >= end_s:
        raise VsbenchError(f"range start {start} is not before end {end}", TIME_HINT)
    params = [("query", promql), ("start", f"{start_s:.3f}"), ("end", f"{end_s:.3f}"), ("step", step)]
    return _result_list(api(cluster, "query_range", params))


# --- Window math ------------------------------------------------------------------


def _window_samples(series: dict[str, Any], start: float, end: float) -> list[tuple[float, float]]:
    samples = []
    for stamp, value in series.get("values") or []:
        moment = float(stamp)
        if start <= moment <= end:
            samples.append((moment, float(value)))
    return samples


def counter_delta(series: list[dict[str, Any]], start: float, end: float) -> list[tuple[dict[str, Any], float, float]]:
    """(labels, increase, seconds) per matrix series, between the first sample >= start
    and the last sample <= end. A drop between samples is a counter reset (the new value
    is the increase since the reset). Series with fewer than 2 samples are skipped."""
    out = []
    for item in series:
        samples = _window_samples(item, start, end)
        if len(samples) < 2:
            continue
        increase = 0.0
        for (_, before), (_, after) in itertools.pairwise(samples):
            increase += after - before if after >= before else after
        out.append((dict(item.get("metric") or {}), increase, samples[-1][0] - samples[0][0]))
    return out


def _quantile_name(q: float) -> str:
    return f"p{q * 100:g}"


def _parse_le(text: Any) -> float | None:
    try:
        return float(text)  # "+Inf" parses to inf
    except (TypeError, ValueError):
        return None


def histogram_quantiles(
    bucket_series: list[dict[str, Any]], start: float, end: float, qs: Sequence[float] = (0.5, 0.9, 0.99)
) -> dict[str, Any]:
    """Quantiles (ms) from the window increases of cumulative `_bucket` series summed per `le`.

    Linear interpolation inside the bucket like histogram_quantile(); each value is
    {"value": ms, "tag": "bucket_interp", "bucket": [lo_ms, hi_ms]}, or tag "capped"
    (value = highest finite bound) when the quantile falls in the +Inf bucket.
    Returns {} when there were no observations.
    """
    per_le: dict[float, float] = {}
    for labels, increase, _ in counter_delta(bucket_series, start, end):
        bound = _parse_le(labels.get("le"))
        if bound is not None:
            per_le[bound] = per_le.get(bound, 0.0) + increase
    if not per_le:
        return {}
    bounds = sorted(per_le)
    cumulative, running = [], 0.0
    for bound in bounds:
        running = max(running, per_le[bound])  # enforce monotonic buckets like Prometheus
        cumulative.append(running)
    if cumulative[-1] <= 0:
        return {}
    return {_quantile_name(q): _bucket_quantile(q, bounds, cumulative) for q in qs}


def _bucket_quantile(q: float, bounds: list[float], cumulative: list[float]) -> dict[str, Any]:
    rank = q * cumulative[-1]
    index = next(i for i, count in enumerate(cumulative) if count >= rank)
    lower = bounds[index - 1] if index > 0 else 0.0
    below = cumulative[index - 1] if index > 0 else 0.0
    upper = bounds[index]
    if math.isinf(upper):  # above the highest finite bound: report that bound
        return {"value": _ms(lower), "tag": "capped", "bucket": [_ms(lower), None]}
    inside = cumulative[index] - below
    fraction = (rank - below) / inside if inside > 0 else 1.0
    value = lower + (upper - lower) * fraction
    return {"value": round(value * 1000, 4), "tag": "bucket_interp", "bucket": [_ms(lower), _ms(upper)]}


def _ms(seconds: float) -> float:
    return round(seconds * 1000, 6)


# --- Server metrics of a benchmark window -------------------------------------------


@dataclass(frozen=True)
class _Window:
    cluster: str
    start: float
    end: float
    names: dict[str, str]

    @property
    def seconds(self) -> float:
        return self.end - self.start

    @property
    def range_s(self) -> int:
        return int(math.ceil(self.seconds)) + 2 * config.SCRAPE_INTERVAL_S

    def samples(self, selector: str) -> list[dict[str, Any]]:
        """Raw samples covering [start - scrape, end + scrape]."""
        return query(self.cluster, f"{selector}[{self.range_s}s]", time=self.end + config.SCRAPE_INTERVAL_S)

    def node(self, labels: dict[str, Any]) -> str:
        """Node name for an `instance` label (bare IP or IP:port); unknown IPs stay as-is."""
        instance = str(labels.get("instance", ""))
        host = instance.rsplit(":", 1)[0] if instance.count(":") == 1 else instance
        return self.names.get(host, host or "?")


def _node_names(state: dict[str, Any]) -> dict[str, str]:
    names = {}
    for item in state.get("nodes") or []:
        for key in ("private_ip", "public_ip"):
            if item.get(key):
                names[item[key]] = item["name"]
    return names


def _label_value(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


def _wait_for_scrape(end: float) -> None:
    delay = end + config.SCRAPE_INTERVAL_S + _SETTLE_S - _now().timestamp()
    if delay > _MAX_SCRAPE_WAIT_S:
        end_text = iso_ms(datetime.datetime.fromtimestamp(end, datetime.timezone.utc))
        raise VsbenchError(
            f"window end {end_text} is {delay:.0f}s in the future",
            "check the clocks of this machine and the client node (timedatectl)",
        )
    if delay > 0:
        log(f"waiting {delay:.0f}s for the final Prometheus scrape")
        time.sleep(delay)


def server_metrics(
    cluster: str,
    window_start: datetime.datetime | str,
    window_end: datetime.datetime | str,
    keyspace: str,
    index: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Server-side metrics of [window_start, window_end] (client-node clock: the bench log's
    `Starting search` / `Gathering measurements` times, as datetimes or ISO strings).

    Returns {vs_qps, vs_qps_by_node, vs_mean_ms, vs_latency_ms, cpu_pct, cpu_max_core_pct,
    mem_used_gb (GiB), scylla_reactor_pct, net_allowance_exceeded, bottleneck, missing};
    `missing` lists metric families that had no samples. Raises VsbenchError for windows
    shorter than MIN_WINDOW_S and when Prometheus is unreachable.
    """
    start, end = _epoch(window_start, _now()), _epoch(window_end, _now())
    if end - start < MIN_WINDOW_S:
        raise VsbenchError(
            f"short window: {end - start:.0f}s < {MIN_WINDOW_S}s, server metrics need at least 3 scrapes",
            "measure with --duration >= 30s",
        )
    _wait_for_scrape(end)
    win = _Window(cluster, start, end, _node_names(state))
    missing: list[str] = []
    out: dict[str, Any] = {}
    out.update(_vs_metrics(win, keyspace, index, missing))
    out.update(_cpu_metrics(win, missing))
    out["mem_used_gb"] = _mem_metrics(win, missing)
    out["scylla_reactor_pct"] = _reactor_metrics(win, missing)
    out["net_allowance_exceeded"] = _allowance_metrics(win, missing)
    out["bottleneck"] = _bottleneck(out, state)
    out["missing"] = missing
    return out


def _vs_metrics(win: _Window, keyspace: str, index: str, missing: list[str]) -> dict[str, Any]:
    matchers = f'{{keyspace="{_label_value(keyspace)}",index_name="{_label_value(index)}"}}'
    counts = counter_delta(win.samples(f"request_latency_seconds_count{matchers}"), win.start, win.end)
    if not counts:
        missing.append("request_latency_seconds")
        return {"vs_qps": None, "vs_qps_by_node": {}, "vs_mean_ms": None, "vs_latency_ms": {}}
    by_node: dict[str, float] = {}
    for labels, increase, seconds in counts:
        name = win.node(labels)
        by_node[name] = by_node.get(name, 0.0) + (increase / seconds if seconds > 0 else 0.0)
    sums = counter_delta(win.samples(f"request_latency_seconds_sum{matchers}"), win.start, win.end)
    total = sum(increase for _, increase, _ in counts)
    mean = sum(increase for _, increase, _ in sums) / total * 1000 if total > 0 and sums else None
    buckets = win.samples(f"request_latency_seconds_bucket{matchers}")
    return {
        "vs_qps": round(sum(by_node.values()), 1),
        "vs_qps_by_node": {name: round(qps, 1) for name, qps in by_node.items()},
        "vs_mean_ms": round(mean, 4) if mean is not None else None,
        "vs_latency_ms": histogram_quantiles(buckets, win.start, win.end),
    }


def _busy_pct(total: float, idle: float) -> float:
    return 100.0 * (1.0 - idle / total)


def _cpu_metrics(win: _Window, missing: list[str]) -> dict[str, Any]:
    deltas = counter_delta(win.samples("node_cpu_seconds_total"), win.start, win.end)
    if not deltas:
        missing.append("node_cpu_seconds_total")
        return {"cpu_pct": {}, "cpu_max_core_pct": {}}
    cores: dict[tuple[str, str], tuple[float, float]] = {}
    for labels, increase, _ in deltas:
        key = (win.node(labels), str(labels.get("cpu", "")))
        total, idle = cores.get(key, (0.0, 0.0))
        cores[key] = (total + increase, idle + (increase if labels.get("mode") == "idle" else 0.0))
    per_node: dict[str, tuple[float, float]] = {}
    max_core: dict[str, float] = {}
    for (name, _), (total, idle) in cores.items():
        node_total, node_idle = per_node.get(name, (0.0, 0.0))
        per_node[name] = (node_total + total, node_idle + idle)
        if total > 0:
            max_core[name] = max(max_core.get(name, 0.0), _busy_pct(total, idle))
    return {
        "cpu_pct": {name: round(_busy_pct(t, i), 1) for name, (t, i) in per_node.items() if t > 0},
        "cpu_max_core_pct": {name: round(pct, 1) for name, pct in max_core.items()},
    }


def _mem_metrics(win: _Window, missing: list[str]) -> dict[str, float]:
    result = query(win.cluster, "node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes", time=win.end)
    used = {win.node(item.get("metric") or {}): round(float(item["value"][1]) / GIB, 2) for item in result}
    if not used:
        missing.append("node_memory_MemAvailable_bytes")
    return used


def _reactor_metrics(win: _Window, missing: list[str]) -> dict[str, float]:
    promql = f"avg by (instance) (avg_over_time(scylla_reactor_utilization[{int(math.ceil(win.seconds))}s]))"
    result = query(win.cluster, promql, time=win.end)
    reactor = {win.node(item.get("metric") or {}): round(float(item["value"][1]), 1) for item in result}
    if not reactor:
        missing.append("scylla_reactor_utilization")
    return reactor


def _allowance_metrics(win: _Window, missing: list[str]) -> dict[str, int]:
    deltas = counter_delta(win.samples('{__name__=~"node_ethtool_.*allowance_exceeded"}'), win.start, win.end)
    exceeded: dict[str, int] = {}
    for labels, increase, _ in deltas:
        name = win.node(labels)
        exceeded[name] = exceeded.get(name, 0) + int(round(increase))
    if not exceeded:
        missing.append("node_ethtool_*allowance_exceeded")
    return exceeded


def _bottleneck(out: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    """Busiest node: reactor utilization for Scylla (it busy-polls), host CPU otherwise."""
    roles = {item["name"]: item.get("role") for item in state.get("nodes") or []}
    candidates = {name: pct for name, pct in (out.get("cpu_pct") or {}).items() if roles.get(name) != "scylla"}
    candidates.update(out.get("scylla_reactor_pct") or {})
    if not candidates:
        return None
    name = max(candidates, key=lambda key: candidates[key])
    return {"node": name, "pct": candidates[name]}


# --- Formatting ----------------------------------------------------------------------


def _format_labels(metric: dict[str, Any]) -> str:
    labels = dict(metric)
    name = labels.pop("__name__", "")
    inner = ", ".join(f'{key}="{labels[key]}"' for key in sorted(labels))
    return f"{name}{{{inner}}}" if inner or not name else name


def _format_values(values: list[list[Any]]) -> str:
    """Every value of a short series; else a whole-range summary plus the last TAIL_POINTS."""
    first, last = (
        iso_ms(datetime.datetime.fromtimestamp(float(v[0]), datetime.timezone.utc)) for v in (values[0], values[-1])
    )
    span = f"(n={len(values)}, {first}..{last})"
    if len(values) <= TAIL_POINTS:
        return " ".join(str(value) for _, value in values) + f" {span}"
    tail = " ".join(str(value) for _, value in values[-TAIL_POINTS:])
    return f"{_range_summary(values)} | last {TAIL_POINTS}: {tail} {span}"


def _range_summary(values: list[list[Any]]) -> str:
    """`min=.. avg=.. max=.. first=..` over the finite values (NaN/Inf counted as non_finite)."""
    finite = [n for n in (_to_float(value) for _, value in values) if n is not None and math.isfinite(n)]
    first = f"first={values[0][1]}"
    if not finite:
        return f"no finite values, {first}"
    stats = [f"{name}={_number(fn(finite))}" for name, fn in (("min", min), ("avg", statistics.fmean), ("max", max))]
    skipped = len(values) - len(finite)
    return " ".join([*stats, first, *([f"non_finite={skipped}"] if skipped else [])])


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _number(value: float) -> str:
    return str(int(value)) if value.is_integer() and abs(value) < 1e15 else f"{value:.6g}"


def format_vector(result: list[dict[str, Any]]) -> str:
    """Compact `labels => value` lines; a matrix series shows its values (a long one: a
    min/avg/max/first summary of the whole range plus the last TAIL_POINTS), count and span."""
    lines = []
    for item in result:
        labels = _format_labels(item.get("metric") or {})
        if item.get("values"):
            lines.append(f"{labels} => {_format_values(item['values'])}")
        else:
            value = item.get("value") or [None, None]
            lines.append(f"{labels} => {value[1]}")
    return "\n".join(sorted(lines)) if lines else "(empty result)"
