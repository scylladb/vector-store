# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Display of benchmark results: text and markdown tables, the `results` and `compare` rows.

Pure presentation over the records and comparisons that `results` produces; nothing here
reads or writes results.jsonl.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from .results import _canonical, _dig, _is_number, _numbers, index_build_seconds

Columns = Sequence[str | tuple[str, str]]
_MD_ESCAPE = str.maketrans({"|": "\\|", "\n": " "})
_DIGEST_RE = re.compile(r"(sha256:[0-9a-f]{12})[0-9a-f]+")


def _fmt_num(value: float) -> str:
    magnitude = abs(value)
    digits = 0 if magnitude >= 1000 else 1 if magnitude >= 100 else 2 if magnitude >= 1 else 3
    return "0" if value == 0 else f"{value:.{digits}f}"


def format_latency(entry: dict[str, Any] | None) -> str:
    """`<=1.00ms` (floored), `>100ms` (capped), `~0.725ms` (bucket_interp), `3.20ms` (exact)."""
    if not entry:
        return "-"
    value, tag = entry.get("value"), entry.get("tag")
    if tag == "capped":
        return ">100ms" if value is None else f">{_fmt_num(value)}ms"
    if value is None:
        return "?"
    return {"floored": "<=", "bucket_interp": "~"}.get(tag, "") + f"{_fmt_num(value)}ms"


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return _fmt_num(value)
    if isinstance(value, dict) and "tag" in value:
        return format_latency(value)
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value) or "-"
    return str(value)


def _table_values(rows: list[dict[str, Any]], columns: Columns) -> tuple[list[str], list[list[Any]]]:
    cols = [(c, c) if isinstance(c, str) else (c[0], c[1]) for c in columns]
    raw = [[row.get(key) if key in row else _dig(row, *key.split(".")) for _, key in cols] for row in rows]
    return [header for header, _ in cols], raw


def format_table(rows: list[dict[str, Any]], columns: Columns) -> str:
    """Aligned text table; a column is a key (dotted for nesting) or (header, key)."""
    headers, raw = _table_values(rows, columns)
    cells = [[_cell(v) for v in values] for values in raw]
    widths = [max([len(header)] + [len(r[i]) for r in cells]) for i, header in enumerate(headers)]
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)).rstrip()]
    lines.append("  ".join("-" * w for w in widths))
    for values, row_cells in zip(raw, cells, strict=True):
        triples = zip(values, row_cells, widths, strict=True)
        lines.append("  ".join(c.rjust(w) if _is_number(v) else c.ljust(w) for v, c, w in triples).rstrip())
    return "\n".join(lines)


def format_markdown(rows: list[dict[str, Any]], columns: Columns) -> str:
    """GitHub/Jira-friendly markdown table (numeric columns right-aligned)."""
    headers, raw = _table_values(rows, columns)
    numeric = [any(_is_number(r[i]) for r in raw) for i in range(len(headers))]
    lines = ["| " + " | ".join(h.translate(_MD_ESCAPE) for h in headers) + " |"]
    lines.append("|" + "|".join("---:" if n else "---" for n in numeric) + "|")
    lines.extend("| " + " | ".join(_cell(v).translate(_MD_ESCAPE) for v in values) + " |" for values in raw)
    return "\n".join(lines)


def _spec(text: str) -> list[tuple[str, str]]:
    """Column spec "header=dotted.key header ..." (key defaults to the header)."""
    return [(header, key or header) for header, _, key in (item.partition("=") for item in text.split())]


RESULTS_COLUMNS = _spec(
    "run_id kind conc=params.concurrency build=versions.vector_store.build_id label qps=client_metrics.qps "
    "mean_ms=client_metrics.mean_ms p50=client_metrics.latency_ms.p50 p99=client_metrics.latency_ms.p99 "
    "recall=client_metrics.recall.avg vs_qps=server_metrics.vs_qps vs_mean_ms=server_metrics.vs_mean_ms "
    "vs_p99=server_metrics.vs_latency_ms.p99 cpu_vs/client=cpu build_s flags"
)


def summary_row(record: dict[str, Any]) -> dict[str, Any]:
    """The record plus display fields (label, cpu, build_s) for format_table(rows, RESULTS_COLUMNS)."""
    cpu = _dig(record, "server_metrics", "cpu_pct") or {}
    vs_cpu = _numbers(v for k, v in cpu.items() if k.startswith("vs-"))
    client_cpu = cpu.get("client")
    cpu_text = f"{max(vs_cpu):.0f}/{client_cpu:.0f}" if vs_cpu and _is_number(client_cpu) else None
    label = record.get("label") or record.get("arm")
    return {**record, "label": label, "cpu": cpu_text, "build_s": index_build_seconds(record)}


_COMPARE_CELLS = (("qps", "qps"), ("mean_ms", "client_mean_ms"), ("vs_mean_ms", "server_mean_ms"))
_COMPARE_CELLS += (("recall", "recall_avg"), ("build_s", "build_s"))
COMPARE_COLUMNS = _spec(
    "variant kind conc=concurrency n qps qps_d% mean_ms mean_ms_d% vs_mean_ms vs_mean_ms_d% "
    "recall recall_d% qps_cv build_s build_s_d% flags"
)


def compare_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Display rows of compare() for format_table(rows, COMPARE_COLUMNS): `variant` (build_id,
    plus the group's values of the setup fields that differ in a forced compare),
    `median [min-max]` cells and `+x.x%` deltas marked (noise) or (n<2)."""
    differences = result.get("differences") or {}
    rows = []
    for group in result.get("groups") or []:
        setup = _setup_label(group.get("setup") or {}, differences.get(str(group.get("kind")), {}))
        label = str(group.get("build_id") or group.get("version") or "?") + (f" {setup}" if setup else "")
        cv = _dig(group, "qps", "cv")
        row = {**group, "variant": label, "qps_cv": f"{cv:.1%}" if cv is not None else None}
        for column, metric in _COMPARE_CELLS:
            row[column] = _stat_cell(group.get(metric))
            row[f"{column}_d%"] = "base" if group.get("baseline") else _delta_cell(_dig(group, "delta", metric))
        rows.append(row)
    return rows


def _setup_label(setup: dict[str, Any], diff: dict[str, list[Any]]) -> str:
    """`field=value,...`; dict fields (VS env, index options, nodes) show only the differing
    sub-keys, e.g. `maximum_node_connections=16` or `vs.type=r8g.4xlarge`."""
    parts = []
    for field, values in diff.items():
        nested = all(v is None or isinstance(v, dict) for v in values)
        flat = [_flatten(v or {}) if nested else {field: v} for v in values]
        mine = _flatten(setup.get(field) or {}) if nested else {field: setup.get(field)}
        keys = sorted({k for f in flat for k in f if len({_canonical(g.get(k)) for g in flat}) > 1})
        parts += [f"{k}={_label_value(mine.get(k))}" for k in keys]
    return ",".join(parts)


def _flatten(data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in data.items():
        flat.update(_flatten(value, f"{prefix}{key}.") if isinstance(value, dict) else {f"{prefix}{key}": value})
    return flat


def _label_value(value: Any) -> str:
    """'' for None, JSON for lists/bools, image digests cut to 12 hex digits."""
    text = "" if value is None else value if isinstance(value, str) else _canonical(value)
    return _DIGEST_RE.sub(r"\1", text)


def _stat_cell(stats: dict[str, Any] | None) -> str | None:
    if not stats:
        return None
    median = _fmt_num(stats["median"])
    return median if stats["n"] < 2 else f"{median} [{_fmt_num(stats['min'])}-{_fmt_num(stats['max'])}]"


def _delta_cell(delta: dict[str, Any] | None) -> str | None:
    if not delta:
        return None
    noise = {None: " (n<2)", True: " (noise)", False: ""}[delta["within_noise"]]
    return f"{delta['pct']:+.1f}%{noise}"
