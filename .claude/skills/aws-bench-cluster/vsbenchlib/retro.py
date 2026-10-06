# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Command history, notes and the retrospective digest (`vsbench history|note|retro`).

History lives in one global append-only file (state.history_file()): cli.py writes a
`start` line before each command and an `end` line after it (also on SIGTERM/SIGHUP/
SIGINT); `note` adds `note` lines. A start without an end means the process was killed
(SIGKILL, e.g. by the agent's Bash timeout) unless that process is still alive.

The digest also lists the proposals of the last MAX_RETROSPECTIVES files in
state.retrospectives_dir() (`<utc-ts>-<cluster>.md`, written by the agent). The format
`retro` reads is a `## Proposals` heading with one top-level `- ` bullet per proposal:

    ## Proposals
    - **Doc**: explain wait-serving in SKILL.md (evidence: ...)
    - **Code**: add `vsbench indexes` (an earlier retrospective had it too)
      - nested bullets are details, not proposals

The section ends at the next heading of the same or a higher level. Tolerated variants:
any heading level or title containing "Proposals", `*`/`+`/`1.` bullets, a deeper heading
per proposal, and (when no heading names Proposals) a bold title line starting with
"Proposals": `**Proposals**:`, or `- **Proposals**:` followed by nested bullets, which
ends at the next bullet of its own level.
"""

from __future__ import annotations

import datetime
import functools
import os
import re
from pathlib import Path
from typing import Any

from . import config, proc
from . import state as st
from .proc import VsbenchError

NOTE_KINDS = ("workaround", "surprise", "user-correction", "idea", "time-sink")
DEFAULT_NOTE_KIND = "note"
RETRY_WINDOW_S = 30 * 60
SLOWEST = 10
MAX_EXAMPLES = 5
MAX_RETROSPECTIVES = 10
MAX_PROPOSALS_PER_FILE = 10
MAX_TEXT = 160
# Commands whose use hints at a missing subcommand.
RAW_COMMANDS = ("exec", "ssh", "bench raw")
# Commands with a second word (`deploy vs`, `bench search`, ...).
GROUP_COMMANDS = ("deploy", "bench", "job", "dataset", "prom", "results")
_VALUE_OPTIONS = ("-c", "--cluster", "--profile", "--region")
_FLAG_OPTIONS = ("-v", "--verbose")
INTERRUPT_EXITS = (129, 130, 143)  # SIGHUP, SIGINT, SIGTERM (128 + signal)
FAILED_STATUSES = ("failed", "killed", "interrupted")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
_BOLD_TITLE_RE = re.compile(r"^(\s*)((?:[-*+]|\d+[.)])\s+)?\*\*([^*]+?)\*\*(.*)$")
_PROPOSALS_RE = re.compile(r"proposals", re.IGNORECASE)  # anywhere in a heading
_BOLD_PROPOSALS_RE = re.compile(r"^\W*proposals\b", re.IGNORECASE)  # a bold title must start with it
_ITEM_RE = re.compile(r"^(\s*)(?:[-*+]|\d+[.)])\s+(.*)$")
_BOLD_RANK = 7  # bold titles rank below every heading (1-6); a bold list item ranks below a bare one
UTC = datetime.timezone.utc


# --- skill revision -------------------------------------------------------------
@functools.lru_cache(maxsize=1)
def skill_rev() -> str | None:
    """`<short hash of the last commit touching the skill>[-dirty]`; None when unknown."""
    skill = str(config.SKILL_DIR)
    try:
        last = proc.run(["git", "-C", skill, "log", "-1", "--format=%h", "--", "."], check=False, timeout=5)
        status = proc.run(["git", "-C", skill, "status", "--porcelain", "--", "."], check=False, timeout=5)
    except (VsbenchError, OSError):
        return None
    if last.returncode != 0 or status.returncode != 0:
        return None
    return (last.stdout.strip() or "untracked") + ("-dirty" if status.stdout.strip() else "")


# --- time helpers ---------------------------------------------------------------------
def parse_time(text: str) -> datetime.datetime:
    """ISO 8601 (Z, offset or naive UTC; a date alone is midnight UTC), `now`, or a duration
    ago: `6h` or `now-6h`. `-6h` works too, but argparse before Python 3.14 reads
    `--since -6h` as two options, so document and suggest `6h`."""
    value = text.strip()
    if value == "now":
        return proc.utcnow()
    relative = re.sub(r"^(?:now\s*)?-\s*", "", value)
    if re.fullmatch(r"\d+(?:\.\d+)?\s*[smhd]", relative):
        return proc.utcnow() - datetime.timedelta(seconds=proc.parse_duration(relative))
    try:
        moment = datetime.datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as err:
        hint = "use an ISO time like 2026-10-06T08:00:00Z, or a duration ago like 6h"
        raise VsbenchError(f"invalid time '{text}'", hint) from err
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _moment(text: Any) -> datetime.datetime | None:
    if not isinstance(text, str) or not text:
        return None
    try:
        return parse_time(text)
    except VsbenchError:
        return None


def _in_window(ts: Any, since: datetime.datetime | None) -> bool:
    if since is None:
        return True
    moment = _moment(ts)
    return moment is not None and moment >= since


# --- argv helpers ---------------------------------------------------------------------
def strip_globals(argv: list[str]) -> list[str]:
    """argv without the global options (-c X, --profile X, --region X, -v), up to `--`."""
    words: list[str] = []
    skip = False
    for index, word in enumerate(argv):
        if word == "--":
            return words + argv[index:]
        if skip:
            skip = False
        elif word in _VALUE_OPTIONS:
            skip = True
        elif word in _FLAG_OPTIONS or word.split("=", 1)[0] in _VALUE_OPTIONS:
            continue
        else:
            words.append(word)
    return words


def command_name(argv: list[str]) -> str:
    """`deploy vs`, `bench search`, `status`, ... ("?" for an empty argv)."""
    words = [w for w in strip_globals(argv) if w != "--"]
    if not words:
        return "?"
    if words[0] in GROUP_COMMANDS and len(words) > 1 and not words[1].startswith("-"):
        return f"{words[0]} {words[1]}"
    return words[0]


def command_text(argv: list[str]) -> str:
    return " ".join(strip_globals(argv))


def format_seconds(seconds: float) -> str:
    """`12.3s` below a minute, else proc.format_duration (`4m`, `1h05m`)."""
    return f"{seconds:.1f}s" if seconds < 60 else proc.format_duration(seconds)


def _short(text: Any, limit: int = MAX_TEXT) -> str:
    flat = " ".join(str(text or "").split())
    return flat if len(flat) <= limit else flat[: limit - 3] + "..."


def _first_line(text: Any) -> str:
    lines = str(text or "").strip().splitlines()
    return lines[0] if lines else ""


# --- notes -----------------------------------------------------------------------------
def note(cluster: str | None, kind: str, text: str) -> None:
    """Append `{"event": "note", ...}` to the history (retro shows it)."""
    if kind not in (*NOTE_KINDS, DEFAULT_NOTE_KIND):
        raise VsbenchError(f"invalid note kind '{kind}'", "use one of: " + ", ".join(NOTE_KINDS))
    body = text.strip()
    if not body:
        raise VsbenchError("empty note", 'pass the text, e.g. vsbench note --kind surprise "..."')
    entry = {"ts": proc.iso(proc.utcnow()), "event": "note", "cluster": cluster, "kind": kind, "text": body}
    st.append_history(entry | {"skill_rev": skill_rev()})


# --- history rows ----------------------------------------------------------------------
def _pid_alive(pid: Any) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    cmdline = Path(f"/proc/{pid}/cmdline")
    try:
        return b"vsbench" in cmdline.read_bytes() if cmdline.exists() else True
    except OSError:
        return True


def _status(end: dict[str, Any] | None, pid: Any) -> str:
    if end is None:
        return "running" if _pid_alive(pid) else "killed"
    code = end.get("exit")
    if code == 0:
        return "ok"
    if code == proc.EXIT_STILL_RUNNING:
        return "detached"
    return "interrupted" if code in INTERRUPT_EXITS else "failed"


def _row(start: dict[str, Any] | None, end: dict[str, Any] | None) -> dict[str, Any]:
    first = start or end or {}
    argv = list(first.get("argv") or [])
    row = {"id": first.get("id"), "ts": first.get("ts"), "cluster": first.get("cluster"), "argv": argv}
    row |= {"command": command_name(argv), "text": command_text(argv), "skill_rev": first.get("skill_rev")}
    row |= {"status": _status(end, (start or {}).get("pid"))}
    end = end or {}
    row |= {"exit": end.get("exit"), "duration_s": end.get("duration_s"), "ended_at": end.get("ts")}
    return row | {"error": end.get("error"), "hint": end.get("hint")}


def command_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pair start/end lines by id, in start order; a start without an end is `killed`
    (or `running` while its pid is alive), an end without a start is kept too."""
    starts: dict[str, dict[str, Any]] = {}
    ends: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for entry in entries:
        ident, event = entry.get("id"), entry.get("event")
        if not isinstance(ident, str) or event not in ("start", "end"):
            continue
        if ident not in starts and ident not in ends:
            order.append(ident)
        (starts if event == "start" else ends)[ident] = entry
    return [_row(starts.get(ident), ends.get(ident)) for ident in order]


def history_rows(since: datetime.datetime | None, failed_only: bool, last: int | None) -> list[dict[str, Any]]:
    """Command rows (oldest first): ts, status (ok|failed|interrupted|killed|running|detached),
    exit, duration_s, cluster, command, text, error, hint, skill_rev."""
    rows = [r for r in command_rows(st.read_history()) if _in_window(r["ts"], since)]
    if failed_only:
        rows = [r for r in rows if r["status"] in FAILED_STATUSES]
    return rows[-last:] if last else rows


# --- digest ----------------------------------------------------------------------------
def _normalize(text: str) -> str:
    """Error text without ids, hashes and numbers, so similar failures group together."""
    text = re.sub(r"\b\d{8}T\d{6}Z-[a-z-]+-[0-9a-f]{4}\b", "JOB", text)
    text = re.sub(r"\b[0-9a-f]{7,64}\b", "X", text)
    return re.sub(r"\d+(?:\.\d+)?", "N", text)


def _failure_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if row["status"] != "failed":
            continue
        error = _first_line(row.get("error")) or f"exit {row.get('exit')}"
        hint = _first_line(row.get("hint"))
        group = groups.setdefault(
            (_normalize(error), hint),
            {"error": _short(error), "hint": hint or None, "count": 0, "exits": [], "commands": []},
        )
        group["count"] += 1
        group["exits"] = sorted({*group["exits"], row.get("exit")}, key=str)
        group["first_ts"] = group.get("first_ts") or row["ts"]
        group["last_ts"] = row["ts"]
        if row["text"] not in group["commands"] and len(group["commands"]) < MAX_EXAMPLES:
            group["commands"].append(_short(row["text"]))
    return sorted(groups.values(), key=lambda g: (-g["count"], str(g["last_ts"])))


def _retries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A failed command repeated (same cluster and arguments) within RETRY_WINDOW_S."""
    retries = []
    for index, row in enumerate(rows):
        failed_at = _moment(row["ts"])
        if row["status"] not in FAILED_STATUSES or failed_at is None:
            continue
        for later in rows[index + 1 :]:
            moment = _moment(later["ts"])
            if moment is None or (moment - failed_at).total_seconds() > RETRY_WINDOW_S:
                break
            if later["text"] == row["text"] and later["cluster"] == row["cluster"]:
                item = {"command": _short(row["text"]), "failed_at": row["ts"], "failed_status": row["status"]}
                retries.append(item | {"retried_at": later["ts"], "retry_status": later["status"]})
                break
    return retries


def _raw_commands(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row["command"] in RAW_COMMANDS:
            item = counts.setdefault(row["text"], {"command": _short(row["text"]), "count": 0, "statuses": []})
            item["count"] += 1
            item["statuses"] = sorted({*item["statuses"], row["status"]})
    return sorted(counts.values(), key=lambda item: -item["count"])


def _slowest(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    timed = [r for r in rows if isinstance(r.get("duration_s"), (int, float))]
    timed.sort(key=lambda r: -float(r["duration_s"]))
    keys = ("ts", "command", "status", "duration_s")
    return [{k: r[k] for k in keys} | {"text": _short(r["text"])} for r in timed[:SLOWEST]]


def _record_group(record: dict[str, Any]) -> str | None:
    return record.get("comparison_id") or record.get("series_id")


def _high_cv(records: list[dict[str, Any]]) -> list[str]:
    """`<series or comparison>@<concurrency>` groups whose QPS CV exceeds results.HIGH_CV."""
    from . import results

    by_group: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        group = _record_group(record)
        if group and str(record.get("kind", "")).startswith("search"):
            by_group.setdefault(group, []).append(record)
    noisy = []
    for group, members in by_group.items():
        for row in results.aggregate(members):
            cv = (row.get("qps") or {}).get("cv")
            if isinstance(cv, (int, float)) and cv > results.HIGH_CV:
                noisy.append(f"{group}@{row.get('concurrency')} (cv {cv:.0%})")
    return noisy


def _result_flags(clusters: list[str], since: datetime.datetime | None) -> dict[str, Any]:
    """Result-quality flags of the records (started in the window) of the touched clusters."""
    from . import results

    records = []
    for cluster in clusters:
        try:
            loaded = results.load_records(cluster)
        except (VsbenchError, OSError, ValueError) as err:
            proc.debug(f"retro: cannot read results of {cluster}: {err}")
            continue
        records += [r for r in loaded if _in_window(r.get("started_at"), since)]
    flags: dict[str, dict[str, Any]] = {}
    for record in records:
        for flag in record.get("flags") or []:
            item = flags.setdefault(flag, {"count": 0, "runs": []})
            item["count"] += 1
            if len(item["runs"]) < MAX_EXAMPLES:
                item["runs"].append(record.get("run_id"))
    noisy = _high_cv(records)
    if noisy:
        flags["high_cv"] = {"count": len(noisy), "runs": noisy[:MAX_EXAMPLES]}
    return {"clusters": clusters, "records": len(records), "flags": flags}


def _safe_result_flags(clusters: list[str], since: datetime.datetime | None) -> dict[str, Any]:
    """_result_flags, degraded to an `error` entry when results.py cannot be used."""
    try:
        return _result_flags(clusters, since)
    except (ImportError, VsbenchError, KeyError, TypeError, ValueError) as err:
        return {"clusters": clusters, "records": 0, "flags": {}, "error": f"{type(err).__name__}: {err}"}


def _indent(line: str) -> int:
    return len(line.expandtabs(4)) - len(line.expandtabs(4).lstrip())


def _title(line: str) -> tuple[str, int, str] | None:
    """(title, rank, inline text) of a heading or bold title line; a lower rank is a higher level."""
    heading = _HEADING_RE.match(line)
    if heading:
        return heading.group(2).strip(), len(heading.group(1)), ""
    bold = _BOLD_TITLE_RE.match(line)
    if not bold:
        return None
    _, marker, title, rest = bold.groups()
    rank = _BOLD_RANK + (1 + _indent(line) if marker else 0)
    return title.strip().rstrip(":."), rank, rest.strip().lstrip(":.-").strip()


def _section_start(lines: list[str]) -> tuple[int, int, int, str] | None:
    """(line number, rank, indent, inline text) of the first "Proposals" heading, else of
    the first bold "Proposals" title."""
    bold = None
    for number, line in enumerate(lines):
        title = _title(line)
        if title is None:
            continue
        if title[1] < _BOLD_RANK and _PROPOSALS_RE.search(title[0]):
            return number, title[1], _indent(line), title[2]
        if bold is None and title[1] >= _BOLD_RANK and _BOLD_PROPOSALS_RE.search(title[0]):
            bold = (number, title[1], _indent(line), title[2])
    return bold


def _proposals(path: Path) -> list[str] | None:
    """Proposals of the "Proposals" section (format in the module docstring); None when
    the file has no such section."""
    lines = path.read_text(errors="replace").splitlines()
    start = _section_start(lines)
    if start is None:
        return None
    number, rank, indent, inline = start
    base: int | None = None  # indent of the first proposal
    items: list[str] = []
    for line in lines[number + 1 :]:
        title, item = _title(line), _ITEM_RE.match(line)
        if title and title[1] <= rank:
            break
        if item and rank > _BOLD_RANK and _indent(line) <= indent:
            break  # a sibling of a `- **Proposals**:` list item
        if title and title[1] < _BOLD_RANK:
            items.append(_short(title[0]))  # a deeper heading is one proposal
        elif item and (base is None or _indent(line) <= base):
            base = _indent(line) if base is None else base
            items.append(_short(item.group(2)))
        if len(items) >= MAX_PROPOSALS_PER_FILE:
            break
    return items or ([_short(inline)] if inline else [])


def _retrospectives() -> list[dict[str, Any]]:
    """[{file, proposals, has_proposals}] of the last MAX_RETROSPECTIVES files."""
    directory = st.retrospectives_dir()
    if not directory.is_dir():
        return []
    files = sorted(directory.glob("*.md"))[-MAX_RETROSPECTIVES:]
    out = []
    for path in files:
        try:
            proposals = _proposals(path)
        except OSError as err:
            out.append({"file": path.name, "proposals": [], "has_proposals": False, "error": str(err)})
            continue
        out.append({"file": path.name, "proposals": proposals or [], "has_proposals": proposals is not None})
    return out


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    revs = sorted({r["skill_rev"] for r in rows if r.get("skill_rev")})
    return {"commands": len(rows), "by_status": counts, "skill_revs": revs}


def digest(since: datetime.datetime | None) -> dict[str, Any]:
    """Everything the retrospective needs, from the history and the touched clusters' results."""
    entries = st.read_history()
    rows = [r for r in command_rows(entries) if _in_window(r["ts"], since)]
    notes = [e for e in entries if e.get("event") == "note" and _in_window(e.get("ts"), since)]
    clusters = sorted({str(c) for c in [*(r["cluster"] for r in rows), *(n.get("cluster") for n in notes)] if c})
    killed = [r for r in rows if r["status"] in ("killed", "interrupted")]
    return {
        "since": proc.iso(since) if since else None,
        "generated_at": proc.iso(proc.utcnow()),
        "summary": _summary(rows),
        "failures": _failure_groups(rows),
        "killed": [
            {k: r[k] for k in ("ts", "status", "exit", "cluster")} | {"text": _short(r["text"])} for r in killed
        ],
        "retries": _retries(rows),
        "raw_commands": _raw_commands(rows),
        "slowest": _slowest(rows),
        "result_flags": _safe_result_flags(clusters, since),
        "notes": [{k: n.get(k) for k in ("ts", "cluster", "kind", "text")} for n in notes],
        "retrospectives": _retrospectives(),
    }


# --- text ---------------------------------------------------------------------------------
def _section(title: str, lines: list[str]) -> list[str]:
    return ["", f"{title}:", *(lines or ["  (none)"])]


def _failure_lines(groups: list[dict[str, Any]]) -> list[str]:
    lines = []
    for group in groups:
        exits = ",".join(str(e) for e in group["exits"])
        lines.append(f"  {group['count']}x {group['error']} [exit {exits}] (last {group['last_ts']})")
        if group.get("hint"):
            lines.append(f"      hint: {group['hint']}")
        lines.append("      e.g. " + " | ".join(group["commands"]))
    return lines


def _flag_lines(block: dict[str, Any]) -> list[str]:
    lines = [f"  clusters: {', '.join(block['clusters']) or '-'}; {block['records']} records in the window"]
    if block.get("error"):
        lines.append(f"  (results unreadable: {block['error']})")
    for flag, item in sorted(block["flags"].items()):
        lines.append(f"  {flag}: {item['count']} ({', '.join(str(r) for r in item['runs'])})")
    return lines


def _retro_lines(retros: list[dict[str, Any]]) -> list[str]:
    lines = []
    for item in retros:
        lines.append(f"  {item['file']}" + (f" (unreadable: {item['error']})" if item.get("error") else ""))
        empty = "    (Proposals section is empty)" if item.get("has_proposals") else "    (no Proposals section)"
        lines += [f"    - {p}" for p in item["proposals"]] or [empty]
    return lines


def format_digest(d: dict[str, Any]) -> str:
    summary = d["summary"]
    counts = ", ".join(f"{k} {v}" for k, v in sorted(summary["by_status"].items())) or "none"
    revs = ", ".join(summary["skill_revs"]) or "?"
    lines = [f"retrospective digest since {d['since'] or 'the beginning'} (generated {d['generated_at']})"]
    lines.append(f"commands: {summary['commands']} ({counts}); skill revisions: {revs}")
    lines += _section("failures, grouped by error", _failure_lines(d["failures"]))
    killed = [f"  {k['ts']} {k['status']} (exit {k['exit']}): {k['text']}" for k in d["killed"]]
    lines += _section("killed or interrupted (no result recorded by vsbench)", killed)
    retries = [
        f"  {r['command']}: {r['failed_status']} at {r['failed_at']}, retried {r['retried_at']} -> {r['retry_status']}"
        for r in d["retries"]
    ]
    lines += _section(f"retries (same command within {RETRY_WINDOW_S // 60} min of a failure)", retries)
    raw = [f"  {r['count']}x {r['command']} ({','.join(r['statuses'])})" for r in d["raw_commands"]]
    lines += _section("exec/ssh/bench raw calls (candidates for new subcommands)", raw)
    slow = [f"  {format_seconds(s['duration_s']):>7}  {s['status']:<11} {s['text']}" for s in d["slowest"]]
    lines += _section("slowest commands", slow)
    lines += _section("result-quality flags", _flag_lines(d["result_flags"]))
    notes = [f"  {n['ts']} [{n['kind']}] {n['cluster'] or '-'}: {n['text']}" for n in d["notes"]]
    lines += _section("notes", notes)
    lines += _section(f"previous retrospectives in {st.retrospectives_dir()}", _retro_lines(d["retrospectives"]))
    return "\n".join(lines)
