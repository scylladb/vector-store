# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""vsbench command line: parsing, dispatch, locking, history, signals and exit codes.

The handlers live in cli_cluster.py and cli_bench.py and are looked up by name at
dispatch time. They import the other modules lazily (`load_module`), so a missing or
broken module only breaks its own commands. stdout carries results (tables, or one
JSON document with --json); progress, warnings and errors go to stderr.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import importlib
import json
import os
import re
import secrets
import signal
import sys
import time
import traceback
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from types import FrameType
from typing import Any, NoReturn

from . import awsapi, config, proc, retro
from . import state as st
from .awsapi import Aws
from .cli_types import (
    _billing_project,
    _count,
    _duration,
    _env_key,
    _env_pair,
    _int_list,
    _param,
    _positive,
    _since,
    _ttl,
)
from .proc import VsbenchError

DEFAULT_CLUSTER = "default"
FOREGROUND_S = config.DEFAULT_FOREGROUND_SECONDS
TTL_WARN_S = 3600
LOG_SERVICES = ("scylla", "vector-store", "userdata", "monitoring")
ERROR_TEXT_LIMIT = 2000
RESULTS_LAST = 20  # `results --last` default
WAIT_HELP = "foreground budget; then exit 75 and the work goes on (resume with the hint)"
TIME_FORMS = "now, now-15m (s/m/h/d), ISO 8601 UTC or Unix epoch"
VALUE_LIKE_RE = re.compile(r"-\.?\d")  # -15m, -6h, -1 are values (3.14's rule; 3.10-3.13 saw -15m as an option)
_SIGNALS = tuple(getattr(signal, name) for name in ("SIGINT", "SIGTERM", "SIGHUP") if hasattr(signal, name))
Handler = Callable[["Context", argparse.Namespace], int]


class UsageError(VsbenchError):
    exit_code = proc.EXIT_USAGE


class Interrupted(KeyboardInterrupt):
    """SIGINT/SIGTERM/SIGHUP turned into an exception, so cleanups and the history end line run."""

    def __init__(self, signum: int) -> None:
        super().__init__(f"interrupted by {signal.Signals(signum).name}")
        self.signum = signum


def duration_text(seconds: int) -> str:
    """540 -> 9m, 7200 -> 2h, 30 -> 30s (the form --timeout/--ttl accept)."""
    unit, size = next(((u, s) for u, s in (("h", 3600), ("m", 60)) if seconds and seconds % s == 0), ("s", 1))
    return f"{seconds // size}{unit}"


class _HelpFormatter(argparse.HelpFormatter):
    """Appends `(default: X)` to options that have help text and a real default (not None,
    False or []); durations (type=_duration) show as 9m/2h rather than seconds."""

    def _get_help_string(self, action: argparse.Action) -> str | None:
        text, default = action.help, action.default
        unset = default is None or default is False or default is argparse.SUPPRESS or default == []
        if not text or unset or "%(default)" in text or not action.option_strings:
            return text
        shown = duration_text(default) if action.type is _duration and isinstance(default, int) else cell(default)
        return f"{text} (default: {shown.replace('%', '%%')})"  # help strings are %-formatted


class _Parser(argparse.ArgumentParser):
    """Usage errors raise UsageError; help shows defaults; `-15m`-like values parse everywhere."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("formatter_class", _HelpFormatter)
        super().__init__(*args, **kwargs)
        self._negative_number_matcher = VALUE_LIKE_RE

    def error(self, message: str) -> NoReturn:
        raise UsageError(message, self.format_usage().strip())


@dataclass(frozen=True)
class Context:
    cluster: str
    profile: str
    region: str
    argv: tuple[str, ...]

    def aws(self) -> Aws:
        return Aws(self.profile, self.region)


def load_module(name: str) -> Any:
    """Import vsbenchlib.<name> lazily; a broken module fails only the commands that need it."""
    try:
        return importlib.import_module(f"{__package__}.{name}")
    except (ImportError, SyntaxError) as err:
        raise VsbenchError(
            f"vsbench module '{name}' cannot be loaded: {err}", "this part of vsbench is broken"
        ) from err


def out(text: str = "") -> None:
    sys.stdout.write(text + "\n")


# --- parser -------------------------------------------------------------------------------
def _globals(suppress: bool) -> argparse.ArgumentParser:
    """Global options; repeated on every subcommand (SUPPRESS keeps the top-level value)."""
    parser = argparse.ArgumentParser(add_help=False)
    default = argparse.SUPPRESS if suppress else None
    parser.add_argument("-c", "--cluster", default=default, help="cluster name (else $VSBENCH_CLUSTER, else default)")
    parser.add_argument("--profile", default=default, help="AWS profile (else state, $AWS_PROFILE, config)")
    parser.add_argument("--region", default=default, help="AWS region (else state, $AWS_REGION, config)")
    flag_default = argparse.SUPPRESS if suppress else False
    parser.add_argument("-v", "--verbose", action="store_true", default=flag_default, help="debug output")
    return parser


def _add(sub: Any, common: argparse.ArgumentParser, name: str, func: str, text: str, **flags: Any) -> Any:
    """A leaf command. flags: lock (cluster_lock), extra (none|optional|required args after
    `--`), warn (TTL/credential warnings), history (start/end lines)."""
    parser = sub.add_parser(name, help=text, description=text, parents=[common])
    parser.set_defaults(func=func, lock=flags.get("lock", False), extra_mode=flags.get("extra", "none"))
    parser.set_defaults(warn=flags.get("warn", True), history=flags.get("history", True))
    return parser


def _group(sub: Any, common: argparse.ArgumentParser, name: str, text: str, dest: str) -> Any:
    parser = sub.add_parser(name, help=text, description=text, parents=[common])
    return parser.add_subparsers(dest=dest, required=True, metavar="SUBCOMMAND", parser_class=_Parser)


def _up_command(sub: Any, common: argparse.ArgumentParser) -> None:
    p = _add(sub, common, "up", "cmd_up", "create the cluster (minutes: run it in the background)", lock=True)
    p.add_argument("--scylla-nodes", type=_positive, default=1, help="Scylla nodes (3 for --rf 3)")
    p.add_argument("--vs-nodes", type=_positive, default=1, help="Vector Store nodes")
    for role in config.ROLES:
        p.add_argument(f"--{role}-type", default=config.DEFAULT_INSTANCE_TYPES[role], help=f"{role} instance type")
    p.add_argument("--az", help="use only this availability zone (default: the first candidate with capacity)")
    p.add_argument("--subnet-id", help="launch into this subnet (default: the default VPC, else the SCT VPC)")
    p.add_argument("--ttl", type=_ttl, default=config.DEFAULT_TTL, help="the nodes power off and terminate after")
    billing = config.DEFAULT_BILLING_PROJECT
    p.add_argument("--billing-project", type=_billing_project, default=billing, help="finops billing_project tag")
    disk = config.DEFAULT_DISK_GB
    p.add_argument("--node-disk-gb", type=_positive, default=disk["scylla"], help="root disk of Scylla/VS nodes")
    p.add_argument("--client-disk-gb", type=_positive, default=disk["client"], help="client disk (datasets)")
    p.add_argument("--dry-run", action="store_true", help="print the plan and cost; create nothing")
    p.add_argument("--keep-on-failure", action="store_true", help="do not terminate nodes when up fails")


def _lifecycle_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    p = _add(sub, common, "doctor", "cmd_doctor", "check local tools, AWS identity and credentials", warn=False)
    p.add_argument("--json", action="store_true")
    text = "get AWS credentials through the Okta device flow: prints the URL to approve, waits, reports the expiry"
    p = _add(sub, common, "login", "cmd_login", text, warn=False)
    p.add_argument("--username", help="Okta user e-mail (default: $OKTA_USERNAME, then git user.email)")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=600, help="how long to wait for the approval")
    _up_command(sub, common)
    p = _add(sub, common, "down", "cmd_down", "terminate the cluster and delete its AWS resources", lock=True)
    p.add_argument("--yes", action="store_true", help="do not ask (only after the user agreed)")
    p.add_argument("--purge", action="store_true", help="also delete the local cluster dir (results)")
    text = "list clusters (AWS) in the current region and every region with local vsbench state"
    p = _add(sub, common, "list", "cmd_list", text, warn=False)
    p.add_argument("--all-owners", action="store_true", help="not only yours")
    p.add_argument("--json", action="store_true")
    p = _add(sub, common, "status", "cmd_status", "versions, services, TTL and the Grafana tunnel")
    p.add_argument("--json", action="store_true")
    p.add_argument("--refresh", action="store_true", help="re-read instances from AWS first")
    text = "move the TTL (nodes first, then the ExpiresAt tag); works while another command runs"
    p = _add(sub, common, "extend", "cmd_extend", text)  # no cluster lock: st.update serializes the write
    when = p.add_mutually_exclusive_group(required=True)
    when.add_argument("--ttl", type=_duration, help="new TTL from now, e.g. 12h")
    when.add_argument("--until", help="absolute time with Z or +HH:MM")
    p.add_argument("--shorten", action="store_true", help="allow an earlier expiry")
    _add(sub, common, "refresh-ip", "cmd_refresh_ip", "allow ssh from your current public IP (AWS)")
    p = _add(sub, common, "build", "cmd_build", "cross-compile for arm64; prints the build_id", warn=False)
    p.add_argument("--source", default="local", help="local | local:+label | git:<ref>")
    p.add_argument("--jobs", type=_positive, default=8, help="parallel compile jobs")
    p = _add(sub, common, "builds", "cmd_builds", "list cached builds (--nodes: also on the nodes)", warn=False)
    p.add_argument("--nodes", action="store_true")
    p.add_argument("--json", action="store_true")


def _deploy_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    deploy = _group(sub, common, "deploy", "deploy scylla, vs, bench, monitoring or all", "component")
    p = _add(deploy, common, "scylla", "cmd_deploy", "deploy Scylla", lock=True)
    p.add_argument("--image", help="nightly | release:<ver> | <image ref> (default: the pinned image)")
    p.add_argument("--wipe", action="store_true", help="stop all nodes and wipe their data first")
    p.add_argument("--refresh", action="store_true", help="re-resolve the pinned floating ref")
    p = _add(deploy, common, "vs", "cmd_deploy", "deploy Vector Store", lock=True)
    p.add_argument("--source", help="release:<ver>|release:latest|git:<ref>|local[:+label]|build:<id>")
    p.add_argument("--env", type=_env_pair, action="append", default=[], metavar="K=V", help="set a VS env variable")
    p.add_argument("--unset", type=_env_key, action="append", default=[], metavar="K", help="drop a VS env variable")
    p.add_argument("--refresh", action="store_true", help="re-resolve the pinned floating ref")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)
    p = _add(deploy, common, "bench", "cmd_deploy", "deploy the benchmark binary to the client", lock=True)
    p.add_argument("--source", help="git:<ref> | local[:+label] | build:<id>")
    p.add_argument("--refresh", action="store_true", help="re-resolve the pinned floating ref")
    _add(deploy, common, "monitoring", "cmd_deploy", "deploy scylla-monitoring on the client", lock=True)
    p = _add(deploy, common, "all", "cmd_deploy", "deploy every component not deployed yet", lock=True)
    p.add_argument("--force", action="store_true", help="redeploy deployed components too")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)
    text = "wait until every Vector Store node is SERVING; records a pending index build"
    p = _add(sub, common, "wait-serving", "cmd_wait_serving", text, lock=True)  # it writes state and results
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)


def _tool_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    p = _add(sub, common, "ssh", "cmd_ssh", "ssh to a node, optionally running CMD", extra="optional")
    p.add_argument("node")
    p.add_argument("words", nargs="*", metavar="CMD")
    p.add_argument("--i-mean-it", action="store_true", help="allow power-off and TTL-disarming commands")
    p = _add(sub, common, "exec", "cmd_exec", "run a shell command on nodes (output capped)", extra="optional")
    p.add_argument("target", help="all | scylla | vs | client | node1,node2")
    p.add_argument("words", nargs="*", metavar="CMD")
    p.add_argument("--json", action="store_true")
    p.add_argument("--tail", type=_count, default=50, help="last N lines per node (0: all)")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help="kill the command after")
    p.add_argument("--i-mean-it", action="store_true", help="allow power-off and TTL-disarming commands")
    p = _add(sub, common, "push", "cmd_push", "copy a local file to NODE:PATH")
    p.add_argument("local")
    p.add_argument("dest", metavar="NODE:PATH")
    p = _add(sub, common, "pull", "cmd_pull", "copy NODE:PATH to a local path")
    p.add_argument("source", metavar="NODE:PATH")
    p.add_argument("local")
    p = _add(sub, common, "logs", "cmd_logs", "service logs of a node")
    p.add_argument("node")
    p.add_argument("--service", choices=LOG_SERVICES, help="default: by role (scylla, vector-store, monitoring)")
    p.add_argument("-n", "--lines", type=_positive, default=100, help="last N lines")
    p.add_argument("--since", type=_duration, help="only the last DURATION, e.g. 10m")
    prom = _group(sub, common, "prom", "query Prometheus on the client", "prom_cmd")
    p = _add(prom, common, "query", "cmd_prom", "instant query")
    p.add_argument("promql")
    p.add_argument("--time", help=f"evaluation time: {TIME_FORMS} (default: now)")
    p.add_argument("--raw", action="store_true", help="print the JSON result")
    p = _add(prom, common, "range", "cmd_prom", "range query")
    p.add_argument("promql")
    p.add_argument("--start", default="now-15m", help=TIME_FORMS)
    p.add_argument("--end", default="now", help=TIME_FORMS)
    p.add_argument("--step", default="15s", help="resolution")
    p.add_argument("--raw", action="store_true", help="print the JSON result")
    p = _add(prom, common, "api", "cmd_prom", "GET /api/v1/PATH with k=v parameters (JSON)")
    p.add_argument("path")
    p.add_argument("params", nargs="*", type=_param, metavar="k=v")


def _dataset_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    dataset = _group(sub, common, "dataset", "dataset catalog and downloads on the client", "dataset_cmd")
    p = _add(dataset, common, "list", "cmd_dataset", "the dataset catalog", warn=False)
    p.add_argument("--json", action="store_true")
    p = _add(dataset, common, "fetch", "cmd_dataset", "download a dataset to the client", lock=True)
    p.add_argument("key")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)
    _add(dataset, common, "status", "cmd_dataset", "datasets present on the client")


def _bench_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    bench = _group(sub, common, "bench", "load data and run benchmarks", "bench_cmd")
    for name, text in (("load", "load a dataset and build an index"), ("index", "rebuild the index with new options")):
        p = _add(bench, common, name, "cmd_bench", text, lock=True)
        if name == "load":
            p.add_argument("dataset")
            p.add_argument("--rf", type=_positive, default=1, help="replication factor (needs that many Scylla nodes)")
            p.add_argument("--concurrency", type=_positive, default=512, help="upload concurrency (parallel inserts)")
            p.add_argument("--local-index", action="store_true", help="local (filtering) index: needs --bucket")
            p.add_argument("--resume", action="store_true", help="skip the phases already completed")
        p.add_argument("--index-options", help="CQL map, e.g. \"{'similarity_function': 'COSINE'}\"")
        p.add_argument("--index-timeout", dest="index_timeout_s", type=_duration, default=7200, help="build limit")
        p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)
    for name, text, lock in (
        ("search", "search benchmark (-- EXTRA_ARGS go to the tool)", True),
        ("validate", "check that a search would be accepted (no lock, runs nothing)", False),
    ):
        p = _add(bench, common, name, "cmd_bench", text, lock=lock, extra="optional")
        p.add_argument("kind", choices=("cql", "http"))
        p.add_argument("--concurrency", type=_int_list, default=[64], help="comma list, e.g. 16,64,128")
        p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)
        _search_options(p, repeat=1)
    _ab_command(bench, common)
    p = _add(bench, common, "rerun", "cmd_bench", "repeat a recorded search with the current deployment", lock=True)
    p.add_argument("run_id")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)
    text = "run the benchmark tool with ARGS after --"
    p = _add(bench, common, "raw", "cmd_bench", text, lock=True, extra="required")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)


def _ab_command(bench: Any, common: argparse.ArgumentParser) -> None:
    text = "compare two Vector Store sources in ABBA order (-- EXTRA_ARGS go to every search)"
    p = _add(bench, common, "ab", "cmd_bench", text, lock=True, extra="optional")
    p.add_argument("--a", required=True, help="source of arm A")
    p.add_argument("--b", required=True, help="source of arm B")
    p.add_argument("--kind", choices=("cql", "http"), default="cql", help="search kind")
    p.add_argument("--concurrency", type=_int_list, default=[64], help="comma list, e.g. 16,64,128")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, help="per-search budget (min/default: run time+10m)")
    _search_options(p, repeat=2)


def _search_options(parser: argparse.ArgumentParser, repeat: int) -> None:
    parser.add_argument("--limit", type=_positive, default=10, help="k nearest neighbours per query")
    parser.add_argument("--duration", dest="duration_s", type=_duration, default=60, help="measured time per run")
    parser.add_argument("--warmup", dest="warmup_s", type=_duration, default=30, help="unmeasured, before each run")
    help_repeat = "runs per concurrency" if repeat == 1 else "runs per arm (ABBA order)"
    parser.add_argument("--repeat", type=_positive, default=repeat, help=f"{help_repeat}; 3+ for a noise estimate")
    parser.add_argument("--bucket", type=_count, help="filter bucket 0-8 (a --local-index load needs it)")
    parser.add_argument("--label", help="free text stored with the results")
    parser.add_argument("--perf", type=lambda t: t.split(","), default=[], help="nodes to profile, e.g. vs-0,scylla-0")


def _job_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    job = _group(sub, common, "job", "detached jobs on the nodes", "job_cmd")
    p = _add(job, common, "list", "cmd_job", "jobs of this cluster")
    p.add_argument("--json", action="store_true")
    p = _add(job, common, "status", "cmd_job", "state and last lines of a job")
    p.add_argument("job_id")
    p = _add(job, common, "logs", "cmd_job", "a job's log")
    p.add_argument("job_id")
    p.add_argument("--tail", type=_count, default=50, help="last N lines (0: all)")
    p = _add(job, common, "wait", "cmd_job", "wait for a job and record its results", lock=True)
    p.add_argument("job_id")
    p.add_argument("--timeout", dest="timeout_s", type=_duration, default=FOREGROUND_S, help=WAIT_HELP)
    p = _add(job, common, "cancel", "cmd_job", "stop a job")  # no lock: the escape hatch while a command runs
    p.add_argument("job_id")


def _results_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    p = _add(sub, common, "results", "cmd_results", "recorded results (compare: medians and deltas)", warn=False)
    p.add_argument("--last", type=_count, default=RESULTS_LAST, help="last N records (0: all)")
    p.add_argument("--kind", help="kind prefix, e.g. search, search-cql, load, index-build")
    p.add_argument("--series", help="only this series_id")
    p.add_argument("--comparison", help="only this comparison_id (bench ab)")
    p.add_argument("--json", action="store_true", help="print JSON")
    p.add_argument("--format", choices=("table", "md"), default="table", help="md: a Markdown table")
    results = p.add_subparsers(dest="results_cmd", required=False, metavar="compare", parser_class=_Parser)
    p = _add(results, common, "compare", "cmd_results_compare", "compare runs, series or comparisons", warn=False)
    p.add_argument("ids", nargs="+", metavar="RUN_OR_SERIES_ID")
    p.add_argument("--force", action="store_true", help="compare even if the setups differ (say why)")
    # SUPPRESS: a value given before `compare` (results --json compare X) must survive the subparser.
    p.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help="print JSON")
    p.add_argument("--format", choices=("table", "md"), default=argparse.SUPPRESS, help="md: a Markdown table")


def _meta_commands(sub: Any, common: argparse.ArgumentParser) -> None:
    _job_commands(sub, common)
    _results_commands(sub, common)
    _add(sub, common, "collect", "cmd_collect", "save a Prometheus snapshot and node logs locally", lock=True)
    p = _add(sub, common, "note", "cmd_note", "record a note for the retrospective", warn=False, history=False)
    kinds = (*retro.NOTE_KINDS, retro.DEFAULT_NOTE_KIND)
    p.add_argument("--kind", choices=kinds, default=retro.DEFAULT_NOTE_KIND, help="what the note is about")
    p.add_argument("text", nargs="+")
    since = "ISO time (e.g. the session start) or a duration ago, e.g. 6h"
    p = _add(sub, common, "retro", "cmd_retro", "digest for the retrospective", warn=False, history=False)
    p.add_argument("--since", type=_since, help=since)
    p.add_argument("--json", action="store_true")
    p = _add(sub, common, "history", "cmd_history", "command history", warn=False, history=False)
    p.add_argument("--failed", action="store_true", help="only failed commands")
    p.add_argument("--last", type=_count, default=30, help="last N commands (0: all)")
    p.add_argument("--since", type=_since, help=since)
    p.add_argument("--json", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    text = "AWS benchmark clusters for ScyllaDB Vector Store (see SKILL.md)."
    parser = _Parser(prog="vsbench", description=text, parents=[_globals(False)])
    common = _globals(True)
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND", parser_class=_Parser)
    groups = (_lifecycle_commands, _deploy_commands, _tool_commands, _dataset_commands, _bench_commands, _meta_commands)
    for adder in groups:
        adder(sub, common)
    return parser


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse argv; words after the first `--` go to args.extra for exec/ssh/bench search/raw."""
    parser = build_parser()
    cut = argv.index("--") if "--" in argv else len(argv)
    try:
        args = parser.parse_args(argv[:cut])
    except UsageError:
        if cut == len(argv):
            raise
        args = _with_extra(parser.parse_args(argv), [])
        if args.extra_mode != "none":
            raise
        return args
    if args.extra_mode == "none" and cut < len(argv):  # a plain `--`, e.g. before a note starting with -
        return _with_extra(parser.parse_args(argv), [])
    args = _with_extra(args, argv[cut + 1 :])
    if args.extra_mode == "required" and not args.extra:
        raise UsageError("this command needs arguments after --", "e.g. vsbench bench raw -- --help")
    return args


def _with_extra(args: argparse.Namespace, extra: list[str]) -> argparse.Namespace:
    args.extra = list(extra)
    return args


# --- output helpers ----------------------------------------------------------------------
def cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.4g}"
    if isinstance(value, (list, tuple)) and not any(isinstance(v, (dict, list, tuple)) for v in value):
        return ",".join(cell(v) for v in value) or "-"
    if isinstance(value, (dict, list, tuple)):
        return short(json.dumps(value, default=str, separators=(",", ":")), 200)
    return str(value)


def short(text: str, limit: int) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= limit else flat[: limit - 3] + "..."


def table(rows: list[dict[str, Any]], columns: Sequence[str]) -> str:
    if not rows:
        return "(none)"
    cells = [[cell(row.get(column)) for column in columns] for row in rows]
    widths = [max([len(column)] + [len(line[i]) for line in cells]) for i, column in enumerate(columns)]
    lines = ["  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)).rstrip()]
    lines += ["  ".join(v.ljust(w) for v, w in zip(line, widths, strict=True)).rstrip() for line in cells]
    return "\n".join(lines)


def _flat(value: Any) -> bool:
    return isinstance(value, dict) and len(value) <= 10 and not any(isinstance(v, (dict, list)) for v in value.values())


def _kv(item: dict[str, Any]) -> str:
    return " ".join(f"{key}={cell(value)}" for key, value in item.items())


def tree(value: Any, indent: str = "  ", depth: int = 3) -> list[str]:
    """Compact indented rendering of nested dicts/lists (status-like documents); flat dicts
    become one `k=v k=v` line, deeper levels than `depth` compact JSON."""
    if isinstance(value, dict):
        lines = []
        for key, item in value.items():
            if _flat(item) and item:
                lines.append(f"{indent}{key}: {_kv(item)}")
            elif isinstance(item, (dict, list)) and item and depth > 1:
                lines += [f"{indent}{key}:", *tree(item, indent + "  ", depth - 1)]
            else:
                lines.append(f"{indent}{key}: {cell(item)}")
        return lines
    if isinstance(value, list):
        lines = []
        for item in value:
            if _flat(item):
                lines.append(f"{indent}- {_kv(item)}")
            elif isinstance(item, dict) and item and depth > 1:
                nested = tree(item, indent + "  ", depth - 1)
                lines += [f"{indent}- {nested[0].lstrip()}", *nested[1:]]
            else:
                lines.append(f"{indent}- {cell(item)}")
        return lines or [f"{indent}(none)"]
    return [f"{indent}{cell(value)}"]


def print_doc(doc: Any) -> None:
    out("\n".join(tree(doc, "")) if isinstance(doc, (dict, list)) else cell(doc))


def print_records(records: list[dict[str, Any]], fmt: str = "table") -> None:
    if not records:
        out("(no results)")
        return
    display = load_module("results_format")
    rows = [display.summary_row(r) for r in records]
    render = display.format_markdown if fmt == "md" else display.format_table
    out(render(rows, display.RESULTS_COLUMNS))


# --- context and warnings -------------------------------------------------------------
def cluster_name(args: argparse.Namespace) -> str:
    """-c, else $VSBENCH_CLUSTER, else `default`."""
    name = args.cluster or os.environ.get("VSBENCH_CLUSTER") or DEFAULT_CLUSTER
    try:
        return st.validate_cluster_name(name)
    except VsbenchError as err:
        raise UsageError(str(err), err.hint) from err


def _guess_cluster(argv: list[str]) -> str:
    """The cluster named by an argv that did not parse (for its history lines)."""
    for index, word in enumerate(argv):
        if word in ("-c", "--cluster") and index + 1 < len(argv):
            return argv[index + 1]
        if word.startswith("--cluster="):
            return word.split("=", 1)[1]
    return os.environ.get("VSBENCH_CLUSTER") or DEFAULT_CLUSTER


def _load_state(cluster: str) -> st.State | None:
    try:
        return st.load(cluster)
    except (VsbenchError, OSError, ValueError):
        return None


def make_context(args: argparse.Namespace, cluster: str, argv: list[str], current: st.State | None) -> Context:
    """AWS profile/region: CLI, else the cluster state, else the environment, else config."""
    known = current or {}
    env = os.environ
    profile = args.profile or known.get("profile") or env.get("AWS_PROFILE") or awsapi.default_profile()
    region = args.region or known.get("region") or env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION")
    return Context(cluster, profile, region or config.DEFAULT_REGION, tuple(argv))


def _seconds_until(moment: datetime.datetime, now: datetime.datetime) -> float:
    aware = moment if moment.tzinfo else moment.replace(tzinfo=datetime.timezone.utc)
    return (aware - now).total_seconds()


def expiry_warnings(state: st.State | None, profile: str, now: datetime.datetime) -> list[str]:
    """Offline checks: the cluster TTL ends within TTL_WARN_S (or passed); the profile's
    credentials expire within MIN_CREDENTIALS_SECONDS."""
    warnings = []
    known = state or {}
    expires, cluster = known.get("expires_at"), known.get("cluster")
    if expires and known.get("nodes") and not known.get("terminated_at"):
        try:
            left: float | None = _seconds_until(proc.parse_iso(expires), now)
        except ValueError:
            left = None
        if left is not None and left <= 0:
            warnings.append(f"cluster '{cluster}' passed its TTL at {expires}; its nodes power off and terminate")
        elif left is not None and left < TTL_WARN_S:
            keep = f"keep it with: vsbench -c {cluster} extend --ttl 12h"
            warnings.append(f"cluster '{cluster}' expires in {proc.format_duration(left)} ({expires}); {keep}")
    expiry = awsapi.credentials_expiry(profile)
    cred_left = _seconds_until(expiry, now) if expiry else None
    if cred_left is not None and 0 < cred_left < config.MIN_CREDENTIALS_SECONDS:
        what = f"AWS credentials of profile {profile} expire in {proc.format_duration(cred_left)}"
        warnings.append(f"{what} (up/down/list need them); {config.LOGIN_HINT}")
    return warnings


# --- signals, errors and exit codes ----------------------------------------------------
@dataclass
class SignalGuard:
    """SIGINT/SIGTERM/SIGHUP raise Interrupted while the command runs (`active`). Outside that
    window, and after the first raise, a signal is only remembered, so rollbacks, the lock
    release and the history end line are not cut short (SIGKILL still works)."""

    active: bool = False
    signum: int | None = None

    def handle(self, signum: int, frame: FrameType | None) -> None:
        self.signum = self.signum or signum
        if self.active:
            self.active = False
            raise Interrupted(signum)


@contextlib.contextmanager
def signal_guard() -> Iterator[SignalGuard]:
    guard = SignalGuard()
    previous = {}
    for signum in _SIGNALS:
        try:
            previous[signum] = signal.signal(signum, guard.handle)
        except (ValueError, OSError):  # not the main thread
            continue
    try:
        yield guard
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def exit_code_for(exc: BaseException) -> int:
    """VsbenchError.exit_code (1, usage 2, AWS auth 3, capacity 4, precondition 5, still running
    75); 128 + signal for interrupts; 1 for anything unexpected."""
    if isinstance(exc, Interrupted):
        return 128 + exc.signum
    if isinstance(exc, KeyboardInterrupt):
        return 128 + signal.SIGINT
    if isinstance(exc, VsbenchError):
        return int(exc.exit_code)
    if isinstance(exc, BrokenPipeError):
        return 128 + getattr(signal, "SIGPIPE", 13)
    if isinstance(exc, SystemExit):
        return exc.code if isinstance(exc.code, int) else (0 if exc.code is None else proc.EXIT_ERROR)
    return proc.EXIT_ERROR


def _err(line: str, prefix: str = "vsbench: ") -> None:
    try:
        sys.stderr.write(f"{prefix}{line}\n")
        sys.stderr.flush()
    except (OSError, ValueError):  # stderr gone (SIGHUP): the history still gets the error
        pass


def _silence_stdout() -> None:
    try:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
    except (OSError, ValueError):
        pass


def report(exc: BaseException) -> int:
    """Print `error:`/`hint:` (usage errors: the usage line first); return the exit code."""
    code = exit_code_for(exc)
    if isinstance(exc, VsbenchError):
        usage = exc.hint if exc.hint and exc.hint.startswith("usage:") else None
        if usage:
            _err(usage, prefix="")
        _err(f"error: {exc}")
        if exc.hint and not usage:
            _err(f"hint: {exc.hint}")
    elif isinstance(exc, KeyboardInterrupt):
        _err(str(exc) if isinstance(exc, Interrupted) else "interrupted")
    elif isinstance(exc, BrokenPipeError):
        _silence_stdout()
    elif not isinstance(exc, SystemExit):
        try:
            traceback.print_exception(exc)  # a bug in vsbench: keep the traceback
        except (OSError, ValueError):
            pass
        _err(f"internal error: {type(exc).__name__}: {exc}")
    return code


# --- history and dispatch ------------------------------------------------------------------
def _now() -> str:
    return proc.iso(proc.utcnow())


def _append_history(entry: dict[str, Any]) -> None:
    try:
        st.append_history(entry)
    except OSError as err:
        proc.warn(f"cannot write the history: {err}")


def _end_entry(base: dict[str, Any], code: int, failure: BaseException | None, started: float) -> dict[str, Any]:
    error = None
    if failure is not None and code != 0:
        error = str(failure) or type(failure).__name__
        error = error if len(error) <= ERROR_TEXT_LIMIT else error[:ERROR_TEXT_LIMIT] + "..."
    hint = getattr(failure, "hint", None) if code != 0 else None
    hint = None if isinstance(hint, str) and hint.startswith("usage:") else hint  # argparse usage: noise
    entry = base | {"ts": _now(), "event": "end", "exit": code, "duration_s": round(time.monotonic() - started, 1)}
    return entry | {"error": error, "hint": hint, "skill_rev": retro.skill_rev()}


def _start_entry(base: dict[str, Any]) -> dict[str, Any]:
    return base | {"ts": _now(), "event": "start", "pid": os.getpid(), "skill_rev": retro.skill_rev()}


def resolve_handler(name: str) -> Handler:
    from . import cli_bench, cli_cluster

    for module in (cli_cluster, cli_bench):
        handler = getattr(module, name, None)
        if callable(handler):
            return handler  # type: ignore[no-any-return]
    raise VsbenchError(f"internal error: no handler named {name}")


def _execute(args: argparse.Namespace, cluster: str, argv: list[str], guard: SignalGuard) -> tuple[int, Any]:
    """Warnings, lock and handler; returns (exit code, the exception or None)."""
    try:
        current = _load_state(cluster)
        ctx = make_context(args, cluster, argv, current)
        if args.warn:
            for message in expiry_warnings(current, ctx.profile, proc.utcnow()):
                proc.warn(message)
        handler = resolve_handler(args.func)
        with contextlib.ExitStack() as stack:
            if args.lock:
                stack.enter_context(st.cluster_lock(cluster, " ".join(argv)))
            guard.active = True
            try:
                if guard.signum is not None:
                    raise Interrupted(guard.signum)
                code = handler(ctx, args)
            finally:
                guard.active = False
        return int(code or 0), None
    except BaseException as exc:  # every outcome maps to an exit code and a history line
        return report(exc), exc


def _usage_failure(argv: list[str], err: UsageError, started: float) -> int:
    code = report(err)
    base = {"id": secrets.token_hex(6), "cluster": _guess_cluster(argv), "argv": argv}
    _append_history(_start_entry(base))
    _append_history(_end_entry(base, code, err, started))
    return code


def run(args: argparse.Namespace, argv: list[str]) -> int:
    started = time.monotonic()
    try:
        cluster = cluster_name(args)
    except UsageError as err:
        return _usage_failure(argv, err, started)
    base = {"id": secrets.token_hex(6), "cluster": cluster, "argv": argv}
    if args.history:
        _append_history(_start_entry(base))
    with signal_guard() as guard:
        code, failure = _execute(args, cluster, argv, guard)
        if args.history:
            _append_history(_end_entry(base, code, failure, started))
    return code


def main(argv: Sequence[str] | None = None) -> int:
    words = list(sys.argv[1:] if argv is None else argv)
    started = time.monotonic()
    try:
        args = parse_args(words)
    except UsageError as err:
        return _usage_failure(words, err, started)
    except SystemExit as exc:  # --help
        return exit_code_for(exc)
    proc.VERBOSE = bool(getattr(args, "verbose", False))
    return run(args, words)
