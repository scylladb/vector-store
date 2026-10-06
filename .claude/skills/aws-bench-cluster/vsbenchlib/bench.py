# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Benchmarks as detached jobs on the client: datasets, load, index, search, A/B. A job is a
step script (`step NAME -- CMD` lines; node/bench-job.sh) in NODE_JOBS/<id>/steps.sh;
finalize_job() (idempotent, also run by `job wait`) turns it into records and state.
The job machinery lives in bench_jobs.py; its public names are re-exported here."""

from __future__ import annotations

import dataclasses
import datetime
import importlib
import inspect
import json
import math
import re
import shlex
import statistics
import time
from collections.abc import Sequence
from dataclasses import dataclass
from types import ModuleType
from typing import Any

from . import build, config, proc, prom, remote, results  # noqa: F401 -- prom: tests patch bench.prom
from . import state as st
from .bench_jobs import (  # noqa: F401 -- re-exported: bench.<name> is the public API
    CLIENT,
    ERROR_TAIL_LINES,
    FAILURE_HINTS,
    FINAL_STATUSES,
    FINALIZE_RESERVE_S,
    KEYSPACE,
    PHASES,
    STEP_LIB,
    STEP_PHASES,
    TABLE,
    Finalized,
    _ensure_idle,
    _follow,
    _job,
    _start_job,
    _with_job,
    finalize_job,
    follow_budget,
    job_script,
    job_wait,
    options_mismatch,
    step_line,
)
from .proc import PreconditionError, VsbenchError
from .state import State

BENCH_BIN = f"{config.NODE_BENCH_DIR}/vector-search-benchmark"
FETCH_SCRIPT = "fetch-dataset.sh"
BASE_URL = "https://assets.zilliz.com/benchmark"
DEFAULT_INDEX_TIMEOUT_S = 2 * 3600
WAIT_GONE_TIMEOUT_S = 600
STEP_SLACK_S = 600  # a search step's timeout = its duration + this
STEP_OVERHEAD_S = 15  # per tool invocation: query loading + the 2 s start delay
EXPIRY_MARGIN_S = 600
MIN_COUNT_RATIO = 0.99
# bench ab expiry estimate (the TTL check; nothing waits on these):
AB_SOURCES = {"release", "git", "local", "build"}
AB_BUILD_ESTIMATE_S = 900  # a cold local/git cross build (5-15 min); release/build sources need none
AB_SWITCH_OVERHEAD_S = 120  # per build switch besides the index rebuild: upload, restart, settle
REBUILD_S_PER_M_ROWS = 600  # index rebuild after a restart when no build of this dataset is recorded
MIN_REBUILD_S = 60
DEPLOY_NAMES = {"scylla": "scylla", "vector_store": "vs", "bench": "bench", "monitoring": "monitoring"}
SERVICES = ("scylla", "vector_store", "bench")
RESERVED_FLAGS = set("--data-dir --scylla --vector-store --limit --duration --concurrency --bucket".split())
RESERVED_FLAGS |= {"--index", "--keyspace", "--table", "--from"}
STATE_CHANGING = {"drop-table", "drop-index", "build-table", "build-index", "delete-rows"}
_FILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_CQL_PAIR_RE = re.compile(r"\s*'([A-Za-z_][A-Za-z0-9_]*)'\s*:\s*(?:'([^']*)'|([A-Za-z0-9_.+-]+))\s*")


@dataclass(frozen=True)
class LoadOptions:
    dataset: str
    index_options: str | None = None
    rf: int = 1
    concurrency: int = 512
    local_index: bool = False
    resume: bool = False
    index_timeout_s: int = DEFAULT_INDEX_TIMEOUT_S
    timeout_s: int = config.DEFAULT_FOREGROUND_SECONDS


@dataclass(frozen=True)
class IndexOptions:
    index_options: str | None = None
    index_timeout_s: int = DEFAULT_INDEX_TIMEOUT_S
    timeout_s: int = config.DEFAULT_FOREGROUND_SECONDS


@dataclass(frozen=True)
class SearchOptions:
    kind: str = "cql"
    limit: int = 10
    duration_s: int = 60
    warmup_s: int = 30
    concurrency: tuple[int, ...] = (64,)
    repeat: int = 1
    bucket: int | None = None
    label: str | None = None
    timeout_s: int = config.DEFAULT_FOREGROUND_SECONDS
    extra_args: tuple[str, ...] = ()
    comparison_id: str | None = None  # set by ab()
    arm: str | None = None
    first_repeat: int = 1


@dataclass(frozen=True)
class AbOptions(SearchOptions):
    """bucket and extra_args go to every search; timeout_s is each search's foreground budget, at least
    its duration + STEP_SLACK_S (None: exactly that)."""

    a: str = ""  # VS source of arm A (required)
    b: str = ""
    repeat: int = 2  # runs per arm
    timeout_s: int | None = None  # type: ignore[assignment]
    deploy_timeout_s: int = DEFAULT_INDEX_TIMEOUT_S


def _deploy() -> ModuleType:  # imported lazily: only validation and ab need it
    return importlib.import_module(f"{__package__}.deploy")


def catalog() -> dict[str, dict[str, Any]]:
    """datasets.json without its `_about` keys; raises VsbenchError when malformed."""
    path = config.DATASETS_FILE
    try:
        entries = {k: v for k, v in json.loads(path.read_text()).items() if not k.startswith("_")}
    except (OSError, ValueError, AttributeError) as err:
        raise VsbenchError(f"cannot read the dataset catalog {path}: {err}") from err
    for key, entry in entries.items():
        names = [f.get("name", "") for f in entry.get("files") or []] if isinstance(entry, dict) else []
        if not names or not all(_FILE_RE.match(n) for n in names) or not {"dir", "similarity", "rows"} <= set(entry):
            raise VsbenchError(f"invalid catalog entry '{key}' in {path}")
    return entries


def dataset(key: str) -> dict[str, Any]:
    entries = catalog()
    if key not in entries:
        raise VsbenchError(f"unknown dataset '{key}'", f"known: {', '.join(entries)} (vsbench dataset list)")
    return {**entries[key], "key": key}


def data_dir(key: str) -> str:
    return f"{config.NODE_DATASETS_DIR}/{key}"


def dataset_status(cluster: str) -> dict[str, Any]:
    """Datasets on the client: {dir, free_gb, datasets: [{dataset, complete, gb, expected_gb}]}."""
    st.require(cluster)
    command = f"cd {shlex.quote(config.NODE_DATASETS_DIR)} 2>/dev/null || exit 0; df -B1 --output=avail . | tail -n1"
    command += '; for d in */; do k=${d%/}; c=0; [ -e "$k/.complete" ] && c=1; '
    command += 'echo "$k $c $(du -sb -- "$k" | cut -f1)"; done'
    lines, entries = remote.run(cluster, CLIENT, command).stdout.split("\n"), catalog()
    free = int(lines[0]) / 1e9 if lines[0].strip().isdigit() else None
    rows = [p for p in (line.split() for line in lines[1:]) if len(p) == 3 and p[1] in "01" and p[2].isdigit()]
    datasets = [{"dataset": k, "complete": c == "1", "gb": round(int(b) / 1e9, 2)} for k, c, b in rows]
    datasets = [{**d, "expected_gb": entries.get(d["dataset"], {}).get("download_gb")} for d in datasets]
    return {"dir": config.NODE_DATASETS_DIR, "free_gb": round(free, 1) if free else None, "datasets": datasets}


def rack_count(state: State) -> int:
    """Distinct racks of the deployed Scylla nodes: deploy stores {node: "rackN"} (an int is accepted
    too); the number of Scylla nodes when nothing is recorded (deploy puts each node in its own rack)."""
    nodes = len(st.nodes(state, "scylla"))
    racks = ((state.get("deployed") or {}).get("scylla") or {}).get("racks")
    if isinstance(racks, dict) and racks:
        return len({str(rack) for rack in racks.values()})
    if isinstance(racks, int) and not isinstance(racks, bool) and racks > 0:
        return racks
    return nodes


def check_rf(state: State, rf: int) -> None:
    """rf must be 1 or the number of racks, and <= nodes."""
    nodes, racks = len(st.nodes(state, "scylla")), rack_count(state)
    allowed = sorted({1, racks})
    if rf < 1 or rf > nodes or rf not in allowed:
        hint = "use " + " or ".join(f"--rf {v}" for v in allowed) + " (1 or the number of racks)"
        raise PreconditionError(f"--rf {rf} does not fit {nodes} Scylla node(s) in {racks} rack(s)", hint)


def _check_deployed(state: State, components: Sequence[str]) -> None:
    for name in (DEPLOY_NAMES[c] for c in components if not (state.get("deployed") or {}).get(c)):
        raise PreconditionError(f"{name} is not deployed", f"vsbench -c {state.get('cluster')} deploy {name}")


def index_options_cql(text: str | None, similarity: str) -> tuple[str, dict[str, str]]:
    """Parse a CQL map such as "{'similarity_function': 'COSINE', 'quantization': 'I8'}"; returns (canonical
    CQL map, options), adding the dataset's similarity_function when absent."""
    body, hint = (text or "{}").strip(), "use a CQL map with quoted keys, e.g. \"{'similarity_function': 'COSINE'}\""
    items = [_CQL_PAIR_RE.fullmatch(item) for item in filter(str.strip, body[1:-1].split(","))]
    if not (body.startswith("{") and body.endswith("}")) or not all(items):
        raise VsbenchError(f"invalid --index-options {text!r}", hint)
    options = {m.group(1): m.group(2) if m.group(2) is not None else m.group(3) for m in items if m}
    if "similarity_function" not in options:
        options = {"similarity_function": similarity, **options}
    elif options["similarity_function"].upper() != similarity.upper():
        proc.warn(f"similarity_function {options['similarity_function']} is not the ground truth's {similarity}")
    return "{" + ", ".join(f"'{key}': '{value}'" for key, value in options.items()) + "}", options


def _check_search_args(opts: SearchOptions) -> None:  # ranges are checked by the tool (it fails at once)
    conc = list(opts.concurrency)
    if opts.kind not in ("cql", "http") or opts.repeat < 1 or opts.duration_s < 1 or opts.warmup_s < 0:
        raise VsbenchError("invalid search options", "kind cql|http, --repeat >= 1, --duration >= 1s, --warmup >= 0s")
    if not conc or len(set(conc)) < len(conc) or (opts.bucket is not None and not 0 <= opts.bucket <= 8):
        raise VsbenchError(f"invalid --concurrency {conc} or --bucket", "distinct concurrency values; bucket 0-8")
    for arg in (a for a in opts.extra_args if a.split("=", 1)[0] in RESERVED_FLAGS):
        raise VsbenchError(f"{arg} is set by vsbench", "use the vsbench options (--limit, --duration, ...)")


def _check_search_load(state: State, opts: SearchOptions) -> dict[str, Any]:  # combinations that panic the tool
    cluster, load = state.get("cluster"), state.get("load") or {}
    phases, key = load.get("phases") or {}, load.get("dataset")
    _check_deployed(state, SERVICES)
    if not phases.get("table"):
        raise PreconditionError("no dataset is loaded", f"load one: vsbench -c {cluster} bench load <dataset>")
    if st.built_index(state) is None:
        hint = f"finish it: vsbench -c {cluster} bench load {key} --resume (or bench index)"
        raise PreconditionError(f"index {load.get('index')} is not built", hint)
    if load.get("options_ok") is False:
        hint = f"rebuild it: vsbench -c {cluster} bench index --index-options \"{{'similarity_function': ...}}\""
        raise PreconditionError("Vector Store ignored some requested index options", hint)
    if opts.bucket is not None and not load.get("local_index"):
        hint = f"reload: vsbench -c {cluster} bench load {key} --local-index"
        raise PreconditionError("--bucket needs a local index (on a global index the tool panics)", hint)
    if load.get("local_index") and opts.kind == "http":
        raise PreconditionError("search-http does not work with a local index", "use cql with --bucket N")
    if load.get("local_index") and opts.bucket is None:
        raise PreconditionError("a local index needs --bucket N for search-cql", "pass --bucket 0-8 (0 = 50% of rows)")
    return load


def search_seconds(opts: SearchOptions) -> int:
    per_run = opts.duration_s + STEP_OVERHEAD_S + (opts.warmup_s + STEP_OVERHEAD_S if opts.warmup_s else 0)
    return len(list(opts.concurrency)) * opts.repeat * per_run + 30


def _check_expiry(state: State, seconds: int, now: datetime.datetime, detail: str | None = None) -> None:
    """PreconditionError (with an extend hint) when `seconds` of work plus a margin outlive the cluster."""
    expires = state.get("expires_at")
    if expires and now + datetime.timedelta(seconds=seconds + EXPIRY_MARGIN_S) > results.parse_timestamp(expires):
        hint = f"extend it first: vsbench -c {state.get('cluster')} extend --ttl {math.ceil(seconds / 3600) + 1}h"
        needs = f"~{proc.format_duration(seconds)}" + (f" ({detail})" if detail else "")
        raise PreconditionError(f"this needs {needs}; the cluster expires at {expires}", hint)


def _vs_view(cluster: str, state: State) -> dict[str, dict[str, Any]]:
    """deploy.vs_status() ({node: {status, indexes, info}}, maybe under "nodes") as {vs node: view}."""
    raw = _deploy().vs_status(cluster)
    nodes, view = (raw.get("nodes", raw) if isinstance(raw, dict) else {}), {}
    for name in (n["name"] for n in st.nodes(state, "vs")):
        info = nodes.get(name) if isinstance(nodes.get(name), dict) else {}
        engine = (info.get("info") if isinstance(info.get("info"), dict) else info).get("engine")
        view[name] = {"status": info.get("status"), "indexes": info.get("indexes") or [], "engine": engine}
    return view


def check_serving(view: dict[str, dict[str, Any]], load: dict[str, Any], cluster: str) -> dict[str, Any]:
    """All VS nodes and the index SERVING, count >= 99% of rows; returns {engine, options}."""
    keyspace, name, rows = load.get("keyspace", KEYSPACE), load.get("index"), int(load.get("rows") or 0)
    problems, engine, options = [], None, None
    for node, info in view.items():
        entry = next((i for i in info["indexes"] if i.get("keyspace") == keyspace and i.get("index") == name), {})
        if info["status"] != "SERVING":
            problems.append(f"{node} is {info['status'] or 'unreachable'}")
        elif entry.get("status") != "SERVING" or (entry.get("count") or 0) < rows * MIN_COUNT_RATIO:
            state_text = f"{entry.get('status')}, {entry.get('count')} of {rows} rows" if entry else "missing"
            problems.append(f"{node}: index {keyspace}.{name} {state_text}")
        else:
            engine, options = engine or info["engine"], options or entry.get("options")
    if problems or not view:
        hint = f"wait: vsbench -c {cluster} wait-serving (details: vsbench -c {cluster} status)"
        raise PreconditionError("Vector Store is not ready: " + ("; ".join(problems) or "no vs nodes"), hint)
    return {"engine": engine, "options": options}


def validate_search(state: State, opts: SearchOptions) -> dict[str, Any]:
    """PreconditionError for searches that would panic or mismeasure; returns the live {engine, options}."""
    _check_search_args(opts)
    load = _check_search_load(state, opts)
    _check_expiry(state, search_seconds(opts), proc.utcnow())
    if opts.duration_s < results.MIN_WINDOW_S:
        proc.warn(f"--duration {opts.duration_s}s < {results.MIN_WINDOW_S}s: server metrics will be null")
    return check_serving(_vs_view(state["cluster"], state), load, state["cluster"])


def validate(cluster: str, opts: SearchOptions) -> dict[str, Any]:
    """`bench validate`: validate_search against the cluster's state and live index."""
    return validate_search(st.require(cluster), opts)


def _addrs(state: State, role: str) -> list[str]:
    port = config.PORT_CQL if role == "scylla" else config.PORT_VS
    addrs = [f"{n['private_ip']}:{port}" for n in st.nodes(state, role) if n.get("private_ip")]
    if not addrs:
        raise VsbenchError(f"no {role} node with a private IP in the state", "check: vsbench status --refresh")
    return addrs


def fetch_step(entry: dict[str, Any]) -> str:
    files = "\n".join(f"{f['name']} {'-' if f.get('bytes') is None else f['bytes']}" for f in entry["files"])
    env = [f"DATASET_DIR={data_dir(entry['key'])}", f"BASE_URL={BASE_URL}/{entry['dir']}", f"FILES={files}"]
    return step_line("fetch", ["env", *env, "bash", f"{config.NODE_SCRIPTS}/{FETCH_SCRIPT}"])


def _build_index_step(state: State, name: str, options: str, local: bool, timeout_s: int) -> str:
    vs = [w for a in _addrs(state, "vs") for w in ("--vector-store", a)]
    argv = [BENCH_BIN, "build-index", "--scylla", _addrs(state, "scylla")[0], *vs, "--options", options]
    return step_line("build-index", argv + ["--index", name] + (["--local"] if local else []), timeout_s)


def _drop_index_steps(state: State, old: str) -> list[str]:
    urls = [f"http://{addr}" for addr in _addrs(state, "vs")]
    drop = [BENCH_BIN, "drop-index", "--scylla", _addrs(state, "scylla")[0], "--index", old]
    wait = ["wait_index_gone", KEYSPACE, old, str(WAIT_GONE_TIMEOUT_S), *urls]
    return [step_line("drop-index", drop), step_line("wait-gone", wait)]


def load_steps(state: State, entry: dict[str, Any], opts: LoadOptions, plan: dict[str, Any]) -> list[str]:
    """Steps of a load job; plan = {todo (phases), index, options (CQL map), old_index (dropped first)}."""
    todo, directory, scylla, bench = plan["todo"], data_dir(entry["key"]), _addrs(state, "scylla")[0], BENCH_BIN
    steps = [fetch_step(entry)] if "fetch" in todo else []
    if "buckets" in todo and opts.local_index:
        steps.append(step_line("buckets", [bench, "build-buckets", "--data-dir", directory]))
    elif "buckets" in todo:  # build-table would read a stale buckets.bin
        steps.append(step_line("clear-buckets", ["rm", "-f", "--", f"{directory}/buckets.bin"]))
    if "table" in todo:
        table = ["--data-dir", directory, "--scylla", scylla, "--rf", str(opts.rf)]
        steps.append(step_line("drop-table", [bench, "drop-table", "--scylla", scylla]))
        steps.append(step_line("build-table", [bench, "build-table", *table, "--concurrency", str(opts.concurrency)]))
    elif plan.get("old_index"):
        steps += _drop_index_steps(state, plan["old_index"])
    if "index" in todo:
        steps.append(_build_index_step(state, plan["index"], plan["options"], opts.local_index, opts.index_timeout_s))
    return steps


def search_plan(opts: SearchOptions) -> list[dict[str, Any]]:
    plan, repeats = [], range(opts.first_repeat, opts.first_repeat + opts.repeat)
    for conc, rep in ((c, r) for c in opts.concurrency for r in repeats):
        warmup = f"warmup-c{conc}-r{rep}" if opts.warmup_s > 0 else None
        plan.append({"name": f"search-c{conc}-r{rep}", "warmup": warmup, "concurrency": conc, "repeat_index": rep})
    return plan


def search_steps(state: State, opts: SearchOptions, load: dict[str, Any], plan: Sequence[dict[str, Any]]) -> list[str]:
    """A warmup step (same arguments, --duration <warmup>, discarded) before every measured step."""
    base = [BENCH_BIN, f"search-{opts.kind}", "--data-dir", load.get("data_dir") or data_dir(load["dataset"])]
    http = [*(w for a in _addrs(state, "vs") for w in ("--vector-store", a)), "--index", load["index"]]
    base += ["--scylla", _addrs(state, "scylla")[0]] if opts.kind == "cql" else http
    tail = (["--bucket", str(opts.bucket)] if opts.bucket is not None else []) + list(opts.extra_args)
    steps = []
    for item in plan:
        for name, seconds in ((item["warmup"], opts.warmup_s), (item["name"], opts.duration_s)):
            run = ["--limit", str(opts.limit), "--duration", f"{seconds}s", "--concurrency", str(item["concurrency"])]
            steps += [step_line(name, [*base, *run, *tail], seconds + STEP_SLACK_S)] if name else []
    return steps


def new_index_name(now: datetime.datetime, avoid: str | None = None) -> str:
    name = f"vsb_idx_{now.astimezone(datetime.timezone.utc):%Y%m%d%H%M%S}"
    return f"{name}_2" if name == avoid else name


def fetch(cluster: str, key: str, timeout_s: int) -> dict[str, Any]:
    started, entry = time.monotonic(), dataset(key)
    _ensure_idle(cluster)
    job_id = remote.new_job_id("fetch")
    _start_job(cluster, job_id, "fetch", [fetch_step(entry)], {"dataset": key}, scripts=(FETCH_SCRIPT,))
    job_wait(cluster, job_id, timeout_s, started=started)
    return {"job_id": job_id, "dataset": key, "dir": data_dir(key), "status": "complete"}


def load_todo(previous: dict[str, Any] | None, opts: LoadOptions) -> list[str]:
    """Phases to run: all, or (--resume) from the first incomplete one."""
    if not opts.resume:
        return list(PHASES)
    if not previous:
        raise PreconditionError("nothing to resume: no load is recorded", f"vsbench bench load {opts.dataset}")
    for key, wanted in (("dataset", opts.dataset), ("rf", opts.rf), ("local_index", opts.local_index)):
        if previous.get(key) != wanted:
            message = f"cannot resume: the recorded load has {key}={previous.get(key)!r}, not {wanted!r}"
            raise PreconditionError(message, "run it without --resume to start over")
    done = previous.get("phases") or {}
    return list(PHASES[next((i for i, phase in enumerate(PHASES) if not done.get(phase)), len(PHASES)) :])


def planned_load(previous: dict[str, Any], entry: dict[str, Any], opts: LoadOptions, plan: dict[str, Any]) -> dict:
    """state.load while a load job runs: phases of skipped steps kept, the rest cleared."""
    todo, kept = plan["todo"], (previous.get("phases") or {}) if opts.resume else {}
    run_id = previous.get("run_id") if opts.resume and "table" not in todo else plan["job_id"]
    load = {"dataset": entry["key"], "data_dir": data_dir(entry["key"]), "keyspace": KEYSPACE, "table": TABLE}
    load.update(index=plan["index"], index_options=None, index_options_cql=plan["options"], options_ok=None)
    load.update(similarity=entry["similarity"], rf=opts.rf, local_index=opts.local_index, rows=entry["rows"])
    load.update(loaded_at=None, run_id=run_id, pending_job=plan["job_id"])
    return {**load, "phases": {phase: None if phase in todo else kept.get(phase) for phase in PHASES}}


def load(cluster: str, opts: LoadOptions) -> dict[str, Any]:
    """Load a dataset and build an index (job kind "load"); --resume skips completed phases.
    opts.timeout_s is the foreground budget of the whole command (StillRunning when the job outlives it)."""
    started, state, entry = time.monotonic(), st.require(cluster), dataset(opts.dataset)
    check_rf(state, opts.rf)  # before anything is dropped
    if not 1 <= opts.concurrency <= 1_000_000 or opts.index_timeout_s < 1:
        raise VsbenchError("invalid --concurrency or --index-timeout", "concurrency 1-1000000, timeout >= 1s")
    _check_deployed(state, SERVICES)
    previous = dict(_ensure_idle(cluster).get("load") or {})
    todo = load_todo(previous, opts)
    if not todo:
        proc.log(f"the load of {opts.dataset} is already complete (index {previous.get('index')})")
        return {"job_id": None, "status": "complete", "load": previous, "records": []}
    text = opts.index_options or (previous.get("index_options_cql") if opts.resume else None)
    options, requested = index_options_cql(text, entry["similarity"])
    job_id, name = remote.new_job_id("load"), new_index_name(proc.utcnow(), previous.get("index"))
    plan = {"job_id": job_id, "todo": todo, "index": name, "options": options}
    plan["old_index"] = previous.get("index") if "table" not in todo else None
    params = {**dataclasses.asdict(opts), **plan, "index_options": options, "requested": requested}
    new_load, scripts = planned_load(previous, entry, opts, plan), (FETCH_SCRIPT,) if "fetch" in todo else ()
    steps = load_steps(st.require(cluster), entry, opts, plan)
    _start_job(cluster, job_id, "load", steps, params, change=lambda s: {**s, "load": new_load}, scripts=scripts)
    records = job_wait(cluster, job_id, opts.timeout_s, started=started)
    return {"job_id": job_id, "status": "complete", "load": st.require(cluster).get("load"), "records": records}


def index(cluster: str, opts: IndexOptions) -> dict[str, Any]:
    """Drop the current index, wait until VS forgets it, build a new one (job kind "index")."""
    started = time.monotonic()
    _check_deployed(st.require(cluster), SERVICES)
    state = _ensure_idle(cluster)
    previous = dict(state.get("load") or {})
    if not (previous.get("phases") or {}).get("table"):
        raise PreconditionError("no dataset is loaded", f"load one: vsbench -c {cluster} bench load <dataset>")
    if opts.index_timeout_s < 1:
        raise VsbenchError("invalid --index-timeout", "use a duration such as 2h")
    similarity = dataset(previous["dataset"])["similarity"]
    options, requested = index_options_cql(opts.index_options or previous.get("index_options_cql"), similarity)
    job_id, old = remote.new_job_id("index"), previous.get("index")
    name = new_index_name(proc.utcnow(), old)
    params = {**dataclasses.asdict(opts), "old_index": old, "index": name, "todo": ["index"]}
    params.update(index_options=options, requested=requested)
    new_load = {**previous, "index": name, "index_options": None, "index_options_cql": options, "options_ok": None}
    new_load.update(phases={**(previous.get("phases") or {}), "index": None}, pending_job=job_id)
    build = _build_index_step(state, name, options, bool(previous.get("local_index")), opts.index_timeout_s)
    steps = (_drop_index_steps(state, old) if old else []) + [build]
    _start_job(cluster, job_id, "index", steps, params, change=lambda s: {**s, "load": new_load})
    return {"job_id": job_id, "index": name, "records": job_wait(cluster, job_id, opts.timeout_s, started=started)}


def search(cluster: str, opts: SearchOptions) -> list[dict[str, Any]]:
    """Warmup + measured run per concurrency x repeat (job kind "search"); returns the records."""
    started, state = time.monotonic(), _ensure_idle(cluster)
    live, load, plan = validate_search(state, opts), state["load"], search_plan(opts)
    params = {**dataclasses.asdict(opts), "steps": plan, "index": load["index"], "vs_engine": live["engine"]}
    params.update(concurrency=list(opts.concurrency), extra_args=list(opts.extra_args), index_options=live["options"])
    params["snapshot"] = {"deployed": state.get("deployed"), "load": load}
    job_id = remote.new_job_id("search")
    _start_job(cluster, job_id, "search", search_steps(state, opts, load, plan), params)
    proc.log(f"{len(plan)} measured run(s), about {proc.format_duration(search_seconds(opts))}")
    return job_wait(cluster, job_id, opts.timeout_s, started=started)


def ab_order(repeat: int) -> list[str]:
    return [arm for i in range(repeat) for arm in (("A", "B") if i % 2 == 0 else ("B", "A"))]


def _deploy_arm(cluster: str, arm: str, pin: str, comparison_id: str, opts: AbOptions) -> list[str]:
    """deploy_vs, then wait_serving (all VS SERVING, loaded index >= 99%); returns new index-build run ids."""
    before, deploy, kwargs = {r.get("run_id") for r in results.load_records(cluster)}, _deploy(), {}
    if "record_extra" in inspect.signature(deploy.deploy_vs).parameters:
        kwargs["record_extra"] = {"comparison_id": comparison_id, "arm": arm, "label": opts.label}
    deploy.deploy_vs(cluster, pin, {}, [], False, opts.deploy_timeout_s, **kwargs)
    deploy.wait_serving(cluster, opts.deploy_timeout_s)
    fresh = [r for r in results.load_records(cluster) if r.get("run_id") not in before]
    return [r["run_id"] for r in fresh if r.get("kind") == "index-build"]


def ab_switches(order: Sequence[str], build_ids: dict[str, Any] | None = None, deployed: Any = None) -> list[bool]:
    """Per position of `order`: does deploying its arm restart Vector Store on another build? An arm
    whose build id is unknown (not built yet) counts as a switch whenever the arm changes."""
    known, flags, current = build_ids or {}, [], deployed
    for position, arm in enumerate(order):
        build_id = known.get(arm)
        flags.append(build_id != current if build_id else position == 0 or arm != order[position - 1])
        current = build_id
    return flags


def rebuild_seconds(cluster: str, load: dict[str, Any]) -> int:
    """Index rebuild after a VS restart: the median recorded build of this load (else of its dataset),
    else REBUILD_S_PER_M_ROWS per million rows."""
    try:
        records = results.load_records(cluster)
    except VsbenchError:
        records = []
    kinds, dataset_key = ("index-build", "load"), load.get("dataset")
    same = [r for r in records if r.get("kind") in kinds and r.get("dataset") == dataset_key and not r.get("exit")]
    mine = [r for r in same if r.get("load_run_id") == load.get("run_id")]
    seconds = [s for s in map(results.index_build_seconds, mine or same) if s]
    if seconds:
        return math.ceil(statistics.median(seconds))
    return max(MIN_REBUILD_S, math.ceil(int(load.get("rows") or 0) / 1e6 * REBUILD_S_PER_M_ROWS))


def ab_estimate(
    cluster: str, state: State, base: SearchOptions, switches: Sequence[bool], builds_s: int
) -> tuple[int, str]:
    """(seconds, breakdown) of A/B runs: builds, deploy + index rebuild per switch, one search per position."""
    per_switch = AB_SWITCH_OVERHEAD_S + rebuild_seconds(cluster, state.get("load") or {})
    per_search, count, fmt = search_seconds(base), sum(1 for s in switches if s), proc.format_duration
    total = builds_s + count * per_switch + len(switches) * per_search
    parts = [f"builds {fmt(builds_s)}"] if builds_s else []
    parts += [f"{count} build switch(es) x {fmt(per_switch)} (deploy + index rebuild)"]
    return total, ", ".join(parts + [f"{len(switches)} search(es) x {fmt(per_search)}"])


def _ab_check_expiry(cluster: str, state: State, base: SearchOptions, switches: list[bool], builds_s: int) -> None:
    seconds, detail = ab_estimate(cluster, state, base, switches, builds_s)
    _check_expiry(state, seconds, proc.utcnow(), detail)


def _ab_build_arms(specs: dict[str, build.SourceSpec]) -> dict[str, dict[str, Any]]:
    arms = {}
    for arm, spec in specs.items():  # resolve once: every run of an arm uses the same binary
        record = build.build(spec)
        arms[arm] = {"source": str(spec), "pin": record.get("pin") or f"build:{record['build_id']}"}
        arms[arm].update(build_id=record.get("build_id"), version=record.get("version"))
    return arms


def _ab_check_remaining(cluster: str, base: SearchOptions, order: list[str], position: int, run: dict) -> None:
    """Before each deploy: the remaining runs must fit the cluster's current expiry (extend runs unlocked)."""
    state, arms = st.require(cluster), run["arms"]
    deployed = ((state.get("deployed") or {}).get("vector_store") or {}).get("build_id")
    switches = ab_switches(order[position - 1 :], {a: arms[a]["build_id"] for a in arms}, deployed)
    try:
        _ab_check_expiry(cluster, state, base, switches, 0)
    except PreconditionError as err:
        done = f"runs so far: vsbench -c {cluster} results compare {run['comparison_id']}"
        message = f"bench ab stopped before run {position}/{len(order)}: {err}"
        raise PreconditionError(message, f"{err.hint}; {done}") from err


def _ab_deployed(cluster: str, comparison_id: str, order: list[str], arms: dict[str, Any]) -> dict[str, Any]:
    """Log which arm stays deployed (the last of the order) and how to switch to the other one."""
    last = order[-1]
    other = "B" if last == "A" else "A"
    proc.log(f"ab {comparison_id} done; arm {last} ({arms[last]['pin']}) stays deployed")
    proc.log(f"arm {other} instead: vsbench -c {cluster} deploy vs --source {arms[other]['pin']}")
    proc.log(f"compare with: vsbench -c {cluster} results compare {comparison_id}")
    return {"arm": last, **{key: arms[last][key] for key in ("source", "pin", "build_id")}}


def ab(cluster: str, opts: AbOptions) -> dict[str, Any]:
    """A/B two VS sources in ABBA order; per run: deploy_vs, index rebuild, warmup + search.
    opts.bucket/extra_args go to every search; the last arm of the order stays deployed."""
    base = dataclasses.replace(opts, repeat=1, concurrency=tuple(opts.concurrency))
    if opts.repeat < 1 or not opts.a or not opts.b or (opts.timeout_s is not None and opts.timeout_s < 1):
        raise VsbenchError("bench ab needs --a, --b, --repeat >= 1 and --timeout >= 1s", "e.g. --a release:latest")
    specs = {arm: build.parse_source(source, AB_SOURCES) for arm, source in (("A", opts.a), ("B", opts.b))}
    _check_search_args(base)
    state = _ensure_idle(cluster)
    _check_search_load(state, base)
    order = ab_order(opts.repeat)
    builds_s = AB_BUILD_ESTIMATE_S * len({str(s) for s in specs.values() if s.kind in ("git", "local")})
    _ab_check_expiry(cluster, state, base, ab_switches(order), builds_s)
    run = {"comparison_id": results.new_run_id("ab"), "arms": _ab_build_arms(specs)}
    comparison_id, arms, records, builds = run["comparison_id"], run["arms"], [], []
    per_search = max(opts.timeout_s or 0, search_seconds(base) + STEP_SLACK_S)
    for position, arm in enumerate(order, start=1):
        _ab_check_remaining(cluster, base, order, position, run)
        proc.log(f"ab {comparison_id}: run {position}/{len(order)}, arm {arm} = {arms[arm]['pin']}")
        builds += [{"arm": arm, "run_id": r} for r in _deploy_arm(cluster, arm, arms[arm]["pin"], comparison_id, opts)]
        changes = {"comparison_id": comparison_id, "arm": arm, "first_repeat": order[:position].count(arm)}
        records += search(cluster, dataclasses.replace(base, **changes, timeout_s=per_search))
    deployed = _ab_deployed(cluster, comparison_id, order, arms)
    result = {"comparison_id": comparison_id, "order": order, "arms": arms, "deployed": deployed}
    return {**result, "records": records, "index_builds": builds}


def _options_drift(recorded: Any, current: Any) -> list[str]:
    if not isinstance(recorded, dict) or not isinstance(current, dict):
        return []
    drift = [f"{key} {old} -> {new}" for key, (old, new) in options_mismatch(recorded, current).items()]
    drift += [f"{key} {recorded[key]} -> (none)" for key in recorded if key not in current]
    return drift + [f"{key} (none) -> {current[key]}" for key in current if key not in recorded]


def rerun_drift(record: dict[str, Any], load: dict[str, Any]) -> list[str]:
    """How the current load differs from a recorded search's: dataset, load run, effective index options."""
    pairs = (("dataset", "dataset", "dataset"), ("load run", "load_run_id", "run_id"))
    drift = [f"{what} {record.get(a)} -> {load.get(b)}" for what, a, b in pairs if record.get(a) != load.get(b)]
    options = _options_drift((record.get("index") or {}).get("options"), load.get("index_options"))
    return drift + ([f"index options {', '.join(options)}"] if options else [])


def rerun(cluster: str, run_id: str, timeout_s: int) -> list[dict[str, Any]]:
    """Replay a recorded search's parameters on the current load and deployment (new series); warns
    when the dataset, load or index options differ from the recorded run's."""
    record = next((r for r in results.load_records(cluster) if r.get("run_id") == run_id), None)
    if record is None or not str(record.get("kind")).startswith("search-"):
        raise VsbenchError(f"no search run '{run_id}'", f"list runs with: vsbench -c {cluster} results")
    p, kind = record.get("params") or {}, record["kind"].removeprefix("search-")
    opts = SearchOptions(kind, p["limit"], p["duration_s"], p.get("warmup_s") or 0, (p["concurrency"],))
    extra = {"label": record.get("label"), "timeout_s": timeout_s, "extra_args": tuple(p.get("extra_args") or ())}
    extra["bucket"] = p.get("bucket")
    drift = rerun_drift(record, st.require(cluster).get("load") or {})
    if drift:
        proc.warn(
            f"{run_id} ran on a different setup ({'; '.join(drift)}); the new runs are not a like-for-like repeat"
        )
    proc.log(f"rerunning {run_id}: {kind}, concurrency {p['concurrency']}, {opts.duration_s}s")
    return search(cluster, dataclasses.replace(opts, **extra))


def raw(cluster: str, args: list[str], timeout_s: int) -> int:
    """Run the benchmark tool with `args` as a job; its output goes to stdout. Returns the exit code."""
    if not args:
        raise VsbenchError("bench raw needs the tool's arguments", "e.g. vsbench bench raw -- search-cql --help")
    started = time.monotonic()
    _check_deployed(_ensure_idle(cluster), ("bench",))
    if args[0] in STATE_CHANGING:
        proc.warn(f"{args[0]} changes the data behind state.load and vsbench will not know; prefer bench load/index")
    job_id = remote.new_job_id("raw")
    _start_job(cluster, job_id, "raw", [step_line("raw", [BENCH_BIN, *args])], {"args": list(args)})
    code = _follow(cluster, job_id, follow_budget(timeout_s, started), raw_output=True)
    finalize_job(cluster, job_id)
    return code


_SPEC = "run_id conc=params.concurrency qps=client_metrics.qps mean_ms=client_metrics.mean_ms"
_SPEC += " p50=client_metrics.latency_ms.p50 p99=client_metrics.latency_ms.p99 recall=client_metrics.recall.avg"
_SPEC += " vs_mean_ms=server_metrics.vs_mean_ms vs_p99=server_metrics.vs_latency_ms.p99 exit flags"
SEARCH_COLUMNS = [(h, k or h) for h, _, k in (item.partition("=") for item in _SPEC.split())]


def format_summary(records: Sequence[dict[str, Any]]) -> str:
    """Concise text: one line per load/index-build record, a table of search runs, notes."""
    searches, lines = [r for r in records if str(r.get("kind", "")).startswith("search-")], []
    for r in (r for r in records if r not in searches):
        details, index = r.get("load") or r.get("index_build") or {}, r.get("index") or {}
        took = [f"{k}={details[k]:.1f}" for k in ("upload_s", "build_index_s") if details.get(k) is not None]
        lines.append(f"{r.get('run_id')} {r.get('kind')} exit={r.get('exit')} {' '.join(took)}".rstrip())
        lines.append(f"  index={index.get('name')} options={json.dumps(index.get('options'))}")
        lines += [f"  error: {r['error']}"] if r.get("error") else []
    lines += [results.format_table(searches, SEARCH_COLUMNS)] if searches else []
    notes = {f"server metrics: {r['server_metrics_error']}" for r in searches if r.get("server_metrics_error")}
    notes |= {f"{r['run_id']} failed: {r['error']}" for r in searches if r.get("error")}
    return "\n".join(lines + [f"note: {note}" for note in sorted(notes)])
