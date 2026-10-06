# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""`vsbench bench churn`: a paced stream of single-row inserts (the benchmark tool's
`insert-rows`) as a detached job that runs next to `bench search`, so ingest can be measured
under query load (VECTOR-951). The rows get ids from CHURN_ID_BASE upwards, far above any
dataset's ids, and state.load.churn_rows counts the acknowledged ones, so `status` can compare
the index with the base table and search records made meanwhile carry the `churned` flag."""

from __future__ import annotations

import dataclasses
import time
from dataclasses import dataclass
from typing import Any

from . import config, proc, remote
from . import state as st
from .bench_jobs import CLIENT, _start_job, job_wait, step_line
from .proc import PreconditionError
from .state import State

KIND = "churn"
CHURN_ID_BASE = 1 << 40  # dataset ids are 0..rows-1; churn ids start here
MIN_DURATION_S = 10
STEP_SLACK_S = 120  # the step's timeout beyond its duration: connection setup and the last acks
BENCH_BIN = f"{config.NODE_BENCH_DIR}/vector-search-benchmark"
NO_INSERT_ROWS_HINT = (
    "the deployed benchmark build has no insert-rows (VECTOR-1030, scylladb/vector-store#626): "
    "vsbench deploy bench --source git:<ref with it> or --source local"
)


@dataclass(frozen=True)
class ChurnOptions:
    rate: int  # rows per second; 0 = as fast as the concurrency allows
    duration_s: int = 180
    concurrency: int = 64
    label: str | None = None
    timeout_s: int = config.DEFAULT_FOREGROUND_SECONDS
    extra_args: tuple[str, ...] = ()


def running_churn(state: State) -> str | None:
    """The id of a churn job still marked running, or None."""
    jobs = state.get("jobs") or {}
    return next((i for i, j in jobs.items() if j.get("kind") == KIND and j.get("status") == "running"), None)


def _dimension(load: dict[str, Any]) -> int:
    """The vector dimension: from the effective index options, else from the dataset catalog."""
    dimensions = (load.get("index_options") or {}).get("dimensions")
    if isinstance(dimensions, int) and dimensions > 0:
        return dimensions
    from . import bench  # the catalog lives in bench.py, which re-exports this module

    return int(bench.dataset(load["dataset"])["dim"])


def validate_churn(cluster: str, state: State, opts: ChurnOptions) -> dict[str, Any]:
    """The checks before anything runs; returns the plan {scylla, dimension, start_id}."""
    for component, name in (("scylla", "scylla"), ("bench", "bench")):
        if not (state.get("deployed") or {}).get(component):
            raise PreconditionError(f"{name} is not deployed", f"vsbench -c {cluster} deploy {name}")
    load = state.get("load") or {}
    if not load.get("index") or not all((load.get("phases") or {}).values()):
        raise PreconditionError("no complete load to insert into", f"vsbench -c {cluster} bench load <dataset>")
    if opts.rate < 0 or opts.duration_s < MIN_DURATION_S or opts.concurrency < 1:
        raise PreconditionError(f"churn needs --rate >= 0, --duration >= {MIN_DURATION_S}s and --concurrency >= 1")
    if (busy := running_churn(state)) is not None:
        raise PreconditionError(f"churn job {busy} is still running", f"vsbench -c {cluster} job wait {busy}")
    scylla = next((n["private_ip"] for n in st.nodes(state, "scylla") if n.get("private_ip")), None)
    if scylla is None:
        raise PreconditionError("no Scylla node with a private IP in the state", "check: vsbench status --refresh")
    plan = {"scylla": f"{scylla}:{config.PORT_CQL}", "dimension": _dimension(load)}
    probe = remote.run(cluster, CLIENT, f"{BENCH_BIN} insert-rows --help", check=False, timeout=60)
    if probe.returncode != 0:
        raise PreconditionError("the deployed benchmark tool cannot run insert-rows", NO_INSERT_ROWS_HINT)
    return {**plan, "start_id": CHURN_ID_BASE + int(load.get("churn_rows") or 0)}


def churn(cluster: str, opts: ChurnOptions) -> list[dict[str, Any]]:
    """Start the insert stream as a job of kind "churn" and follow it; returns its records. The
    command takes no cluster lock and the other bench commands ignore a running churn job, so a
    search can run alongside. opts.timeout_s is the foreground budget (StillRunning after it)."""
    started, state = time.monotonic(), st.require(cluster)
    plan, load = validate_churn(cluster, state, opts), state["load"]
    argv = [BENCH_BIN, "insert-rows", "--scylla", plan["scylla"], "--dimension", str(plan["dimension"])]
    argv += ["--start-id", str(plan["start_id"]), "--rate", str(opts.rate), "--duration", f"{opts.duration_s}s"]
    argv += ["--concurrency", str(opts.concurrency), *opts.extra_args]
    params = {**dataclasses.asdict(opts), "extra_args": list(opts.extra_args), **plan, "index": load["index"]}
    params["snapshot"] = {"deployed": state.get("deployed"), "load": load}
    job_id = remote.new_job_id(KIND)
    _start_job(cluster, job_id, KIND, [step_line(KIND, argv, opts.duration_s + STEP_SLACK_S)], params)
    rate = f"{opts.rate} rows/s" if opts.rate else f"uncapped, {opts.concurrency} in flight"
    proc.log(f"inserting for {proc.format_duration(opts.duration_s)} ({rate}); bench search may run alongside")
    return job_wait(cluster, job_id, opts.timeout_s, started=started)
