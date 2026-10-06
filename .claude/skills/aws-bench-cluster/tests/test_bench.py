# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.bench, datasets.json, node/bench-job.sh and node/fetch-dataset.sh.

No network, AWS or ssh: remote/prom/deploy/build are mocked; the node scripts run
locally (curl only reads file:// URLs).
"""

from __future__ import annotations

import copy
import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import bench, config, remote, results  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import PreconditionError, StillRunning, VsbenchError  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
NODE = Path(__file__).resolve().parent.parent / "node"
INDEX = "vsb_idx_20261006100000"
JOB = "20261006T100318Z-search-ab12"
OPTIONS_CQL = "{'similarity_function': 'COSINE'}"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def node(name: str, role: str, index: int, ip: str) -> dict[str, Any]:
    types = {"scylla": "i8g.2xlarge", "vs": "r8g.4xlarge", "client": "r8g.2xlarge"}
    entry = {"name": name, "role": role, "index": index, "instance_id": f"i-{name.replace('-', '')}"}
    return {**entry, "instance_type": types[role], "private_ip": ip, "public_ip": "3.3.3.3"}


def make_state(scylla: int = 1, vs: int = 1, loaded: bool = True, **extra: Any) -> dict[str, Any]:
    nodes = [node(f"scylla-{i}", "scylla", i, f"10.0.1.{10 + i}") for i in range(scylla)]
    nodes += [node(f"vs-{i}", "vs", i, f"10.0.2.{10 + i}") for i in range(vs)]
    nodes.append(node("client", "client", 0, "10.0.3.10"))
    deployed = {"scylla": {"image": "scylladb/scylla@sha256:aa", "version": "2026.2.0"}, "bench": {"build_id": "b-1"}}
    deployed["vector_store"] = {"build_id": "1.11.0-1234abcd", "version": "1.11.0", "env": {"RUST_LOG": "info"}}
    deployed["monitoring"] = {"version": "4.16.1"}
    phases = dict.fromkeys(bench.PHASES, "2026-10-06T10:00:00Z")
    load = {"dataset": "cohere-100k", "data_dir": "/var/lib/vsbench/datasets/cohere-100k", "keyspace": bench.KEYSPACE}
    load.update(table=bench.TABLE, index=INDEX, index_options={"similarity_function": "COSINE"}, rows=100000, rf=1)
    load.update(index_options_cql=OPTIONS_CQL, similarity="COSINE", local_index=False, phases=phases, run_id="L1")
    load.update(pending_job=None, options_ok=True, loaded_at="2026-10-06T10:00:00Z")
    state = {"schema": 1, "cluster": "t", "region": "us-east-1", "az": "us-east-1b", "nodes": nodes}
    state.update(deployed=deployed, load=load if loaded else None, jobs={}, expires_at="2099-01-01T00:00:00Z")
    return {**state, **extra}


def vs_status(state: dict[str, Any], status: str = "SERVING", count: int = 100000, index: str = INDEX) -> dict:
    entry = {"keyspace": bench.KEYSPACE, "index": index, "status": "SERVING", "count": count, "build_progress": 100}
    entry["options"] = {"similarity_function": "COSINE", "maximum_node_connections": 16}
    info = {"status": status, "info": {"engine": "usearch-2.22.0", "version": "1.11.0"}, "indexes": [entry]}
    return {n["name"]: info for n in st.nodes(state, "vs")}


def section(name: str, text: str, code: int | None = 0, begin: str = "2026-10-06T10:04:03Z") -> str:
    end = "" if code is None else f"=== VSBENCH STEP END {name} {code} 2026-10-06T10:05:11Z\n"
    return f"=== VSBENCH STEP BEGIN {name} {begin}\n{text.rstrip()}\n{end}"


def progress(code: int | None, running: bool = False) -> remote.JobProgress:
    return remote.JobProgress("", 0, code, None if code is None else "2026-10-06T10:06:00Z", running=running)


class HomeTestCase(unittest.TestCase):
    """A private VSBENCH_HOME with a saved cluster state; remote calls are mocked per test."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="vsbench-bench-test-")
        patcher = mock.patch.dict(os.environ, {"VSBENCH_HOME": self.tmp})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.warn = self.patch(bench.proc, "warn")
        self.patch(bench.proc, "log")
        self.state = make_state()
        st.save("t", self.state)

    def patch(self, target: Any, name: str, **kw: Any) -> mock.MagicMock:
        patcher = mock.patch.object(target, name, **kw)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def save(self, state: dict[str, Any]) -> None:
        self.state = state
        st.save("t", state)

    def add_job(self, job_id: str, kind: str, params: dict[str, Any], **extra: Any) -> None:
        job = {"kind": kind, "node": "client", "started_at": "2026-10-06T10:03:18Z", "params": params}
        self.save(bench._with_job(st.load("t"), job_id, {**job, "status": "running", **extra}))

    def fake_deploy(self, status: dict[str, Any] | None = None) -> mock.MagicMock:
        fake = mock.MagicMock()
        fake.vs_status.return_value = status if status is not None else vs_status(self.state)
        self.patch(bench, "_deploy", return_value=fake)
        return fake


class CatalogTest(unittest.TestCase):
    def test_catalog_has_the_vdbb_datasets_with_verified_sizes(self) -> None:
        entries = bench.catalog()
        self.assertEqual(set(entries), {"cohere-100k", "cohere-1m", "cohere-10m", "openai-50k", "openai-500k"})
        self.assertEqual([k for k, e in entries.items() if e.get("default")], ["cohere-1m"])
        self.assertEqual([k for k, e in entries.items() if e.get("smoke")], ["cohere-100k"])
        for key, entry in entries.items():
            names = [f["name"] for f in entry["files"]]
            self.assertIn("test.parquet", names, key)
            self.assertIn("neighbors.parquet", names, key)
            self.assertFalse(any("shuffle" in n for n in names), key)  # the tool would load both variants
            self.assertTrue(all(isinstance(f["bytes"], int) and f["bytes"] > 0 for f in entry["files"]), key)
            self.assertAlmostEqual(sum(f["bytes"] for f in entry["files"]) / 1e9, entry["download_gb"], delta=0.01)
        trains = [f["name"] for f in entries["cohere-10m"]["files"] if "train" in f["name"]]
        self.assertEqual(trains, [f"train-{i:02d}-of-10.parquet" for i in range(10)])
        self.assertEqual(entries["cohere-10m"]["gt_width"], 10000)

    def test_unknown_dataset(self) -> None:
        with self.assertRaises(VsbenchError) as ctx:
            bench.dataset("sift-1m")
        self.assertIn("cohere-1m", ctx.exception.hint or "")

    def test_malformed_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "datasets.json"
            bad.write_text(
                json.dumps({"x": {"dir": "d", "similarity": "COSINE", "rows": 1, "files": [{"name": "../a"}]}})
            )
            with mock.patch.object(config, "DATASETS_FILE", bad), self.assertRaises(VsbenchError):
                bench.catalog()


class OptionsTest(unittest.TestCase):
    def test_default_and_added_similarity(self) -> None:
        self.assertEqual(bench.index_options_cql(None, "COSINE"), (OPTIONS_CQL, {"similarity_function": "COSINE"}))
        cql, options = bench.index_options_cql("{'quantization': 'I8', 'maximum_node_connections': 32}", "COSINE")
        self.assertEqual(
            cql, "{'similarity_function': 'COSINE', 'quantization': 'I8', 'maximum_node_connections': '32'}"
        )
        self.assertEqual(options["maximum_node_connections"], "32")

    def test_invalid_options(self) -> None:
        for text in ("similarity_function=COSINE", "{'a': 'b'; DROP}", "{a: 'b'}", '{"a": "b"}'):
            with self.subTest(text=text), self.assertRaises(VsbenchError):
                bench.index_options_cql(text, "COSINE")

    def test_other_similarity_warns(self) -> None:
        with mock.patch.object(bench.proc, "warn") as warn:
            bench.index_options_cql("{'similarity_function': 'euclidean'}", "COSINE")
        warn.assert_called_once()

    def test_mismatch(self) -> None:
        requested = {"similarity_function": "cosine", "maximum_node_connections": "32", "oversampling": "2.0"}
        self.assertEqual(
            bench.options_mismatch(requested, {"similarity_function": "COSINE", "maximum_node_connections": 32}), {}
        )
        got = bench.options_mismatch(requested, {"similarity_function": "EUCLIDEAN", "maximum_node_connections": 16})
        self.assertEqual(got, {"similarity_function": ["cosine", "EUCLIDEAN"], "maximum_node_connections": ["32", 16]})


class RfTest(unittest.TestCase):
    def test_rf_matrix(self) -> None:
        cases = [(1, 1, True), (1, 3, False), (3, 1, True), (3, 3, True), (3, 2, False), (3, 4, False), (3, 0, False)]
        for nodes, rf, ok in cases:
            with self.subTest(nodes=nodes, rf=rf):
                if ok:
                    bench.check_rf(make_state(scylla=nodes), rf)
                else:
                    self.assertRaises(PreconditionError, bench.check_rf, make_state(scylla=nodes), rf)

    def test_racks_from_deploy(self) -> None:  # deploy_scylla stores {node: "rackN"}: count distinct racks
        state = make_state(scylla=4)
        state["deployed"]["scylla"]["racks"] = {f"scylla-{i}": f"rack{i // 2 + 1}" for i in range(4)}
        bench.check_rf(state, 2)
        self.assertRaises(PreconditionError, bench.check_rf, state, 4)
        state["deployed"]["scylla"]["racks"] = 2  # a plain count is accepted too
        bench.check_rf(state, 2)


class ScriptTest(unittest.TestCase):
    def test_search_script_golden(self) -> None:
        state = make_state()
        opts = bench.SearchOptions(kind="cql", concurrency=(64, 128), duration_s=60, warmup_s=30)
        steps = bench.search_steps(state, opts, state["load"], bench.search_plan(opts))
        b, data = config.NODE_BENCH_DIR + "/vector-search-benchmark", "/var/lib/vsbench/datasets/cohere-100k"
        common = f"{b} search-cql --data-dir {data} --scylla 10.0.1.10:9042 --limit 10"
        expected = [
            f"step warmup-c64-r1 --timeout 630s -- {common} --duration 30s --concurrency 64",
            f"step search-c64-r1 --timeout 660s -- {common} --duration 60s --concurrency 64",
            f"step warmup-c128-r1 --timeout 630s -- {common} --duration 30s --concurrency 128",
            f"step search-c128-r1 --timeout 660s -- {common} --duration 60s --concurrency 128",
        ]
        self.assertEqual(steps, expected)
        script = bench.job_script(JOB, "search", steps)
        header = f"#!/usr/bin/env bash\n# vsbench search job {JOB}; generated by bench.py, see node/bench-job.sh.\n"
        header += "set -euo pipefail\nsource /var/lib/vsbench/scripts/bench-job.sh\n"
        self.assertEqual(script, header + "\n".join(expected) + "\n")

    def test_search_http_two_vs_no_warmup_repeat(self) -> None:
        state = make_state(vs=2)
        opts = bench.SearchOptions(kind="http", warmup_s=0, repeat=2, first_repeat=3, extra_args=("--x",))
        plan = bench.search_plan(opts)
        self.assertEqual([p["name"] for p in plan], ["search-c64-r3", "search-c64-r4"])
        steps = bench.search_steps(state, opts, state["load"], plan)
        self.assertEqual(len(steps), 2)
        self.assertIn("--vector-store 10.0.2.10:6080 --vector-store 10.0.2.11:6080 --index " + INDEX, steps[0])
        self.assertNotIn("--scylla", steps[0])
        self.assertTrue(steps[0].endswith("--concurrency 64 --x"))

    def test_load_steps_fresh_global_and_local(self) -> None:
        state, entry = make_state(), bench.dataset("cohere-100k")
        plan = {"todo": list(bench.PHASES), "index": "vsb_idx_new", "options": OPTIONS_CQL, "old_index": None}
        steps = bench.load_steps(state, entry, bench.LoadOptions("cohere-100k", rf=1, concurrency=256), plan)
        names = [s.split()[1] for s in steps]
        self.assertEqual(names, ["fetch", "clear-buckets", "drop-table", "build-table", "build-index"])
        table = "--data-dir /var/lib/vsbench/datasets/cohere-100k --scylla 10.0.1.10:9042 --rf 1 --concurrency 256"
        self.assertIn(f"build-table {table}", steps[3])
        self.assertIn("--timeout 7200s", steps[4])
        self.assertIn("--options " + bench.shlex.quote(OPTIONS_CQL) + " --index vsb_idx_new", steps[4])
        self.assertIn("$'FILES=train.parquet 312652957\\ntest.parquet 3126879\\nneighbors.parquet 3163592'", steps[0])
        local = bench.load_steps(state, entry, bench.LoadOptions("cohere-100k", local_index=True), plan)
        self.assertEqual(local[1].split()[1], "buckets")
        self.assertTrue(local[4].endswith("--local"))

    def test_load_steps_resume_index_drops_the_attempt(self) -> None:
        state, entry = make_state(), bench.dataset("cohere-100k")
        plan = {"todo": ["index"], "index": "vsb_idx_new", "options": OPTIONS_CQL, "old_index": "vsb_idx_old"}
        steps = bench.load_steps(state, entry, bench.LoadOptions("cohere-100k", resume=True), plan)
        self.assertEqual([s.split()[1] for s in steps], ["drop-index", "wait-gone", "build-index"])
        self.assertIn("wait_index_gone vsb_keyspace vsb_idx_old 600 http://10.0.2.10:6080", steps[1])

    def test_new_index_name(self) -> None:
        now = datetime.datetime(2026, 10, 6, 10, 0, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(bench.new_index_name(now), "vsb_idx_20261006100000")
        self.assertEqual(bench.new_index_name(now, "vsb_idx_20261006100000"), "vsb_idx_20261006100000_2")

    def test_ab_order(self) -> None:
        self.assertEqual(bench.ab_order(1), ["A", "B"])
        self.assertEqual(bench.ab_order(2), ["A", "B", "B", "A"])
        self.assertEqual(bench.ab_order(3), ["A", "B", "B", "A", "A", "B"])
        self.assertEqual(bench.ab_order(4), ["A", "B", "B", "A"] * 2)


class NodeScriptTest(unittest.TestCase):
    """Run generated step scripts and the node helpers locally with fake tools."""

    def setUp(self) -> None:
        self.dir = Path(tempfile.mkdtemp(prefix="vsbench-node-test-"))
        self.addCleanup(shutil.rmtree, self.dir, True)

    def run_steps(self, script: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        fake = self.dir / "fake-tool"
        fake.write_text("#!/usr/bin/env bash\nprintf '<%s>\\n' \"$@\"\nprintf 'FILES=[%s]\\n' \"${FILES:-}\"\n")
        fake.chmod(0o755)
        script = script.replace(f"{config.NODE_SCRIPTS}/bench-job.sh", str(NODE / "bench-job.sh"))
        script = script.replace(bench.BENCH_BIN, str(fake))
        script = script.replace(f"bash {config.NODE_SCRIPTS}/fetch-dataset.sh", str(fake))
        path = self.dir / "steps.sh"
        path.write_text(script)
        full_env = {**os.environ, "VSBENCH_CPUSET": "", **(env or {})}
        return subprocess.run(["bash", str(path)], capture_output=True, text=True, env=full_env, timeout=60)

    def test_generated_steps_pass_arguments_intact(self) -> None:
        state, entry = make_state(), bench.dataset("cohere-100k")
        plan = {"todo": ["fetch", "index"], "index": "vsb_idx_new", "options": OPTIONS_CQL, "old_index": None}
        steps = bench.load_steps(state, entry, bench.LoadOptions("cohere-100k"), plan)
        out = self.run_steps(bench.job_script(JOB, "load", steps))
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("FILES=[train.parquet 312652957\ntest.parquet 3126879\nneighbors.parquet 3163592]", out.stdout)
        self.assertIn(f"<--options>\n<{OPTIONS_CQL}>\n<--index>\n<vsb_idx_new>", out.stdout)
        names = [s["name"] for s in results.step_sections(out.stdout)]
        self.assertEqual(names, ["fetch", "build-index"])
        self.assertTrue(all(s["exit"] == 0 for s in results.step_sections(out.stdout)))

    def test_step_failure_stops_the_job_and_fresh_lines(self) -> None:
        steps = [
            bench.step_line("one", ["echo", "x"]),
            bench.step_line("two", ["false"]),
            bench.step_line("3", ["true"]),
        ]
        out = self.run_steps(bench.job_script(JOB, "raw", steps))
        self.assertEqual(out.returncode, 1)
        sections = results.step_sections(out.stdout)
        self.assertEqual([(s["name"], s["exit"]) for s in sections], [("one", 0), ("two", 1)])
        self.assertIn("vsbench: cpuset=all", out.stdout)

    def test_step_timeout_and_markers_in_a_log_file(self) -> None:
        script = bench.job_script(JOB, "raw", [bench.step_line("slow", ["sleep", "5"], 1)])
        script = script.replace(f"{config.NODE_SCRIPTS}/bench-job.sh", str(NODE / "bench-job.sh"))
        path, log, env = self.dir / "s.sh", self.dir / "log", {**os.environ, "VSBENCH_CPUSET": ""}
        path.write_text("printf 'partial'\n" + script.split("\n", 1)[1])
        with open(log, "w") as handle:  # a real job log is a file: markers then start on a fresh line
            code = subprocess.run(["bash", str(path)], stdout=handle, stderr=subprocess.STDOUT, env=env, timeout=60)
        code = code.returncode
        self.assertEqual(code, 124)
        text = log.read_text()
        self.assertIn("partial\n", text)  # the BEGIN marker starts on a fresh line
        self.assertEqual(results.step_sections(text)[0]["exit"], 124)

    def test_wait_index_gone(self) -> None:
        if not shutil.which("jq"):
            self.skipTest("jq is not installed")
        api = self.dir / "api" / "v1"
        api.mkdir(parents=True)
        (api / "indexes").write_text(json.dumps([{"keyspace": "vsb_keyspace", "index": "old"}]))
        lib = NODE / "bench-job.sh"
        gone = f"source {lib}; step wait-gone -- wait_index_gone vsb_keyspace other 5 file://{self.dir}"
        out = subprocess.run(
            ["bash", "-c", gone], capture_output=True, text=True, env={**os.environ, "VSBENCH_CPUSET": ""}
        )
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("is gone", out.stdout)
        stays = f"source {lib}; step wait-gone -- wait_index_gone vsb_keyspace old 1 file://{self.dir}"
        out = subprocess.run(
            ["bash", "-c", stays], capture_output=True, text=True, env={**os.environ, "VSBENCH_CPUSET": ""}
        )
        self.assertEqual(out.returncode, 1)
        self.assertIn("still lists vsb_keyspace.old", out.stdout)

    def fetch(self, files: str, base: Path, target: Path) -> subprocess.CompletedProcess[str]:
        env = {**os.environ, "DATASET_DIR": str(target), "BASE_URL": f"file://{base}", "FILES": files}
        return subprocess.run(
            ["bash", str(NODE / "fetch-dataset.sh")], capture_output=True, text=True, env=env, timeout=60
        )

    def test_fetch_dataset_downloads_resumes_and_skips(self) -> None:
        base, target = self.dir / "src", self.dir / "dst"
        base.mkdir()
        (base / "train.parquet").write_bytes(b"x" * 1000)
        (base / "test.parquet").write_bytes(b"y" * 10)
        target.mkdir()
        (target / "train.parquet.part").write_bytes(b"x" * 400)  # resumed with curl -C -
        out = self.fetch("train.parquet 1000\ntest.parquet -\n", base, target)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertEqual((target / "train.parquet").read_bytes(), b"x" * 1000)
        self.assertEqual((target / ".complete").read_text(), "train.parquet 1000\ntest.parquet 10\n")
        self.assertFalse((target / "train.parquet.part").exists())
        (base / "train.parquet").unlink()  # complete files are not fetched again
        out = self.fetch("train.parquet 1000\ntest.parquet 10", base, target)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("dataset complete", out.stdout)

    def test_fetch_dataset_failures(self) -> None:
        base, target = self.dir / "src", self.dir / "dst"
        base.mkdir()
        (base / "test.parquet").write_bytes(b"y" * 10)
        out = self.fetch("test.parquet 11", base, target)  # wrong size: never renamed, not retried
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("has 10 bytes but the catalog expects 11", out.stdout)
        self.assertFalse((target / "test.parquet").exists())
        (target / "test.parquet.part").unlink()
        (target / "shuffle_train.parquet").write_bytes(b"z")
        out = self.fetch("test.parquet 10", base, target)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("would be loaded as train data", out.stdout)
        self.assertNotEqual(self.fetch("bad name 10", base, target).returncode, 0)


class ValidationTest(HomeTestCase):
    def check(self, state: dict[str, Any], opts: bench.SearchOptions, status: dict | None = None) -> dict[str, Any]:
        self.fake_deploy(status if status is not None else vs_status(state))
        return bench.validate_search(state, opts)

    def test_global_cql_ok(self) -> None:
        live = self.check(self.state, bench.SearchOptions("cql"))
        self.assertEqual(live["engine"], "usearch-2.22.0")
        self.assertEqual(live["options"]["similarity_function"], "COSINE")

    def test_matrix(self) -> None:
        local = make_state()
        local["load"]["local_index"] = True
        not_built = make_state()
        not_built["load"]["phases"]["index"] = None
        pending = make_state()
        pending["load"]["pending_job"] = "J"
        ignored = make_state()
        ignored["load"]["options_ok"] = False
        no_bench = make_state()
        no_bench["deployed"]["bench"] = None
        refused = [
            (make_state(loaded=False), bench.SearchOptions("cql"), "no dataset"),
            (not_built, bench.SearchOptions("cql"), "not built"),
            (pending, bench.SearchOptions("cql"), "not built"),
            (ignored, bench.SearchOptions("cql"), "ignored"),
            (self.state, bench.SearchOptions("cql", bucket=2), "local index"),
            (local, bench.SearchOptions("http"), "local index"),
            (local, bench.SearchOptions("cql"), "--bucket"),
            (no_bench, bench.SearchOptions("cql"), "bench is not deployed"),
            (make_state(expires_at="2000-01-01T00:00:00Z"), bench.SearchOptions("cql"), "expires"),
        ]
        for state, opts, text in refused:
            with self.subTest(text=text), self.assertRaises(PreconditionError) as ctx:
                self.check(state, opts)
            self.assertIn(text, str(ctx.exception))
        self.check(local, bench.SearchOptions("cql", bucket=3))

    def test_live_vs_state(self) -> None:
        for status, text in (
            (vs_status(self.state, status="BOOTSTRAPPING"), "vs-0 is BOOTSTRAPPING"),
            (vs_status(self.state, count=50000), "50000 of 100000"),
            (vs_status(self.state, index="other"), "missing"),
            ({}, "unreachable"),
        ):
            with self.subTest(text=text), self.assertRaises(PreconditionError) as ctx:
                self.check(self.state, bench.SearchOptions("cql"), status)
            self.assertIn(text, str(ctx.exception))
        self.check(self.state, bench.SearchOptions("cql"), {"nodes": vs_status(self.state, count=99500)})

    def test_bad_arguments(self) -> None:
        for opts in (
            bench.SearchOptions("cql", concurrency=(64, 64)),
            bench.SearchOptions("cql", extra_args=("--limit=5",)),
            bench.SearchOptions("cql", extra_args=("--from", "x")),
            bench.SearchOptions("grpc"),
            bench.SearchOptions("cql", repeat=0),
        ):
            with self.subTest(opts=opts), self.assertRaises(VsbenchError):
                self.check(self.state, opts)

    def test_short_duration_warns(self) -> None:
        self.check(self.state, bench.SearchOptions("cql", duration_s=10))
        self.assertIn("server metrics will be null", self.warn.call_args[0][0])


class LoadPlanTest(unittest.TestCase):
    def test_load_todo(self) -> None:
        previous = make_state()["load"]
        self.assertEqual(bench.load_todo(previous, bench.LoadOptions("cohere-100k")), list(bench.PHASES))
        self.assertEqual(bench.load_todo(previous, bench.LoadOptions("cohere-100k", resume=True)), [])
        previous["phases"].update(table=None, index=None)
        self.assertEqual(bench.load_todo(previous, bench.LoadOptions("cohere-100k", resume=True)), ["table", "index"])
        for opts in (bench.LoadOptions("cohere-1m", resume=True), bench.LoadOptions("cohere-100k", rf=3, resume=True)):
            with self.subTest(opts=opts), self.assertRaises(PreconditionError):
                bench.load_todo(previous, opts)
        self.assertRaises(PreconditionError, bench.load_todo, None, bench.LoadOptions("cohere-100k", resume=True))

    def test_planned_load(self) -> None:
        previous, entry = make_state()["load"], bench.dataset("cohere-100k")
        plan = {"job_id": "J2", "todo": ["index"], "index": "vsb_idx_new", "options": OPTIONS_CQL}
        planned = bench.planned_load(previous, entry, bench.LoadOptions("cohere-100k", resume=True), plan)
        self.assertEqual(planned["phases"]["table"], "2026-10-06T10:00:00Z")
        self.assertIsNone(planned["phases"]["index"])
        self.assertEqual((planned["run_id"], planned["pending_job"], planned["index"]), ("L1", "J2", "vsb_idx_new"))
        fresh = bench.planned_load(
            previous, entry, bench.LoadOptions("cohere-100k"), {**plan, "todo": list(bench.PHASES)}
        )
        self.assertEqual(set(fresh["phases"].values()), {None})
        self.assertEqual(fresh["run_id"], "J2")


def index_log() -> str:
    return section("drop-index", "") + section("wait-gone", "") + section("build-index", fixture("build-index.log"))


def search_params(state: dict[str, Any], **kw: Any) -> dict[str, Any]:
    opts = bench.SearchOptions("cql", concurrency=(64, 128), **kw)
    params = {**bench.dataclasses.asdict(opts), "steps": bench.search_plan(opts), "index": INDEX}
    params.update(concurrency=list(opts.concurrency), extra_args=[], vs_engine="usearch-2.22.0")
    params["snapshot"] = {"deployed": state["deployed"], "load": state["load"]}
    params["index_options"] = {"similarity_function": "COSINE"}
    return params


class FinalizeSearchTest(HomeTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.poll = self.patch(remote, "job_poll", return_value=progress(143))
        self.log = self.patch(remote, "job_log", return_value=fixture("job-search.log"))
        self.prom = self.patch(bench.prom, "server_metrics", return_value={"vs_qps": 7000.0, "cpu_pct": {"vs-0": 50.0}})
        self.add_job(JOB, "search", search_params(self.state))

    def test_records_measured_runs_and_the_interrupted_one(self) -> None:
        records = bench.finalize_job("t", JOB)
        self.assertEqual([r["run_id"] for r in records], [f"{JOB}-c64-r1", f"{JOB}-c128-r1"])
        ok, failed = records
        self.assertEqual((ok["kind"], ok["exit"], ok["series_id"], ok["repeat_index"]), ("search-cql", 0, JOB, 1))
        self.assertEqual(ok["client_metrics"]["qps"], 6953.2)
        self.assertEqual(ok["client_metrics"]["latency_ms"]["p50"]["tag"], "floored")
        self.assertEqual(ok["server_metrics"]["vs_qps"], 7000.0)
        self.assertEqual(ok["window"]["start"], "2026-10-06T10:04:05.970Z")
        self.assertEqual((ok["index"]["name"], ok["versions"]["vs_engine"]), (INDEX, "usearch-2.22.0"))
        self.assertEqual(ok["params"]["concurrency"], 64)
        args = self.prom.call_args[0]
        self.assertEqual(
            args[:5], ("t", "2026-10-06T10:04:05.970602Z", "2026-10-06T10:04:10.972073Z", bench.KEYSPACE, INDEX)
        )
        self.assertEqual(
            (failed["exit"], failed["client_metrics"], failed["failed_step"]), (143, None, "search-c128-r1")
        )
        self.assertIn("Readings buckets", failed["error_tail"])
        stored = [r["run_id"] for r in results.load_records("t")]
        self.assertEqual(stored, [r["run_id"] for r in records])
        self.assertTrue((st.paths("t").results_dir / f"{JOB}.log").exists())
        job = st.load("t")["jobs"][JOB]
        self.assertEqual((job["status"], job["exit_code"], job["failed_step"]), ("failed", 143, "search-c128-r1"))

    def test_idempotent(self) -> None:
        first = bench.finalize_job("t", JOB)
        self.poll.reset_mock()
        self.log.reset_mock()
        self.prom.reset_mock()
        self.assertEqual(bench.finalize_job("t", JOB), first)
        self.poll.assert_not_called()
        self.log.assert_not_called()
        self.prom.assert_not_called()
        self.assertEqual(len(results.load_records("t")), 2)

    def test_server_metrics_errors_are_recorded(self) -> None:
        self.prom.side_effect = VsbenchError("short window: 5s < 30s")
        record = bench.finalize_job("t", JOB)[0]
        self.assertIsNone(record["server_metrics"])
        self.assertEqual(record["server_metrics_error"], "short window: 5s < 30s")
        self.assertEqual(record["client_metrics"]["qps"], 6953.2)

    def test_no_monitoring(self) -> None:
        params = search_params(self.state)
        params["snapshot"]["deployed"] = {**params["snapshot"]["deployed"], "monitoring": None}
        self.add_job(JOB, "search", params)
        record = bench.finalize_job("t", JOB)[0]
        self.assertEqual(record["server_metrics_error"], "monitoring is not deployed")
        self.prom.assert_not_called()

    def test_still_running(self) -> None:
        self.poll.return_value = progress(None, running=True)
        self.assertRaises(PreconditionError, bench.finalize_job, "t", JOB)

    def test_panic_and_failed_warmup(self) -> None:
        log = section("warmup-c64-r1", fixture("search-cql.log")) + section(
            "search-c64-r1", fixture("search-cql-panic.log"), 101
        )
        self.log.return_value = log
        self.poll.return_value = progress(101)
        record = bench.finalize_job("t", JOB)[0]
        self.assertEqual((record["exit"], record["client_metrics"]), (101, None))
        self.assertIn("ALLOW FILTERING", record["error"])
        self.add_job("20261006T110000Z-search-cd34", "search", search_params(self.state))
        self.log.return_value = section("warmup-c64-r1", fixture("search-cql-panic.log"), 101)
        record = bench.finalize_job("t", "20261006T110000Z-search-cd34")[0]
        self.assertEqual(
            (record["run_id"], record["failed_step"]), ("20261006T110000Z-search-cd34-c64-r1", "warmup-c64-r1")
        )

    def test_job_wait_raises_with_hints_after_recording(self) -> None:
        self.log.return_value = section("warmup-c64-r1", fixture("search-cql.log")) + section(
            "search-c64-r1", fixture("search-cql-panic.log"), 101
        )
        self.poll.return_value = progress(101)
        self.patch(remote, "job_follow", return_value=101)
        with self.assertRaises(VsbenchError) as ctx:
            bench.job_wait("t", JOB, 60)
        self.assertIn("in step search-c64-r1 (exit 101)", str(ctx.exception))
        self.assertIn("--local-index", ctx.exception.hint or "")
        self.assertEqual(len(results.load_records("t")), 1)

    def test_job_wait_still_running(self) -> None:
        self.patch(remote, "job_follow", side_effect=StillRunning("still running", "vsbench job wait"))
        self.assertRaises(StillRunning, bench.job_wait, "t", JOB, 1)
        self.assertEqual(st.load("t")["jobs"][JOB]["status"], "running")


class FinalizeBuildTest(HomeTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.poll = self.patch(remote, "job_poll", return_value=progress(0))
        self.log = self.patch(remote, "job_log")
        self.http = self.patch(remote, "http_json", return_value={"options": {"similarity_function": "COSINE"}})
        state = make_state()
        state["load"].update(index="vsb_idx_new", index_options=None, options_ok=None, pending_job="J-load")
        state["load"]["phases"] = dict.fromkeys(bench.PHASES)
        self.save(state)
        params = {"index": "vsb_idx_new", "index_options": OPTIONS_CQL, "requested": {"similarity_function": "COSINE"}}
        params.update(todo=list(bench.PHASES), old_index=None, rf=1, concurrency=512, local_index=False, resume=False)
        self.add_job("J-load", "load", params)

    def load_log(self, index_code: int = 0) -> str:
        log = section("fetch", "vsbench: dataset complete") + section("clear-buckets", "")
        log += section("drop-table", "") + section("build-table", fixture("build-table.log"))
        return log + section("build-index", fixture("build-index.log") if index_code == 0 else "", index_code)

    def test_load_success(self) -> None:
        self.log.return_value = self.load_log()
        record = bench.finalize_job("t", "J-load")[0]
        self.assertEqual(record["kind"], "load")
        self.assertEqual((record["load"]["upload_s"], record["load"]["index_s"]), (0.08442, 3.03))
        self.assertEqual(results.index_build_seconds(record), 3.03)
        self.assertEqual(record["index"]["options"], {"similarity_function": "COSINE"})
        load = st.load("t")["load"]
        self.assertTrue(all(load["phases"].values()))
        self.assertEqual((load["options_ok"], load["pending_job"]), (True, None))
        self.assertEqual(load["index_options"], {"similarity_function": "COSINE"})
        self.assertIn("/api/v1/indexes/vsb_keyspace/vsb_idx_new", self.http.call_args[0][2])
        self.assertEqual(st.load("t")["jobs"]["J-load"]["status"], "finalized")

    def test_ignored_options_fail_the_job(self) -> None:
        self.log.return_value = self.load_log()
        self.http.return_value = {"options": {"similarity_function": "EUCLIDEAN"}}
        self.patch(remote, "job_follow", return_value=0)
        with self.assertRaises(VsbenchError) as ctx:
            bench.job_wait("t", "J-load", 60)
        self.assertIn("ignored index options", str(ctx.exception))
        self.assertIs(st.load("t")["load"]["options_ok"], False)

    def test_index_timeout_keeps_completed_phases(self) -> None:
        self.log.return_value = self.load_log(index_code=124)
        self.poll.return_value = progress(124)
        bench.finalize_job("t", "J-load")
        load = st.load("t")["load"]
        self.assertTrue(load["phases"]["table"])
        self.assertIsNone(load["phases"]["index"])
        self.http.assert_not_called()
        job = st.load("t")["jobs"]["J-load"]
        self.assertEqual((job["status"], job["failed_step"], job["error"]), ("failed", "build-index", "exit 124"))

    def test_superseded_load_is_not_overwritten(self) -> None:
        self.log.return_value = self.load_log()
        state = st.load("t")
        state["load"]["pending_job"] = "J-newer"
        self.save(state)
        bench.finalize_job("t", "J-load")
        self.assertEqual(st.load("t")["load"]["pending_job"], "J-newer")

    def test_index_job_records_index_build(self) -> None:
        params = {"index": "vsb_idx_new", "index_options": OPTIONS_CQL, "requested": {"similarity_function": "COSINE"}}
        self.add_job("J-index", "index", {**params, "todo": ["index"], "old_index": INDEX, "index_timeout_s": 60})
        state = st.load("t")
        state["load"].update(pending_job="J-index", phases=dict.fromkeys(bench.PHASES, "x") | {"index": None})
        self.save(state)
        self.log.return_value = index_log()
        record = bench.finalize_job("t", "J-index")[0]
        self.assertEqual(record["kind"], "index-build")
        self.assertEqual(record["index_build"]["previous_index"], INDEX)
        self.assertEqual(results.index_build_seconds(record), 3.03)
        self.assertEqual(st.load("t")["load"]["phases"]["index"], "2026-10-06T10:05:11Z")


class CommandTest(HomeTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uploads: dict[str, str] = {}
        self.patch(remote, "ensure_script")
        upload = self.patch(remote, "upload")
        upload.side_effect = lambda c, n, local, path, **kw: self.uploads.update({path: Path(local).read_text()})
        self.start = self.patch(remote, "job_start")
        self.follow = self.patch(remote, "job_follow", return_value=0)
        self.poll = self.patch(remote, "job_poll", return_value=progress(0))
        self.log = self.patch(remote, "job_log", return_value="")
        self.patch(bench.prom, "server_metrics", return_value={"vs_qps": 1.0})
        self.fake_deploy()

    def test_search_runs_a_job_and_records(self) -> None:
        self.log.return_value = section("warmup-c64-r1", "") + section("search-c64-r1", fixture("search-cql.log"))
        records = bench.search("t", bench.SearchOptions("cql", label="base"))
        (job_id,) = st.load("t")["jobs"]
        self.assertEqual([r["run_id"] for r in records], [f"{job_id}-c64-r1"])
        self.assertEqual(records[0]["label"], "base")
        script = self.uploads[f"{config.NODE_JOBS}/{job_id}/steps.sh"]
        self.assertIn("step search-c64-r1 --timeout 660s", script)
        self.start.assert_called_once_with("t", "client", job_id, script=f"{config.NODE_JOBS}/{job_id}/steps.sh")
        self.assertEqual(st.load("t")["jobs"][job_id]["status"], "finalized")

    def test_running_job_blocks_new_ones(self) -> None:
        self.add_job(JOB, "search", search_params(self.state))
        self.poll.return_value = progress(None, running=True)
        with self.assertRaises(PreconditionError) as ctx:
            bench.search("t", bench.SearchOptions("cql"))
        self.assertIn(f"job wait {JOB}", ctx.exception.hint or "")
        self.start.assert_not_called()

    def test_ended_job_is_finalized_first(self) -> None:
        self.add_job(JOB, "fetch", {"dataset": "cohere-100k"})
        bench.fetch("t", "cohere-100k", 60)
        self.assertEqual(st.load("t")["jobs"][JOB]["status"], "finalized")
        self.assertEqual(len(st.load("t")["jobs"]), 2)

    def test_load_checks_rf_before_touching_the_cluster(self) -> None:
        with self.assertRaises(PreconditionError):
            bench.load("t", bench.LoadOptions("cohere-100k", rf=3))
        self.poll.assert_not_called()
        self.start.assert_not_called()

    def test_load_resume_runs_only_missing_phases(self) -> None:
        state = st.load("t")
        state["load"]["phases"]["index"] = None
        self.save(state)
        self.patch(remote, "http_json", return_value={"options": {"similarity_function": "COSINE"}})
        self.log.return_value = index_log()
        result = bench.load("t", bench.LoadOptions("cohere-100k", resume=True))
        script = next(iter(self.uploads.values()))
        self.assertEqual(
            [line.split()[1] for line in script.splitlines()[4:]], ["drop-index", "wait-gone", "build-index"]
        )
        self.assertIn(f"--index {INDEX}", script.splitlines()[4])
        self.assertTrue(all(result["load"]["phases"].values()))
        self.assertEqual(result["load"]["run_id"], "L1")  # the data is still the one of load L1

    def test_load_nothing_to_resume(self) -> None:
        result = bench.load("t", bench.LoadOptions("cohere-100k", resume=True))
        self.assertIsNone(result["job_id"])
        self.start.assert_not_called()

    def test_index_drops_waits_and_rebuilds(self) -> None:
        self.patch(
            remote, "http_json", return_value={"options": {"similarity_function": "COSINE", "quantization": "I8"}}
        )
        self.log.return_value = section("build-index", fixture("build-index.log"))
        result = bench.index("t", bench.IndexOptions("{'quantization': 'I8'}"))
        script = next(iter(self.uploads.values()))
        self.assertEqual(
            [line.split()[1] for line in script.splitlines()[4:]], ["drop-index", "wait-gone", "build-index"]
        )
        self.assertIn("'quantization'", script)
        load = st.load("t")["load"]
        self.assertEqual((load["index"], load["options_ok"]), (result["index"], True))
        self.assertNotEqual(result["index"], INDEX)

    def test_ab_runs_abba_with_pinned_builds(self) -> None:
        calls: list[tuple[str, Any]] = []

        def deploy_vs(cluster: str, source: str, *args: Any, record_extra: dict | None = None) -> dict:
            calls.append(("deploy", source, record_extra["arm"] if record_extra else None))
            return {}

        def search(cluster: str, opts: bench.SearchOptions) -> list[dict[str, Any]]:
            calls.append(("search", opts.arm, opts.first_repeat, opts.comparison_id, opts.repeat))
            return [{"arm": opts.arm}]

        fake = self.fake_deploy()
        fake.deploy_vs = deploy_vs
        builds = {"release:1.11.0": {"build_id": "release-1.11.0", "pin": "release:1.11.0"}}
        builds["local"] = {"build_id": "x-1", "pin": "build:x-1"}
        self.patch(bench.build, "build", side_effect=lambda spec: builds[str(spec)])
        self.patch(bench, "search", side_effect=search)
        result = bench.ab("t", bench.AbOptions(a="release:1.11.0", b="local", repeat=2))
        self.assertEqual(result["order"], ["A", "B", "B", "A"])
        pins = [c[1] for c in calls if c[0] == "deploy"]
        self.assertEqual(pins, ["release:1.11.0", "build:x-1", "build:x-1", "release:1.11.0"])
        searches = [c for c in calls if c[0] == "search"]
        self.assertEqual([(c[1], c[2], c[4]) for c in searches], [("A", 1, 1), ("B", 1, 1), ("B", 2, 1), ("A", 2, 1)])
        self.assertEqual({c[3] for c in searches}, {result["comparison_id"]})
        self.assertEqual([c[2] for c in calls if c[0] == "deploy"], ["A", "B", "B", "A"])
        self.assertEqual(len(result["records"]), 4)

    def test_rerun_replays_parameters(self) -> None:
        params = {
            "limit": 5,
            "duration_s": 40,
            "warmup_s": 10,
            "concurrency": 32,
            "bucket": None,
            "extra_args": ["--q"],
        }
        record = {"run_id": "R1", "kind": "search-http", "label": "x", "params": params}
        results.append("t", record)
        search = self.patch(bench, "search", return_value=[])
        bench.rerun("t", "R1", 99)
        opts = search.call_args[0][1]
        self.assertEqual((opts.kind, opts.limit, opts.duration_s, opts.warmup_s), ("http", 5, 40, 10))
        self.assertEqual((opts.concurrency, opts.extra_args, opts.label, opts.timeout_s), ((32,), ("--q",), "x", 99))
        self.assertRaises(VsbenchError, bench.rerun, "t", "missing", 1)

    def test_raw_streams_output_and_returns_the_code(self) -> None:
        lines = ("=== VSBENCH STEP BEGIN raw t", "hello")
        self.follow.side_effect = lambda c, n, j, t, on_line, **kw: [on_line(x) for x in lines] and 3
        with mock.patch("builtins.print") as printed:
            self.assertEqual(bench.raw("t", ["--version"], 60), 3)
        printed.assert_called_once_with("hello", flush=True)
        self.assertIn(f"-- {bench.BENCH_BIN} --version", next(iter(self.uploads.values())))

    def test_summary(self) -> None:
        self.log.return_value = section("warmup-c64-r1", "") + section("search-c64-r1", fixture("search-cql.log"))
        text = bench.format_summary(bench.search("t", bench.SearchOptions("cql")))
        self.assertIn("6953", text)
        self.assertIn("<=1.00ms", text)
        self.assertLessEqual(len(text.splitlines()), 15)
        state = copy.deepcopy(self.state)
        load_record = results.make_record(
            "load", state, {}, None, None, None, {"run_id": "L", "load": {"upload_s": 1.5}}
        )
        self.assertIn("upload_s=1.5", bench.format_summary([load_record]))


if __name__ == "__main__":
    unittest.main()
