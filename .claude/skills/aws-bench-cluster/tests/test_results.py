# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.results (log parsing, records, aggregation, comparison)."""

from __future__ import annotations

import copy
import datetime
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import results  # noqa: E402
from vsbenchlib import results_format  # noqa: E402
from vsbenchlib.proc import PreconditionError, VsbenchError  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
UTC = datetime.timezone.utc


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def sample_state() -> dict[str, Any]:
    return {
        "schema": 1,
        "cluster": "default",
        "region": "us-east-1",
        "az": "us-east-1b",
        "nodes": [
            {"name": "client", "role": "client", "index": 0, "instance_id": "i-c", "instance_type": "r8g.2xlarge"},
            {"name": "vs-0", "role": "vs", "index": 0, "instance_id": "i-v0", "instance_type": "r8g.4xlarge"},
            {"name": "scylla-0", "role": "scylla", "index": 0, "instance_id": "i-s0", "instance_type": "i8g.2xlarge"},
        ],
        "deployed": {
            "scylla": {"image": "scylladb/scylla-nightly@sha256:abc", "version": "2026.2.0~dev"},
            "vector_store": {
                "build_id": "1.11.0-1-gabc-12345678",
                "version": "1.11.0-1-gabc",
                "source": "git:master",
                "commit": "a" * 40,
                "dirty": False,
                "env": {"RUST_LOG": "info"},
            },
            "bench": {"build_id": "bench-1", "version": "0.0.0-dev", "source": "git:master", "commit": "b" * 40},
            "monitoring": None,
        },
        "load": {
            "dataset": "cohere-1m",
            "index": "vsb_idx_1",
            "index_options": {"similarity_function": "COSINE"},
            "rf": 1,
            "local_index": False,
            "rows": 1000000,
            "run_id": "load-1",
        },
    }


SEARCH_PARAMS = {"limit": 10, "duration_s": 60, "warmup_s": 30, "concurrency": 64, "bucket": None, "extra_args": []}


def search_record(**overrides: Any) -> dict[str, Any]:
    """A make_record() search record with simple, overridable metrics."""
    state = overrides.pop("state", sample_state())
    params = {**SEARCH_PARAMS, **overrides.pop("params", {})}
    parsed = results.parse_bench_log(fixture("search-cql.log"))
    parsed = {**parsed, "qps": overrides.pop("qps", parsed["qps"])}
    server = overrides.pop("server", None)
    window = overrides.pop("window", ("2026-10-06T10:00:00Z", "2026-10-06T10:01:00Z"))
    return results.make_record("search-cql", state, params, parsed, server, window, overrides)


class ParseDurationTest(unittest.TestCase):
    def test_units(self) -> None:
        self.assertEqual(results.parse_duration_ms("354.4µs"), (0.3544, "exact"))
        self.assertEqual(results.parse_duration_ms("354.4μs"), (0.3544, "exact"))
        self.assertEqual(results.parse_duration_ms("354.4us"), (0.3544, "exact"))
        self.assertEqual(results.parse_duration_ms("850.0ns"), (0.00085, "exact"))
        self.assertEqual(results.parse_duration_ms("25.1ms"), (25.1, "exact"))
        self.assertEqual(results.parse_duration_ms("3.03s"), (3030.0, "exact"))

    def test_floor_and_cap(self) -> None:
        self.assertEqual(results.parse_duration_ms("1.0ms"), (1.0, "floored"))
        self.assertEqual(results.parse_duration_ms("18446744073709551616.0s"), (None, "capped"))

    def test_invalid(self) -> None:
        self.assertEqual(results.parse_duration_ms("fast"), (None, "invalid"))
        self.assertEqual(results.parse_duration_ms("12 parsecs"), (None, "invalid"))


class ParseBenchLogTest(unittest.TestCase):
    def test_search_cql(self) -> None:
        parsed = results.parse_bench_log(fixture("search-cql.log"))
        self.assertEqual(parsed["qps"], 6953.2)
        self.assertEqual(parsed["queries"], 34776)
        self.assertEqual(parsed["duration_s"], 5.0)
        self.assertEqual(parsed["latency"]["min"], {"value": 0.3544, "tag": "exact"})
        self.assertEqual(parsed["latency"]["p50"], {"value": 1.0, "tag": "floored"})
        self.assertEqual(parsed["latency"]["p75"], {"value": 1.3, "tag": "exact"})
        self.assertEqual(parsed["latency"]["p99"], {"value": 3.2, "tag": "exact"})
        self.assertEqual(parsed["latency"]["max"], {"value": 25.1, "tag": "exact"})
        self.assertEqual(
            sorted(parsed["latency"]), sorted(["min", "p1", "p10", "p25", "p50", "p75", "p90", "p99", "max"])
        )
        self.assertEqual(parsed["recall"], {"min": 20.0, "avg": 63.5, "max": 100.0})
        self.assertEqual(parsed["started_at"], "2026-10-06T10:04:05.970602Z")
        self.assertEqual(parsed["gathering_at"], "2026-10-06T10:04:10.972073Z")
        self.assertEqual(parsed["search_kind"], "cql")
        self.assertEqual((parsed["timeouts"], parsed["errors"], parsed["panicked"]), (0, [], False))
        self.assertEqual(parsed["per_node"], {})

    def test_search_http_per_node(self) -> None:
        parsed = results.parse_bench_log(fixture("search-http.log"))
        self.assertEqual(parsed["qps"], 25180.8)
        self.assertIsNone(parsed["recall"])
        self.assertEqual(parsed["search_kind"], "http")
        self.assertEqual(sorted(parsed["per_node"]), ["0", "1"])
        node0 = parsed["per_node"]["0"]
        self.assertEqual((node0["qps"], node0["queries"]), (12590.3, 62931))
        self.assertEqual(node0["latency"]["min"], {"value": 0.1285, "tag": "exact"})
        self.assertEqual(node0["latency"]["p99"]["tag"], "floored")
        self.assertEqual(parsed["per_node"]["1"]["latency"]["min"]["value"], 0.1218)
        self.assertEqual(parsed["latency"]["p99"], {"value": 1.0, "tag": "floored"})

    def test_ansi_colours_are_stripped(self) -> None:
        plain = results.parse_bench_log(fixture("search-cql.log"))
        coloured = results.parse_bench_log(fixture("search-cql-color.log"))
        self.assertEqual(coloured, plain)

    def test_build_logs(self) -> None:
        table = results.parse_bench_log(fixture("build-table.log"))
        self.assertEqual(table["took"], {"build_table_s": 0.08442})
        self.assertEqual(table["dimension"], 16)
        self.assertEqual(table["errors"], [])  # WARN lines are not errors
        index = results.parse_bench_log(fixture("build-index.log"))
        self.assertEqual(index["took"], {"build_index_s": 3.03})
        buckets = results.parse_bench_log(fixture("build-buckets-color.log"))
        self.assertEqual((buckets["qps"], buckets["errors"]), (None, []))

    def test_drop_and_delete_took_lines(self) -> None:
        text = (
            "2026-10-06T10:00:00.000000Z  INFO Drop Index took 12.50ms\n"
            "2026-10-06T10:00:01.000000Z  INFO Drop Table took 1.20s\n"
            "2026-10-06T10:00:02.000000Z  INFO Delete rows took 950.00µs\n"
        )
        took = results.parse_bench_log(text)["took"]
        self.assertEqual(took, {"drop_index_s": 0.0125, "drop_table_s": 1.2, "delete_rows_s": 0.00095})

    def test_timeouts_and_capped_percentile(self) -> None:
        parsed = results.parse_bench_log(fixture("search-cql-timeouts.log"))
        self.assertEqual(parsed["timeouts"], 3)
        self.assertEqual(parsed["errors"], [])  # timeouts are counted, not listed
        self.assertEqual(parsed["latency"]["p99"], {"value": None, "tag": "capped"})
        self.assertEqual(parsed["latency"]["p90"], {"value": 88.8, "tag": "exact"})
        self.assertEqual(parsed["latency"]["max"], {"value": 10000.0, "tag": "exact"})
        self.assertEqual(parsed["recall"]["min"], 0.0)

    def test_panic(self) -> None:
        parsed = results.parse_bench_log(fixture("search-cql-panic.log"))
        self.assertTrue(parsed["panicked"])
        self.assertIsNone(parsed["qps"])
        self.assertEqual(parsed["error_count"], 3)
        self.assertIn("ALLOW FILTERING", parsed["errors"][0])
        self.assertTrue(parsed["errors"][1].startswith("panic: thread 'tokio-runtime-worker' panicked at"))
        self.assertIn("DbError(Invalid", parsed["errors"][1])
        self.assertNotIn("RUST_BACKTRACE", " ".join(parsed["errors"]))
        self.assertIn("JoinError::Panic", parsed["errors"][2])

    def test_errors_are_deduplicated_and_capped(self) -> None:
        lines = [f"2026-10-06T10:00:00.{i:06d}Z ERROR connection reset {i % 30}" for i in range(100)]
        parsed = results.parse_bench_log("\n".join(lines))
        self.assertEqual(parsed["error_count"], 100)
        self.assertEqual(len(parsed["errors"]), results.MAX_ERRORS)

    def test_garbage_is_ignored(self) -> None:
        parsed = results.parse_bench_log("hello\n2026-10-06T10:00:00Z  INFO queries: lots\n\x00\n")
        self.assertIsNone(parsed["queries"])


class StepSectionsTest(unittest.TestCase):
    def test_multi_step_job_log(self) -> None:
        sections = results.step_sections(fixture("job-search.log"))
        self.assertEqual([s["name"] for s in sections], ["warmup-c64-r1", "search-c64-r1", "search-c128-r1"])
        warmup, measured, killed = sections
        self.assertEqual(
            (warmup["begin"], warmup["end"], warmup["exit"]), ("2026-10-06T10:03:19Z", "2026-10-06T10:03:53Z", 0)
        )
        self.assertEqual(results.parse_bench_log(warmup["text"])["qps"], 4983.4)
        self.assertEqual(results.parse_bench_log(measured["text"]), results.parse_bench_log(fixture("search-cql.log")))
        self.assertEqual((killed["end"], killed["exit"]), (None, None))
        self.assertIn("Readings buckets", killed["text"])
        self.assertNotIn("VSBENCH", measured["text"])

    def test_failed_step_and_begin_without_end(self) -> None:
        log = (
            "=== VSBENCH STEP BEGIN a 2026-10-06T10:00:00Z\n"
            "line a\n"
            "=== VSBENCH STEP BEGIN b 2026-10-06T10:00:01Z\n"
            "line b\n"
            "=== VSBENCH STEP END b 124 2026-10-06T10:00:02Z\n"
            "outside\n"
        )
        sections = results.step_sections(log)
        self.assertEqual(
            [(s["name"], s["exit"], s["text"]) for s in sections], [("a", None, "line a"), ("b", 124, "line b")]
        )

    def test_no_markers(self) -> None:
        self.assertEqual(results.step_sections(fixture("search-cql.log")), [])


class TimestampTest(unittest.TestCase):
    def test_parse_timestamp(self) -> None:
        expected = datetime.datetime(2026, 10, 6, 10, 4, 5, 970602, tzinfo=UTC)
        self.assertEqual(results.parse_timestamp("2026-10-06T10:04:05.970602Z"), expected)
        self.assertEqual(results.parse_timestamp("2026-10-06T10:04:05.970602123Z"), expected)
        self.assertEqual(results.parse_timestamp("2026-10-06T12:04:05.970602+02:00"), expected)
        self.assertEqual(results.parse_timestamp("2026-10-06T10:04:05").microsecond, 0)
        with self.assertRaises(VsbenchError):
            results.parse_timestamp("yesterday")

    def test_iso_ms(self) -> None:
        moment = datetime.datetime(2026, 10, 6, 10, 4, 5, 970602, tzinfo=UTC)
        self.assertEqual(results.iso_ms(moment), "2026-10-06T10:04:05.970Z")


class MakeRecordTest(unittest.TestCase):
    def test_search_record(self) -> None:
        parsed = results.parse_bench_log(fixture("search-cql.log"))
        window = (parsed["started_at"], parsed["gathering_at"])
        extra = {"run_id": "r1", "series_id": "job-1", "repeat_index": 2, "vs_engine": "usearch-2.22.0", "foo": 1}
        params = {**SEARCH_PARAMS, "duration_s": 5}
        record = results.make_record("search-cql", sample_state(), params, parsed, None, window, extra)
        self.assertEqual((record["run_id"], record["series_id"], record["repeat_index"]), ("r1", "job-1", 2))
        self.assertEqual(
            record["window"], {"start": "2026-10-06T10:04:05.970Z", "end": "2026-10-06T10:04:10.972Z", "seconds": 5.001}
        )
        client = record["client_metrics"]
        self.assertAlmostEqual(client["mean_ms"], 64 * 5.0 * 1000 / 34776, places=4)
        self.assertEqual(client["latency_ms"]["p50"]["tag"], "floored")
        self.assertEqual(client["recall"]["avg"], 63.5)
        self.assertEqual(record["dataset"], "cohere-1m")
        self.assertEqual(record["load_run_id"], "load-1")
        self.assertEqual(
            record["index"],
            {"name": "vsb_idx_1", "options": {"similarity_function": "COSINE"}, "rf": 1, "local": False},
        )
        self.assertEqual(record["cluster"]["scylla"], {"count": 1, "type": "i8g.2xlarge", "instance_ids": ["i-s0"]})
        self.assertEqual(record["cluster"]["client"]["instance_ids"], ["i-c"])
        self.assertEqual(record["versions"]["vector_store"]["build_id"], "1.11.0-1-gabc-12345678")
        self.assertEqual(record["versions"]["vs_engine"], "usearch-2.22.0")
        self.assertEqual(record["versions"]["scylla_image"], "scylladb/scylla-nightly@sha256:abc")
        self.assertEqual(record["log"], "results/r1.log")
        self.assertEqual(record["foo"], 1)
        self.assertIn("latency_floored", record["flags"])
        self.assertIn("short_window", record["flags"])
        self.assertEqual(record["fairness_key"], results.fairness_key(record))
        self.assertEqual(len(record["fairness_key"]), 16)
        json.dumps(record)  # serialisable

    def test_defaults(self) -> None:
        record = results.make_record("load", {"nodes": []}, None, None, None, None)
        self.assertRegex(record["run_id"], r"^\d{8}T\d{6}Z-load-[0-9a-f]{4}$")
        self.assertIsNone(record["client_metrics"])
        self.assertIsNone(record["window"])
        self.assertEqual(record["flags"], [])
        self.assertEqual(record["cluster"]["vs"], {"count": 0, "type": None, "instance_ids": []})

    def test_effective_index_options_override_state(self) -> None:
        record = search_record(index_options={"similarity_function": "DOT_PRODUCT"}, index_name="idx2")
        self.assertEqual(record["index"]["options"], {"similarity_function": "DOT_PRODUCT"})
        self.assertEqual(record["index"]["name"], "idx2")


class FlagsTest(unittest.TestCase):
    def test_server_flags(self) -> None:
        server = {"cpu_pct": {"client": 85.0, "vs-0": 50.0}, "net_allowance_exceeded": {"client": 3, "vs-0": 0}}
        record = search_record(server=server)
        self.assertIn("client_saturated", record["flags"])
        self.assertIn("net_allowance_exceeded", record["flags"])
        self.assertNotIn("short_window", record["flags"])

    def test_no_flags_when_quiet(self) -> None:
        server = {"cpu_pct": {"client": 40.0}, "net_allowance_exceeded": {"client": 0}}
        record = search_record(server=server)
        self.assertEqual(results.flags_for({**record, "client_metrics": {"timeouts": 0, "latency_ms": {}}}), [])

    def test_timeouts_and_capped(self) -> None:
        parsed = results.parse_bench_log(fixture("search-cql-timeouts.log"))
        record = results.make_record("search-cql", sample_state(), SEARCH_PARAMS, parsed, None, None)
        self.assertIn("timeouts", record["flags"])
        self.assertIn("latency_capped", record["flags"])
        self.assertNotIn("latency_floored", record["flags"])

    def test_plateau(self) -> None:
        previous = search_record(qps=10000.0, params={"concurrency": 64})
        server = {"cpu_pct": {"vs-0": 55.0, "client": 60.0}}
        flat = search_record(qps=10300.0, params={"concurrency": 128}, server=server)
        self.assertIn("plateau", results.flags_for(flat, previous))
        growing = search_record(qps=12000.0, params={"concurrency": 128}, server=server)
        self.assertNotIn("plateau", results.flags_for(growing, previous))
        busy = search_record(qps=10300.0, params={"concurrency": 128}, server={"cpu_pct": {"vs-0": 95.0}})
        self.assertNotIn("plateau", results.flags_for(busy, previous))
        self.assertNotIn("plateau", results.flags_for(flat, None))
        lower = search_record(qps=10300.0, params={"concurrency": 32}, server=server)
        self.assertNotIn("plateau", results.flags_for(lower, previous))


class FairnessTest(unittest.TestCase):
    def test_key_ignores_build_kind_and_concurrency(self) -> None:
        base = search_record()
        state = sample_state()
        state["deployed"]["vector_store"]["build_id"] = "other-build"
        other_build = search_record(state=state, params={"concurrency": 128})
        self.assertEqual(base["fairness_key"], other_build["fairness_key"])

    def test_key_changes_with_setup(self) -> None:
        base = search_record()
        self.assertNotEqual(base["fairness_key"], search_record(params={"limit": 100})["fairness_key"])
        state = sample_state()
        state["deployed"]["vector_store"]["env"] = {"RUST_LOG": "info", "VECTOR_STORE_THREADS": "8"}
        self.assertNotEqual(base["fairness_key"], search_record(state=state)["fairness_key"])
        state = sample_state()
        state["nodes"][1]["instance_id"] = "i-replaced"
        self.assertNotEqual(base["fairness_key"], search_record(state=state)["fairness_key"])

    def test_key_is_order_independent(self) -> None:
        record = search_record()
        shuffled = copy.deepcopy(record)
        shuffled["versions"]["vs_env"] = dict(reversed(list({"B": "2", "A": "1"}.items())))
        record["versions"]["vs_env"] = {"A": "1", "B": "2"}
        self.assertEqual(results.fairness_key(record), results.fairness_key(shuffled))


class StorageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        patcher = mock.patch.dict(os.environ, {"VSBENCH_HOME": self.tmp.name})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.tmp.cleanup)

    def test_append_is_idempotent(self) -> None:
        record = search_record(run_id="r1")
        results.append("default", record)
        results.append("default", record)
        loaded = results.load_records("default")
        self.assertEqual([r["run_id"] for r in loaded], ["r1"])
        self.assertEqual(loaded[0]["client_metrics"]["qps"], 6953.2)

    def test_load_records_missing_and_corrupt(self) -> None:
        self.assertEqual(results.load_records("default"), [])
        results.append("default", search_record(run_id="r1"))
        results_file = Path(self.tmp.name) / "clusters" / "default" / "results" / "results.jsonl"
        with open(results_file, "a") as handle:
            handle.write("{not json\n\n")
        with mock.patch.object(results, "warn") as warn:
            results.append("default", search_record(run_id="r2"))
            loaded = results.load_records("default")
        self.assertEqual([r["run_id"] for r in loaded], ["r1", "r2"])
        warn.assert_called()

    def test_append_adds_plateau_from_series(self) -> None:
        server = {"cpu_pct": {"vs-0": 40.0, "client": 50.0}}
        first = search_record(run_id="c64", series_id="s", qps=10000.0, params={"concurrency": 64}, server=server)
        second = search_record(run_id="c128", series_id="s", qps=10100.0, params={"concurrency": 128}, server=server)
        self.assertNotIn("plateau", second["flags"])
        results.append("default", first)
        results.append("default", second)
        stored = {r["run_id"]: r for r in results.load_records("default")}
        self.assertIn("plateau", stored["c128"]["flags"])
        self.assertNotIn("plateau", stored["c64"]["flags"])

    def test_save_log(self) -> None:
        self.assertEqual(results.save_log("default", "r1", "hello\n"), "results/r1.log")
        self.assertEqual((Path(self.tmp.name) / "clusters/default/results/r1.log").read_text(), "hello\n")

    def test_record_index_build(self) -> None:
        with mock.patch.object(results.state_mod, "load", return_value=sample_state()):
            record = results.record_index_build(
                "default",
                {
                    "seconds": {"vs-0": 42.5, "vs-1": 40.0},
                    "build_id": "new-build",
                    "trigger": "deploy-vs",
                    "comparison_id": "cmp-1",
                    "arm": "B",
                    "run_id": "ib-1",
                },
            )
        self.assertEqual(record["kind"], "index-build")
        self.assertEqual((record["comparison_id"], record["arm"], record["run_id"]), ("cmp-1", "B", "ib-1"))
        self.assertEqual(record["index_build"]["seconds"], {"vs-0": 42.5, "vs-1": 40.0})
        self.assertEqual(record["versions"]["vector_store"]["build_id"], "new-build")
        self.assertEqual(results.index_build_seconds(record), 42.5)
        self.assertEqual([r["run_id"] for r in results.load_records("default")], ["ib-1"])

    def test_index_build_seconds_variants(self) -> None:
        self.assertEqual(results.index_build_seconds({"index_build": {"build_index_s": 3.03, "seconds": 9}}), 3.03)
        self.assertEqual(results.index_build_seconds({"index_build": {"seconds": 7}}), 7.0)
        self.assertEqual(results.index_build_seconds({"load": {"index_s": 120.5}}), 120.5)
        self.assertIsNone(results.index_build_seconds({}))


def variant(build: str, qps: float, conc: int = 64, kind: str = "search-cql", **extra: Any) -> dict[str, Any]:
    state = sample_state()
    state["deployed"]["vector_store"]["build_id"] = build
    params = {"concurrency": conc, **extra.pop("params", {})}
    record = search_record(state=state, qps=qps, params=params, **extra)
    return {**record, "kind": kind}


class AggregateCompareTest(unittest.TestCase):
    def test_aggregate_groups_and_stats(self) -> None:
        records = [
            variant("A", 100.0),
            variant("A", 110.0),
            variant("A", 90.0),
            variant("B", 200.0),
            variant("A", 50.0, conc=128),
        ]
        rows = results.aggregate(records)
        self.assertEqual(
            [(r["build_id"], r["concurrency"], r["n"]) for r in rows], [("A", 64, 3), ("B", 64, 1), ("A", 128, 1)]
        )
        qps = rows[0]["qps"]
        self.assertEqual((qps["median"], qps["min"], qps["max"], qps["n"]), (100.0, 90.0, 110.0, 3))
        self.assertAlmostEqual(qps["cv"], 0.1, places=4)
        self.assertIsNone(rows[1]["qps"]["cv"])
        self.assertAlmostEqual(rows[0]["client_mean_ms"]["median"], 64 * 5.0 * 1000 / 34776, places=3)
        self.assertEqual(rows[0]["recall_avg"]["median"], 63.5)
        self.assertIsNone(rows[0]["server_mean_ms"])
        self.assertEqual(rows[0]["fairness_key"], records[0]["fairness_key"])

    def test_aggregate_splits_setups_of_one_build(self) -> None:
        rows = results.aggregate([sweep_record("16", 1000.0), sweep_record("32", 1500.0), sweep_record("16", 990.0)])
        self.assertEqual([(r["n"], r["qps"]["median"]) for r in rows], [(2, 995.0), (1, 1500.0)])

    def test_compare_deltas_and_noise(self) -> None:
        records = [variant("A", 100.0), variant("B", 120.0), variant("B", 125.0), variant("A", 104.0)]
        result = results.compare(records)
        self.assertTrue(result["fairness_ok"])
        base, other = result["groups"]
        self.assertTrue(base["baseline"])
        self.assertIsNone(base["delta"]["qps"])
        self.assertEqual(other["delta"]["qps"], {"pct": round((122.5 - 102) / 102 * 100, 2), "within_noise": False})
        overlapping = results.compare(
            [variant("A", 100.0), variant("A", 110.0), variant("B", 105.0), variant("B", 115.0)]
        )
        self.assertTrue(overlapping["groups"][1]["delta"]["qps"]["within_noise"])

    def test_compare_single_runs_cannot_judge_noise(self) -> None:
        result = results.compare([variant("A", 100.0), variant("B", 110.0)])
        self.assertIsNone(result["groups"][1]["delta"]["qps"]["within_noise"])
        self.assertTrue(any("single run" in w for w in result["warnings"]))

    def test_compare_baseline_per_concurrency(self) -> None:
        records = [
            variant("A", 100.0),
            variant("A", 200.0, conc=128),
            variant("B", 110.0),
            variant("B", 180.0, conc=128),
        ]
        groups = results.compare(records)["groups"]
        deltas = {(g["build_id"], g["concurrency"]): g["delta"]["qps"] for g in groups}
        self.assertEqual(deltas[("B", 64)]["pct"], 10.0)
        self.assertEqual(deltas[("B", 128)]["pct"], -10.0)

    def test_compare_refuses_unfair(self) -> None:
        records = [variant("A", 100.0), variant("B", 110.0, params={"limit": 100})]
        with self.assertRaises(PreconditionError) as caught:
            results.compare(records)
        self.assertIn("limit", str(caught.exception))
        self.assertIn("--force", caught.exception.hint or "")
        forced = results.compare(records, force=True)
        self.assertFalse(forced["fairness_ok"])
        self.assertIn("limit", forced["differences"]["search-cql"])
        self.assertTrue(any("--force" in w for w in forced["warnings"]))

    def test_fairness_checked_per_kind(self) -> None:
        index_build = results.make_record(
            "index-build", sample_state(), {}, None, None, None, {"index_build": {"seconds": 5}}
        )
        result = results.compare([variant("A", 100.0), index_build])
        self.assertTrue(result["fairness_ok"])
        self.assertEqual(result["groups"][1]["build_s"]["median"], 5)

    def test_compare_warnings(self) -> None:
        saturated = variant("B", 90.0, server={"cpu_pct": {"client": 95.0}})
        result = results.compare([variant("A", 100.0), variant("A", 150.0), saturated])
        joined = " ".join(result["warnings"])
        self.assertIn("client_saturated", joined)
        self.assertIn("CV", joined)

    def test_compare_empty(self) -> None:
        with self.assertRaises(VsbenchError):
            results.compare([])


class FormatTest(unittest.TestCase):
    def test_format_latency(self) -> None:
        self.assertEqual(results_format.format_latency({"value": 1.0, "tag": "floored"}), "<=1.00ms")
        self.assertEqual(results_format.format_latency({"value": None, "tag": "capped"}), ">100ms")
        self.assertEqual(results_format.format_latency({"value": 10000.0, "tag": "capped"}), ">10000ms")
        self.assertEqual(
            results_format.format_latency({"value": 0.725, "tag": "bucket_interp", "bucket": [0.5, 1.0]}), "~0.725ms"
        )
        self.assertEqual(results_format.format_latency({"value": 3.2, "tag": "exact"}), "3.20ms")
        self.assertEqual(results_format.format_latency(None), "-")

    def test_format_table(self) -> None:
        rows = [{"a": "x", "b": 1.5, "c": None}, {"a": "longer", "b": 12345.0, "c": ["f1", "f2"]}]
        text = results_format.format_table(rows, ["a", ("B", "b"), "c"])
        lines = text.splitlines()
        self.assertEqual(lines[0].split(), ["a", "B", "c"])
        self.assertEqual(lines[2].split(), ["x", "1.50", "-"])
        self.assertEqual(lines[3].split(), ["longer", "12345", "f1,f2"])
        self.assertTrue(lines[2].index("1.50") > lines[3].index("12345") - 1)  # numbers right-aligned

    def test_format_table_dotted_keys(self) -> None:
        text = results_format.format_table([{"m": {"qps": 2.0}}], [("qps", "m.qps")])
        self.assertEqual(text.splitlines()[2].strip(), "2.00")

    def test_format_markdown(self) -> None:
        text = results_format.format_markdown([{"a": "x|y", "b": 2}], ["a", "b"])
        self.assertEqual(text.splitlines(), ["| a | b |", "|---|---:|", "| x\\|y | 2 |"])

    def test_summary_and_compare_rows(self) -> None:
        server = {"cpu_pct": {"vs-0": 61.0, "client": 40.0}, "vs_qps": 6900.0, "vs_mean_ms": 0.61}
        row = results_format.summary_row(search_record(run_id="r1", server=server, arm="A"))
        self.assertEqual((row["run_id"], row["label"], row["cpu"], row["build_s"]), ("r1", "A", "61/40", None))
        table = results_format.format_table([row], results_format.RESULTS_COLUMNS)
        header, _, line = table.splitlines()
        self.assertEqual(header.split()[:4], ["run_id", "kind", "conc", "build"])
        self.assertEqual(line.split()[:6], ["r1", "search-cql", "64", "1.11.0-1-gabc-12345678", "A", "6953"])
        self.assertIn("<=1.00ms", line)
        self.assertIn("61/40", line)
        result = results.compare([variant("A", 100.0), variant("A", 104.0), variant("B", 120.0), variant("B", 126.0)])
        rows = results_format.compare_rows(result)
        self.assertEqual(rows[0]["qps_d%"], "base")
        self.assertEqual(rows[1]["qps_d%"], "+20.6%")
        self.assertEqual(rows[1]["qps"], "123.0 [120.0-126.0]")
        self.assertEqual(rows[1]["recall_d%"], "+0.0% (noise)")
        self.assertEqual(rows[1]["vs_mean_ms_d%"], None)
        text = results_format.format_table(rows, results_format.COMPARE_COLUMNS)
        self.assertIn("qps_d%", text.splitlines()[0])
        self.assertIn("+20.6%", text)
        self.assertIn("| variant |", results_format.format_markdown(rows, results_format.COMPARE_COLUMNS))

    def test_single_run_delta_marker(self) -> None:
        rows = results_format.compare_rows(results.compare([variant("A", 100.0), variant("B", 110.0)]))
        self.assertEqual(rows[1]["qps_d%"], "+10.0% (n<2)")

    def test_variant_labels_show_env_differences(self) -> None:
        state = sample_state()
        state["deployed"]["vector_store"]["env"] = {"RUST_LOG": "info", "THREADS": "8"}
        records = [search_record(), search_record(state=state)]
        rows = results_format.compare_rows(results.compare(records, force=True))
        self.assertEqual(
            [r["variant"] for r in rows], ["1.11.0-1-gabc-12345678 THREADS=", "1.11.0-1-gabc-12345678 THREADS=8"]
        )

    def test_unforced_labels_are_the_build(self) -> None:
        result = results.compare([variant("A", 100.0), variant("B", 120.0)])
        self.assertEqual([g["setup"] for g in result["groups"]], [{}, {}])
        self.assertEqual([r["variant"] for r in results_format.compare_rows(result)], ["A", "B"])


def sweep_record(connections: str, qps: float) -> dict[str, Any]:
    options = {"similarity_function": "COSINE", "maximum_node_connections": connections}
    return search_record(qps=qps, index_options=options)


class ForcedCompareTest(unittest.TestCase):
    BUILD = "1.11.0-1-gabc-12345678"

    def test_index_option_sweep_shows_a_delta(self) -> None:
        records = [sweep_record("16", 1000.0), sweep_record("16", 1010.0)]
        records += [sweep_record("32", 1500.0), sweep_record("32", 1490.0)]
        with self.assertRaises(PreconditionError):
            results.compare(records)
        result = results.compare(records, force=True)
        groups = result["groups"]
        self.assertEqual([(g["n"], g["qps"]["median"]) for g in groups], [(2, 1005.0), (2, 1495.0)])
        self.assertEqual(groups[1]["delta"]["qps"], {"pct": round(490 / 1005 * 100, 2), "within_noise": False})
        self.assertEqual(groups[1]["setup"]["index_options"]["maximum_node_connections"], "32")
        rows = results_format.compare_rows(result)
        self.assertEqual(
            [r["variant"] for r in rows],
            [f"{self.BUILD} maximum_node_connections=16", f"{self.BUILD} maximum_node_connections=32"],
        )
        self.assertEqual((rows[0]["qps_d%"], rows[1]["qps_d%"]), ("base", "+48.8%"))
        self.assertFalse(any("noisy" in w for w in result["warnings"]))

    def test_scylla_image_labels_use_a_short_digest(self) -> None:
        states = [sample_state(), sample_state()]
        for state, digest in zip(states, ("e" * 64, "f" * 64), strict=True):
            state["deployed"]["scylla"]["image"] = f"scylladb/scylla-nightly@sha256:{digest}"
        records = [search_record(state=states[0], qps=100.0), search_record(state=states[1], qps=90.0)]
        rows = results_format.compare_rows(results.compare(records, force=True))
        self.assertEqual(
            [r["variant"] for r in rows],
            [
                f"{self.BUILD} scylla_image=scylladb/scylla-nightly@sha256:{'e' * 12}",
                f"{self.BUILD} scylla_image=scylladb/scylla-nightly@sha256:{'f' * 12}",
            ],
        )
        self.assertEqual(rows[1]["qps_d%"], "-10.0% (n<2)")

    def test_several_fields_and_nested_nodes(self) -> None:
        state = sample_state()
        state["deployed"]["bench"]["build_id"] = "bench-2"
        state["nodes"][1]["instance_type"] = "r8g.8xlarge"
        records = [search_record(qps=100.0), search_record(state=state, params={"limit": 100}, qps=80.0)]
        rows = results_format.compare_rows(results.compare(records, force=True))
        self.assertEqual(
            [r["variant"] for r in rows],
            [
                f"{self.BUILD} limit=10,bench_build_id=bench-1,vs.type=r8g.4xlarge",
                f"{self.BUILD} limit=100,bench_build_id=bench-2,vs.type=r8g.8xlarge",
            ],
        )
        self.assertEqual(rows[1]["qps_d%"], "-20.0% (n<2)")

    def test_build_and_setup_differ_together(self) -> None:
        records = [variant("A", 100.0, params={"limit": 10}), variant("B", 150.0, params={"limit": 100})]
        records.append(variant("B", 120.0, params={"limit": 10}))
        rows = results_format.compare_rows(results.compare(records, force=True))
        self.assertEqual([r["variant"] for r in rows], ["A limit=10", "B limit=100", "B limit=10"])
        self.assertEqual([r["qps_d%"] for r in rows], ["base", "+50.0% (n<2)", "+20.0% (n<2)"])


if __name__ == "__main__":
    unittest.main()
