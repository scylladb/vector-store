# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.prom (Prometheus API over ssh, window math, server metrics).

prom-server-window.json holds recorded-shape Prometheus responses for one 60 s window
(generated with simple rates so the expected numbers can be checked by hand):
samples every 10 s at window offsets -8, 2, ..., 62 -> in-window samples 2..52 (50 s).
- request_latency_seconds_count: vs-0 (10.0.2.20) +1000/s, vs-1 (10.0.2.21) +500/s
- request_latency_seconds_sum: vs-0 +0.6 s/s, vs-1 +0.4 s/s -> mean 50 s / 75000 = 0.6667 ms
- buckets (cumulative fraction): vs-0 0.5ms:0.2 1ms:0.9 2ms:0.99 5ms+:1;
  vs-1 0.5ms:0.2 1ms:0.8 2ms:0.99 5ms+:1 -> window counts 0.5:15000 1:65000 2:74250 5:75000
  p50: rank 37500 in (0.5,1] -> 0.5 + 0.5 * 22500/50000 = 0.725 ms
  p90: rank 67500 in (1,2]   -> 1 + 2500/9250 = 1.2703 ms
  p99: rank 74250 in (1,2]   -> 2.0 ms
- node_cpu_seconds_total busy per core: scylla-0 .9/.9, vs-0 .6/.4, vs-1 .3/.3, client .95/.75
- memory used (GiB): scylla-0 8, vs-0 32, vs-1 31.5, client 4; reactor scylla-0 42.5
- ethtool: vs-0 bw_in flat (0), client pps 10 -> 17 inside the window (7)
"""

from __future__ import annotations

import datetime
import itertools
import json
import shlex
import subprocess
import sys
import types
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import prom  # noqa: E402
from vsbenchlib.proc import PreconditionError, VsbenchError  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 10, 6, 12, 0, 0, tzinfo=UTC)

STATE = {
    "nodes": [
        {"name": "scylla-0", "role": "scylla", "index": 0, "private_ip": "10.0.1.10", "public_ip": "3.0.0.10"},
        {"name": "vs-0", "role": "vs", "index": 0, "private_ip": "10.0.2.20", "public_ip": "3.0.0.20"},
        {"name": "vs-1", "role": "vs", "index": 1, "private_ip": "10.0.2.21", "public_ip": "3.0.0.21"},
        {"name": "client", "role": "client", "index": 0, "private_ip": "10.0.3.30", "public_ip": "3.0.0.30"},
    ]
}


def completed(payload: Any, returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess[str]:
    stdout = payload if isinstance(payload, str) else json.dumps(payload)
    return subprocess.CompletedProcess(["ssh"], returncode, stdout=stdout, stderr=stderr)


def curl_request(command: str) -> tuple[str, dict[str, str]]:
    """(url, {param: value}) from the curl command prom.api() sends to the client."""
    argv = shlex.split(command)
    url = next(a for a in argv if a.startswith("http://"))
    params = {}
    for flag, value in itertools.pairwise(argv):
        if flag == "--data-urlencode":
            key, _, val = value.partition("=")
            params[key] = val
    return url, params


def series(values: list[tuple[float, float]], **labels: str) -> dict[str, Any]:
    return {"metric": labels, "values": [[t, str(v)] for t, v in values]}


class ApiTest(unittest.TestCase):
    def test_success_and_command(self) -> None:
        payload = {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "2"]}]}}
        with mock.patch.object(prom, "_run_on_client", return_value=completed(payload)) as run:
            data = prom.api("c1", "/query", {"query": 'up{job="x y"}', "time": "12.5"})
        self.assertEqual(data, payload["data"])
        cluster, command, timeout = run.call_args.args
        self.assertEqual(cluster, "c1")
        self.assertGreater(timeout, 60)
        argv = shlex.split(command)
        self.assertEqual(argv[:4], ["curl", "-sS", "-g", "-G"])
        url, params = curl_request(command)
        self.assertEqual(url, "http://127.0.0.1:9090/api/v1/query")
        self.assertEqual(params, {"query": 'up{job="x y"}', "time": "12.5"})

    def test_list_params_and_api_prefix(self) -> None:
        payload = {"status": "success", "data": ["a"]}
        with mock.patch.object(prom, "_run_on_client", return_value=completed(payload)) as run:
            prom.api("c1", "api/v1/label/__name__/values", [("match[]", '{job="vector_search"}')])
        url, params = curl_request(run.call_args.args[1])
        self.assertEqual(url, "http://127.0.0.1:9090/api/v1/label/__name__/values")
        self.assertEqual(params, {"match[]": '{job="vector_search"}'})

    def test_error_status(self) -> None:
        payload = {"status": "error", "errorType": "bad_data", "error": "parse error at char 3"}
        with mock.patch.object(prom, "_run_on_client", return_value=completed(payload)):
            with self.assertRaises(VsbenchError) as caught:
                prom.api("c1", "query", {"query": "up{"})
        self.assertIn("bad_data: parse error", str(caught.exception))

    def test_curl_failure(self) -> None:
        failed = completed("", returncode=7, stderr="curl: (7) Failed to connect to 127.0.0.1 port 9090")
        with mock.patch.object(prom, "_run_on_client", return_value=failed):
            with self.assertRaises(VsbenchError) as caught:
                prom.api("c1", "targets")
        self.assertIn("Failed to connect", str(caught.exception))
        self.assertIn("deploy monitoring", caught.exception.hint or "")

    def test_non_json(self) -> None:
        with mock.patch.object(prom, "_run_on_client", return_value=completed("<html>502</html>")):
            with self.assertRaises(VsbenchError) as caught:
                prom.api("c1", "targets")
        self.assertIn("non-JSON", str(caught.exception))

    def test_warnings_are_logged(self) -> None:
        payload = {"status": "success", "data": {}, "warnings": ["too many samples"]}
        with mock.patch.object(prom, "_run_on_client", return_value=completed(payload)):
            with mock.patch.object(prom, "warn") as warn:
                prom.api("c1", "query", {"query": "up"})
        warn.assert_called_once_with("prometheus: too many samples")

    def test_post(self) -> None:
        payload = {"status": "success", "data": {"name": "20261006T120000Z-abc"}}
        with mock.patch.object(prom, "_run_on_client", return_value=completed(payload)) as run:
            self.assertEqual(prom.api("c1", "admin/tsdb/snapshot", post=True), {"name": "20261006T120000Z-abc"})
        argv = shlex.split(run.call_args.args[1])
        self.assertEqual(argv[3:5], ["-X", "POST"])
        self.assertNotIn("-G", argv)

    def test_rejects_bad_paths(self) -> None:
        for path in ("../../-/quit", "query; rm -rf /", "query?x=1"):
            with self.assertRaises(VsbenchError):
                prom.api("c1", path)

    def test_calls_remote_run_with_spec_signature(self) -> None:
        payload = {"status": "success", "data": {"resultType": "vector", "result": []}}
        fake = types.ModuleType("vsbenchlib.remote")
        fake.run = mock.Mock(return_value=completed(payload))  # type: ignore[attr-defined]
        fake.is_ssh_failure = lambda result: result.returncode == 255  # type: ignore[attr-defined]
        with mock.patch.dict(sys.modules, {"vsbenchlib.remote": fake}):
            with mock.patch("vsbenchlib.remote", fake, create=True):
                self.assertEqual(prom.query("c1", "up"), [])
        args, kwargs = fake.run.call_args
        self.assertEqual(args[:2], ("c1", "client"))
        self.assertIn("api/v1/query", args[2])
        self.assertEqual(kwargs["check"], False)
        self.assertIn("timeout", kwargs)

    def test_ssh_failure_uses_remote_hint(self) -> None:
        fake = types.ModuleType("vsbenchlib.remote")
        fake.run = mock.Mock(return_value=completed("", returncode=255, stderr="Connection timed out"))  # type: ignore[attr-defined]
        fake.is_ssh_failure = lambda result: result.returncode == 255  # type: ignore[attr-defined]
        fake.ssh_failure_hint = mock.Mock(return_value="run vsbench refresh-ip")  # type: ignore[attr-defined]
        with mock.patch.dict(sys.modules, {"vsbenchlib.remote": fake}):
            with mock.patch("vsbenchlib.remote", fake, create=True):
                with self.assertRaises(VsbenchError) as caught:
                    prom.query("c1", "up")
        self.assertIn("ssh to client failed", str(caught.exception))
        self.assertEqual(caught.exception.hint, "run vsbench refresh-ip")
        fake.ssh_failure_hint.assert_called_once_with("c1", "Connection timed out")


class QueryTest(unittest.TestCase):
    def test_query_scalar_and_time(self) -> None:
        payload = {"status": "success", "data": {"resultType": "scalar", "result": [100, "3"]}}
        with mock.patch.object(prom, "_run_on_client", return_value=completed(payload)) as run:
            with mock.patch.object(prom, "_now", return_value=NOW):
                result = prom.query("c1", "1+2", time="-1m")
        self.assertEqual(result, [{"metric": {}, "value": [100, "3"]}])
        self.assertEqual(curl_request(run.call_args.args[1])[1]["time"], f"{NOW.timestamp() - 60:.3f}")

    def test_query_range(self) -> None:
        payload = {"status": "success", "data": {"resultType": "matrix", "result": [series([(1, 1)], job="a")]}}
        with mock.patch.object(prom, "_run_on_client", return_value=completed(payload)) as run:
            with mock.patch.object(prom, "_now", return_value=NOW):
                result = prom.query_range("c1", "up", "now-15m", "now", "15s")
        self.assertEqual(len(result), 1)
        url, params = curl_request(run.call_args.args[1])
        self.assertEqual(url, "http://127.0.0.1:9090/api/v1/query_range")
        self.assertEqual(params["start"], f"{NOW.timestamp() - 900:.3f}")
        self.assertEqual(params["end"], f"{NOW.timestamp():.3f}")
        self.assertEqual(params["step"], "15s")

    def test_query_range_rejects_reversed(self) -> None:
        with self.assertRaises(VsbenchError):
            prom.query_range("c1", "up", "now", "-15m", "15s")


class ParseTimeTest(unittest.TestCase):
    def test_forms(self) -> None:
        now_s = NOW.timestamp()
        self.assertEqual(prom.parse_time("now", NOW), now_s)
        self.assertEqual(prom.parse_time("-15m", NOW), now_s - 900)
        self.assertEqual(prom.parse_time("now-1h", NOW), now_s - 3600)
        self.assertEqual(prom.parse_time("now - 90s", NOW), now_s - 90)
        self.assertEqual(prom.parse_time("+2d", NOW), now_s + 172800)
        self.assertEqual(prom.parse_time("2026-10-06T12:00:00Z", NOW), now_s)
        self.assertEqual(prom.parse_time("2026-10-06T14:00:00.5+02:00", NOW), now_s + 0.5)
        self.assertEqual(prom.parse_time("1791280800", NOW), 1791280800.0)
        self.assertEqual(prom.parse_time("1791280800.25", NOW), 1791280800.25)

    def test_invalid(self) -> None:
        for text in ("yesterday", "-15x", "12"):
            with self.assertRaises(VsbenchError):
                prom.parse_time(text, NOW)

    def test_hint_never_suggests_a_value_starting_with_a_dash(self) -> None:
        # before Python 3.14, argparse reads `--start -15m` as two options
        with self.assertRaises(VsbenchError) as caught:
            prom.parse_time("yesterday", NOW)
        hint = caught.exception.hint or ""
        self.assertIn("now-15m", hint)
        self.assertNotRegex(hint, r"(^|[\s,(])-\d")
        self.assertEqual(prom.parse_time("now-15m", NOW), NOW.timestamp() - 900)


class CounterDeltaTest(unittest.TestCase):
    def test_window_selection(self) -> None:
        data = [series([(95, 0), (100, 10), (110, 30), (120, 60), (125, 100)], instance="a")]
        self.assertEqual(prom.counter_delta(data, 100, 120), [({"instance": "a"}, 50.0, 20.0)])

    def test_counter_reset(self) -> None:
        data = [series([(0, 100), (10, 150), (20, 30), (30, 80)], instance="a")]
        # 100 -> 150 (+50), reset -> 30 (+30), 30 -> 80 (+50)
        self.assertEqual(prom.counter_delta(data, 0, 30), [({"instance": "a"}, 130.0, 30.0)])

    def test_too_few_samples(self) -> None:
        data = [series([(0, 1), (10, 2)], instance="a"), series([(5, 1)], instance="b")]
        self.assertEqual(prom.counter_delta(data, 5, 10), [])
        self.assertEqual(prom.counter_delta([], 0, 10), [])


class HistogramQuantilesTest(unittest.TestCase):
    def buckets(self, counts: dict[str, float], instance: str = "a") -> list[dict[str, Any]]:
        return [series([(0, 0), (60, count)], le=le, instance=instance) for le, count in counts.items()]

    def test_interpolation(self) -> None:
        data = self.buckets({"0.0005": 20, "0.001": 90, "0.002": 100, "+Inf": 100})
        result = prom.histogram_quantiles(data, 0, 60, qs=(0.1, 0.5, 0.95))
        self.assertEqual(result["p10"], {"value": 0.25, "tag": "bucket_interp", "bucket": [0.0, 0.5]})
        self.assertAlmostEqual(result["p50"]["value"], 0.5 + 0.5 * 30 / 70, places=4)
        self.assertEqual(result["p50"]["bucket"], [0.5, 1.0])
        self.assertEqual(result["p95"], {"value": 1.5, "tag": "bucket_interp", "bucket": [1.0, 2.0]})

    def test_sums_instances(self) -> None:
        data = self.buckets({"0.001": 10, "0.002": 10, "+Inf": 10}, "a")
        data += self.buckets({"0.001": 0, "0.002": 10, "+Inf": 10}, "b")
        result = prom.histogram_quantiles(data, 0, 60, qs=(0.5,))
        self.assertEqual(result["p50"], {"value": 1.0, "tag": "bucket_interp", "bucket": [0.0, 1.0]})

    def test_capped_in_inf_bucket(self) -> None:
        data = self.buckets({"5": 1, "10": 2, "+Inf": 100})
        self.assertEqual(
            prom.histogram_quantiles(data, 0, 60, qs=(0.99,))["p99"],
            {"value": 10000.0, "tag": "capped", "bucket": [10000.0, None]},
        )

    def test_no_observations(self) -> None:
        self.assertEqual(prom.histogram_quantiles(self.buckets({"0.001": 0, "+Inf": 0}), 0, 60), {})
        self.assertEqual(prom.histogram_quantiles([], 0, 60), {})

    def test_non_monotonic_buckets_are_fixed(self) -> None:
        data = self.buckets({"0.001": 50, "0.002": 40, "0.005": 100, "+Inf": 100})
        result = prom.histogram_quantiles(data, 0, 60, qs=(0.5,))
        self.assertEqual(result["p50"]["bucket"], [0.0, 1.0])


class ServerMetricsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = json.loads((FIXTURES / "prom-server-window.json").read_text())
        self.start = datetime.datetime.fromtimestamp(self.fixture["start"], UTC)
        self.end = datetime.datetime.fromtimestamp(self.fixture["end"], UTC)
        self.requests: list[dict[str, str]] = []

    def respond(self, cluster: str, command: str, timeout: float) -> subprocess.CompletedProcess[str]:
        _, params = curl_request(command)
        self.requests.append(params)
        for key, response in self.fixture["responses"].items():
            if key in params["query"]:
                return completed(response)
        return completed({"status": "success", "data": {"resultType": "vector", "result": []}})

    def run_metrics(self, now: datetime.datetime) -> tuple[dict[str, Any], mock.Mock]:
        with mock.patch.object(prom, "_run_on_client", side_effect=self.respond):
            with mock.patch.object(prom, "_now", return_value=now):
                with mock.patch.object(prom.time, "sleep") as sleep:
                    with mock.patch.object(prom, "log"):
                        result = prom.server_metrics(
                            "c1", self.start, self.end, "vsb_keyspace", "vsb_idx_20261006095000", STATE
                        )
        return result, sleep

    def test_full_window(self) -> None:
        result, sleep = self.run_metrics(self.end + datetime.timedelta(minutes=5))
        sleep.assert_not_called()
        self.assertEqual(result["vs_qps"], 1500.0)
        self.assertEqual(result["vs_qps_by_node"], {"vs-0": 1000.0, "vs-1": 500.0})
        self.assertEqual(result["vs_mean_ms"], 0.6667)
        latency = result["vs_latency_ms"]
        self.assertEqual(latency["p50"], {"value": 0.725, "tag": "bucket_interp", "bucket": [0.5, 1.0]})
        self.assertEqual(latency["p90"], {"value": 1.2703, "tag": "bucket_interp", "bucket": [1.0, 2.0]})
        self.assertEqual(latency["p99"], {"value": 2.0, "tag": "bucket_interp", "bucket": [1.0, 2.0]})
        self.assertEqual(result["cpu_pct"], {"scylla-0": 90.0, "vs-0": 50.0, "vs-1": 30.0, "client": 85.0})
        self.assertEqual(result["cpu_max_core_pct"], {"scylla-0": 90.0, "vs-0": 60.0, "vs-1": 30.0, "client": 95.0})
        self.assertEqual(result["mem_used_gb"], {"scylla-0": 8.0, "vs-0": 32.0, "vs-1": 31.5, "client": 4.0})
        self.assertEqual(result["scylla_reactor_pct"], {"scylla-0": 42.5})
        self.assertEqual(result["net_allowance_exceeded"], {"vs-0": 0, "client": 7})
        self.assertEqual(result["bottleneck"], {"node": "client", "pct": 85.0})
        self.assertEqual(result["missing"], [])

    def test_queries_use_window_selectors(self) -> None:
        self.run_metrics(self.end + datetime.timedelta(minutes=5))
        by_query = {r["query"]: r for r in self.requests}
        count = 'request_latency_seconds_count{keyspace="vsb_keyspace",index_name="vsb_idx_20261006095000"}[80s]'
        self.assertIn(count, by_query)
        self.assertEqual(by_query[count]["time"], f"{self.fixture['end'] + 10:.3f}")
        memory = "node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes"
        self.assertEqual(by_query[memory]["time"], f"{self.fixture['end']:.3f}")
        reactor = "avg by (instance) (avg_over_time(scylla_reactor_utilization[60s]))"
        self.assertIn(reactor, by_query)

    def test_waits_for_final_scrape(self) -> None:
        _, sleep = self.run_metrics(self.end)
        sleep.assert_called_once_with(15.0)

    def test_accepts_log_timestamps(self) -> None:
        with mock.patch.object(prom, "_run_on_client", side_effect=self.respond):
            with mock.patch.object(prom, "_now", return_value=self.end + datetime.timedelta(minutes=5)):
                result = prom.server_metrics(
                    "c1",
                    "2026-10-06T10:00:00.000000Z",
                    "2026-10-06T10:01:00.000000Z",
                    "vsb_keyspace",
                    "vsb_idx_20261006095000",
                    STATE,
                )
        self.assertEqual(result["vs_qps"], 1500.0)

    def test_short_window(self) -> None:
        with self.assertRaises(VsbenchError) as caught:
            prom.server_metrics("c1", self.start, self.start + datetime.timedelta(seconds=20), "ks", "idx", STATE)
        self.assertIn("short window", str(caught.exception))

    def test_future_window_end(self) -> None:
        with mock.patch.object(prom, "_now", return_value=self.end - datetime.timedelta(minutes=10)):
            with self.assertRaises(VsbenchError) as caught:
                prom.server_metrics("c1", self.start, self.end, "ks", "idx", STATE)
        self.assertIn("future", str(caught.exception))

    def test_missing_families(self) -> None:
        empty = completed({"status": "success", "data": {"resultType": "matrix", "result": []}})
        with mock.patch.object(prom, "_run_on_client", return_value=empty):
            with mock.patch.object(prom, "_now", return_value=self.end + datetime.timedelta(minutes=5)):
                result = prom.server_metrics("c1", self.start, self.end, "ks", "idx", STATE)
        self.assertIsNone(result["vs_qps"])
        self.assertIsNone(result["bottleneck"])
        self.assertEqual(len(result["missing"]), 5)
        self.assertIn("request_latency_seconds", result["missing"])


class WindowNodeTest(unittest.TestCase):
    def test_instance_mapping(self) -> None:
        win = prom._Window("c1", 0, 60, prom._node_names(STATE))
        self.assertEqual(win.node({"instance": "10.0.2.20"}), "vs-0")
        self.assertEqual(win.node({"instance": "10.0.3.30:9100"}), "client")
        self.assertEqual(win.node({"instance": "3.0.0.10:9180"}), "scylla-0")
        self.assertEqual(win.node({"instance": "10.9.9.9:9100"}), "10.9.9.9")
        self.assertEqual(win.node({}), "?")


class FormatVectorTest(unittest.TestCase):
    def test_vector(self) -> None:
        result = [
            {"metric": {"__name__": "up", "job": "b", "instance": "10.0.0.2"}, "value": [1, "0"]},
            {"metric": {"__name__": "up", "job": "a", "instance": "10.0.0.1"}, "value": [1, "1"]},
        ]
        self.assertEqual(
            prom.format_vector(result).splitlines(),
            ['up{instance="10.0.0.1", job="a"} => 1', 'up{instance="10.0.0.2", job="b"} => 0'],
        )

    def test_matrix_and_scalar(self) -> None:
        values = [(1791280800 + 10 * i, i) for i in range(20)]
        text = prom.format_vector([series(values, job="a")])
        self.assertTrue(text.startswith('{job="a"} => min=0 avg=9.5 max=19 first=0 | last 12: 8 9 10'), text)
        self.assertIn(" 19 (n=20, 2026-10-06T10:00:00.000Z..2026-10-06T10:03:10.000Z)", text)
        self.assertEqual(prom.format_vector([{"metric": {}, "value": [1, "3"]}]), "{} => 3")
        self.assertEqual(prom.format_vector([]), "(empty result)")

    def test_short_matrix_prints_every_value(self) -> None:
        values = [(1791280800 + 15 * i, i) for i in range(prom.TAIL_POINTS)]
        text = prom.format_vector([series(values, job="a")])
        self.assertEqual(text, '{job="a"} => 0 1 2 3 4 5 6 7 8 9 10 11 (n=12, ' + text.split("(n=12, ")[1])
        self.assertNotIn("min=", text)

    def test_long_range_summary_shows_an_early_dip(self) -> None:
        # the documented `--start now-15m --step 15s` gives 61 points; a dip in the first 11 is outside the tail
        values = [(1791280800 + 15 * i, 200 if i < 11 else 1000) for i in range(61)]
        text = prom.format_vector([series(values, instance="vs-0")])
        self.assertIn("min=200 avg=", text)
        self.assertIn("max=1000 first=200 | last 12: 1000", text)
        self.assertIn("(n=61, 2026-10-06T10:00:00.000Z..2026-10-06T10:15:00.000Z)", text)
        avg = float(text.split("avg=")[1].split()[0])
        self.assertAlmostEqual(avg, (11 * 200 + 50 * 1000) / 61, places=2)

    def test_range_summary_skips_non_finite_values(self) -> None:
        values = [[1791280800 + i, v] for i, v in enumerate(["NaN", "+Inf", "1.5", "2.5"] + ["2"] * 10)]
        text = prom.format_vector([{"metric": {"job": "a"}, "values": values}])
        self.assertIn("min=1.5 avg=2 max=2.5 first=NaN non_finite=2 |", text)
        only_nan = [[1791280800 + i, "NaN"] for i in range(13)]
        self.assertIn("no finite values, first=NaN |", prom.format_vector([{"metric": {}, "values": only_nan}]))


def vector_payload(items: list[tuple[dict[str, str], str]]) -> dict[str, Any]:
    result = [{"metric": metric, "value": [1_700_000_000, value]} for metric, value in items]
    return {"status": "success", "data": {"resultType": "vector", "result": result}}


class IndexStatusTest(unittest.TestCase):
    """prom.index_status: index rows vs the rows vsbench put into the table, CDC lags vs their baselines."""

    STATE = {
        "load": {"index": "vsb_idx_1", "keyspace": "vsb_keyspace", "rows": 1_000_000, "churn_rows": 225_000},
        "deployed": {"monitoring": {"version": "4.16.1"}},
    }

    def responder(self, size: str | None, fine: str, wide: str):
        def respond(cluster: str, command: str, timeout: float) -> subprocess.CompletedProcess[str]:
            _url, params = curl_request(command)
            promql = params["query"]
            self.assertIn('keyspace="vsb_keyspace",index_name="vsb_idx_1"', promql)
            if promql.startswith("max(index_size"):
                return completed(vector_payload([({}, size)] if size is not None else []))
            if "cdc_last_processed_timestamp_seconds" in promql:
                return completed(vector_payload([({"reader": "fine"}, fine), ({"reader": "wide"}, wide)]))
            self.assertTrue(promql.startswith("cdc_reader_up"))
            return completed(vector_payload([({"reader": "fine"}, "1"), ({"reader": "wide"}, "1")]))

        return respond

    def test_behind_names_the_missing_rows_and_the_lagging_reader(self) -> None:
        with mock.patch.object(prom, "_run_on_client", side_effect=self.responder("1100000", "12.5", "150.2")):
            doc = prom.index_status("c1", self.STATE)
        self.assertEqual((doc["index_size"], doc["base_rows"], doc["missing"]), (1_100_000, 1_225_000, 125_000))
        self.assertEqual((doc["fine_lag_s"], doc["wide_lag_s"], doc["wide_reader_up"]), (12.5, 150.2, 1.0))
        self.assertEqual(doc["verdict"], "behind: 125000 rows not in the index, wide lag 150s (idle ~46s)")

    def test_in_sync_tolerates_the_idle_lags(self) -> None:
        state = {**self.STATE, "load": {**self.STATE["load"], "churn_rows": 0}}
        with mock.patch.object(prom, "_run_on_client", side_effect=self.responder("1000000", "11.0", "47.0")):
            doc = prom.index_status("c1", state)
        self.assertEqual((doc["missing"], doc["verdict"]), (0, "in sync"))

    def test_unknown_without_a_scraped_size_and_extra_rows(self) -> None:
        with mock.patch.object(prom, "_run_on_client", side_effect=self.responder(None, "11.0", "47.0")):
            doc = prom.index_status("c1", self.STATE)
        self.assertIsNone(doc["index_size"])
        self.assertTrue(doc["verdict"].startswith("unknown"))
        with mock.patch.object(prom, "_run_on_client", side_effect=self.responder("1225002", "11.0", "47.0")):
            doc = prom.index_status("c1", self.STATE)
        self.assertEqual(doc["verdict"], "2 rows in the index beyond vsbench's count (rows written outside vsbench?)")

    def test_no_load_and_no_monitoring(self) -> None:
        self.assertEqual(prom.index_status("c1", {}), {"note": "no load"})
        with self.assertRaises(PreconditionError):
            prom.index_status("c1", {"load": self.STATE["load"]})


if __name__ == "__main__":
    unittest.main()
