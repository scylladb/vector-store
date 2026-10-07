# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.bench_jobs and the bench.py fixes made with it: the racks contract with
deploy, built_index, failed-job reporting, foreground budgets, `bench ab` options/TTL/result and
`bench rerun` drift warnings. No network, AWS or ssh: remote/prom/deploy/build are mocked."""

from __future__ import annotations

import datetime
import sys
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests.test_bench import (  # noqa: E402
    JOB,
    HomeTestCase,
    fixture,
    index_log,
    make_state,
    progress,
    search_params,
    section,
)
from vsbenchlib import bench, bench_jobs, deploy, remote, results  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import PreconditionError, VsbenchError  # noqa: E402

BUILDS = {
    "release:1.11.0": {"build_id": "release-1.11.0", "pin": "release:1.11.0", "version": "1.11.0"},
    "local": {"build_id": "x-1", "pin": "build:x-1", "version": "1.12.0-dev"},
}


def expiring_in(minutes: int) -> str:
    moment = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=minutes)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class ReExportTest(HomeTestCase):
    def test_moved_names_stay_importable_from_bench(self) -> None:
        for name in ("finalize_job", "job_wait", "step_line", "job_script", "options_mismatch", "_ensure_idle"):
            with self.subTest(name=name):
                self.assertIs(getattr(bench, name), getattr(bench_jobs, name))
        self.assertEqual(
            (bench.KEYSPACE, bench.CLIENT, bench.PHASES), (bench_jobs.KEYSPACE, "client", bench_jobs.PHASES)
        )


class RackContractTest(HomeTestCase):
    def test_check_rf_reads_the_racks_deploy_scylla_writes(self) -> None:
        state = make_state(scylla=3)
        state["deployed"].update(scylla=None, monitoring=None)
        self.save(state)
        image = ("scylladb/scylla@sha256:" + "aa" * 32, "2026.2.0")
        with (
            mock.patch.object(deploy, "require_no_bench_job"),
            mock.patch.object(deploy, "resolve_scylla_image", return_value=image),
            mock.patch.object(deploy, "_scylla_rollout"),
            mock.patch.object(deploy, "_scylla_version", return_value="2026.2.0"),
        ):
            deployed = deploy.deploy_scylla("t", None, False, False)
        self.assertIsInstance(deployed["deployed"]["scylla"]["racks"], dict)
        bench.check_rf(deployed, 1)
        bench.check_rf(deployed, 3)
        with self.assertRaises(PreconditionError) as ctx:
            bench.check_rf(deployed, 2)
        self.assertIn("3 rack(s)", str(ctx.exception))

    def test_rack_count_shapes(self) -> None:
        state = make_state(scylla=4)
        self.assertEqual(bench.rack_count(state), 4)  # nothing recorded: one rack per node
        for racks, expected in (({"scylla-0": "rack1", "scylla-1": "rack1"}, 1), (2, 2), ({}, 4), (True, 4)):
            state["deployed"]["scylla"]["racks"] = racks
            with self.subTest(racks=racks):
                self.assertEqual(bench.rack_count(state), expected)


class BuiltIndexTest(HomeTestCase):
    def test_search_needs_a_built_index(self) -> None:
        state = make_state()
        state["load"]["index"] = None  # phases say built, but no index exists
        with self.assertRaises(PreconditionError) as ctx:
            bench._check_search_load(state, bench.SearchOptions("cql"))
        self.assertIn("not built", str(ctx.exception))


class FailedJobTest(HomeTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.follow = self.patch(remote, "job_follow")
        self.poll = self.patch(remote, "job_poll")
        self.log = self.patch(remote, "job_log", return_value="")

    def test_job_wait_raises_for_a_job_already_finalized_as_failed(self) -> None:
        cases = [
            (
                {"exit_code": 1, "failed_step": "build-table", "error": "boom"},
                "failed in step build-table (exit 1): boom",
            ),
            ({"error": "the job is not on the node"}, "failed without an exit code: the job is not on the node"),
            ({"exit_code": 0, "error": "Vector Store ignored index options (x)"}, "ignored index options"),
        ]
        for number, (extra, text) in enumerate(cases):
            job_id = f"J{number}"
            self.add_job(job_id, "load", {}, **{**extra, "status": "failed"})
            with self.subTest(text=text), self.assertRaises(VsbenchError) as ctx:
                bench.job_wait("t", job_id, 60)
            self.assertIn(text, str(ctx.exception))
            self.assertIn(f"job logs {job_id}", ctx.exception.hint or "")
        self.follow.assert_not_called()

    def test_job_wait_on_a_finalized_job_returns_its_records(self) -> None:
        self.add_job("J", "search", {}, status="finalized", exit_code=0, run_ids=["J-c64-r1"])
        results.append("t", {"run_id": "J-c64-r1", "kind": "search-cql"})
        self.assertEqual([r["run_id"] for r in bench.job_wait("t", "J", 60)], ["J-c64-r1"])

    def test_repeated_job_wait_fails_every_time(self) -> None:
        self.add_job(JOB, "search", search_params(self.state))
        panic = section("search-c64-r1", fixture("search-cql-panic.log"), 101)
        self.log.return_value = section("warmup-c64-r1", fixture("search-cql.log")) + panic
        self.poll.return_value, self.follow.return_value = progress(101), 101
        self.patch(bench.prom, "server_metrics", return_value={})
        for attempt in (1, 2):
            with self.subTest(attempt=attempt), self.assertRaises(VsbenchError) as ctx:
                bench.job_wait("t", JOB, 60)
            self.assertIn("in step search-c64-r1 (exit 101)", str(ctx.exception))
            self.assertIn("--local-index", ctx.exception.hint or "")

    def test_ensure_idle_warns_about_failed_earlier_jobs(self) -> None:
        self.add_job("J-fetch", "fetch", {"dataset": "cohere-100k"})
        self.poll.return_value = progress(1)
        self.log.return_value = section("fetch", "error: curl: (22) 404", 1)
        bench._ensure_idle("t")
        self.assertIn("J-fetch (fetch) failed in step fetch", self.warn.call_args[0][0])
        self.add_job("J-gone", "load", {})
        self.poll.side_effect = remote.JobNotFound("no such job")
        bench._ensure_idle("t")
        self.assertIn("J-gone (load) failed: the job is not on the node", self.warn.call_args[0][0])
        self.assertEqual(st.load("t")["jobs"]["J-gone"]["status"], "failed")


class JobCommandCase(HomeTestCase):
    """Remote and Prometheus mocked like test_bench.CommandTest."""

    def setUp(self) -> None:
        super().setUp()
        self.patch(remote, "ensure_script")
        self.patch(remote, "upload")
        self.patch(remote, "job_start")
        self.follow = self.patch(remote, "job_follow", return_value=0)
        self.poll = self.patch(remote, "job_poll", return_value=progress(0))
        self.log = self.patch(remote, "job_log", return_value="")
        self.patch(bench.prom, "server_metrics", return_value={"vs_qps": 1.0})
        self.fake_deploy()

    def clock(self, start: float, at_follow: float) -> None:
        """The command starts at `start`; job_wait/follow_budget run at `at_follow` (monotonic seconds)."""
        self.patch(bench, "time", new=mock.Mock(monotonic=mock.Mock(return_value=start)))
        self.patch(bench_jobs, "time", new=mock.Mock(monotonic=mock.Mock(return_value=at_follow)))


class BudgetTest(JobCommandCase):
    def test_follow_budget(self) -> None:
        self.clock(0.0, 100.0)
        reserve = bench.FINALIZE_RESERVE_S
        self.assertEqual(bench.follow_budget(540, 100.0), 540 - reserve)
        self.assertEqual(bench.follow_budget(540, 0.0), 540 - reserve - 100)
        self.assertEqual(bench.follow_budget(540, -1000.0), 1)
        self.assertEqual(bench.follow_budget(40, 100.0), 30)  # small budgets keep most of it

    def test_commands_follow_for_their_budget_from_the_start_minus_the_reserve(self) -> None:
        self.patch(remote, "http_json", return_value={"options": {"similarity_function": "COSINE"}})
        expected = 540 - bench.FINALIZE_RESERVE_S - 200
        commands = {
            "search": lambda: bench.search("t", bench.SearchOptions("cql", timeout_s=540)),
            "index": lambda: bench.index("t", bench.IndexOptions(timeout_s=540)),
            "fetch": lambda: bench.fetch("t", "cohere-100k", 540),
            "raw": lambda: bench.raw("t", ["--version"], 540),
            "load": lambda: bench.load("t", bench.LoadOptions("cohere-100k", resume=True, timeout_s=540)),
        }
        logs = {"search": section("search-c64-r1", fixture("search-cql.log")), "index": index_log()}
        logs["load"] = index_log()
        for name, run in commands.items():
            with self.subTest(command=name):
                self.clock(1000.0, 1200.0)  # 200 s pass before the job is followed
                self.log.return_value = logs.get(name, "")
                if name == "load":  # resume the index phase only
                    state = st.load("t")
                    state["load"]["phases"]["index"] = None
                    self.save(state)
                run()
                self.assertEqual(self.follow.call_args[0][3], expected)

    def test_job_wait_alone_keeps_the_reserve(self) -> None:
        self.add_job(JOB, "fetch", {"dataset": "cohere-100k"})
        self.clock(0.0, 0.0)
        bench.job_wait("t", JOB, 540)
        self.assertEqual(self.follow.call_args[0][3], 540 - bench.FINALIZE_RESERVE_S)


class AbTest(JobCommandCase):
    def run_ab(self, opts: bench.AbOptions, on_search: Any = None) -> tuple[dict[str, Any], list[Any]]:
        searches: list[bench.SearchOptions] = []

        def search(cluster: str, options: bench.SearchOptions) -> list[dict[str, Any]]:
            searches.append(options)
            if on_search:
                on_search(len(searches))
            return [{"arm": options.arm}]

        fake = self.fake_deploy()
        fake.deploy_vs = lambda cluster, source, *args, record_extra=None: {}
        self.patch(bench.build, "build", side_effect=lambda spec: BUILDS[str(spec)])
        self.patch(bench, "search", side_effect=search)
        return bench.ab("t", opts), searches

    def local_index(self) -> None:
        state = st.load("t")
        state["load"]["local_index"] = True
        self.save(state)

    def test_bucket_extra_args_and_timeout_reach_every_search(self) -> None:
        self.local_index()
        opts = bench.AbOptions(a="release:1.11.0", b="local", bucket=2, extra_args=("--x",), timeout_s=5000)
        _result, searches = self.run_ab(opts)
        self.assertEqual(len(searches), 4)
        self.assertEqual({(o.bucket, o.extra_args, o.timeout_s) for o in searches}, {(2, ("--x",), 5000)})

    def test_search_timeout_is_at_least_what_a_search_needs(self) -> None:
        needed = bench.search_seconds(bench.SearchOptions()) + bench.STEP_SLACK_S
        for timeout in (None, 10):
            with self.subTest(timeout=timeout):
                _result, searches = self.run_ab(bench.AbOptions(a="release:1.11.0", b="local", timeout_s=timeout))
                self.assertEqual({o.timeout_s for o in searches}, {needed})

    def test_options_are_validated_like_search(self) -> None:
        refused = [
            (bench.AbOptions(a="release:1.11.0", b="local", bucket=2), PreconditionError, "needs a local index"),
            (bench.AbOptions(a="release:1.11.0", b="local", extra_args=("--limit=5",)), VsbenchError, "set by"),
            (bench.AbOptions(a="release:1.11.0", b="local", timeout_s=0), VsbenchError, "--timeout"),
            (bench.AbOptions(a="nope:1", b="local"), VsbenchError, "invalid source"),
        ]
        for opts, error, text in refused:
            with self.subTest(text=text), self.assertRaises(error) as ctx:
                self.run_ab(opts)
            self.assertIn(text, str(ctx.exception))
        self.local_index()
        with self.assertRaises(PreconditionError) as ctx:
            self.run_ab(bench.AbOptions(a="release:1.11.0", b="local"))
        self.assertIn("--bucket N", str(ctx.exception))

    def test_ttl_estimate_counts_builds_switches_and_recorded_rebuilds(self) -> None:
        record = {"run_id": "IB", "kind": "index-build", "dataset": "cohere-100k", "load_run_id": "L1"}
        results.append("t", {**record, "index_build": {"build_index_s": 1200.0}, "exit": 0})
        state = st.load("t")
        state["expires_at"] = expiring_in(70)  # the old 4 x (search + 600 s) estimate fit in this
        self.save(state)
        with self.assertRaises(PreconditionError) as ctx:
            self.run_ab(bench.AbOptions(a="release:1.11.0", b="local"))
        message = str(ctx.exception)
        self.assertIn("builds 15m", message)
        self.assertIn("3 build switch(es) x 22m", message)  # 120 s deploy + 1200 s recorded rebuild
        self.assertIn("extend --ttl", ctx.exception.hint or "")
        bench.build.build.assert_not_called()  # refused before building anything

    def test_rebuild_estimate(self) -> None:
        load = make_state()["load"]
        self.assertEqual(bench.rebuild_seconds("t", load), bench.MIN_REBUILD_S)  # 100k rows, nothing recorded
        self.assertEqual(bench.rebuild_seconds("t", {**load, "rows": 10_000_000}), 6000)
        for run_id, load_run, seconds in (("a", "L0", 100), ("b", "L1", 300), ("c", "L1", 500), ("d", "L1", 700)):
            block = {"build_index_s": seconds}
            record = {"run_id": run_id, "kind": "index-build", "dataset": "cohere-100k", "load_run_id": load_run}
            results.append("t", {**record, "index_build": block, "exit": 0})
        results.append("t", {"run_id": "e", "kind": "index-build", "dataset": "other", "index_build": {"build_s": 9}})
        self.assertEqual(bench.rebuild_seconds("t", load), 500)  # median of this load's builds
        self.assertEqual(bench.rebuild_seconds("t", {**load, "run_id": "L9"}), 400)  # else of the dataset

    def test_ab_switches(self) -> None:
        order = ["A", "B", "B", "A"]
        self.assertEqual(bench.ab_switches(order), [True, True, False, True])
        self.assertEqual(bench.ab_switches(order, {"A": "a", "B": "b"}, "a"), [False, True, False, True])
        self.assertEqual(bench.ab_switches(order, {"A": "x", "B": "x"}, "x"), [False] * 4)

    def test_stops_before_a_run_that_would_outlive_the_cluster(self) -> None:
        def expire_soon(count: int) -> None:
            state = st.load("t")
            state["expires_at"] = expiring_in(5)
            st.save("t", state)

        with self.assertRaises(PreconditionError) as ctx:
            self.run_ab(bench.AbOptions(a="release:1.11.0", b="local"), on_search=expire_soon)
        self.assertIn("bench ab stopped before run 2/4", str(ctx.exception))
        self.assertIn("results compare", ctx.exception.hint or "")
        self.assertEqual(bench.search.call_count, 1)

    def test_result_says_which_arm_stays_deployed(self) -> None:
        for repeat, arm, pin in ((2, "A", "release:1.11.0"), (1, "B", "build:x-1")):
            with self.subTest(repeat=repeat):
                result, _searches = self.run_ab(bench.AbOptions(a="release:1.11.0", b="local", repeat=repeat))
                self.assertEqual((result["deployed"]["arm"], result["deployed"]["pin"]), (arm, pin))
                logged = " ".join(str(call.args[0]) for call in bench.proc.log.call_args_list)
                self.assertIn(f"arm {arm} ({pin}) stays deployed", logged)


class RerunDriftTest(JobCommandCase):
    def record(self, **changes: Any) -> dict[str, Any]:
        params = {"limit": 10, "duration_s": 60, "warmup_s": 30, "concurrency": 64, "bucket": None, "extra_args": []}
        record = {"run_id": "R1", "kind": "search-cql", "label": "x", "params": params, "dataset": "cohere-100k"}
        record.update(load_run_id="L1", index={"options": {"similarity_function": "COSINE"}})
        return {**record, **changes}

    def rerun(self, record: dict[str, Any]) -> None:
        results.append("t", record)
        self.patch(bench, "search", return_value=[])
        bench.rerun("t", record["run_id"], 60)

    def test_same_setup_does_not_warn(self) -> None:
        self.rerun(self.record())
        self.warn.assert_not_called()

    def test_different_setup_warns(self) -> None:
        options = {"similarity_function": "COSINE", "maximum_node_connections": 32}
        cases = [
            (self.record(run_id="R2", dataset="cohere-1m"), "dataset cohere-1m -> cohere-100k"),
            (self.record(run_id="R3", load_run_id="L0"), "load run L0 -> L1"),
            (self.record(run_id="R4", index={"options": options}), "maximum_node_connections 32 -> (none)"),
        ]
        for record, text in cases:
            with self.subTest(text=text):
                self.rerun(record)
                self.assertIn(text, self.warn.call_args[0][0])
                self.assertIn("not a like-for-like repeat", self.warn.call_args[0][0])
        state = st.load("t")
        state["load"]["index_options"] = {"similarity_function": "COSINE", "maximum_node_connections": 16}
        self.save(state)
        self.rerun(self.record(run_id="R5", index={"options": options}))
        self.assertIn("maximum_node_connections 32 -> 16", self.warn.call_args[0][0])


class ProfileTest(HomeTestCase):
    """`bench search --perf NODE`: captures start when a measured step begins; finalize pulls them."""

    def setUp(self) -> None:
        super().setUp()
        self.state = make_state(scylla=1, vs=1)
        st.save("t", self.state)
        self.save = lambda s: st.save("t", s)
        st.save("t", bench._with_job(st.load("t"), JOB, {"kind": "search", "node": "client", "params": {}}))
        self.patch(remote, "ensure_script", side_effect=lambda c, n, s: f"/var/lib/vsbench/scripts/{s}")
        self.start = self.patch(remote, "job_start")
        self.patch(remote, "new_job_id", side_effect=[f"p-{i}" for i in range(10)])
        self.patch(bench.proc, "log")
        self.warn = self.patch(bench.proc, "warn")

    def test_trigger_starts_captures_on_measured_steps_only(self) -> None:
        opts = bench.SearchOptions("cql", duration_s=60, perf=("vs-0", "scylla-0"))
        plan = bench.search_plan(opts)
        on_step = bench.profile_trigger("t", JOB, st.load("t"), opts, plan)
        assert on_step is not None
        on_step("warmup-c64-r1")
        self.start.assert_not_called()
        on_step("search-c64-r1")
        self.assertEqual(self.start.call_count, 2)
        (first, second) = (c.kwargs for c in self.start.call_args_list)
        self.assertEqual(self.start.call_args_list[0].args, ("t", "vs-0", "p-0"))
        self.assertEqual(first["script"], "/var/lib/vsbench/scripts/profile-step.sh")
        self.assertEqual(
            first["env"],
            {
                "OUT_DIR": "/var/lib/vsbench/jobs/p-0/profile",
                "RECORD_S": "50",
                "DELAY_S": "4",
                "PROCESS": "vector-store",
            },
        )
        self.assertEqual(second["env"]["CONTAINER"], "scylla")
        self.assertEqual(
            st.load("t")["jobs"][JOB]["params"]["profiles"], {"search-c64-r1": {"vs-0": "p-0", "scylla-0": "p-1"}}
        )

    def test_trigger_is_none_without_profile_and_tolerates_a_failed_start(self) -> None:
        opts = bench.SearchOptions("cql", duration_s=60)
        self.assertIsNone(bench.profile_trigger("t", JOB, st.load("t"), opts, bench.search_plan(opts)))
        opts = bench.SearchOptions("cql", duration_s=60, perf=("vs-0",))
        self.start.side_effect = VsbenchError("ssh down")
        on_step = bench.profile_trigger("t", JOB, st.load("t"), opts, bench.search_plan(opts))
        assert on_step is not None
        on_step("search-c64-r1")
        self.warn.assert_called_once()
        self.assertNotIn("profiles", st.load("t")["jobs"][JOB]["params"])

    def test_profile_nodes_and_duration_are_validated(self) -> None:
        for opts, text in (
            (bench.SearchOptions("cql", perf=("client",)), "only Scylla and Vector Store"),
            (bench.SearchOptions("cql", duration_s=10, perf=("vs-0",)), "--duration >= 20s"),
        ):
            with self.assertRaises(PreconditionError) as ctx:
                bench._check_profile(self.state, opts)
            self.assertIn(text, str(ctx.exception))
        with self.assertRaises(VsbenchError):
            bench._check_profile(self.state, bench.SearchOptions("cql", perf=("nope",)))
        bench._check_profile(self.state, bench.SearchOptions("cql", perf=("vs-0", "scylla-0")))

    def test_finalize_pulls_the_reports_or_records_the_error(self) -> None:
        polls = {"p-ok": progress(0), "p-bad": progress(1), "p-slow": progress(None)}
        self.patch(remote, "job_poll", side_effect=lambda c, n, j, o, **kw: polls[j])
        pulled = self.patch(remote, "download")
        self.patch(bench_jobs, "PROFILE_WAIT_S", new=0)
        profile = bench_jobs._pull_profiles("t", "run-1", {"vs-0": "p-ok", "scylla-0": "p-bad", "vs-1": "p-slow"})
        root = st.paths("t").root
        self.assertEqual(
            profile["vs-0"],
            {
                "job_id": "p-ok",
                "dir": "results/artifacts/profiles/run-1/vs-0",
                "files": ["perf.txt", "perf-dso.txt", "pidstat.txt"],
            },
        )
        self.assertEqual(
            pulled.call_args_list[0].args,
            (
                "t",
                "vs-0",
                "/var/lib/vsbench/jobs/p-ok/profile/perf.txt",
                root / "results/artifacts/profiles/run-1/vs-0/perf.txt",
            ),
        )
        self.assertIn("failed (exit 1)", profile["scylla-0"]["error"])
        self.assertIn("still running", profile["vs-1"]["error"])
        self.assertEqual(pulled.call_count, 3)


if __name__ == "__main__":
    import unittest

    unittest.main()
