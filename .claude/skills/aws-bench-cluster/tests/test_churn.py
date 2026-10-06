# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""`vsbench bench churn`: the insert stream as a job that the other bench commands run next to."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.test_bench import HomeTestCase, make_state, progress, section  # noqa: E402
from vsbenchlib import bench, bench_jobs, churn, config, remote, results  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import PreconditionError  # noqa: E402

SUMMARY = """\
2026-10-06T23:16:21.1Z  INFO t=170s issued=212626 acked=212613 failed=0 rate=1250/s (target 1250/s)
2026-10-06T23:16:31.4Z  INFO duration: 180.0s
2026-10-06T23:16:31.4Z  INFO rows issued: 225001
2026-10-06T23:16:31.4Z  INFO rows acked: 224989
2026-10-06T23:16:31.4Z  INFO rows failed: 12
2026-10-06T23:16:31.4Z  INFO last vector_id: 1099511852776
2026-10-06T23:16:31.4Z  INFO insert rate: 1249.9/s (target 1250/s)
"""


def probe(code: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["ssh"], code, "", "")


class ValidationTest(HomeTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.run = self.patch(remote, "run", return_value=probe())

    def test_plan_takes_the_dimension_from_the_index_options_and_counts_churned_rows(self) -> None:
        state = st.load("t")
        state["load"].update(index_options={"dimensions": 768, "similarity_function": "COSINE"}, churn_rows=1000)
        self.save(state)
        plan = churn.validate_churn("t", state, churn.ChurnOptions(rate=1250))
        self.assertEqual(plan, {"scylla": "10.0.1.10:9042", "dimension": 768, "start_id": churn.CHURN_ID_BASE + 1000})
        self.assertIn("insert-rows --help", self.run.call_args.args[2])

    def test_dimension_falls_back_to_the_catalog(self) -> None:
        with mock.patch.object(bench, "dataset", return_value={"dim": 1536}):
            plan = churn.validate_churn("t", st.load("t"), churn.ChurnOptions(rate=0))
        self.assertEqual(plan["dimension"], 1536)

    def test_refusals_come_before_the_probe(self) -> None:
        cases = [
            (make_state(loaded=False), churn.ChurnOptions(rate=1), "no complete load"),
            (make_state(), churn.ChurnOptions(rate=1, duration_s=5), "--duration >= 10s"),
            (make_state(deployed={}), churn.ChurnOptions(rate=1), "scylla is not deployed"),
        ]
        for state, opts, text in cases:
            with self.subTest(text=text), self.assertRaises(PreconditionError) as ctx:
                churn.validate_churn("t", state, opts)
            self.assertIn(text, str(ctx.exception))
        self.run.assert_not_called()

    def test_a_running_churn_and_a_tool_without_insert_rows_are_refused(self) -> None:
        self.add_job("J-churn", "churn", {"index": "i"})
        with self.assertRaises(PreconditionError) as ctx:
            churn.validate_churn("t", st.load("t"), churn.ChurnOptions(rate=1))
        self.assertIn("churn job J-churn is still running", str(ctx.exception))
        self.run.return_value = probe(2)
        with self.assertRaises(PreconditionError) as ctx:
            churn.validate_churn("t", make_state(), churn.ChurnOptions(rate=1))
        self.assertEqual(ctx.exception.hint, churn.NO_INSERT_ROWS_HINT)


class JobTest(HomeTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uploads: dict[str, str] = {}
        self.patch(remote, "ensure_script")
        upload = self.patch(remote, "upload")
        upload.side_effect = lambda c, n, local, path, **kw: self.uploads.update({path: Path(local).read_text()})
        self.patch(remote, "run", return_value=probe())
        self.start = self.patch(remote, "job_start")
        self.patch(remote, "job_follow", return_value=0)
        self.poll = self.patch(remote, "job_poll", return_value=progress(0))
        self.log = self.patch(remote, "job_log", return_value=section("churn", SUMMARY))

    def test_churn_runs_insert_rows_as_a_job_and_records_it(self) -> None:
        records = bench.churn(
            "t", bench.ChurnOptions(rate=1250, duration_s=180, label="r2", extra_args=("--report", "5s"))
        )
        (job_id,) = st.load("t")["jobs"]
        script = self.uploads[f"{config.NODE_JOBS}/{job_id}/steps.sh"]
        self.assertIn("step churn --timeout 300s", script)
        argv = f"insert-rows --scylla 10.0.1.10:9042 --dimension 768 --start-id {churn.CHURN_ID_BASE} --rate 1250"
        self.assertIn(f"{argv} --duration 180s --concurrency 64 --report 5s", script)
        self.start.assert_called_once_with("t", "client", job_id, script=f"{config.NODE_JOBS}/{job_id}/steps.sh")
        (record,) = records
        self.assertEqual((record["kind"], record["run_id"], record["label"]), ("churn", job_id, "r2"))
        details = record["churn"]
        self.assertEqual(
            (details["rows_acked"], details["rows_failed"], details["achieved_rate"]), (224989, 12, 1249.9)
        )
        self.assertEqual((details["start_id"], details["last_id"]), (churn.CHURN_ID_BASE, 1099511852776))
        state = st.load("t")
        self.assertEqual(state["load"]["churn_rows"], 224989)
        self.assertEqual(state["jobs"][job_id]["status"], "finalized")
        self.assertIn("rows_acked=224989", bench.format_summary(records))

    def test_the_other_commands_ignore_a_running_churn(self) -> None:
        self.add_job("J-churn", "churn", {"index": "i"})
        self.poll.return_value = progress(None, running=True)
        bench_jobs._ensure_idle("t")  # no PreconditionError
        self.add_job("J-search", "search", {})
        with self.assertRaises(PreconditionError):
            bench_jobs._ensure_idle("t")

    def test_a_search_during_churn_is_flagged(self) -> None:
        state = st.load("t")
        state["load"]["churn_rows"] = 5000
        self.save(state)
        self.patch(bench.prom, "server_metrics", return_value={"vs_qps": 1.0})
        self.fake_deploy()
        fixture = (Path(__file__).parent / "fixtures" / "search-cql.log").read_text()
        self.log.return_value = section("warmup-c64-r1", "") + section("search-c64-r1", fixture)
        (record,) = bench.search("t", bench.SearchOptions("cql"))
        self.assertEqual(record["churn_rows"], 5000)
        self.assertIn("churned", results.flags_for(record))

    def test_a_missing_summary_fails_the_job_but_keeps_the_record(self) -> None:
        self.log.return_value = section("churn", "2026-10-06T23:16:31.4Z  INFO t=10s issued=100 acked=100 failed=0")
        self.add_job("J-churn", "churn", {"index": "vsb_idx_1", "rate": 1, "duration_s": 60, "concurrency": 1})
        state = st.load("t")
        state["load"]["index"] = "vsb_idx_1"
        self.save(state)
        (record,) = bench.finalize_job("t", "J-churn")
        self.assertEqual(record["error"], "no insert-rows summary in the log")
        self.assertIsNone(record["churn"]["rows_acked"])
        self.assertEqual(st.load("t")["jobs"]["J-churn"]["status"], "failed")
        self.assertNotIn("churn_rows", st.load("t")["load"])
