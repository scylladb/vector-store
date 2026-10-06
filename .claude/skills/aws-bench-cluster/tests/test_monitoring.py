# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.monitoring (ssh is faked) and node/monitoring-start.sh (functions of
the script are sourced and run locally, without root or docker)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests.test_deploy import CLUSTER, NIGHTLY, NODE_DIR, SCYLLA_VERSION, DeployTestCase, bash  # noqa: E402
from vsbenchlib import config, deploy, monitoring  # noqa: E402


class ReExportTest(unittest.TestCase):
    def test_deploy_reexports_the_monitoring_api(self) -> None:
        names = ("deploy_monitoring", "update_monitoring_targets", "monitoring_status", "parse_target_report")
        for name in (*names, "MONITORING_SCRIPT", "LOADGEN_JOB", "MONITORING_TIMEOUT_S"):
            with self.subTest(name=name):
                self.assertIs(getattr(deploy, name), getattr(monitoring, name))

    def test_script_fields_keeps_the_last_identifier_keys(self) -> None:
        fields = monitoring.script_fields("a=1\nnot a key=2\nb = x=y\na=3\nnoequals\n")
        self.assertEqual(fields, {"a": "3", "b": "x=y"})
        self.assertIs(deploy._kv, monitoring.script_fields)


class ParseTest(unittest.TestCase):
    def test_parse_target_report(self) -> None:
        report = monitoring.parse_target_report("job=scylla 1 1 10s\nproblem=scylla x: down\nfatal=bad\ndashboards=m\n")
        self.assertEqual(report["jobs"], {"scylla": {"up": 1, "down": 1, "scrape_interval": "10s"}})
        self.assertEqual(report["problems"], ["scylla x: down", "bad"])


class DeployMonitoringTest(DeployTestCase):
    def test_monitoring_passes_expectations_and_records_dashboards(self) -> None:
        self.deployed(scylla={"image": NIGHTLY, "version": SCYLLA_VERSION})
        state = deploy.deploy_monitoring(CLUSTER)
        env = self.fake.scripts[0][2]
        expect = "scylla:2:1,node_exporter:2:1,vector_search:2:0,vector_search_os:2:1,loadgen_os:1:1"
        self.assertEqual(env["EXPECT_JOBS"], expect)
        ips = ("10.0.0.10,10.0.0.11", "10.0.1.10,10.0.1.11", "10.0.2.10")
        self.assertEqual((env["SCYLLA_IPS"], env["VS_IPS"], env["CLIENT_IP"]), ips)
        versions = (SCYLLA_VERSION, config.SCYLLA_MONITORING_VERSION, "10")
        self.assertEqual((env["SCYLLA_VERSION"], env["SM_VERSION"], env["SCRAPE_S"]), versions)
        info = state["deployed"]["monitoring"]
        self.assertEqual((info["dashboards"], info["cpuset"], info["version"]), ("master", "7", versions[1]))

    def test_monitoring_status_and_targets_update(self) -> None:
        self.assertFalse(monitoring.monitoring_status(CLUSTER)["deployed"])
        monitoring.update_monitoring_targets(CLUSTER)
        self.assertEqual(self.fake.scripts, [])
        self.deployed(monitoring={"version": "4.16.1", "dashboards": "master"})
        status = monitoring.monitoring_status(CLUSTER)
        self.assertEqual((status["ready"], status["jobs"]["scylla"]["up"], len(status["problems"])), (True, 2, 1))
        monitoring.update_monitoring_targets(CLUSTER)
        self.assertEqual(self.fake.actions(monitoring.MONITORING_SCRIPT)[-1], ("client", "targets"))


class MonitoringScriptTest(unittest.TestCase):
    SCRIPT = str(NODE_DIR / "monitoring-start.sh")

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def test_write_targets_atomically_with_bare_ips(self) -> None:
        script = 'source "$1"; MON_DIR=$2 RUN_AS=$(id -un) CLUSTER_LABEL=vsbench DC=dc1 SCYLLA_IPS=10.0.0.1,10.0.0.2 '
        self.assertEqual(bash(script + "VS_IPS=; write_targets", self.SCRIPT, str(self.dir)).returncode, 0)
        files = {p.name: p.read_text() for p in (self.dir / "targets").iterdir()}
        scylla = "- targets: [10.0.0.1, 10.0.0.2]\n  labels: {cluster: vsbench, dc: dc1}\n"
        self.assertEqual(files["scylla_servers.yml"], scylla)
        self.assertEqual(files["node_exporter_servers.yml"], scylla)
        self.assertEqual(files["vector_search_servers.yml"], "[]\n")
        self.assertEqual((files["scylla_manager_servers.yml"], files["scylla_manager_agents.yml"]), ("", "[]\n"))
        self.assertEqual(len(files), 5)  # no temp files left behind

    def test_patch_template_removes_exactly_two_lines(self) -> None:
        head = "global:\n  scrape_interval: 20s # Default Scrape\n- job_name: node_exporter\n"
        patched = "  scrape_interval: 1m # By default, scrape targets every 20 second.\n"
        patched += "  scrape_timeout: 20s # Timeout before trying to scape a target again\n"
        template = self.dir / "prometheus.yml.template"
        template.write_text(head + patched + "  file_sd_configs:\n")
        self.assertEqual(bash('source "$1"; patch_template "$2"', self.SCRIPT, str(template)).returncode, 0)
        self.assertEqual(template.read_text(), head + "  file_sd_configs:\n")
        failed = bash('source "$1"; patch_template "$2"', self.SCRIPT, str(template))
        self.assertEqual(failed.returncode, 1)
        self.assertIn("upgrading-pins", failed.stderr)

    def test_choose_dashboards(self) -> None:
        (self.dir / "grafana" / "build" / "ver_2026.3").mkdir(parents=True)
        script = 'source "$1"; SM_DIR=$2 SCYLLA_VERSION=$3; choose_dashboards; echo "$DASHBOARDS"'
        cases = (("2026.3.2-0.20260920.abc", "2026.3"), (SCYLLA_VERSION, "master"), ("2026.2.8-0.1", "master"))
        for version, want in (*cases, ("", "master")):
            with self.subTest(version=version):
                self.assertEqual(bash(script, self.SCRIPT, str(self.dir), version).stdout.strip(), want)

    def test_target_report(self) -> None:
        up = {"labels": {"job": "node_exporter"}, "health": "up", "scrapeUrl": "u1", "scrapeInterval": "10s"}
        down = {"labels": {"job": "vector_search"}, "health": "down", "scrapeUrl": "u2", "lastError": "refused"}
        payload = json.dumps({"status": "success", "data": {"activeTargets": [up, down | {"scrapeInterval": "10s"}]}})
        report = 'source "$1"; EXPECT_JOBS=$2 SCRAPE_S=10; target_report <<<"$3"'
        out = bash(report, self.SCRIPT, "node_exporter:1:1,vector_search:1:0,loadgen_os:1:1", payload).stdout
        self.assertEqual(monitoring.parse_target_report(out)["problems"], ["loadgen_os: 0 targets, expected 1"])
        out = bash(report, self.SCRIPT, "vector_search:1:1", payload).stdout
        self.assertEqual(monitoring.parse_target_report(out)["problems"], ["vector_search u2: down refused"])
        out = bash(report, self.SCRIPT, "node_exporter:1:1", payload.replace('"10s"', '"1m"')).stdout
        self.assertIn("fatal=node_exporter is scraped every 1m, expected 10s", out)


if __name__ == "__main__":
    unittest.main()
