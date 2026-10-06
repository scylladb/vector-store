# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.provision: an in-memory EC2 fake stands in for `aws`; ssh is mocked.

The fake and the base test case live in tests/test_teardown.py (teardown.py is the lower layer).
"""

from __future__ import annotations

import datetime
import itertools
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import awsapi, config, proc, provision, remote, teardown  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.awsapi import AuthError, AwsError, CapacityError  # noqa: E402
from vsbenchlib.proc import PreconditionError, VsbenchError  # noqa: E402

try:  # `-t .`: tests/ is a package
    from .test_teardown import OWNER, FakeAws, ProvisionTest, completed, launched_roles, opt, warnings
except ImportError:  # tests/ is the top-level directory
    from test_teardown import OWNER, FakeAws, ProvisionTest, completed, launched_roles, opt, warnings  # type: ignore

FOREIGN_TAGS = [{"Key": "VsBenchOwner", "Value": "szymon"}, {"Key": "VsBenchCluster", "Value": "wasik-c1"}]


class PlanTest(unittest.TestCase):
    def test_node_plan_order_and_names(self) -> None:
        plan = provision.node_plan(provision.UpOptions(scylla_nodes=2, vs_nodes=2, client_disk_gb=300))
        self.assertEqual([n["name"] for n in plan], ["scylla-0", "scylla-1", "vs-0", "vs-1", "client"])
        self.assertEqual(plan[-1]["disk_gb"], 300)
        with self.assertRaises(VsbenchError):
            provision.node_plan(provision.UpOptions(vs_nodes=0))

    def test_estimate_cost(self) -> None:
        cost = provision.estimate_cost({"scylla": ("i8g.2xlarge", 1), "vs": ("r8g.4xlarge", 2)})
        self.assertAlmostEqual(cost or 0, 0.686 + 2 * 0.943)
        self.assertIsNone(provision.estimate_cost({"scylla": ("x9.huge", 1)}))
        for size in ("12xlarge", "16xlarge", "24xlarge", "48xlarge"):  # the expensive sizes have a price
            self.assertIn(f"i8g.{size}", config.PRICES)
        self.assertEqual(provision.price_split(["i8g.16xlarge", "x9.huge", "x9.huge"]), (5.492, ["x9.huge"]))

    def test_tags_keep_alive_only_on_instances(self) -> None:
        node = {"role": "vs", "index": 1}
        expires = datetime.datetime(2026, 10, 7, tzinfo=datetime.timezone.utc)
        specs = {
            s["ResourceType"]: awsapi.tags_of(s)
            for s in provision.instance_tag_specs(OWNER, "c1", "BP: x", node, expires)
        }
        self.assertEqual(set(specs), {"instance", "volume", "network-interface"})
        self.assertEqual(specs["instance"]["keep"], "alive")
        self.assertNotIn("keep", specs["volume"])
        self.assertNotIn("keep", specs["network-interface"])
        for tags in specs.values():
            self.assertEqual(tags["Name"], f"vsbench-{OWNER}-c1-vs-1")
            self.assertEqual(
                (tags["VsBenchIndex"], tags["ExpiresAt"], tags["billing_project"]),
                ("1", "2026-10-07T00:00:00Z", "BP: x"),
            )
        self.assertNotIn("keep", provision.base_tags(OWNER, "c1", "bp"))

    def test_parse_until(self) -> None:
        utc = datetime.timezone.utc
        self.assertEqual(provision.parse_until("2026-10-08T10:00:00Z"), datetime.datetime(2026, 10, 8, 10, tzinfo=utc))
        self.assertEqual(provision.parse_until("2026-10-08T12:00:00+02:00").astimezone(utc).hour, 10)
        for bad in ("2026-10-08T10:00", "tomorrow", "2026-13-08T10:00:00Z"):
            with self.assertRaises(VsbenchError, msg=bad):
                provision.parse_until(bad)


class PlacementTest(unittest.TestCase):
    def test_intersection_and_default_vpc(self) -> None:
        aws = FakeAws(
            offerings={
                "i8g.2xlarge": ["us-east-1a", "us-east-1b"],
                "r8g.2xlarge": ["us-east-1a", "us-east-1b", "us-east-1c"],
            }
        )
        got = provision.az_candidates(aws, ["i8g.2xlarge", "r8g.2xlarge"], None, None)  # type: ignore[arg-type]
        self.assertEqual([(p.az, p.subnet_id, p.source) for p in got], [("us-east-1a", "subnet-da", "default-vpc")])

    def test_sct_vpc_fallback_uses_exact_subnet_names(self) -> None:
        aws = FakeAws(default_vpc=False, sct=True)
        got = provision.az_candidates(aws, ["i8g.2xlarge"], None, None)  # type: ignore[arg-type]
        self.assertEqual([(p.az, p.vpc_id, p.source) for p in got], [("us-east-1b", "vpc-sct", "sct-vpc")])

    def test_default_vpc_preferred_then_sct_per_az(self) -> None:
        aws = FakeAws(sct=True)
        got = provision.az_candidates(aws, ["i8g.2xlarge"], None, None)  # type: ignore[arg-type]
        self.assertEqual([(p.az, p.source) for p in got], [("us-east-1a", "default-vpc"), ("us-east-1b", "sct-vpc")])

    def test_az_and_subnet_restrictions(self) -> None:
        aws = FakeAws(sct=True)
        got = provision.az_candidates(aws, ["i8g.2xlarge"], "us-east-1b", None)  # type: ignore[arg-type]
        self.assertEqual([p.az for p in got], ["us-east-1b"])
        got = provision.az_candidates(aws, ["i8g.2xlarge"], None, "subnet-x")  # type: ignore[arg-type]
        self.assertEqual([(p.az, p.subnet_id, p.source) for p in got], [("us-east-1b", "subnet-x", "subnet-id")])
        with self.assertRaises(PreconditionError):
            provision.az_candidates(aws, ["i8g.2xlarge"], "us-east-1c", None)  # type: ignore[arg-type]

    def test_instance_type_checks(self) -> None:
        aws = FakeAws()
        bad_arch = provision.node_plan(provision.UpOptions(vs_type="m7i.large"))
        with self.assertRaisesRegex(PreconditionError, "not arm64"):
            provision.check_instance_types(aws, bad_arch)  # type: ignore[arg-type]
        no_nvme = provision.node_plan(provision.UpOptions(scylla_type="r8g.2xlarge"))
        with self.assertRaisesRegex(PreconditionError, "instance storage"):
            provision.check_instance_types(aws, no_nvme)  # type: ignore[arg-type]


class UpTest(ProvisionTest):
    def test_happy_path(self) -> None:
        state = self.up()
        self.assertTrue(state["created_at"])
        self.assertIsNone(state["pending_launch"])
        self.assertEqual(state["az"], "us-east-1a")
        self.assertEqual([n["name"] for n in st.nodes(state)], ["scylla-0", "vs-0", "client"])
        self.assertTrue(all(n["public_ip"] and n["private_ip"] for n in state["nodes"]))
        self.assertEqual(state["aws"]["operator_cidr"], "1.2.3.4/32")
        runs = self.aws.ops("run-instances")
        self.assertEqual([opt(a, "--instance-type") for a in runs], ["i8g.2xlarge", "r8g.4xlarge", "r8g.2xlarge"])
        tokens = [opt(a, "--client-token") for a in runs]
        self.assertEqual(len(set(tokens)), 3)
        self.assertTrue(all(len(t) <= 64 for t in tokens))
        self.assertEqual(state["aws"]["client_tokens"], tokens)  # the proof of ownership for a later resume
        first = runs[0]
        self.assertEqual(opt(first, "--count"), "1")
        self.assertEqual(opt(first, "--image-id"), "ami-0123")
        self.assertEqual(opt(first, "--instance-initiated-shutdown-behavior"), "terminate")
        self.assertIn("HttpTokens=required", opt(first, "--metadata-options"))
        ebs = json.loads(opt(first, "--block-device-mappings"))[0]
        self.assertEqual(
            (ebs["DeviceName"], ebs["Ebs"]["VolumeSize"], ebs["Ebs"]["Encrypted"]), ("/dev/sda1", 50, True)
        )
        self.assertEqual(json.loads(opt(runs[2], "--block-device-mappings"))[0]["Ebs"]["VolumeSize"], 200)
        nic = json.loads(opt(first, "--network-interfaces"))[0]
        self.assertEqual((nic["SubnetId"], nic["Groups"], nic["DeleteOnTermination"]), ("subnet-da", ["sg-1"], True))
        userdata = Path(opt(first, "--user-data").removeprefix("file://")).read_text()
        self.assertIn("NODE_NAME=scylla-0", userdata)
        group = self.aws.groups["sg-1"]
        self.assertNotIn("keep", awsapi.tags_of(group))
        self.assertEqual(group["GroupName"], f"vsbench-{OWNER}-c1")
        rules = {json.dumps(p, sort_keys=True) for p in group["IpPermissions"]}
        self.assertEqual(len(rules), 2)
        self.assertIn(f"vsbench-{OWNER}-c1", self.aws.key_pairs)
        self.assertEqual(st.paths("c1").known_hosts.read_text(), "")
        self.assertEqual(remote.reset_master.call_count, 3)  # type: ignore[attr-defined]
        ssh_config = remote.ssh_config_text(state, st.paths("c1"))  # the real renderer accepts up's node entries
        self.assertIn("Host scylla-0", ssh_config)
        self.assertIn("HostKeyAlias i-0000", ssh_config)

    def test_non_capacity_failure_rolls_back_everything(self) -> None:
        self.aws.fail_role = "client"
        with self.assertRaises(AwsError):
            self.up()
        self.assertEqual(sorted(self.aws.ops("terminate-instances")[0][1:]), ["i-0000", "i-0001"])
        state = st.load("c1")
        self.assertEqual((state["nodes"], state["pending_launch"]), ([], None))
        self.assertIsNone(state["created_at"])

    def test_keep_on_failure_keeps_nodes(self) -> None:
        self.aws.fail_role = "client"
        with self.assertRaises(AwsError):
            self.up(keep_on_failure=True)
        self.assertEqual(self.aws.ops("terminate-instances"), [])
        self.assertEqual([n["name"] for n in st.load("c1")["nodes"]], ["scylla-0", "vs-0"])

    def test_capacity_error_falls_back_to_next_az(self) -> None:
        self.aws = FakeAws(sct=True)
        self.aws.capacity_fail.add(("us-east-1a", "vs"))
        state = self.up()
        self.assertEqual(state["az"], "us-east-1b")
        self.assertEqual(self.aws.ops("terminate-instances")[0][1:], ["i-0000"])  # the scylla node in 1a
        self.assertEqual({n["instance_id"] for n in state["nodes"]}, {"i-0001", "i-0002", "i-0003"})
        self.assertEqual(state["aws"]["subnet_id"], "subnet-s0")

    def test_capacity_everywhere_raises_capacity_error(self) -> None:
        self.aws.capacity_fail.add(("us-east-1a", "scylla"))
        with self.assertRaises(CapacityError):
            self.up()
        self.assertEqual(self.aws.ops("terminate-instances"), [])

    def test_bootstrap_failure_rolls_back(self) -> None:
        self.marker = "failed"
        with self.assertRaisesRegex(VsbenchError, "bootstrap of scylla-0 failed"):
            self.up()
        self.assertEqual(len(self.aws.ops("terminate-instances")[0]) - 1, 3)

    def test_cloud_init_done_without_marker_fails(self) -> None:
        self.marker = "done"
        with self.assertRaisesRegex(VsbenchError, "failed \\(done\\)"):
            self.up()

    def test_interrupt_rolls_back_and_reraises(self) -> None:
        remote.wait_ssh.side_effect = KeyboardInterrupt  # type: ignore[attr-defined]
        with self.assertRaises(KeyboardInterrupt):
            self.up()
        self.assertEqual(len(self.aws.ops("terminate-instances")), 1)

    def test_bootstrap_timeout(self) -> None:
        self.marker = "running"
        clock = iter(range(0, 100000, 400))
        with mock.patch.object(provision, "_monotonic", side_effect=lambda: next(clock)):
            with self.assertRaisesRegex(VsbenchError, "timed out after 15m"):
                self.up()

    def test_bootstrap_gives_up_while_credentials_allow_a_rollback(self) -> None:
        self.marker = "running"
        now = proc.utcnow()
        later = itertools.chain(
            [now + datetime.timedelta(hours=2)], itertools.repeat(now + datetime.timedelta(minutes=10))
        )
        clock = iter(range(0, 100000, 100))
        with mock.patch.object(awsapi, "credentials_expiry", side_effect=lambda _p: next(later)):
            with mock.patch.object(provision, "_monotonic", side_effect=lambda: next(clock)):
                with self.assertRaisesRegex(VsbenchError, "while the AWS credentials still allow a rollback"):
                    self.up()
        self.assertEqual(len(self.aws.ops("terminate-instances")[0]) - 1, 3)
        later = itertools.chain(
            [now + datetime.timedelta(hours=2)], itertools.repeat(now + datetime.timedelta(minutes=4))
        )
        with mock.patch.object(awsapi, "credentials_expiry", side_effect=lambda _p: next(later)):
            with self.assertRaisesRegex(AuthError, "expire too soon"):
                provision.up("c2", self.aws, provision.UpOptions())  # type: ignore[arg-type]

    def test_refuses_when_live_instances_exist(self) -> None:
        self.up()
        calls = len(self.aws.ops("run-instances"))
        with self.assertRaisesRegex(PreconditionError, "already has instances"):
            self.up()
        self.assertEqual(len(self.aws.ops("run-instances")), calls)

    def test_dry_run_creates_nothing_and_names_the_account(self) -> None:
        plan = self.up(dry_run=True)
        self.assertTrue(plan["dry_run"])
        self.assertEqual(plan["az_candidates"][0]["az"], "us-east-1a")
        self.assertEqual(self.aws.ops("run-instances") + self.aws.ops("create-security-group"), [])
        self.assertIsNone(st.load("c1"))
        self.assertEqual(
            (plan["account"], plan["profile"], plan["region"]), (config.EXPECTED_ACCOUNT, "test-profile", "us-east-1")
        )
        self.assertEqual((plan["would_adopt"], plan["unpriced_types"]), ([], []))

    def test_preflight_validation(self) -> None:
        with self.assertRaisesRegex(VsbenchError, "outside"):
            self.up(ttl="10m")
        with self.assertRaisesRegex(VsbenchError, "outside"):
            self.up(ttl="8d")
        soon = proc.utcnow() + datetime.timedelta(minutes=10)
        with mock.patch.object(awsapi, "credentials_expiry", return_value=soon):
            with self.assertRaises(AuthError):
                self.up()

    def test_other_account_needs_an_explicit_profile(self) -> None:
        self.aws.account = "123456789012"
        with self.assertRaisesRegex(PreconditionError, "account 123456789012, not 797456418907"):
            self.up(dry_run=True)
        self.assertEqual(self.up(dry_run=True, explicit_profile=True)["account"], "123456789012")

    def test_refuses_to_overwrite_a_live_cluster_in_another_region(self) -> None:
        self.up()
        before = st.load("c1")
        self.aws.region = "us-west-2"
        for dry_run in (True, False):
            with self.assertRaisesRegex(PreconditionError, "still has nodes in us-east-1"):
                self.up(dry_run=dry_run)
        self.assertEqual(st.load("c1"), before)

    def test_key_pair_reimported_on_mismatch(self) -> None:
        name = f"vsbench-{OWNER}-c1"
        self.aws.key_pairs[name] = "ssh-ed25519 AAAAOTHER old"
        self.aws.key_tags[name] = [{"Key": "VsBenchOwner", "Value": OWNER}, {"Key": "VsBenchCluster", "Value": "c1"}]
        self.up()
        self.assertEqual(len(self.aws.ops("delete-key-pair")), 1)
        self.assertIn("AAAAC3NzaLOCAL", self.aws.key_pairs[name])

    def test_existing_security_group_is_reused_and_ssh_rule_synced(self) -> None:
        old_ssh = {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "9.9.9.9/32"}]}
        ours = [{"Key": "VsBenchOwner", "Value": OWNER}, {"Key": "VsBenchCluster", "Value": "c1"}]
        group = {"GroupId": "sg-9", "GroupName": f"vsbench-{OWNER}-c1", "VpcId": "vpc-default", "Tags": ours}
        self.aws.groups["sg-9"] = group | {"IpPermissions": [old_ssh]}
        state = self.up()
        self.assertEqual(state["aws"]["security_group_id"], "sg-9")
        self.assertEqual(self.aws.ops("create-security-group"), [])
        self.assertEqual(opt(self.aws.ops("revoke-security-group-ingress")[0], "--cidr"), "9.9.9.9/32")

    def test_same_named_resources_of_another_owner_are_never_reused(self) -> None:
        name = f"vsbench-{OWNER}-c1"
        old_ssh = {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "9.9.9.9/32"}]}
        group = {"GroupId": "sg-9", "GroupName": name, "VpcId": "vpc-default", "Tags": FOREIGN_TAGS}
        self.aws.groups["sg-9"] = group | {"IpPermissions": [old_ssh]}
        with self.assertRaisesRegex(PreconditionError, "security group .* tagged VsBenchOwner=szymon"):
            self.up()
        self.assertEqual(self.aws.ops("revoke-security-group-ingress") + self.aws.ops("run-instances"), [])
        del self.aws.groups["sg-9"]
        self.aws.key_pairs[name], self.aws.key_tags[name] = "ssh-ed25519 AAAAOTHER x", FOREIGN_TAGS
        self.aws.calls.clear()
        with self.assertRaisesRegex(PreconditionError, "key pair .* tagged"):
            self.up()
        self.assertEqual(self.aws.ops("delete-key-pair") + self.aws.ops("import-key-pair"), [])
        self.assertEqual(self.aws.key_pairs[name], "ssh-ed25519 AAAAOTHER x")

    def test_eventually_consistent_not_found_is_retried(self) -> None:
        self.aws.fail_once["authorize-security-group-ingress"] = [AwsError("no sg yet", "InvalidGroup.NotFound")]
        self.aws.fail_once["run-instances"] = [AwsError("no key yet", "InvalidKeyPair.NotFound")]
        state = self.up()
        self.assertTrue(state["created_at"])
        runs = [opt(a, "--client-token") for a in self.aws.ops("run-instances")]
        self.assertEqual((len(runs), runs[0]), (4, runs[1]))  # the retry reuses the client token


class RollbackTest(ProvisionTest):
    def test_known_ids_are_terminated_before_discovery(self) -> None:
        self.aws.fail_role = "client"
        with self.assertRaises(AwsError):
            self.up()
        ops = [op for _, op, _ in self.aws.calls]
        failed = len(ops) - 1 - ops[::-1].index("run-instances")
        self.assertEqual(ops[failed + 1], "terminate-instances")

    def test_interrupted_launch_is_found_by_polling_its_client_token(self) -> None:
        self.aws.interrupt_role, self.aws.interrupt_hidden_for = "vs", 3  # invisible to the first 3 describes
        with self.assertRaises(KeyboardInterrupt):
            self.up()
        terminated = {i for a in self.aws.ops("terminate-instances") for i in a[1:]}
        self.assertEqual(terminated, {"i-0000", "i-0001"})
        self.assertEqual(self.aws.instances["i-0001"]["State"]["Name"], "terminated")
        teardown._sleep.assert_any_call(teardown.ROLLBACK_TOKEN_POLL_S)  # type: ignore[attr-defined]
        self.assertIsNone(st.load("c1")["pending_launch"])

    def test_unresolved_launch_keeps_pending_launch_and_says_so(self) -> None:
        self.aws.interrupt_role, self.aws.interrupt_hidden_for = "vs", 10**6
        with self.assertRaises(KeyboardInterrupt):
            self.up()
        pending = st.load("c1")["pending_launch"]
        self.assertEqual(
            (pending["node"], pending["client_token"]), ("vs-0", self.aws.instances["i-0001"]["ClientToken"])
        )
        self.assertTrue(any("possible orphan" in w and "down --yes" in w for w in warnings(provision)))
        self.assertEqual(self.aws.instances["i-0000"]["State"]["Name"], "terminated")

    def test_capacity_fallback_survives_eventual_consistency(self) -> None:
        self.aws = FakeAws(sct=True)
        self.aws.capacity_fail.add(("us-east-1a", "vs"))
        self.aws.fail_once["terminate-instances"] = [AwsError("not yet", "InvalidInstanceID.NotFound")]
        self.aws.stale_reads = 1  # the first poll still reads the terminated scylla-0 as pending
        state = self.up()
        self.assertEqual(state["az"], "us-east-1b")
        self.assertEqual(self.aws.instances["i-0000"]["State"]["Name"], "terminated")

    def test_failed_fallback_rollback_keeps_the_capacity_error(self) -> None:
        self.aws = FakeAws(sct=True)
        self.aws.capacity_fail.add(("us-east-1a", "vs"))
        self.aws.fail_once["terminate-instances"] = [AwsError("boom", "InternalError")]
        with self.assertRaisesRegex(CapacityError, "the rollback failed"):
            self.up()
        self.assertEqual(launched_roles(self.aws), ["scylla", "vs"])  # 1b was not tried with a half-cleaned 1a
        self.assertEqual(self.aws.instances["i-0000"]["State"]["Name"], "terminated")  # up's own rollback

    def test_capacity_failure_at_boot_falls_back_to_the_next_az(self) -> None:
        self.aws = FakeAws(sct=True)
        self.aws.boot_capacity_fail.add(("us-east-1a", "scylla"))
        state = self.up()
        self.assertEqual(state["az"], "us-east-1b")
        self.assertEqual(sorted(self.aws.ops("terminate-instances")[0][1:]), ["i-0000", "i-0001", "i-0002"])


class ResumeTest(ProvisionTest):
    def interrupted(self) -> None:
        self.aws.fail_role = "vs"
        with self.assertRaises(AwsError):
            self.up(keep_on_failure=True)
        self.aws.fail_role = None

    def test_resume_adopts_tagged_instances(self) -> None:
        self.interrupted()
        self.assertEqual(st.load("c1")["pending_launch"]["node"], "vs-0")
        plan = self.up(dry_run=True)
        self.assertEqual((plan["would_adopt"], plan["resume_note"]), (["i-0000"], None))
        state = self.up()
        self.assertEqual(launched_roles(self.aws), ["scylla", "vs", "vs", "client"])  # scylla-0 adopted
        self.assertEqual(st.node(state, "scylla-0")["instance_id"], "i-0000")
        self.assertTrue(state["created_at"])

    def test_refuses_instances_launched_from_another_vsbench_home(self) -> None:
        self.aws.capacity_fail.add(("us-east-1a", "scylla"))
        with self.assertRaises(CapacityError):
            self.up()  # rolled back: an unfinished state with no nodes and no pending launch
        self.aws.capacity_fail.clear()
        with tempfile.TemporaryDirectory() as other, mock.patch.dict(os.environ, {"VSBENCH_HOME": other}):
            self.up()  # the same owner creates c1 elsewhere
        before = len(self.aws.calls)
        for dry_run in (True, False):
            with self.assertRaisesRegex(PreconditionError, "not launched by this local state"):
                self.up(dry_run=dry_run)
        touched = {op for _, op, _ in self.aws.calls[before:]}
        self.assertFalse(
            touched & {"terminate-instances", "delete-key-pair", "import-key-pair", "revoke-security-group-ingress"}
        )
        self.assertEqual({i["State"]["Name"] for i in self.aws.instances.values()}, {"running"})

    def test_resume_keeps_the_expiry_and_warns_about_another_ttl(self) -> None:
        self.interrupted()
        kept = st.load("c1")["expires_at"]
        self.assertIn("extend --ttl 72h", self.up(ttl="72h", dry_run=True)["resume_note"])
        state = self.up(ttl="72h")
        self.assertEqual(state["expires_at"], kept)
        self.assertIn(f"keeps the expiry {kept}", warnings(provision)[-1])

    def test_resumed_dry_run_reports_the_adoption(self) -> None:  # rendered by cli_cluster.dry_run_lines
        self.interrupted()
        plan = self.up(dry_run=True, ttl="72h")
        self.assertEqual(
            (plan["profile"], plan["account"], plan["region"]), ("test-profile", config.EXPECTED_ACCOUNT, "us-east-1")
        )
        self.assertEqual(plan["would_adopt"], ["i-0000"])
        self.assertEqual(plan["kept_expires_at"], st.load("c1")["expires_at"])
        self.assertIn("extend --ttl 72h", plan["resume_note"])  # the resume note
        self.assertIsInstance(plan["cost_per_hour"], float)

    def test_resume_in_another_account_needs_no_explicit_profile(self) -> None:
        self.aws.account = "123456789012"
        self.aws.fail_role = "vs"
        with self.assertRaises(AwsError):
            self.up(keep_on_failure=True, explicit_profile=True)
        self.aws.fail_role = None
        # the cli takes the profile from the state, so the resume has no explicit --profile
        self.assertEqual(self.up(dry_run=True)["would_adopt"], ["i-0000"])
        st.update("c1", lambda s: s | {"terminated_at": proc.iso(proc.utcnow())})
        with self.assertRaisesRegex(PreconditionError, "not 797456418907"):
            self.up(dry_run=True)  # a terminated state proves nothing

    def test_resume_resyncs_a_stale_expires_tag(self) -> None:
        self.interrupted()
        self.answer()
        extended = teardown.extend("c1", None, 30 * 3600, None, False)  # no credentials: tag not written
        state = self.up()
        self.assertEqual(state["expires_at"], extended["expires_at"])
        self.assertEqual(awsapi.tags_of(self.aws.instances["i-0000"])["ExpiresAt"], extended["expires_at"])
        self.assertIsNone(state["expires_tag_pending"])


class BudgetTest(ProvisionTest):
    def test_unknown_price_warns_and_checks_the_known_part(self) -> None:
        plan = self.up(dry_run=True, scylla_type="i8g.16xlarge", scylla_nodes=2, vs_type="r8g.metal-24xl")
        self.assertEqual((plan["cost_per_hour"], plan["unpriced_types"]), (None, ["r8g.metal-24xl"]))
        found = warnings(provision)
        self.assertTrue(any("no price for r8g.metal-24xl: the budget check is incomplete" in w for w in found))
        self.assertTrue(any("exceeds the personal budget" in w for w in found))  # 2 x 5.49 + 0.47 already

    def test_other_running_clusters_count_for_the_personal_budget(self) -> None:
        for n in range(3):
            self.aws.add_instance(f"i-big{n}", "big", OWNER, "r8g.16xlarge")
        self.aws.add_instance("i-them", "x", "someone.else", "r8g.16xlarge")
        plan = self.up(dry_run=True)
        self.assertAlmostEqual(plan["other_clusters_cost_per_hour"], 3 * 3.770)
        found = [w for w in warnings(provision) if "exceeds the personal budget" in w]
        self.assertIn("incl. $11.31/h of your other running clusters", found[0])  # someone.else does not count

    def test_billing_project_outside_the_finops_list_warns(self) -> None:
        self.up(dry_run=True)
        self.assertEqual(warnings(provision), [])
        self.up(dry_run=True, billing_project="Vector Search: Shardng")
        self.assertIn("not a known finops project", warnings(provision)[-1])


class DoctorTest(unittest.TestCase):
    def test_reports_checks_without_raising(self) -> None:
        aws = FakeAws()
        which = mock.patch.object(provision.shutil, "which", side_effect=lambda t: None if t == "zstd" else f"/bin/{t}")
        docker = mock.patch.object(proc, "run", return_value=completed("vs-bench-cross:1.89\n"))
        with which, docker, mock.patch.object(awsapi, "credentials_expiry", return_value=None):
            report = provision.doctor(aws)  # type: ignore[arg-type]
        checks = {c["check"]: c["status"] for c in report["checks"]}
        self.assertEqual(
            (checks["zstd"], checks["aws"], checks["identity"], checks["cross-image"]), ("warn", "ok", "ok", "ok")
        )
        self.assertEqual(report["identity"]["owner"], OWNER)
        with mock.patch.object(provision.shutil, "which", return_value=None):
            report = provision.doctor(aws)  # type: ignore[arg-type]
        self.assertFalse(report["ok"])


if __name__ == "__main__":
    unittest.main()
