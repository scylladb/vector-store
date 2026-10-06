# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.teardown (down, list, status refresh, extend, refresh-ip).

Also home of the in-memory EC2 fake and the base test case that tests/test_provision.py reuses.
"""

from __future__ import annotations

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

from vsbenchlib import awsapi, config, proc, provision, remote, teardown  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.awsapi import AuthError, AwsError, CapacityError  # noqa: E402
from vsbenchlib.proc import PreconditionError, VsbenchError  # noqa: E402

OWNER = "szymon.wasik"
ARN = f"arn:aws:sts::797456418907:assumed-role/DeveloperAccessRole/{OWNER}@scylladb.com"
TYPES = {"i8g.2xlarge": ("arm64", True), "r8g.4xlarge": ("arm64", False), "r8g.2xlarge": ("arm64", False)}
TYPES |= {"m7i.large": ("x86_64", False), "i8g.16xlarge": ("arm64", True), "r8g.16xlarge": ("arm64", False)}
TYPES |= {"r8g.metal-24xl": ("arm64", False)}  # no entry in config.PRICES
SUBNET_AZ = {"subnet-da": "us-east-1a", "subnet-dc": "us-east-1c", "subnet-s0": "us-east-1b", "subnet-s1": "us-east-1a"}
SUBNET_AZ |= {"subnet-x": "us-east-1b"}
BOOT_FAILURE = (
    "aws ec2 wait failed: Waiter InstanceRunning failed: Waiter encountered a terminal failure state: "
    'For expression "Reservations[].Instances[].State.Name" we matched expected path: "terminated"'
)


def parse_filters(args: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    if "--filters" not in args:
        return out
    for item in args[args.index("--filters") + 1 :]:
        if item.startswith("--"):
            break
        name, values = item.split(",Values=", 1)
        out[name.removeprefix("Name=")] = values.split(",")
    return out


def opt(args: list[str], name: str) -> str:
    return args[args.index(name) + 1]


def tagged(tags: list[dict[str, str]], filters: dict[str, list[str]]) -> bool:
    return all(awsapi.tags_of({"Tags": tags}).get(k[4:]) in v for k, v in filters.items() if k.startswith("tag:"))


class FakeAws:
    """Records every `Aws.call` and simulates the EC2/SSM/STS calls vsbench makes.

    Knobs for EC2's eventual consistency and failures: `fail_once` (operation -> errors its next calls
    raise), `hidden` (instance id -> describe-instances calls that do not show it yet), `stale_reads`
    (instance-id polls that still read a terminating instance as pending), `interrupt_role` (that
    role's run-instances launches, then the CLI dies), `boot_capacity_fail` ((az, role) terminated at boot).
    """

    def __init__(
        self, offerings: dict[str, list[str]] | None = None, default_vpc: bool = True, sct: bool = False
    ) -> None:
        self.profile, self.region, self.account = "test-profile", "us-east-1", config.EXPECTED_ACCOUNT
        self.calls: list[tuple[str, str, list[str]]] = []
        self.offerings = offerings or {t: ["us-east-1a", "us-east-1b"] for t in TYPES}
        self.default_vpc, self.sct = default_vpc, sct
        self.instances: dict[str, dict[str, Any]] = {}
        self.key_pairs: dict[str, str] = {}
        self.key_tags: dict[str, list[dict[str, str]]] = {}
        self.groups: dict[str, dict[str, Any]] = {}
        self.capacity_fail: set[tuple[str, str]] = set()  # (az, node role) that hit InsufficientInstanceCapacity
        self.boot_capacity_fail: set[tuple[str, str]] = set()
        self.fail_role: str | None = None  # run-instances of this role fails with a non-capacity error
        self.interrupt_role: str | None = None
        self.interrupt_hidden_for = 0  # describe calls that miss the instance of the interrupted launch
        self.fail_once: dict[str, list[Exception]] = {}
        self.hidden: dict[str, int] = {}
        self.stale_reads = 0
        self.sg_dependency_failures = 0
        self.enis: list[str] = []

    def call(self, service: str, operation: str, *args: Any, check: bool = True) -> Any:
        argv = [str(a) for a in args]
        self.calls.append((service, operation, argv))
        if self.fail_once.get(operation):
            raise self.fail_once[operation].pop(0)
        return getattr(self, f"_{service}_{operation.replace('-', '_')}")(argv)

    def ops(self, operation: str) -> list[list[str]]:
        return [a for _, op, a in self.calls if op == operation]

    def add_instance(self, iid: str, cluster: str, owner: str, itype: str, **extra: Any) -> None:
        tags = [{"Key": "VsBenchCluster", "Value": cluster}, {"Key": "VsBenchOwner", "Value": owner}]
        self.instances[iid] = {"InstanceId": iid, "InstanceType": itype, "Tags": tags, "ClientToken": f"t-{iid}"}
        self.instances[iid] |= {"State": {"Name": "running"}} | extra

    def _sts_get_caller_identity(self, args: list[str]) -> Any:
        return {"Account": self.account, "Arn": ARN}

    def _ssm_get_parameter(self, args: list[str]) -> Any:
        return {"Parameter": {"Value": "ami-0123"}}

    def _ec2_describe_images(self, args: list[str]) -> Any:
        return {"Images": [{"ImageId": "ami-0123", "RootDeviceName": "/dev/sda1"}]}

    def _ec2_describe_instance_types(self, args: list[str]) -> Any:
        wanted = args[args.index("--instance-types") + 1 :]
        return {
            "InstanceTypes": [
                {
                    "InstanceType": t,
                    "ProcessorInfo": {"SupportedArchitectures": [TYPES[t][0]]},
                    "InstanceStorageSupported": TYPES[t][1],
                }
                for t in wanted
            ]
        }

    def _ec2_describe_instance_type_offerings(self, args: list[str]) -> Any:
        types = parse_filters(args)["instance-type"]
        offers = [{"InstanceType": t, "Location": az} for t in types for az in self.offerings.get(t, [])]
        return {"InstanceTypeOfferings": offers}

    def _ec2_describe_vpcs(self, args: list[str]) -> Any:
        filters = parse_filters(args)
        if "is-default" in filters:
            return {"Vpcs": [{"VpcId": "vpc-default"}] if self.default_vpc else []}
        return {"Vpcs": [{"VpcId": "vpc-sct"}] if self.sct else []}

    def _ec2_describe_subnets(self, args: list[str]) -> Any:
        if "--subnet-ids" in args:
            return {
                "Subnets": [{"SubnetId": opt(args, "--subnet-ids"), "AvailabilityZone": "us-east-1b", "VpcId": "vpc-x"}]
            }
        vpc = parse_filters(args)["vpc-id"][0]
        if vpc == "vpc-default":  # default-for-az subnets only in a and c
            return {
                "Subnets": [
                    {"SubnetId": f"subnet-d{az[-1]}", "AvailabilityZone": az} for az in ("us-east-1a", "us-east-1c")
                ]
            }
        subnets = [("us-east-1b", "SCT-2-subnet-us-east-1b"), ("us-east-1a", "SCT-2-subnet-us-east-1a-1")]
        return {
            "Subnets": [
                {"SubnetId": f"subnet-s{i}", "AvailabilityZone": az, "Tags": [{"Key": "Name", "Value": name}]}
                for i, (az, name) in enumerate(subnets)
            ]
        }

    def _ec2_describe_key_pairs(self, args: list[str]) -> Any:
        filters = parse_filters(args)
        name = filters["key-name"][0]
        if name not in self.key_pairs or not tagged(self.key_tags.get(name, []), filters):
            return {"KeyPairs": []}
        return {"KeyPairs": [{"KeyName": name, "PublicKey": self.key_pairs[name], "Tags": self.key_tags.get(name, [])}]}

    def _ec2_import_key_pair(self, args: list[str]) -> Any:
        name = opt(args, "--key-name")
        self.key_pairs[name] = Path(opt(args, "--public-key-material").removeprefix("fileb://")).read_text()
        self.key_tags[name] = json.loads(opt(args, "--tag-specifications"))[0]["Tags"]

    def _ec2_delete_key_pair(self, args: list[str]) -> Any:
        self.key_pairs.pop(opt(args, "--key-name"), None)
        self.key_tags.pop(opt(args, "--key-name"), None)

    def _ec2_describe_security_groups(self, args: list[str]) -> Any:
        if "--group-ids" in args:  # like EC2: an unknown id is an error, not an empty list
            found = [g for g in self.groups.values() if g["GroupId"] == opt(args, "--group-ids")]
            if not found:
                raise AwsError(
                    "aws ec2 describe-security-groups failed: InvalidGroup.NotFound", "InvalidGroup.NotFound"
                )
            return {"SecurityGroups": found}
        f = parse_filters(args)
        groups = [g for g in self.groups.values() if g["GroupName"] in f.get("group-name", [g["GroupName"]])]
        groups = [g for g in groups if g["VpcId"] in f.get("vpc-id", [g["VpcId"]])]
        groups = [g for g in groups if g["GroupId"] in f.get("group-id", [g["GroupId"]])]
        return {"SecurityGroups": [g for g in groups if tagged(g["Tags"], f)]}

    def _ec2_create_security_group(self, args: list[str]) -> Any:
        gid = f"sg-{len(self.groups) + 1}"
        tags = json.loads(opt(args, "--tag-specifications"))[0]["Tags"]
        group = {"GroupId": gid, "GroupName": opt(args, "--group-name"), "VpcId": opt(args, "--vpc-id")}
        self.groups[gid] = group | {"Tags": tags, "IpPermissions": []}
        return {"GroupId": gid}

    def _ec2_authorize_security_group_ingress(self, args: list[str]) -> Any:
        self.groups[opt(args, "--group-id")]["IpPermissions"] += json.loads(opt(args, "--ip-permissions"))

    def _ec2_revoke_security_group_ingress(self, args: list[str]) -> Any:
        group = self.groups[opt(args, "--group-id")]
        for perm in group["IpPermissions"]:
            perm["IpRanges"] = [r for r in perm.get("IpRanges", []) if r["CidrIp"] != opt(args, "--cidr")]

    def _ec2_delete_security_group(self, args: list[str]) -> Any:
        gid = opt(args, "--group-id")
        if gid not in self.groups:
            raise AwsError("gone", "InvalidGroup.NotFound")
        if self.sg_dependency_failures:
            self.sg_dependency_failures -= 1
            raise AwsError("in use", "DependencyViolation")
        del self.groups[gid]

    def _ec2_describe_network_interfaces(self, args: list[str]) -> Any:
        return {"NetworkInterfaces": [{"NetworkInterfaceId": e} for e in self.enis]}

    def _ec2_delete_network_interface(self, args: list[str]) -> Any:
        self.enis.remove(opt(args, "--network-interface-id"))

    def _ec2_run_instances(self, args: list[str]) -> Any:
        nic = json.loads(opt(args, "--network-interfaces"))[0]
        az = SUBNET_AZ[nic["SubnetId"]]
        specs = json.loads(Path(opt(args, "--tag-specifications").removeprefix("file://")).read_text())
        tags = next(s["Tags"] for s in specs if s["ResourceType"] == "instance")
        role = awsapi.tags_of({"Tags": tags})["VsBenchRole"]
        if (az, role) in self.capacity_fail:
            raise CapacityError("no capacity", "InsufficientInstanceCapacity")
        if role == self.fail_role:
            raise AwsError("vCPU limit", "VcpuLimitExceeded")
        n = len(self.instances)
        iid = f"i-{n:04d}"
        inst = {"InstanceId": iid, "InstanceType": opt(args, "--instance-type"), "Tags": tags}
        inst |= {"State": {"Name": "pending"}, "Placement": {"AvailabilityZone": az}}
        inst |= {"ClientToken": opt(args, "--client-token"), "PrivateIpAddress": f"10.0.0.{n}"}
        self.instances[iid] = inst | {
            "PublicIpAddress": f"3.0.0.{n}",
            "boot_fail": (az, role) in self.boot_capacity_fail,
        }
        if role == self.interrupt_role:  # the request reached EC2; the CLI died before printing the id
            self.interrupt_role, self.hidden[iid] = None, self.interrupt_hidden_for
            raise KeyboardInterrupt
        return {"Instances": [dict(self.instances[iid])]}

    def _visible(self, iid: str) -> bool:
        if self.hidden.get(iid, 0) > 0:
            self.hidden[iid] -= 1
            return False
        return True

    def _ec2_describe_instances(self, args: list[str]) -> Any:
        visible = [i for i in self.instances.values() if self._visible(i["InstanceId"])]
        if "--instance-ids" in args:
            wanted = args[args.index("--instance-ids") + 1 :]
            found = [i for i in visible if i["InstanceId"] in wanted]
        else:
            found = [i for i in visible if self._matches(i, parse_filters(args))]
        stale = self.stale_reads > 0 and "instance-id" in parse_filters(args)
        self.stale_reads -= 1 if stale else 0
        out = [dict(i) | ({"State": {"Name": "pending"}} if stale else {}) for i in found]
        for inst in found:  # shutting-down completes once it has been observed
            if inst["State"]["Name"] == "shutting-down":
                inst["State"] = {"Name": "terminated"}
        return {"Reservations": [{"Instances": out}]}

    def _matches(self, inst: dict[str, Any], filters: dict[str, list[str]]) -> bool:
        tags = awsapi.tags_of(inst)
        for name, values in filters.items():
            plain = {"instance-state-name": inst["State"]["Name"], "client-token": inst["ClientToken"]}
            value = (plain | {"instance-id": inst["InstanceId"]}).get(name)
            if name == "tag-key":
                value = values[0] if values[0] in tags else None
            elif name.startswith("tag:"):
                value = tags.get(name[4:])
            if value not in values:
                return False
        return True

    def _ec2_wait(self, args: list[str]) -> Any:
        if args[0] != "instance-running":  # instance-terminated fails on a pending read: never used
            raise AssertionError(f"vsbench must not use the {args[0]} waiter")
        failed = False
        for iid in args[args.index("--instance-ids") + 1 :]:
            inst = self.instances[iid]
            if inst.get("boot_fail"):
                reason = {"Code": "Server.InsufficientInstanceCapacity", "Message": "Insufficient capacity."}
                inst |= {"State": {"Name": "terminated"}, "StateReason": reason}
                failed = True
            elif inst["State"]["Name"] in ("pending", "running"):
                inst["State"] = {"Name": "running"}
        if failed:
            raise AwsError(BOOT_FAILURE, None)

    def _ec2_terminate_instances(self, args: list[str]) -> Any:
        ids = args[args.index("--instance-ids") + 1 :]
        if unknown := [i for i in ids if i not in self.instances]:
            raise AwsError(f"The instance IDs '{unknown}' do not exist", "InvalidInstanceID.NotFound")
        for iid in ids:
            if self.instances[iid]["State"]["Name"] != "terminated":
                self.instances[iid]["State"] = {"Name": "shutting-down"}

    def _ec2_create_tags(self, args: list[str]) -> Any:
        ids = args[args.index("--resources") + 1 : args.index("--tags")]
        key, value = opt(args, "--tags").removeprefix("Key=").split(",Value=", 1)
        for iid in ids:
            inst = self.instances[iid]
            inst["Tags"] = [t for t in inst["Tags"] if t["Key"] != key] + [{"Key": key, "Value": value}]


def launched_roles(aws: FakeAws) -> list[str]:
    """VsBenchRole of every run-instances call, in call order (failed calls included)."""
    roles = []
    for args in aws.ops("run-instances"):
        specs = json.loads(Path(opt(args, "--tag-specifications").removeprefix("file://")).read_text())
        roles.append(awsapi.tags_of(specs[0])["VsBenchRole"])
    return roles


def completed(stdout: str = "", returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout, "")


def fake_keygen(cmd: Any, **_kw: Any) -> subprocess.CompletedProcess[str]:
    """Stand-in for `ssh-keygen` (proc.run) writing a fixed key pair."""
    args = [str(c) for c in cmd]
    assert args[0] == "ssh-keygen", args
    key = Path(args[args.index("-f") + 1])
    key.write_text("PRIVATE\n")
    Path(str(key) + ".pub").write_text("ssh-ed25519 AAAAC3NzaLOCAL vsbench\n")
    return completed()


def warnings(*modules: Any) -> list[str]:
    """Every message passed to the (mocked) warn() of the given modules."""
    return [c.args[0] for m in modules for c in m.warn.call_args_list]


class ProvisionTest(unittest.TestCase):
    """Base: a temporary VSBENCH_HOME, the EC2 fake, and ssh/keygen/clock/log mocks for both modules."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patches = [
            mock.patch.dict(os.environ, {"VSBENCH_HOME": self.tmp.name}),
            mock.patch.object(awsapi, "credentials_expiry", return_value=None),
            mock.patch.object(proc, "run", side_effect=fake_keygen),
            mock.patch.object(remote, "write_ssh_config"),
            mock.patch.object(remote, "wait_ssh"),
            mock.patch.object(remote, "reset_master"),
            mock.patch.object(remote, "run", side_effect=self.remote_run),
            mock.patch.object(remote, "run_many"),
        ]
        for module in (provision, teardown):
            patches += [mock.patch.object(module, name) for name in ("_sleep", "log", "warn")]
            patches += [mock.patch.object(module, "operator_cidr", return_value="1.2.3.4/32")]
        self.mocks = [p.start() for p in patches]
        for p in patches:
            self.addCleanup(p.stop)
        self.marker = "ready"
        self.aws = FakeAws()

    def remote_run(self, cluster: str, node: str, command: str, **kw: Any) -> subprocess.CompletedProcess[str]:
        self.assertFalse(kw.get("multiplex", True), "bootstrap polls must not use the ControlMaster")
        if command == provision._MARKER_CMD:
            return completed(self.marker + "\n")
        if "vsbench-ttl.timer" in command:
            epoch = int(proc.parse_iso(st.load(cluster)["expires_at"]).timestamp())
            return completed(f"active\n{epoch}\n")
        return completed("line 42: apt-get install\n")

    def up(self, **kw: Any) -> Any:
        return provision.up("c1", self.aws, provision.UpOptions(**kw))  # type: ignore[arg-type]

    def answer(self, value: str | None = None, bad: str | None = None) -> None:
        """remote.run_many as used by extend: every node echoes the epoch (or `value`), `bad` echoes garbage."""

        def run_many(cluster: str, nodes: list[str], command: str, **kw: Any) -> dict[str, Any]:
            epoch = value or command.split()[2]
            return {n: completed("garbage" if n == bad else epoch + "\n") for n in nodes}

        remote.run_many.side_effect = run_many  # type: ignore[attr-defined]

    def set_tag(self, key: str, value: str) -> None:
        for inst in self.aws.instances.values():
            inst["Tags"] = [t for t in inst["Tags"] if t["Key"] != key] + [{"Key": key, "Value": value}]


class DownTest(ProvisionTest):
    def down(self, **kw: Any) -> dict[str, Any]:
        args = {"assume_yes": True, "purge": False} | kw
        return teardown.down("c1", self.aws, **args)  # type: ignore[arg-type]

    def test_reexported_from_provision(self) -> None:
        for name in ("down", "list_clusters", "extend", "refresh_ip", "refresh_status", "parse_until"):
            self.assertIs(getattr(provision, name), getattr(teardown, name))

    def test_zero_instances_without_state(self) -> None:
        result = self.down(assume_yes=False)
        self.assertEqual((result["terminated"], result["security_groups"], result["key_pair"]), ([], [], None))
        self.assertEqual(self.aws.ops("terminate-instances"), [])
        self.assertIsNone(st.load("c1"))

    def test_requires_yes_without_tty(self) -> None:
        self.up()
        with mock.patch.object(sys, "stdin", mock.Mock(isatty=lambda: False)):
            with self.assertRaisesRegex(PreconditionError, "confirmation"):
                self.down(assume_yes=False)

    def test_full_teardown_with_dependency_violation_and_purge(self) -> None:
        self.up()
        self.aws.sg_dependency_failures, self.aws.enis = 2, ["eni-1"]
        result = self.down()
        self.assertEqual(result["terminated"], ["i-0000", "i-0001", "i-0002"])
        self.assertEqual((result["security_groups"], result["key_pair"]), (["sg-1"], f"vsbench-{OWNER}-c1"))
        self.assertEqual((self.aws.groups, self.aws.key_pairs, self.aws.enis), ({}, {}, []))
        self.assertTrue(st.load("c1")["terminated_at"])
        again = self.down(assume_yes=False, purge=True)
        self.assertEqual(again["terminated"], [])
        self.assertFalse(st.paths("c1").root.exists())

    def test_waits_through_pending_and_stopping_reads(self) -> None:
        # The botocore instance-terminated waiter fails at once on these reads (the fake refuses that waiter).
        self.up()
        self.aws.instances["i-0000"]["State"] = {"Name": "pending"}  # a killed up
        self.aws.instances["i-0001"]["State"] = {"Name": "stopping"}  # stopped from the console
        self.aws.stale_reads = 2
        result = self.down()
        self.assertEqual(result["terminated"], ["i-0000", "i-0001", "i-0002"])
        self.assertEqual({i["State"]["Name"] for i in self.aws.instances.values()}, {"terminated"})
        self.assertGreater(len(self.aws.ops("terminate-instances")), 1)  # re-sent after the pending reads
        self.assertEqual(self.aws.groups, {})

    def test_wait_gives_up_with_a_hint(self) -> None:
        self.up()
        self.aws.stale_reads = 10**6
        with self.assertRaisesRegex(VsbenchError, "not terminated") as ctx:
            self.down()
        self.assertIn("down --yes", ctx.exception.hint or "")
        self.assertTrue(self.aws.groups)  # nothing is deleted before the instances are gone

    def test_never_touches_same_named_resources_of_another_owner(self) -> None:
        # owner 'szymon' + cluster 'wasik-c1' and owner 'szymon.wasik' + 'c1' could share a name; tags decide.
        name, foreign = f"vsbench-{OWNER}-c1", [{"Key": "VsBenchOwner", "Value": "szymon"}]
        foreign += [{"Key": "VsBenchCluster", "Value": "wasik-c1"}]
        self.aws.groups["sg-9"] = {"GroupId": "sg-9", "GroupName": name, "VpcId": "vpc-default", "Tags": foreign}
        self.aws.key_pairs[name], self.aws.key_tags[name] = "ssh-ed25519 AAAAOTHER x", foreign
        result = self.down()
        self.assertEqual((result["security_groups"], result["key_pair"]), ([], None))
        self.assertIn("sg-9", self.aws.groups)
        self.assertIn(name, self.aws.key_pairs)

    def test_keeps_the_local_state_of_another_region(self) -> None:
        self.up()  # state in us-east-1
        self.aws.region = "us-west-2"
        result = self.down(purge=True)
        self.assertIsNone(st.load("c1")["terminated_at"])
        self.assertFalse(result["purged"])
        self.assertTrue(any("us-east-1" in w for w in warnings(teardown)))

    def test_keeps_the_local_state_of_another_account(self) -> None:
        # `down --profile <other account>` finds nothing there; the cluster of the local state keeps running.
        self.up()
        self.aws.account = "123456789012"
        self.aws.instances.clear()
        self.aws.groups.clear()
        self.aws.key_pairs.clear()
        result = self.down(purge=True)
        self.assertEqual(result["terminated"], [])
        self.assertIsNone(st.load("c1")["terminated_at"])
        self.assertFalse(result["purged"])
        self.assertTrue(st.paths("c1").root.exists())
        self.assertTrue(any("123456789012" in w and "kept" in w for w in warnings(teardown)))

    def test_security_group_not_found_is_success_and_timeout_raises(self) -> None:
        teardown.delete_security_group(self.aws, "sg-missing")  # type: ignore[arg-type]
        self.aws.groups["sg-1"] = {"GroupId": "sg-1"}
        self.aws.sg_dependency_failures = 10**6
        clock = iter(range(0, 10**6, 60))
        with mock.patch.object(teardown, "_monotonic", side_effect=lambda: next(clock)):
            with self.assertRaises(AwsError):
                teardown.delete_security_group(self.aws, "sg-1")  # type: ignore[arg-type]


class ExtendTest(ProvisionTest):
    def setUp(self) -> None:
        super().setUp()
        self.up()
        self.expires = proc.parse_iso(st.load("c1")["expires_at"])

    def extend(self, *args: Any, aws: Any = "fake") -> Any:
        return teardown.extend("c1", self.aws if aws == "fake" else aws, *args)

    def test_extend_nodes_first_then_tags(self) -> None:
        self.answer()
        state = self.extend(48 * 3600, None, False)
        target = proc.parse_iso(state["expires_at"])
        self.assertGreater(target, self.expires)
        cmd = remote.run_many.call_args.args[2]  # type: ignore[attr-defined]
        self.assertIn(f"sudo mv {config.NODE_EXPIRES_FILE}.tmp {config.NODE_EXPIRES_FILE}", cmd)
        self.assertIn(str(int(target.timestamp())), cmd)
        self.assertEqual(self.aws.ops("create-tags")[-1][-1], f"Key=ExpiresAt,Value={state['expires_at']}")
        self.assertIsNone(state["expires_tag_pending"])

    def test_partial_node_failure_keeps_earliest_expiry(self) -> None:
        self.answer(bad="vs-0")
        with self.assertRaisesRegex(VsbenchError, "vs-0"):
            self.extend(48 * 3600, None, False)
        self.assertEqual(self.aws.ops("create-tags"), [])
        self.assertEqual(proc.parse_iso(st.load("c1")["expires_at"]), self.expires)

    def test_validation(self) -> None:
        self.answer()
        cases = [
            ((None, None, False), "exactly one"),
            ((3600, None, False), "before the current expiry"),
            ((60, None, True), "15 min"),
            ((None, datetime.datetime(2030, 1, 1), False), "timezone"),
        ]
        for args, message in cases:
            with self.assertRaisesRegex(VsbenchError, message):
                self.extend(*args)
        state = self.extend(3600, None, True, aws=None)
        self.assertLess(proc.parse_iso(state["expires_at"]), self.expires)

    def test_expiry_is_capped_at_the_max_ttl(self) -> None:
        self.answer()
        year = proc.utcnow() + datetime.timedelta(days=365)
        for args in ((8 * 86400, None, False), (None, year, False)):
            with self.assertRaisesRegex(VsbenchError, "more than 7d0h from now"):
                self.extend(*args)
        self.assertEqual(remote.run_many.call_count, 0)  # type: ignore[attr-defined]  # nodes untouched
        state = self.extend(config.MAX_TTL_SECONDS, None, False)
        self.assertEqual(len(str(int(proc.parse_iso(state["expires_at"]).timestamp()))), 10)

    def test_tag_failure_only_warns_and_is_resynced_by_refresh_status(self) -> None:
        self.answer()
        self.aws.fail_once["create-tags"] = [AuthError("expired", "ExpiredToken")]
        state = self.extend(30 * 3600, None, False)
        self.assertGreater(proc.parse_iso(state["expires_at"]), self.expires)
        self.assertIn("not the ExpiresAt tag", warnings(teardown)[-1])
        self.assertEqual(state["expires_tag_pending"], state["expires_at"])
        refreshed = teardown.refresh_status("c1", self.aws)  # type: ignore[arg-type]
        self.assertEqual(self.aws.ops("create-tags")[-1][-1], f"Key=ExpiresAt,Value={state['expires_at']}")
        self.assertIsNone(refreshed["expires_tag_pending"])

    def test_extend_without_credentials_is_resynced_by_refresh_ip(self) -> None:
        self.answer()
        state = self.extend(30 * 3600, None, False, aws=None)
        self.assertEqual(state["expires_tag_pending"], state["expires_at"])
        teardown.refresh_ip("c1", self.aws)  # type: ignore[arg-type]
        tags = self.aws.ops("create-tags")[-1]
        self.assertEqual(
            (tags[1:4], tags[-1]), (["i-0000", "i-0001", "i-0002"], f"Key=ExpiresAt,Value={state['expires_at']}")
        )
        self.assertIsNone(st.load("c1")["expires_tag_pending"])


class StatusTest(ProvisionTest):
    def test_list_clusters_groups_and_flags_overdue(self) -> None:
        self.up()
        shutil.rmtree(st.paths("c1").root)  # no local state: the tag is all there is
        self.set_tag("ExpiresAt", proc.iso(proc.utcnow() - datetime.timedelta(hours=1)))
        rows = teardown.list_clusters(self.aws, OWNER)  # type: ignore[arg-type]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((row["cluster"], row["owner"], row["nodes"], row["states"]), ("c1", OWNER, 3, {"running": 3}))
        self.assertEqual((row["overdue"], row["tag_stale"]), (True, False))
        self.assertAlmostEqual(row["cost_per_hour"], 0.686 + 0.943 + 0.471)
        self.assertEqual(self.aws.ops("create-tags"), [])

    def test_list_prefers_a_later_local_expiry_and_resyncs_the_tag(self) -> None:
        # extend ran without credentials: the nodes and state moved on, the tag is an hour in the past.
        self.up()
        self.answer()
        state = teardown.extend("c1", None, 30 * 3600, None, False)
        self.set_tag("ExpiresAt", proc.iso(proc.utcnow() - datetime.timedelta(hours=1)))
        row = teardown.list_clusters(self.aws, OWNER)[0]  # type: ignore[arg-type]
        self.assertEqual((row["overdue"], row["tag_stale"], row["expires_at"]), (False, True, state["expires_at"]))
        self.assertGreater(row["expires_in_s"], 29 * 3600)
        tags = {awsapi.tags_of(i)["ExpiresAt"] for i in self.aws.instances.values()}
        self.assertEqual(tags, {state["expires_at"]})
        self.assertIsNone(st.load("c1")["expires_tag_pending"])  # the cli shows tag_stale, not OVERDUE

    def test_refresh_status_and_refresh_ip(self) -> None:
        self.up()
        remote.write_ssh_config.reset_mock()  # type: ignore[attr-defined]
        self.aws.instances["i-0001"]["PublicIpAddress"] = "4.4.4.4"
        self.aws.instances["i-0002"]["State"] = {"Name": "terminated"}
        del self.aws.instances["i-0000"]
        state = teardown.refresh_status("c1", self.aws)  # type: ignore[arg-type]
        self.assertEqual([n["aws_state"] for n in st.nodes(state)], ["not-found", "running", "terminated"])
        self.assertEqual(st.node(state, "vs-0")["public_ip"], "4.4.4.4")
        remote.write_ssh_config.assert_called_once()  # type: ignore[attr-defined]
        with mock.patch.object(teardown, "operator_cidr", return_value="5.6.7.8/32"):
            state = teardown.refresh_ip("c1", self.aws)  # type: ignore[arg-type]
        self.assertEqual(state["aws"]["operator_cidr"], "5.6.7.8/32")
        cidrs = [r["CidrIp"] for p in self.aws.groups["sg-1"]["IpPermissions"] for r in p.get("IpRanges", [])]
        self.assertEqual(cidrs, ["5.6.7.8/32"])

    def test_refresh_ip_on_a_deleted_security_group_explains(self) -> None:
        self.up()
        del self.aws.groups["sg-1"]  # EC2 answers InvalidGroup.NotFound for --group-ids
        with self.assertRaisesRegex(VsbenchError, "sg-1 not found") as ctx:
            teardown.refresh_ip("c1", self.aws)  # type: ignore[arg-type]
        self.assertIn("status --refresh", ctx.exception.hint or "")


if __name__ == "__main__":
    unittest.main()
