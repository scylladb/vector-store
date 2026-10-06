# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.guard: the commands `vsbench exec`/`ssh` refuse without --i-mean-it."""

from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import guard, remote  # noqa: E402

POWER_OFF = (
    "sudo shutdown -h now",
    "poweroff",
    "sudo /sbin/halt",
    "reboot -p",
    "sudo reboot -f -p",
    "reboot --poweroff",
    "sudo init 0",
    "telinit 0",
    "sudo systemctl poweroff",
    "sudo systemctl --force poweroff",
    "systemctl kexec",
    "echo o | sudo tee /proc/sysrq-trigger",
    "ls; sudo poweroff",
    "true && sudo halt",
    "x || shutdown now",
    "if true; then halt; fi",
    "{ poweroff; }",
    "(halt)",
    "echo $(halt)",
    "echo `halt`",
    'echo "$(sudo poweroff)"',
    "sudo -n shutdown -h now",
    "sudo -n -u ubuntu -- poweroff",
    "sudo -u root /sbin/halt",
    "sudo -s halt",
    "sudo -E env A=1 poweroff",
    "FOO=1 nohup poweroff",
    "timeout -s KILL 10 halt",
    "nice -n 5 halt",
    "sudo 'halt'",
    "sudo bash -c 'sleep 1; poweroff'",
    'sudo sh -c "docker stop x && shutdown -h +1"',
    "eval 'halt'",
    "sudo systemctl isolate runlevel0.target",
    "systemctl --no-block isolate runlevel0",
    "sudo systemctl start poweroff.target",
)
HARMLESS = (
    "sudo reboot",
    "reboot --help",
    "sudo docker restart scylla",
    "nodetool drain",
    "systemctl status vector-store",
    "systemctl list-dependencies poweroff.target",
    "cat /etc/asphalt",
    "init 3",
    "uptime",
    "echo halt",
    "echo $(date) shutdown",
    "last -x shutdown",
    "docker exec scylla nodetool status",
    # power-off words as search patterns (R1#33)
    "sudo docker logs scylla 2>&1 | grep -i shutdown",
    "journalctl -u vector-store | grep -c halt",
    'sudo journalctl -u scylla | grep -iE "error|shutdown"',
    "grep -E 'a|halt' f",
    "journalctl -b -1 | grep -i shutdown",
    "sudo -n grep -i shutdown /var/log/syslog",
    "sudo -u ubuntu grep -i shutdown f",
    "timeout 5 grep halt f",
    "sudo bash -c 'docker logs x | grep -i \"shutdown|halt\"'",
    # reading the TTL is fine
    "systemctl status vsbench-ttl.timer",
    "systemctl list-timers vsbench-ttl.timer",
    "sudo systemctl start vsbench-ttl.service",
    "journalctl -u vsbench-ttl",
    "journalctl -u vsbench-ttl > /tmp/vsbench-ttl.log",
    "cat /etc/vsbench/expires_at",
    "cat /etc/vsbench/expires_at > /tmp/e",
    "cp /etc/vsbench/expires_at /tmp/e",
    "cat /var/lib/vsbench/ttl.lastgood",
    "docker run --rm -v /etc/vsbench/expires_at:/x:ro alpine cat /x",
)
TTL_TAMPER = (
    "sudo systemctl disable --now vsbench-ttl.timer",
    "sudo systemctl --now disable vsbench-ttl.timer",
    "sudo systemctl stop vsbench-ttl.timer",
    "systemctl mask vsbench-ttl.timer",
    "echo 9999999999 | sudo tee /etc/vsbench/expires_at",
    "sudo sh -c 'echo 1 > /etc/vsbench/expires_at'",
    "echo 1 >> /var/lib/vsbench/ttl.lastgood",
    "sudo rm /var/lib/vsbench/ttl.lastgood",
    "sudo sed -i s/1/2/ /etc/vsbench/expires_at",
    "sudo sed -e 's/1/2/' -i /etc/vsbench/expires_at",
    "sudo mv /tmp/x /etc/vsbench/expires_at",
    "sudo mv /etc/vsbench/expires_at /tmp/x",
    "sudo cp /tmp/x /etc/vsbench/expires_at",
    "printf 9 | sudo dd of=/etc/vsbench/expires_at",
    "sudo chmod -x /usr/local/sbin/vsbench-ttl",
    "sudo rm /etc/systemd/system/vsbench-ttl.timer",
)


class PowerOffTest(unittest.TestCase):
    def test_matches_power_off_commands(self) -> None:
        for cmd in POWER_OFF:
            with self.subTest(cmd=cmd):
                self.assertTrue(remote.is_dangerous(cmd))
                self.assertEqual(remote.refusal_reason(cmd), guard.POWER_OFF_REASON)

    def test_ignores_ordinary_commands_and_search_patterns(self) -> None:
        for cmd in HARMLESS:
            with self.subTest(cmd=cmd):
                self.assertFalse(remote.is_dangerous(cmd))
                self.assertIsNone(remote.refusal_reason(cmd))

    def test_the_reason_tells_how_to_search_for_the_words(self) -> None:
        self.assertIn("shut.down", guard.POWER_OFF_REASON)
        self.assertIn("vsbench logs", guard.POWER_OFF_REASON)

    def test_no_catastrophic_backtracking(self) -> None:
        started = time.monotonic()
        for cmd in ("sudo " + "-u 1 " * 60 + "grep x", "env " + "A=1 " * 300 + "x", "sed " + "-i " * 300 + "x"):
            guard.is_dangerous(cmd)
        self.assertLess(time.monotonic() - started, 1.0)


class TtlTamperTest(unittest.TestCase):
    def test_matches_commands_that_disarm_the_ttl(self) -> None:
        for cmd in TTL_TAMPER:
            with self.subTest(cmd=cmd):
                self.assertTrue(guard.is_ttl_tamper(cmd))
                self.assertTrue(remote.is_dangerous(cmd))  # the cli refuses through is_dangerous
                self.assertEqual(remote.refusal_reason(cmd), guard.TTL_TAMPER_REASON)

    def test_reading_the_ttl_is_not_tampering(self) -> None:
        for cmd in HARMLESS:
            with self.subTest(cmd=cmd):
                self.assertFalse(remote.is_ttl_tamper(cmd))

    def test_the_reason_points_to_extend(self) -> None:
        self.assertIn("vsbench extend", guard.TTL_TAMPER_REASON)

    def test_remote_reexports_the_guard(self) -> None:
        for name in ("is_dangerous", "is_ttl_tamper", "refusal_reason", "POWER_OFF_REASON", "TTL_TAMPER_REASON"):
            with self.subTest(name=name):
                self.assertIs(getattr(remote, name), getattr(guard, name))


if __name__ == "__main__":
    unittest.main()
