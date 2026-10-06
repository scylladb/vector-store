# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""shellcheck on every node/*.sh and cross/*.sh script (all severities must be clean)."""

from __future__ import annotations

import shutil
import subprocess
import sys
import unittest
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent
SCRIPT_GLOBS = ("node/*.sh", "cross/*.sh")
FALLBACK = Path.home() / ".local" / "bin" / "shellcheck"


def shellcheck() -> str | None:
    found = shutil.which("shellcheck")
    if found:
        return found
    return str(FALLBACK) if FALLBACK.is_file() else None


def scripts() -> list[Path]:
    return sorted(path for pattern in SCRIPT_GLOBS for path in SKILL_DIR.glob(pattern))


class ShellcheckTest(unittest.TestCase):
    def test_scripts_exist(self) -> None:
        self.assertTrue(scripts(), f"no scripts found under {SKILL_DIR} ({', '.join(SCRIPT_GLOBS)})")

    def test_shellcheck_clean(self) -> None:
        tool = shellcheck()
        if tool is None:
            message = (
                "SHELLCHECK IS NOT INSTALLED: node/*.sh and cross/*.sh were NOT checked. "
                "Install it (e.g. `uv tool install shellcheck-py`) and rerun."
            )
            sys.stderr.write(f"\n*** {message} ***\n")
            self.skipTest(message)
        for script in scripts():
            with self.subTest(script=str(script.relative_to(SKILL_DIR))):
                result = subprocess.run([tool, script.name], cwd=script.parent, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, f"shellcheck {script}:\n{result.stdout}{result.stderr}")


if __name__ == "__main__":
    unittest.main()
