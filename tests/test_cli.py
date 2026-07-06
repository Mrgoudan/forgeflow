from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import unittest

from helpers import REPO_ROOT, make_pack, make_target_repo, tmpdir


class CliTest(unittest.TestCase):
    """The operator front door drives the same code paths as the daemon:
    emit --drive runs a full multi-workflow orchestration to idle."""

    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)
        self.pack_dir = make_pack(self.base, self.repo)
        self.root = self.base / "ff"

    def _cli(self, *args):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT)
        return subprocess.run(
            [sys.executable, "-m", "forgeflow",
             "--root", str(self.root), "--pack", str(self.pack_dir)] + list(args),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=120)

    def _db(self):
        conn = sqlite3.connect(str(self.root / "state" / "forgeflow.db"))
        conn.row_factory = sqlite3.Row
        return conn

    def test_validate_prints_orchestration_map(self):
        out = self._cli("validate")
        self.assertEqual(out.returncode, 0, out.stderr.decode())
        text = out.stdout.decode()
        self.assertIn("filebug", text)
        self.assertIn("scan -> reproduce -> file -> record", text)
        self.assertIn("finding.triaged", text)
        self.assertIn("-> notify", text)
        self.assertIn("OK: every workflow is total", text)

    def test_validate_fails_loud_on_broken_pack(self):
        (self.pack_dir / "project.yaml").write_text(
            "name: demo\npaths: { repo: /no/such/place }\n")
        out = self._cli("validate")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("does not exist", out.stderr.decode())

    def test_emit_drive_runs_multi_workflow_orchestration(self):
        out = self._cli("emit", "demo.scan_requested",
                        "--data", json.dumps({"key": "cli-1"}), "--drive")
        self.assertEqual(out.returncode, 0, out.stderr.decode())
        text = out.stdout.decode()
        self.assertIn("demo.scan_requested -> filebug", text)
        self.assertIn("executed 2 task(s)", text)   # filebug + notify via bus
        conn = self._db()
        finding = conn.execute("SELECT key, state FROM findings").fetchone()
        self.assertEqual((finding["key"], finding["state"]),
                         ("demo-cli-1", "triaged"))
        tasks = conn.execute("SELECT kind, state FROM tasks ORDER BY id").fetchall()
        self.assertEqual([(t["kind"], t["state"]) for t in tasks],
                         [("filebug", "done"), ("notify", "done")])

    def test_status_and_unpark(self):
        self._cli("emit", "demo.scan_requested",
                  "--data", json.dumps({"key": "cli-2"}), "--drive")
        out = self._cli("status")
        self.assertEqual(out.returncode, 0, out.stderr.decode())
        text = out.stdout.decode()
        self.assertIn("done", text)
        self.assertIn("filebug", text)
        self.assertIn("triaged", text)
        self.assertIn("demo.scan_requested", text)
        # park one manually, then release it via the CLI
        conn = self._db()
        conn.execute("UPDATE tasks SET state='parked', park_reason='operator'"
                     " WHERE kind='notify'")
        conn.commit()
        out = self._cli("unpark")
        self.assertIn("unparked 1 task(s)", out.stdout.decode())
        # once drains the now-eligible task through the same loop
        out = self._cli("once")
        self.assertEqual(out.returncode, 0, out.stderr.decode())
        self.assertIn("executed 1 task(s)", out.stdout.decode())


if __name__ == "__main__":
    unittest.main()
