from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import time
import unittest
from pathlib import Path

from helpers import REPO_ROOT, tmpdir


class CrashResumeTest(unittest.TestCase):
    """Kill -9 mid-workflow, restart, and prove the engine resumes at the
    correct step via task_steps — the completed step's side effect happens
    exactly once."""

    def setUp(self):
        self.base = tmpdir()
        self.repo = self.base / "repo"
        self.repo.mkdir()
        (self.repo / "count.sh").write_text("#!/bin/sh\necho x >> counter.txt\n")
        (self.repo / "slow.sh").write_text(
            "#!/bin/sh\ntouch slow_started\n"
            "while [ -f flag ]; do sleep 0.05; done\n")
        (self.repo / "flag").write_text("hold")
        self.driver = str(Path(__file__).resolve().parent / "resume_driver.py")

    def _db(self):
        conn = sqlite3.connect(str(self.base / "ff" / "state" / "forgeflow.db"))
        conn.row_factory = sqlite3.Row
        return conn

    def test_kill9_then_resume(self):
        # run 1: hangs in 'slow' (flag file held) — kill it there
        p = subprocess.Popen([sys.executable, self.driver, str(self.base)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.time() + 30
        started = self.repo / "slow_started"
        while time.time() < deadline and not started.exists():
            time.sleep(0.05)
        self.assertTrue(started.exists(), "slow step never started; stderr: %s"
                        % p.stderr.read() if p.poll() is not None else "")
        os.kill(p.pid, signal.SIGKILL)
        p.wait()

        conn = self._db()
        task = conn.execute("SELECT * FROM tasks WHERE kind='resume_demo'").fetchone()
        self.assertEqual(task["state"], "running")  # orphaned by the crash
        steps = conn.execute("SELECT step, outcome FROM task_steps"
                             " WHERE task_id=?", (task["id"],)).fetchall()
        self.assertEqual([(s["step"], s["outcome"]) for s in steps], [("first", "ok")])
        self.assertEqual((self.repo / "counter.txt").read_text(), "x\n")
        conn.close()

        # run 2: flag released — must resume at 'slow', NOT re-run 'first'
        (self.repo / "flag").unlink()
        out = subprocess.run([sys.executable, self.driver, str(self.base)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr.decode())
        self.assertIn("final:done", out.stdout.decode())

        conn = self._db()
        task = conn.execute("SELECT * FROM tasks WHERE kind='resume_demo'").fetchone()
        self.assertEqual(task["state"], "done")
        self.assertEqual(task["attempts"], 0)  # crash resume, not a retry
        steps = conn.execute(
            "SELECT step, outcome, attempt FROM task_steps WHERE task_id=?"
            " ORDER BY rowid", (task["id"],)).fetchall()
        self.assertEqual([(s["step"], s["outcome"], s["attempt"]) for s in steps],
                         [("first", "ok", 0), ("slow", "ok", 0)])
        # THE resume proof: 'first' ran exactly once across both processes
        self.assertEqual((self.repo / "counter.txt").read_text(), "x\n")
        # replay-safety: enqueue in run 2 was absorbed by the idempotency key
        n = conn.execute("SELECT count(*) c FROM tasks").fetchone()["c"]
        self.assertEqual(n, 1)


if __name__ == "__main__":
    unittest.main()
