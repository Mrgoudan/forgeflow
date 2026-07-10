"""Chaos: SIGKILL the engine at RANDOM points, repeatedly, and prove
convergence — the resume test pins one kill point exactly; this one sweeps
the space. Invariants under ANY kill schedule:
  - the task converges to done once a run survives;
  - the step ledger holds exactly one row per step for the attempt;
  - committed steps never re-run: each kill can duplicate at most the ONE
    interrupted (uncommitted) step's side effect, so the total side-effect
    count is bounded by steps + kills."""
from __future__ import annotations

import random
import signal
import sqlite3
import subprocess
import sys
import time
import unittest
from pathlib import Path

from helpers import tmpdir

N_KILLS = 5


class ChaosKillTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        (self.base / "repo").mkdir()
        self.driver = str(Path(__file__).resolve().parent / "chaos_driver.py")

    def _spawn(self):
        return subprocess.Popen([sys.executable, self.driver, str(self.base)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def _counts(self):
        out = {}
        for i in range(1, 5):
            p = self.base / "repo" / ("s%d.txt" % i)
            out["s%d" % i] = len(p.read_text().splitlines()) if p.exists() else 0
        return out

    def test_random_kill_schedule_converges(self):
        rng = random.Random()          # unseeded on purpose: sweep kill points
        kills = 0
        for _ in range(N_KILLS):
            p = self._spawn()
            time.sleep(rng.uniform(0.05, 0.55))
            if p.poll() is None:
                p.send_signal(signal.SIGKILL)
                p.wait()
                kills += 1
            else:
                break                  # finished before the kill landed

        # a run that is left alone must converge
        p = self._spawn()
        out, err = p.communicate(timeout=60)
        self.assertEqual(p.returncode, 0, err.decode())
        self.assertIn("final:done", out.decode())

        counts = self._counts()
        # every step ran at least once...
        for step, n in counts.items():
            self.assertGreaterEqual(n, 1, counts)
        # ...and committed steps never re-ran: at most one duplicated
        # side effect per kill (the step that was in flight)
        self.assertLessEqual(sum(counts.values()), 4 + kills, counts)

        # the ledger is consistent: one row per step, single attempt
        conn = sqlite3.connect(str(self.base / "ff" / "state" / "forgeflow.db"))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT attempt, step, count(*) c FROM task_steps"
            " GROUP BY attempt, step").fetchall()
        self.assertEqual(sorted(r["step"] for r in rows),
                         ["s1", "s2", "s3", "s4"])
        for r in rows:
            self.assertEqual((r["attempt"], r["c"]), (0, 1), dict(r))

        # convergence is stable: another run executes nothing new
        before = self._counts()
        p = self._spawn()
        out, _ = p.communicate(timeout=60)
        self.assertIn("final:done", out.decode())
        self.assertEqual(self._counts(), before)


if __name__ == "__main__":
    unittest.main()
