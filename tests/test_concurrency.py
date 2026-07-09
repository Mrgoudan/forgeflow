from __future__ import annotations

import threading
import time
import unittest

from helpers import make_engine, tmpdir

from forgeflow import queue
from forgeflow.blocks import block

# a block that records the PEAK number of concurrent executions of itself, so a
# test can assert that a lane cap actually serializes.
_peak = {"cur": 0, "peak": 0}
_lock = threading.Lock()


@block("test.peaklane", "local", {"ok"})
def _peaklane(ctx, task, prev):
    with _lock:
        _peak["cur"] += 1
        _peak["peak"] = max(_peak["peak"], _peak["cur"])
    time.sleep(0.1)                      # long enough to overlap if allowed
    with _lock:
        _peak["cur"] -= 1
    return "ok", {}


_PROJ = "name: t\nworkflows: [workflows]\nconcurrency: { lanes: { serial: 1 } }\n"
_WF = ("workflow: work\nconsumes: [work.go]\nsteps:\n"
       "  - name: run\n    block: test.peaklane\n    timeout_s: 30\n"
       "    lane: %s\n    outcomes: { ok: done }\n")


def _pack(base, lane):
    p = base / "pack"
    (p / "workflows").mkdir(parents=True)
    (p / "project.yaml").write_text(_PROJ)
    (p / "workflows" / "work.yaml").write_text(_WF % lane)
    return p


class LaneConcurrencyTest(unittest.TestCase):
    def setUp(self):
        _peak["cur"] = 0
        _peak["peak"] = 0

    def _run(self, lane, n=6, workers=4):
        base = tmpdir()
        eng = make_engine(base, _pack(base, lane))
        for i in range(n):
            queue.enqueue(eng.conn, "work", {"i": i})
        executed = eng.run_until_idle(workers=workers)
        done = eng.conn.execute(
            "SELECT count(*) FROM tasks WHERE state='done'").fetchone()[0]
        return executed, done

    def test_uncapped_lane_runs_in_parallel(self):
        # 'free' is not a configured lane -> no cap -> real parallelism
        executed, done = self._run(lane="free")
        self.assertEqual((executed, done), (6, 6))     # every task ran, once
        self.assertGreater(_peak["peak"], 1)            # genuinely concurrent

    def test_capped_lane_serializes(self):
        # 'serial' has cap 1 -> never two block runs at once, even with 4 workers
        executed, done = self._run(lane="serial")
        self.assertEqual((executed, done), (6, 6))
        self.assertEqual(_peak["peak"], 1)              # strictly one at a time

    def test_no_double_claim_under_workers(self):
        # each of N tasks executes exactly once across the pool (atomic claim)
        executed, done = self._run(lane="free", n=20, workers=8)
        self.assertEqual((executed, done), (20, 20))


if __name__ == "__main__":
    unittest.main()
