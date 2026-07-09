from __future__ import annotations

import unittest
from pathlib import Path

from helpers import tmpdir

from forgeflow import blocks, contract, db, queue
from forgeflow.blocks import block


# test-only blocks (registered once at import)
@block("test.flagclass", "local", {"ok", "agent_limit"})
def _flagclass(ctx, task, prev):
    """agent_limit while the flag file exists, ok after — the deterministic
    stand-in for 'the model is rate-limited right now'."""
    return ("agent_limit" if Path(ctx["flag"]).exists() else "ok"), {}


@block("test.rogue2", "local", {"ok"})
def _rogue(ctx, task, prev):
    return "not_declared", {}


@block("test.boom", "local", {"ok"})
def _boom(ctx, task, prev):
    raise RuntimeError("kaboom")


@block("test.stepdir_write", "local", {"ok"})
def _stepdir_write(ctx, task, prev):
    """writes STRAIGHT to _step_dir with no mkdir — passes only if the engine
    created the step dir before running the block (blocks must not defend)."""
    (Path(ctx["_step_dir"]) / "out.txt").write_text("ok")
    return "ok", {}


def shell_step(wf, name, cmd, targets, **kw):
    wf.step(name, blocks.get("shell.run"), timeout_s=kw.pop("timeout_s", 30),
            params={"cmd": cmd}, **kw)
    for outcome, tgt in targets.items():
        wf.on(name, outcome, tgt)
    return wf


def full_shell_map(ok="done"):
    return {"ok": ok, "nonzero": "failed", "mismatch": "failed", "timeout": "failed"}


class ValidateTest(unittest.TestCase):
    def test_unmapped_outcome(self):
        wf = contract.Workflow.define("w")
        wf.step("a", blocks.get("shell.run"), timeout_s=5, params={"cmd": ["true"]})
        wf.on("a", "ok", "done").on("a", "nonzero", "failed").on("a", "mismatch", "failed")
        with self.assertRaisesRegex(SystemExit, "unmapped outcomes.*timeout"):
            wf.validate()

    def test_phantom_outcome(self):
        wf = contract.Workflow.define("w")
        shell_step(wf, "a", ["true"], full_shell_map())
        wf.on("a", "sparkles", "done")
        with self.assertRaisesRegex(SystemExit, "phantom outcomes.*sparkles"):
            wf.validate()

    def test_unknown_target(self):
        wf = contract.Workflow.define("w")
        shell_step(wf, "a", ["true"], dict(full_shell_map(), ok="nowhere"))
        with self.assertRaisesRegex(SystemExit, "unknown target 'nowhere'"):
            wf.validate()

    def test_trap_cycle_rejected(self):
        # a <-> b with no terminal reachable: must refuse at startup
        wf = contract.Workflow.define("w")
        wf.step("a", blocks.get("db.transition"), timeout_s=5,
                params={"to_state": "triaged", "event": "e"})
        wf.on("a", "ok", "b")
        wf.step("b", blocks.get("db.transition"), timeout_s=5,
                params={"to_state": "triaged", "event": "e"})
        wf.on("b", "ok", "a")
        with self.assertRaisesRegex(SystemExit, "no terminal state reachable"):
            wf.validate()

    def test_unreachable_step(self):
        wf = contract.Workflow.define("w")
        shell_step(wf, "a", ["true"], full_shell_map())
        shell_step(wf, "island", ["true"], full_shell_map())
        with self.assertRaisesRegex(SystemExit, "unreachable steps.*island"):
            wf.validate()

    def test_zero_timeout_rejected(self):
        wf = contract.Workflow.define("w")
        with self.assertRaises(TypeError):
            wf.step("a", blocks.get("shell.run"))  # timeout_s is keyword-required


class ExecuteTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.conn = db.connect(self.dir / "t.db")
        self.env = contract.ExecEnv(conn=self.conn, subscriptions={},
                                    data_dir=self.dir / "data",
                                    workspaces_dir=self.dir / "ws")

    def _claim(self, kind, payload=None):
        queue.enqueue(self.conn, kind, payload or {"k": kind})
        return queue.claim(self.conn)

    def test_undeclared_outcome_is_framework_bug(self):
        wf = contract.Workflow.define("w")
        wf.step("a", blocks.get("test.rogue2"), timeout_s=5).on("a", "ok", "done")
        wf.validate()
        task = self._claim("w")
        self.assertEqual(contract.execute(self.env, wf, task), "failed")
        row = self.conn.execute("SELECT error_class FROM tasks WHERE id=?",
                                (task["id"],)).fetchone()
        self.assertEqual(row["error_class"], "framework_bug")

    def test_uncaught_exception_is_framework_bug(self):
        wf = contract.Workflow.define("w")
        wf.step("a", blocks.get("test.boom"), timeout_s=5).on("a", "ok", "done")
        wf.validate()
        task = self._claim("w")
        self.assertEqual(contract.execute(self.env, wf, task), "failed")
        row = self.conn.execute("SELECT error_class FROM tasks WHERE id=?",
                                (task["id"],)).fetchone()
        self.assertEqual(row["error_class"], "framework_bug")

    def test_engine_creates_step_dir(self):
        # the engine must create _step_dir before the block runs; a block that
        # writes directly to it (no mkdir) is proof.
        wf = contract.Workflow.define("w")
        wf.step("a", blocks.get("test.stepdir_write"), timeout_s=5).on("a", "ok", "done")
        wf.validate()
        task = self._claim("w")
        self.assertEqual(contract.execute(self.env, wf, task), "done")
        out = (self.env.data_dir / "tasks" / str(task["id"]) / "a0" / "a" / "out.txt")
        self.assertTrue(out.exists())

    def test_timeout_maps_to_declared_outcome_then_policy(self):
        wf = contract.Workflow.define("w")
        shell_step(wf, "a", ["sleep", "5"], full_shell_map(), timeout_s=1)
        wf.validate()
        task = self._claim("w")
        # timeout outcome -> 'failed' target -> POLICY['timeout']: 1 retry, 60s backoff
        self.assertEqual(contract.execute(self.env, wf, task), "retry_wait")
        row = self.conn.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()
        self.assertEqual(row["attempts"], 1)
        gap = self.conn.execute(
            "SELECT CAST(strftime('%s', next_attempt) AS INTEGER)"
            " - CAST(strftime('%s','now') AS INTEGER) g FROM tasks WHERE id=?",
            (task["id"],)).fetchone()["g"]
        self.assertAlmostEqual(gap, 60, delta=3)
        step_row = self.conn.execute(
            "SELECT outcome FROM task_steps WHERE task_id=?", (task["id"],)).fetchone()
        self.assertEqual(step_row["outcome"], "timeout")
        # force due, fail again -> exhausted -> failed
        self.conn.execute("UPDATE tasks SET next_attempt=datetime('now') WHERE id=?",
                          (task["id"],))
        task2 = queue.claim(self.conn)
        self.assertEqual(task2["id"], task["id"])
        self.assertEqual(contract.execute(self.env, wf, task2), "failed")

    def test_park_then_unpark_completes(self):
        flag = self.dir / "flag"
        flag.write_text("x")
        wf = contract.Workflow.define("w")
        wf.step("a", blocks.get("test.flagclass"), timeout_s=5,
                params={"flag": str(flag)})
        wf.on("a", "ok", "done").on("a", "agent_limit", "failed")
        wf.validate()
        task = self._claim("w")
        # agent_limit outcome -> failed target -> POLICY parks immediately
        self.assertEqual(contract.execute(self.env, wf, task), "parked")
        self.assertIsNone(queue.claim(self.conn))  # parked blocks nothing
        flag.unlink()
        queue.unpark(self.conn)
        task2 = queue.claim(self.conn)
        self.assertEqual(contract.execute(self.env, wf, task2), "done")

    def test_visit_cap_bounds_pingpong(self):
        wf = contract.Workflow.define("w")
        shell_step(wf, "a", ["true"], dict(full_shell_map(), ok="b"))
        shell_step(wf, "b", ["true"], dict(full_shell_map(), ok="a"))
        wf.validate()  # statically fine (nonzero->failed exists)...
        task = self._claim("w")
        self.assertEqual(contract.execute(self.env, wf, task), "failed")
        row = self.conn.execute("SELECT error_class FROM tasks WHERE id=?",
                                (task["id"],)).fetchone()
        self.assertEqual(row["error_class"], "step_budget_exhausted")

    def test_retry_edge_loopback_invalidates_stale_history(self):
        # verify (red_retryable) -> fix -> verify (green): the stale verify
        # row must be replaced, and the walk must terminate 'done'.
        flag = self.dir / "needs-fix"
        flag.write_text("x")
        check = self.dir / "check.sh"
        check.write_text("#!/bin/sh\n[ ! -f %s ] || exit 7\n" % flag)
        wf = contract.Workflow.define("w")
        wf.step("verify", blocks.get("check.suite"), timeout_s=30, params={
            "checks": [{"name": "c", "cmd": ["sh", str(check)],
                        "retryable_exits": [7]}]})
        wf.on("verify", "green", "done").on("verify", "red", "failed")
        wf.on("verify", "red_retryable", "fix").on("verify", "timeout", "failed")
        shell_step(wf, "fix", ["rm", str(flag)], dict(full_shell_map(), ok="verify"))
        wf.validate()
        task = self._claim("w")
        self.assertEqual(contract.execute(self.env, wf, task), "done")
        rows = {r["step"]: r["outcome"] for r in self.conn.execute(
            "SELECT step, outcome FROM task_steps WHERE task_id=?", (task["id"],))}
        self.assertEqual(rows, {"verify": "green", "fix": "ok"})


if __name__ == "__main__":
    unittest.main()
