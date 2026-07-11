"""Payload assembly governance: context assembly counts against the step
timeout, the per-run context manifest, the total context budget, and
`llm show --task` (full-fidelity, preview-clean)."""
from __future__ import annotations

import io
import json
import shutil
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import blocks, cli, contract, db, queue

# throwaway provider + block for the timing tests (module-scope: the
# registries are process-global and refuse duplicates)
SLEEP = {"s": 0.0}


@contract.context_provider("asmtest_slow")
def _slow_provider(env, task, spec):
    time.sleep(SLEEP["s"])
    return {"slept": SLEEP["s"]}


@blocks.block("asmtest.echo", "local", {"ok", "timeout"},
              accepts_context={"asmtest_slow"})
def _echo_block(ctx, task, prev):
    return "ok", {"budget_seen": ctx["_timeout_s"]}


class AssemblyBudgetTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.conn = db.connect(self.dir / "t.db")
        self.env = contract.ExecEnv(conn=self.conn, data_dir=self.dir / "d",
                                    workspaces_dir=self.dir / "w")
        self.wf = (contract.Workflow.define("asm")
                   .step("only", blocks.get("asmtest.echo"), timeout_s=1,
                         context=(("asmtest_slow", {}),))
                   .on("only", "ok", "done")
                   .on("only", "timeout", "failed"))
        self.wf.validate()

    def _run(self):
        queue.enqueue(self.conn, "asm", {"n": SLEEP["s"]})
        task = queue.claim(self.conn)
        return contract.execute(self.env, self.wf, task), task["id"]

    def test_assembly_time_is_deducted_from_block_budget(self):
        SLEEP["s"] = 0.4
        state, tid = self._run()
        self.assertEqual(state, "done")
        row = self.conn.execute("SELECT result FROM task_steps WHERE"
                                " task_id=?", (tid,)).fetchone()
        budget = json.loads(row["result"])["budget_seen"]
        self.assertLess(budget, 0.75)              # 1s minus ~0.4s assembly
        self.assertGreater(budget, 0.0)

    def test_assembly_overrun_is_the_timeout_outcome(self):
        SLEEP["s"] = 1.3
        state, tid = self._run()
        # timeout outcome -> POLICY gives one delayed retry
        self.assertEqual(state, "retry_wait")
        row = self.conn.execute("SELECT error_class FROM tasks WHERE id=?",
                                (tid,)).fetchone()
        self.assertEqual(row["error_class"], "timeout")
        # provider deadline is cleared even on the failure path
        self.assertIsNone(self.env.provider_deadline)


WF_YAML = """\
workflow: agentfix
consumes: [asm.wanted]
steps:
  - name: candidate
    block: agent.run
    llm: fix
    schema: verdict
    timeout_s: 60
    params: { max_context_bytes: %d }
    context:
      - payload
      - select: { corpus: notes, query: "{payload.q}", k: 2 }
    outcomes:
      FIXED: done
      NOOP: done
      agent_limit: parked
      agent_invalid: failed
      agent_backend: parked
      timeout: failed
"""


class ManifestAndShowTaskTest(unittest.TestCase):
    def _build(self, budget):
        base = tmpdir()
        repo = make_target_repo(base)
        cli_dir = base / "cli"
        cli_dir.mkdir()
        fake = cli_dir / "fake_agent_cli.py"
        shutil.copy(str(Path(__file__).parent / "fake_agent_cli.py"),
                    str(fake))
        fake.chmod(0o755)
        (cli_dir / "mode").write_text("good")
        wf_dir = base / "wf"
        wf_dir.mkdir()
        (wf_dir / "agentfix.yaml").write_text(WF_YAML % budget)
        pack_dir = make_pack(base, repo, workflows_dir=wf_dir, extra=(
            "schema: [schema.sql]\n"
            "prompts: { fix: prompts/fix.md }\n"
            "schemas: { verdict: schemas/verdict.yaml }\n"
            "agents:\n  fix: { backend: claude-cli, cli: %s, model: m }\n"
            "corpora:\n  notes: { table: notes, key: id, text: body,"
            " embed_with: hashing }\n" % fake))
        (pack_dir / "schema.sql").write_text(
            "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, body TEXT);")
        (pack_dir / "prompts").mkdir(exist_ok=True)
        (pack_dir / "prompts" / "fix.md").write_text("You fix things.\n")
        (pack_dir / "schemas").mkdir(exist_ok=True)
        (pack_dir / "schemas" / "verdict.yaml").write_text(
            "type: object\nrequired: [verdict]\n"
            "properties:\n  verdict: { enum: [FIXED, NOOP] }\n")
        eng = make_engine(base, pack_dir=pack_dir)
        eng.conn.executemany("INSERT INTO notes VALUES (?,?)",
                             [("n1", "parser crash in intake"),
                              ("n2", "billing rollup quarterly")])
        eng.conn.commit()
        tid = queue.enqueue(eng.conn, "agentfix", {"q": "parser crash"})
        return base, pack_dir, eng, tid

    def test_show_task_renders_full_assembly_preview_clean(self):
        base, pack_dir, eng, tid = self._build(budget=100000)
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli.main(["--root", str(base / "ff"), "--pack",
                             str(pack_dir), "llm", "show", "--task", str(tid)])
        text = out.getvalue()
        self.assertEqual(code, 0, text)
        self.assertIn("prompt_sha=", text)
        self.assertIn("## context: select", text)      # real provider ran
        self.assertIn("parser crash in intake", text)  # against live db state
        self.assertIn("context manifest", text)
        self.assertIn("TOTAL", text)
        self.assertIn("budget 100000", text)
        # preview-clean: no ledger writes, no model calls, no runs pinned
        self.assertEqual(eng.conn.execute(
            "SELECT count(*) FROM context_uses").fetchone()[0], 0)
        self.assertEqual(eng.conn.execute(
            "SELECT count(*) FROM runs").fetchone()[0], 0)

    def test_manifest_written_beside_the_run(self):
        base, pack_dir, eng, tid = self._build(budget=100000)
        self.assertEqual(eng.run_until_idle(), 1)
        self.assertEqual(eng.conn.execute(
            "SELECT state FROM tasks WHERE id=?", (tid,)).fetchone()["state"],
            "done")
        run = eng.conn.execute("SELECT id FROM runs").fetchone()
        manifest = json.loads(
            (base / "ff" / "data" / "runs" / str(run["id"]) /
             "context.json").read_text())
        self.assertEqual(manifest["llm"], "fix")
        providers = [s["provider"] for s in manifest["sections"]]
        self.assertEqual(providers, ["payload", "select"])
        for s in manifest["sections"]:
            self.assertGreater(s["bytes"], 0)
            self.assertEqual(len(s["sha256"]), 64)

    def test_budget_breach_fails_loudly_before_any_model_call(self):
        base, pack_dir, eng, tid = self._build(budget=10)
        eng.run_until_idle()
        row = eng.conn.execute("SELECT state, error_class FROM tasks"
                               " WHERE id=?", (tid,)).fetchone()
        self.assertEqual((row["state"], row["error_class"]),
                         ("failed", "framework_bug"))
        self.assertEqual(eng.conn.execute(
            "SELECT count(*) FROM runs").fetchone()[0], 0)   # never called

    def test_loader_rejects_bad_budget(self):
        base = tmpdir()
        repo = make_target_repo(base)
        wf_dir = base / "wf"
        wf_dir.mkdir()
        (wf_dir / "bad.yaml").write_text(WF_YAML % 0)
        pack_dir = make_pack(base, repo, workflows_dir=wf_dir, extra=(
            "schema: [schema.sql]\n"
            "prompts: { fix: prompts/fix.md }\n"
            "schemas: { verdict: schemas/verdict.yaml }\n"
            "agents:\n  fix: { backend: claude-cli, cli: sh }\n"
            "corpora:\n  notes: { table: notes, key: id, text: body }\n"))
        (pack_dir / "schema.sql").write_text(
            "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, body TEXT);")
        (pack_dir / "prompts").mkdir(exist_ok=True)
        (pack_dir / "prompts" / "fix.md").write_text("p\n")
        (pack_dir / "schemas").mkdir(exist_ok=True)
        (pack_dir / "schemas" / "verdict.yaml").write_text(
            "type: object\nrequired: [verdict]\n"
            "properties:\n  verdict: { enum: [FIXED, NOOP] }\n")
        with self.assertRaisesRegex(SystemExit, "max_context_bytes"):
            make_engine(base, pack_dir=pack_dir)


if __name__ == "__main__":
    unittest.main()
