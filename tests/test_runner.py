from __future__ import annotations

import json
import shutil
import subprocess
import unittest
from pathlib import Path

from helpers import REPO_ROOT, tmpdir

from forgeflow import db, queue, runner
from forgeflow.util import sha256_text

SCHEMA = {"type": "object", "required": ["verdict"],
          "properties": {"verdict": {"enum": ["FIXED", "NOOP"]}}}


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.conn = db.connect(self.dir / "t.db")
        self.cli_dir = self.dir / "cli"
        self.cli_dir.mkdir()
        self.cli = self.cli_dir / "fake_agent_cli.py"
        shutil.copy(str(Path(__file__).parent / "fake_agent_cli.py"), str(self.cli))
        self.cli.chmod(0o755)
        self.binding = {"backend": "claude-cli", "cli": str(self.cli),
                        "model": "test-model"}
        self.task_id = queue.enqueue(self.conn, "k", {"x": 1})
        self.task = queue.claim(self.conn)

    def _mode(self, mode):
        (self.cli_dir / "mode").write_text(mode)

    def _run(self, timeout_s=30):
        return runner.run_agent(
            self.conn, self.task, self.binding, "You are a fixer.", SCHEMA,
            data_dir=self.dir / "data", pack_rev="testrev",
            timeout_s=timeout_s, context_slice={"payload": {"x": 1}})

    def _runs(self):
        return self.conn.execute("SELECT * FROM runs").fetchall()

    def _calls(self):
        p = self.cli_dir / "calls"
        return int(p.read_text()) if p.exists() else 0

    def test_run_pinned_before_exec(self):
        (self.cli_dir / "dbpath").write_text(str(self.dir / "t.db"))
        self._mode("checkdb")
        verdict = self._run()
        # the CLI itself saw the runs row already committed when it executed
        self.assertEqual(verdict["runs_at_exec"], 1)
        run = self._runs()[0]
        self.assertEqual(run["model"], "test-model")
        self.assertEqual(run["pack_rev"], "testrev")
        self.assertEqual(run["verdict"], "FIXED")
        self.assertIsNotNone(run["finished_at"])
        # prompt snapshot archived verbatim; sha matches the pinned one
        prompt = (self.dir / "data" / "runs" / str(run["id"]) / "ask0" /
                  "prompt").read_text()
        self.assertEqual(sha256_text(prompt), run["prompt_sha"])
        self.assertIn("## context: payload", prompt)
        self.assertIn("## output contract", prompt)

    def test_reask_bounded_same_run_row_and_session(self):
        self._mode("invalid_then_good")
        verdict = self._run()
        self.assertEqual(verdict["verdict"], "FIXED")
        self.assertEqual(self._calls(), 2)          # one re-ask
        self.assertEqual(len(self._runs()), 1)      # SAME runs row
        argv2 = (self.cli_dir / "argv.2").read_text()
        self.assertIn("--resume\nsess-1", argv2)    # continued the session
        correction = (self.cli_dir / "stdin.2").read_text()
        self.assertIn("output contract", correction)

    def test_invalid_after_two_reasks_fails(self):
        self._mode("always_invalid")             # valid JSON, enum violation
        with self.assertRaises(runner.RunnerError) as cm:
            self._run()
        self.assertEqual(cm.exception.error_class, "agent_invalid_output")
        self.assertEqual(self._calls(), 3)       # initial + exactly 2 re-asks
        run = self._runs()[0]
        self.assertIsNone(run["verdict"])        # never validated
        self.assertIsNotNone(run["finished_at"])

    def test_nonzero_exit_is_backend_class(self):
        self._mode("fail")
        with self.assertRaises(runner.RunnerError) as cm:
            self._run()
        self.assertEqual(cm.exception.error_class, "agent_backend")

    def test_max_turns_envelope_is_agent_limit(self):
        self._mode("max_turns")
        with self.assertRaises(runner.RunnerError) as cm:
            self._run()
        self.assertEqual(cm.exception.error_class, "agent_limit")

    def test_timeout_escapes_with_run_row_intact(self):
        self._mode("sleep")
        with self.assertRaises(subprocess.TimeoutExpired):
            self._run(timeout_s=1)
        run = self._runs()[0]                    # the crash-evidence pin
        self.assertIsNone(run["verdict"])

    def test_extract_verdict_last_block_wins(self):
        text = ("```json\n{\"verdict\": \"NOOP\"}\n```\nwait actually\n"
                "```json\n{\"verdict\": \"FIXED\"}\n```")
        self.assertEqual(runner.extract_verdict(text, SCHEMA)["verdict"], "FIXED")
        with self.assertRaises(ValueError):
            runner.extract_verdict("no block", SCHEMA)

    def test_prompt_assembly_deterministic(self):
        a = runner.assemble_prompt("base", {"b": 1, "a": {"k": [1, 2]}}, SCHEMA)
        b = runner.assemble_prompt("base", {"a": {"k": [1, 2]}, "b": 1}, SCHEMA)
        self.assertEqual(sha256_text(a), sha256_text(b))


WF_YAML = """\
workflow: agentfix
steps:
  - name: candidate
    block: agent.run
    llm: fix
    schema: verdict
    timeout_s: 60
    context: [payload]
    outcomes:
      FIXED: done
      NOOP: done
      agent_limit: failed
      agent_invalid: failed
      agent_backend: failed
      timeout: failed
"""


class AgentWorkflowTest(unittest.TestCase):
    """agent.run through YAML + loader + engine: schema enums become step
    outcomes; the verdict routes the workflow; everything is pinned."""

    def setUp(self):
        self.dir = tmpdir()
        cli_dir = self.dir / "cli"
        cli_dir.mkdir()
        self.cli = cli_dir / "fake_agent_cli.py"
        shutil.copy(str(Path(__file__).parent / "fake_agent_cli.py"), str(self.cli))
        self.cli.chmod(0o755)
        (cli_dir / "mode").write_text("good")
        pack = self.dir / "pack"
        (pack / "workflows").mkdir(parents=True)
        (pack / "prompts").mkdir()
        (pack / "schemas").mkdir()
        (pack / "workflows" / "agentfix.yaml").write_text(WF_YAML)
        (pack / "prompts" / "fix.md").write_text("You fix things.\n")
        (pack / "schemas" / "verdict.yaml").write_text(
            "type: object\nrequired: [verdict]\n"
            "properties:\n  verdict: { enum: [FIXED, NOOP] }\n")
        (pack / "project.yaml").write_text(
            "name: agentpack\n"
            "workflows: [workflows]\n"
            "prompts: { fix: prompts/fix.md }\n"
            "schemas: { verdict: schemas/verdict.yaml }\n"
            "agents:\n"
            "  fix: { backend: claude-cli, cli: %s, model: test-model }\n"
            % self.cli)
        self.pack_dir = pack

    def test_agent_step_end_to_end(self):
        from forgeflow import config, engine
        eng = engine.Engine(self.dir / "ff", pack=config.load_pack(self.pack_dir))
        queue.enqueue(eng.conn, "agentfix", {"x": 1})
        self.assertEqual(eng.run_until_idle(), 1)
        task = eng.conn.execute("SELECT * FROM tasks").fetchone()
        self.assertEqual(task["state"], "done")
        step = eng.conn.execute("SELECT * FROM task_steps").fetchone()
        self.assertEqual((step["step"], step["outcome"]), ("candidate", "FIXED"))
        run = eng.conn.execute("SELECT * FROM runs").fetchone()
        self.assertEqual(run["verdict"], "FIXED")
        self.assertEqual(run["exit_code"], 0)
        result = json.loads(step["result"])
        self.assertEqual(result["_run_id"], run["id"])

    def test_loader_rejects_unmapped_schema_enum(self):
        wf = self.pack_dir / "workflows" / "agentfix.yaml"
        wf.write_text(WF_YAML.replace("      NOOP: done\n", ""))
        from forgeflow import config, engine
        with self.assertRaisesRegex(SystemExit, r"unmapped outcomes \['NOOP'\]"):
            engine.Engine(self.dir / "ff2", pack=config.load_pack(self.pack_dir))


if __name__ == "__main__":
    unittest.main()
