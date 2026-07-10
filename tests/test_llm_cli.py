"""The LLM experience surface: binding validation (structural at load,
environmental at engine start), backend knobs (params passthrough,
max_turns/extra_args), run latency/re-ask accounting, and the `llm`
CLI (check / show / runs)."""
from __future__ import annotations

import io
import json
import shutil
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from helpers import make_engine, make_pack, make_target_repo, tmpdir
from test_openai_compat import SCHEMA, FakeLLMServer

from forgeflow import cli, config, db, engine, queue, runner


class AgentStructuralValidationTest(unittest.TestCase):
    """config.load_pack: shape problems name the file and field."""

    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)

    def _load(self, agents_yaml):
        return config.load_pack(make_pack(self.base, self.repo,
                                          extra="agents:\n" + agents_yaml))

    def test_valid_bindings_load(self):
        pack = self._load(
            "  fix: { backend: claude-cli, cli: /bin/sh, model: m,"
            " max_turns: 5, extra_args: ['--x'] }\n"
            "  triage: { backend: openai-compat, base_url: 'http://h/v1',"
            " model: m2, params: { temperature: 0 } }\n")
        self.assertEqual(pack.agents["fix"]["max_turns"], 5)
        self.assertEqual(pack.agents["triage"]["params"], {"temperature": 0})

    def test_rejections(self):
        cases = [
            "  a: { backend: nonsense }\n",                       # unknown backend
            "  a: { backend: claude-cli, base_url: 'http://x' }\n",  # wrong key
            "  a: { backend: claude-cli, max_turns: 0 }\n",       # bad max_turns
            "  a: { backend: claude-cli, extra_args: '--x' }\n",  # not a list
            "  a: { backend: openai-compat, model: m }\n",        # no base_url
            "  a: { backend: openai-compat, base_url: 'ftp://x', model: m }\n",
            "  a: { backend: openai-compat, base_url: 'http://x' }\n",  # no model
            "  a: { backend: openai-compat, base_url: 'http://x',"
            " model: m, params: [1] }\n",                         # params type
        ]
        for agents_yaml in cases:
            with self.assertRaises(SystemExit, msg=agents_yaml):
                self._load(agents_yaml)


class AgentEnvironmentValidationTest(unittest.TestCase):
    """engine start: cli resolvable, secrets present — after any replay wrap."""

    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)

    def test_unresolvable_cli_fails_engine_start(self):
        pack_dir = make_pack(self.base, self.repo, extra=(
            "agents:\n  fix: { backend: claude-cli,"
            " cli: definitely-not-a-real-cli-x9 }\n"))
        pack = config.load_pack(pack_dir)          # structure is fine
        with self.assertRaisesRegex(SystemExit, "not found"):
            make_engine(self.base, pack_dir=pack_dir)
        del pack

    def test_resolvable_cli_is_pinned_to_abs_path(self):
        pack_dir = make_pack(self.base, self.repo, extra=(
            "agents:\n  fix: { backend: claude-cli, cli: sh }\n"))
        eng = make_engine(self.base, pack_dir=pack_dir)
        self.assertTrue(Path(eng.pack.agents["fix"]["cli"]).is_absolute())

    def test_missing_api_secret_fails_engine_start(self):
        pack_dir = make_pack(self.base, self.repo, extra=(
            "agents:\n  triage: { backend: openai-compat,"
            " base_url: 'http://127.0.0.1:1/v1', model: m,"
            " api_key_ref: NO_SUCH_REF_X9 }\n"))
        with self.assertRaisesRegex(SystemExit, "LLM_API_KEY_NO_SUCH_REF_X9"):
            make_engine(self.base, pack_dir=pack_dir)

    def test_replay_wrap_skips_live_environment(self):
        """--replay-from on a machine with neither the CLI nor the secret
        must still construct: replay needs only the recording."""
        rec_root = self.base / "rec"
        conn = db.connect(rec_root / "state" / "forgeflow.db")
        conn.close()
        pack_dir = make_pack(self.base, self.repo, extra=(
            "agents:\n"
            "  fix: { backend: claude-cli, cli: definitely-not-real-x9 }\n"))
        with self.assertRaises(SystemExit):
            make_engine(self.base, pack_dir=pack_dir)   # live: refused
        eng = engine.Engine(self.base / "ff2", pack=config.load_pack(pack_dir),
                            replay_from=rec_root)       # replay: fine
        self.assertEqual(eng.pack.agents["fix"]["backend"], "replay")


class BackendKnobsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.conn = db.connect(self.dir / "t.db")
        queue.enqueue(self.conn, "k", {"x": 1})
        self.task = queue.claim(self.conn)

    def test_openai_params_passthrough_and_guards(self):
        server = FakeLLMServer()
        self.addCleanup(server.stop)
        binding = {"backend": "openai-compat", "base_url": server.base_url,
                   "model": "m",
                   "params": {"temperature": 0, "max_tokens": 512,
                              "model": "EVIL", "messages": "EVIL"}}
        verdict = runner.run_agent(
            self.conn, self.task, binding, "You decide.", SCHEMA,
            data_dir=self.dir / "data", pack_rev="r", timeout_s=30, secrets={})
        self.assertEqual(verdict["verdict"], "FIXED")
        body = server.requests[0]["body"]
        self.assertEqual(body["temperature"], 0)
        self.assertEqual(body["max_tokens"], 512)
        self.assertEqual(body["model"], "m")               # not overridable
        self.assertIsInstance(body["messages"], list)      # not overridable

    def test_claude_cli_max_turns_and_extra_args(self):
        cli_dir = self.dir / "cli"
        cli_dir.mkdir()
        fake = cli_dir / "fake_agent_cli.py"
        shutil.copy(str(Path(__file__).parent / "fake_agent_cli.py"), str(fake))
        fake.chmod(0o755)
        (cli_dir / "mode").write_text("good")
        binding = {"backend": "claude-cli", "cli": str(fake), "model": "m",
                   "max_turns": 7, "extra_args": ["--allowedTools", "Bash"]}
        verdict = runner.run_agent(
            self.conn, self.task, binding, "You decide.", SCHEMA,
            data_dir=self.dir / "data", pack_rev="r", timeout_s=30)
        self.assertEqual(verdict["verdict"], "FIXED")
        argv = (cli_dir / "argv.1").read_text().splitlines()
        i = argv.index("--max-turns")
        self.assertEqual(argv[i + 1], "7")
        self.assertIn("--allowedTools", argv)
        self.assertIn("Bash", argv)

    def test_run_records_wall_and_reasks(self):
        server = FakeLLMServer()
        self.addCleanup(server.stop)
        server.mode = "invalid_then_good"
        binding = {"backend": "openai-compat", "base_url": server.base_url,
                   "model": "m"}
        runner.run_agent(self.conn, self.task, binding, "p", SCHEMA,
                         data_dir=self.dir / "data", pack_rev="r",
                         timeout_s=30, secrets={})
        run = self.conn.execute("SELECT * FROM runs").fetchone()
        self.assertEqual(run["reasks"], 1)                 # one correction round
        self.assertIsNotNone(run["wall_ms"])
        self.assertGreaterEqual(run["wall_ms"], 0)


class ProbeTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()

    def test_probe_openai_ok_and_down(self):
        server = FakeLLMServer()
        self.addCleanup(server.stop)
        # NOTE: the fake always answers verdict FIXED; the probe schema wants
        # OK — that is exactly the "transport fine, contract not followed"
        # case, which must be reported as a failure with a clear detail.
        binding = {"backend": "openai-compat", "base_url": server.base_url,
                   "model": "m"}
        r = runner.probe_binding("x", binding, out_dir=self.dir / "p",
                                 secrets={}, timeout_s=10)
        self.assertFalse(r["ok"])
        self.assertIn("output contract", r["detail"])
        binding["base_url"] = "http://127.0.0.1:1/v1"
        r = runner.probe_binding("x", binding, out_dir=self.dir / "p2",
                                 secrets={}, timeout_s=5)
        self.assertFalse(r["ok"])
        self.assertIn("agent_backend", r["detail"])

    def test_probe_claude_cli_follows_contract(self):
        cli_dir = self.dir / "cli"
        cli_dir.mkdir()
        fake = cli_dir / "fake_agent_cli.py"
        shutil.copy(str(Path(__file__).parent / "fake_agent_cli.py"), str(fake))
        fake.chmod(0o755)
        (cli_dir / "mode").write_text("probe_ok")
        binding = {"backend": "claude-cli", "cli": str(fake)}
        r = runner.probe_binding("x", binding, out_dir=self.dir / "p",
                                 timeout_s=15)
        self.assertTrue(r["ok"], r)
        self.assertIn("wall_ms", r)

    def test_probe_replay_reports_recordings(self):
        root = self.dir / "rec"
        conn = db.connect(root / "state" / "forgeflow.db")
        binding = {"backend": "replay", "source": str(root)}
        r = runner.probe_binding("x", binding, out_dir=self.dir / "p")
        self.assertFalse(r["ok"])                          # db but no verdicts
        self.assertEqual(r["recordings"], 0)
        tid = queue.enqueue(conn, "k", {"x": 1})
        conn.execute("INSERT INTO runs(task_id, model, prompt_sha, pack_rev,"
                     " verdict) VALUES (?,?,?,?,?)", (tid, "m", "s", "r", "OK"))
        conn.commit()
        r = runner.probe_binding("x", binding, out_dir=self.dir / "p")
        self.assertTrue(r["ok"])
        self.assertEqual(r["recordings"], 1)


class LlmCliTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)
        self.server = FakeLLMServer()
        self.addCleanup(self.server.stop)
        cli_dir = self.base / "cli"
        cli_dir.mkdir()
        self.fake = cli_dir / "fake_agent_cli.py"
        shutil.copy(str(Path(__file__).parent / "fake_agent_cli.py"),
                    str(self.fake))
        self.fake.chmod(0o755)
        (cli_dir / "mode").write_text("probe_ok")
        pack = self.base / "pack"
        pack.mkdir()
        (pack / "prompts").mkdir()
        (pack / "prompts" / "fix.md").write_text("You fix things.\n")
        (pack / "schemas").mkdir()
        (pack / "schemas" / "verdict.yaml").write_text(
            "type: object\nrequired: [verdict]\n"
            "properties:\n  verdict: { enum: [FIXED, NOOP] }\n")
        (pack / "project.yaml").write_text(
            "name: llmpack\n"
            "prompts: { fix: prompts/fix.md }\n"
            "schemas: { verdict: schemas/verdict.yaml }\n"
            "agents:\n"
            "  fix: { backend: claude-cli, cli: %s, model: probe-model }\n"
            "models:\n"
            "  bertish: { base_url: %s, model: emb-model }\n"
            % (self.fake, self.server.base_url))
        self.pack_dir = pack
        self.root = self.base / "run"

    def _main(self, *argv):
        out = io.StringIO()
        with redirect_stdout(out):
            code = cli.main(list(argv))
        return code, out.getvalue()

    def test_llm_check_probes_agents_and_models(self):
        code, out = self._main("--root", str(self.root), "--pack",
                               str(self.pack_dir), "llm", "check")
        self.assertEqual(code, 0, out)
        self.assertIn("agent fix", out)
        self.assertIn("model bertish", out)
        self.assertNotIn("FAIL", out)
        self.assertIn("2 binding(s) probed, 0 failure(s)", out)

    def test_llm_check_fails_loud_when_backend_down(self):
        self.server.stop()
        code, out = self._main("--root", str(self.root), "--pack",
                               str(self.pack_dir), "llm", "check", "bertish",
                               "--timeout", "5")
        self.assertEqual(code, 1)
        self.assertIn("FAIL", out)

    def test_llm_show_renders_prompt_and_sha(self):
        code, out = self._main("--root", str(self.root), "--pack",
                               str(self.pack_dir), "llm", "show", "fix",
                               "--data", '{"who": "world"}')
        self.assertEqual(code, 0, out)
        self.assertIn("prompt_sha=", out)
        self.assertIn("You fix things.", out)
        self.assertIn('"who":"world"', out)          # canonical context section
        self.assertIn("## output contract", out)

    def test_llm_runs_lists_recorded_runs(self):
        conn = db.connect(self.root / "state" / "forgeflow.db")
        tid = queue.enqueue(conn, "k", {"x": 1})
        conn.execute(
            "INSERT INTO runs(task_id, model, prompt_sha, pack_rev, verdict,"
            " exit_code, wall_ms, reasks, output_path, finished_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))",
            (tid, "probe-model", "s", "r", "FIXED", 0, 812, 1, "p"))
        conn.commit()
        conn.close()
        code, out = self._main("--root", str(self.root), "llm", "runs")
        self.assertEqual(code, 0)
        self.assertIn("probe-model", out)
        self.assertIn("FIXED", out)
        self.assertIn("812ms", out)


if __name__ == "__main__":
    unittest.main()
