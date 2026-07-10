"""Record/replay: a live run archives its schema-valid verdict; a replay
run answers the SAME prompt from that recording without touching any
backend, and a changed prompt is a loud miss — never a stale answer."""
from __future__ import annotations

import shutil
import unittest
from pathlib import Path

from helpers import make_pack, make_target_repo, tmpdir

from forgeflow import config, db, engine, queue, runner

SCHEMA = {"type": "object", "required": ["verdict"],
          "properties": {"verdict": {"enum": ["FIXED", "NOOP"]}}}


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        # the RECORDING root, laid out exactly like a real --root
        self.rec_root = self.dir / "recorded"
        self.rec_conn = db.connect(self.rec_root / "state" / "forgeflow.db")
        self.cli_dir = self.dir / "cli"
        self.cli_dir.mkdir()
        self.cli = self.cli_dir / "fake_agent_cli.py"
        shutil.copy(str(Path(__file__).parent / "fake_agent_cli.py"),
                    str(self.cli))
        self.cli.chmod(0o755)
        (self.cli_dir / "mode").write_text("good")

    def _record(self):
        """One live run against the fake CLI; returns the verdict."""
        tid = queue.enqueue(self.rec_conn, "k", {"x": 1})
        task = queue.claim(self.rec_conn)
        return runner.run_agent(
            self.rec_conn, task, {"backend": "claude-cli", "cli": str(self.cli),
                                  "model": "test-model"},
            "You are a fixer.", SCHEMA, data_dir=self.rec_root / "data",
            pack_rev="rev1", timeout_s=30, context_slice={"payload": {"x": 1}})

    def _calls(self):
        p = self.cli_dir / "calls"
        return int(p.read_text()) if p.exists() else 0

    def _replay(self, context_slice):
        """A separate root replaying from the recording."""
        rep_root = self.dir / "replayed"
        conn = db.connect(rep_root / "state" / "forgeflow.db")
        tid = queue.enqueue(conn, "k", {"x": 1})
        task = queue.claim(conn)
        return conn, runner.run_agent(
            conn, task, {"backend": "replay", "source": str(self.rec_root)},
            "You are a fixer.", SCHEMA, data_dir=rep_root / "data",
            pack_rev="rev2", timeout_s=30, context_slice=context_slice)

    def test_record_writes_verdict_json(self):
        verdict = self._record()
        self.assertEqual(verdict["verdict"], "FIXED")
        run = self.rec_conn.execute("SELECT * FROM runs").fetchone()
        vpath = self.rec_root / "data" / "runs" / str(run["id"]) / "verdict.json"
        self.assertTrue(vpath.is_file())
        self.assertIn('"verdict":"FIXED"', vpath.read_text())
        self.assertNotIn("_run_id", vpath.read_text())   # clean, revalidatable

    def test_replay_same_prompt_no_backend_call(self):
        self._record()
        calls_after_record = self._calls()
        conn, verdict = self._replay({"payload": {"x": 1}})
        self.assertEqual(verdict["verdict"], "FIXED")
        self.assertEqual(self._calls(), calls_after_record)  # no CLI exec
        # the replay run is itself pinned + audited in ITS root
        run = conn.execute("SELECT * FROM runs").fetchone()
        self.assertEqual(run["verdict"], "FIXED")
        self.assertIsNotNone(run["finished_at"])
        marker = (Path(run["output_path"]) / "ask0" / "replay.json")
        self.assertTrue(marker.is_file())

    def test_replay_miss_is_loud_and_fast(self):
        self._record()
        with self.assertRaises(runner.RunnerError) as cm:
            # different context -> different assembled prompt -> miss
            self._replay({"payload": {"x": 2}})
        self.assertEqual(cm.exception.error_class, "agent_invalid_output")
        self.assertIn("replay miss", str(cm.exception))

    def test_replay_from_wraps_every_agent(self):
        self._record()
        base = tmpdir()
        repo = make_target_repo(base)
        pack_dir = make_pack(base, repo, extra=(
            "agents:\n"
            "  fix: { backend: claude-cli, model: m1 }\n"
            "  triage: { backend: openai-compat, base_url: 'http://x', model: m2 }\n"))
        pack = config.load_pack(pack_dir)
        eng = engine.Engine(base / "ff", pack=pack,
                            replay_from=self.rec_root)
        for name in ("fix", "triage"):
            self.assertEqual(eng.pack.agents[name],
                             {"backend": "replay", "source": str(self.rec_root)})
        # a root with no recording is refused outright
        with self.assertRaises(SystemExit):
            engine.Engine(base / "ff2", pack=pack, replay_from=base / "empty")

    def test_pack_replay_binding_validated_at_load(self):
        base = tmpdir()
        repo = make_target_repo(base)
        pack_dir = make_pack(base, repo, extra=(
            "agents:\n  fix: { backend: replay }\n"))
        with self.assertRaises(SystemExit):     # no source
            config.load_pack(pack_dir)
        pack_dir2 = make_pack(base, repo, extra=(
            "agents:\n  fix: { backend: replay, source: /nonexistent-root }\n"))
        with self.assertRaises(SystemExit):     # no recording there
            config.load_pack(pack_dir2)
        self._record()
        pack_dir3 = make_pack(base, repo, extra=(
            "agents:\n  fix: { backend: replay, source: %s }\n" % self.rec_root))
        pack = config.load_pack(pack_dir3)
        self.assertEqual(pack.agents["fix"]["source"], str(self.rec_root))


if __name__ == "__main__":
    unittest.main()
