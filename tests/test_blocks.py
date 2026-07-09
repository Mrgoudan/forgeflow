from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

from helpers import make_target_repo, tmpdir

from forgeflow.blocks import get, run_isolated


class BlocksTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()

    def test_duplicate_registration_is_startup_error(self):
        from forgeflow.blocks import block
        with self.assertRaisesRegex(SystemExit, "registered twice"):
            @block("shell.run", "local", {"ok"})
            def clash(ctx, task, prev):
                return "ok", {}

    def test_unknown_block_is_loud(self):
        with self.assertRaisesRegex(SystemExit, "unknown block"):
            get("does.not.exist")

    def test_shell_run_classifies_by_exit_code(self):
        outcome, result = run_isolated("shell.run", {"cmd": ["true"]})
        self.assertEqual(outcome, "ok")
        outcome, result = run_isolated("shell.run", {"cmd": ["false"]})
        self.assertEqual(outcome, "nonzero")
        self.assertEqual(result["exit_code"], 1)
        outcome, _ = run_isolated("shell.run",
                                  {"cmd": ["false"], "expected_exit": 1})
        self.assertEqual(outcome, "ok")

    def test_shell_run_expected_file_comparison(self):
        expected = self.dir / "expected"
        expected.write_text("hello\n")
        outcome, _ = run_isolated("shell.run", {"cmd": ["echo", "hello"],
                                                "expected_file": str(expected)})
        self.assertEqual(outcome, "ok")
        expected.write_text("goodbye\n")
        outcome, _ = run_isolated("shell.run", {"cmd": ["echo", "hello"],
                                                "expected_file": str(expected)})
        self.assertEqual(outcome, "mismatch")

    def test_shell_run_timeout_escapes(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            run_isolated("shell.run", {"cmd": ["sleep", "5"]}, timeout_s=1)

    def test_shell_run_payload_templating(self):
        out = self.dir / "made-by-task-7"
        outcome, _ = run_isolated(
            "shell.run", {"cmd": ["touch", str(self.dir) + "/made-by-task-{payload.n}"]},
            task={"id": 1, "attempts": 0, "payload": {"n": 7}})
        self.assertEqual(outcome, "ok")
        self.assertTrue(out.exists())

    def test_grep_rules_finds_planted_marker(self):
        repo = make_target_repo(self.dir)
        outcome, result = run_isolated("scan.grep_rules", {
            "repo": str(repo),
            "rules": [{"id": "planted", "pattern": "PLANTED_BUG",
                       "include": ["*.txt"]},
                      {"id": "absent", "pattern": "NOT_THERE_AT_ALL"}]})
        self.assertEqual(outcome, "ok")
        self.assertEqual(result["count"], 1)
        cand = result["candidates"][0]
        self.assertEqual((cand["rule"], cand["path"], cand["line"]),
                         ("planted", "src/main.txt", 2))

    def test_grep_rules_broken_pattern_is_loud(self):
        repo = make_target_repo(self.dir)
        with self.assertRaisesRegex(RuntimeError, "broken pattern"):
            run_isolated("scan.grep_rules", {
                "repo": str(repo),
                "rules": [{"id": "bad", "pattern": "(unclosed"}]})

    def test_oracle_reproduce_confirmed_and_refuted(self):
        repo = make_target_repo(self.dir)
        params = {"cmd": ["sh", "repro.sh"], "cwd": str(repo),
                  "expect": {"exit_code": 0,
                             "output_file": str(repo / "out.txt"),
                             "expected_file": str(repo / "expected.txt")}}
        outcome, result = run_isolated("check.recheck", dict(params))
        self.assertEqual(outcome, "confirmed")
        # remove the planted bug -> same oracle refutes
        main = repo / "src" / "main.txt"
        main.write_text(main.read_text().replace("PLANTED_BUG", "fixed"))
        outcome, result = run_isolated("check.recheck", dict(params))
        self.assertEqual(outcome, "refuted")

    def test_evidence_suite_exit_codes_only(self):
        checks = lambda code: {"checks": [
            {"name": "a", "cmd": ["true"]},
            {"name": "b", "cmd": ["sh", "-c", "exit %d" % code],
             "retryable_exits": [7]},
            {"name": "c", "cmd": ["true"]}]}
        outcome, result = run_isolated("check.suite", checks(0))
        self.assertEqual(outcome, "green")
        self.assertEqual(len(result["checks"]), 3)
        outcome, result = run_isolated("check.suite", checks(7))
        self.assertEqual((outcome, result["failed"]), ("red_retryable", "b"))
        self.assertEqual(len(result["checks"]), 2)  # stopped at first failure
        outcome, result = run_isolated("check.suite", checks(1))
        self.assertEqual((outcome, result["failed"]), ("red", "b"))

    def test_worktree_create_and_drop(self):
        repo = make_target_repo(self.dir)
        task = {"id": 42, "attempts": 0, "payload": {}}
        outcome, result = run_isolated("worktree.create", {"repo": str(repo)},
                                       task=task)
        self.assertEqual(outcome, "ok")
        ws = Path(result["path"])
        self.assertTrue((ws / "src" / "main.txt").exists())
        self.assertEqual(result["branch"], "task-42-a0")
        # re-entry is resumable: same call reuses the surviving worktree
        outcome2, result2 = run_isolated(
            "worktree.create",
            {"repo": str(repo), "_workspaces_dir": str(ws.parent)}, task=task)
        self.assertEqual((outcome2, result2["reused"]), ("ok", True))
        outcome3, _ = run_isolated("worktree.drop",
                                   {"repo": str(repo), "path": str(ws)})
        self.assertEqual(outcome3, "ok")
        self.assertFalse(ws.exists())

    def test_git_fold_and_branch_advanced(self):
        repo = make_target_repo(self.dir)
        base = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                              stdout=subprocess.PIPE).stdout.decode().strip()
        outcome, result = run_isolated("git.branch_advanced",
                                       {"repo": str(repo), "base": base})
        self.assertEqual((outcome, result["commits"]), ("not_advanced", 0))
        (repo / "new1.txt").write_text("1")
        subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-qm", "c1"], check=True)
        (repo / "new2.txt").write_text("2")
        subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-qm", "c2"], check=True)
        outcome, result = run_isolated("git.branch_advanced",
                                       {"repo": str(repo), "base": base})
        self.assertEqual((outcome, result["commits"]), ("advanced", 2))
        outcome, result = run_isolated("git.fold_commit",
                                       {"repo": str(repo), "base": base,
                                        "message": "folded"})
        self.assertEqual(outcome, "ok")
        outcome, result = run_isolated("git.branch_advanced",
                                       {"repo": str(repo), "base": base})
        self.assertEqual((outcome, result["commits"]), ("advanced", 1))
        outcome, _ = run_isolated("git.fold_commit",
                                  {"repo": str(repo), "base": "HEAD",
                                   "message": "x"})
        self.assertEqual(outcome, "nothing")


if __name__ == "__main__":
    unittest.main()
