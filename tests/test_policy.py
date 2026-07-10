"""Pack-configurable retry policy: build_policy validation, pack loading,
and the end-to-end path — a pack-defined error class driving retry_wait ->
park through a real workflow."""
from __future__ import annotations

import unittest

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import config, queue


class BuildPolicyTest(unittest.TestCase):
    def test_defaults_pass_through(self):
        pol = queue.build_policy({})
        self.assertEqual(pol, dict(queue.POLICY))
        self.assertEqual(queue.build_policy(None), dict(queue.POLICY))

    def test_override_engine_class(self):
        pol = queue.build_policy({"timeout": {"max_attempts": 5,
                                              "backoff_base_s": 1}})
        self.assertEqual(pol["timeout"].max_attempts, 5)
        self.assertEqual(pol["timeout"].backoff_base_s, 1)
        # untouched fields keep engine defaults
        self.assertEqual(pol["timeout"].backoff_cap_s,
                         queue.POLICY["timeout"].backoff_cap_s)
        # the engine table itself is never mutated
        self.assertEqual(queue.POLICY["timeout"].max_attempts, 1)

    def test_new_pack_class(self):
        pol = queue.build_policy({"flaky_net": {"max_attempts": 3,
                                                "park_on_exhaust": True}})
        p = pol["flaky_net"]
        self.assertEqual((p.max_attempts, p.park_on_exhaust), (3, True))
        self.assertEqual(p.backoff_base_s, 0)
        self.assertEqual(p.unpark_after_s, queue.UNPARK_AFTER_DEFAULT)

    def test_rejections(self):
        cases = [
            "not-a-dict",                                        # wrong type
            {"Bad Name": {"max_attempts": 1}},                   # class name
            {"timeout": {}},                                     # empty fields
            {"timeout": {"nope": 1}},                            # unknown field
            {"timeout": {"max_attempts": -1}},                   # negative
            {"timeout": {"max_attempts": True}},                 # bool as int
            {"timeout": {"park_on_exhaust": 1}},                 # int as bool
            {"timeout": {"unpark_after_s": -5}},                 # negative
            {"framework_bug": {"max_attempts": 3}},              # consume class
            {"newclass": {"backoff_base_s": 1}},                 # new: incomplete
        ]
        for overrides in cases:
            with self.assertRaises(ValueError, msg=repr(overrides)):
                queue.build_policy(overrides)

    def test_unpark_after_none_and_fail_uses_policy(self):
        pol = queue.build_policy({"agent_limit": {"unpark_after_s": None}})
        self.assertIsNone(pol["agent_limit"].unpark_after_s)
        # fail() consults the passed policy, not the global table
        from forgeflow import db
        conn = db.connect(tmpdir() / "t.db")
        tid = queue.enqueue(conn, "k", {"i": 1})
        pol2 = queue.build_policy({"flaky": {"max_attempts": 0,
                                             "park_on_exhaust": True}})
        self.assertEqual(queue.fail(conn, tid, "flaky", policy=pol2), "parked")


class PackPolicyTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)

    def test_pack_loads_retry_section(self):
        pack_dir = make_pack(self.base, self.repo, extra=(
            "retry:\n"
            "  timeout: { max_attempts: 4 }\n"
            "  policytest_flaky: { max_attempts: 1, park_on_exhaust: true }\n"))
        pack = config.load_pack(pack_dir)
        self.assertEqual(pack.policy["timeout"].max_attempts, 4)
        self.assertIn("policytest_flaky", pack.policy)

    def test_pack_rejects_bad_retry(self):
        pack_dir = make_pack(self.base, self.repo, extra=(
            "retry:\n  framework_bug: { max_attempts: 3 }\n"))
        with self.assertRaises(SystemExit):
            config.load_pack(pack_dir)

    def test_custom_class_drives_retry_then_park(self):
        """A pack block returns a pack-defined outcome mapped to 'failed';
        the pack's retry class turns that into retry_wait, then park on
        exhaustion — the full dispatch -> policy -> queue path."""
        blocks_dir = self.base / "pack" / "blocks"
        blocks_dir.mkdir(parents=True)
        (blocks_dir / "flaky.py").write_text(
            "from forgeflow.blocks import block\n\n"
            "@block('policytest.flaky', 'local', {'policytest_flaky'})\n"
            "def flaky(ctx, task, prev):\n"
            "    return 'policytest_flaky', {}\n")
        wf_dir = self.base / "wf"
        wf_dir.mkdir()
        (wf_dir / "flaky.yaml").write_text(
            "workflow: policytest_wf\n"
            "consumes: [policytest.wanted]\n"
            "steps:\n"
            "  - name: only\n"
            "    block: policytest.flaky\n"
            "    timeout_s: 10\n"
            "    outcomes: { policytest_flaky: failed }\n")
        pack_dir = make_pack(self.base, self.repo, workflows_dir=wf_dir, extra=(
            "blocks: [blocks/flaky.py]\n"
            "retry:\n"
            "  policytest_flaky:\n"
            "    max_attempts: 1\n"
            "    backoff_base_s: 0\n"
            "    backoff_cap_s: 0\n"
            "    park_on_exhaust: true\n"))
        eng = make_engine(self.base, pack_dir=pack_dir)
        queue.enqueue(eng.conn, "policytest_wf", {"key": "p1"})
        eng.run_until_idle()
        row = eng.conn.execute(
            "SELECT state, error_class, attempts FROM tasks").fetchone()
        self.assertEqual(row["state"], "parked")        # exhausted -> park
        self.assertEqual(row["error_class"], "policytest_flaky")
        self.assertEqual(row["attempts"], 2)            # first try + 1 retry


if __name__ == "__main__":
    unittest.main()
