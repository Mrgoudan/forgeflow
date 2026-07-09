from __future__ import annotations

import subprocess
import unittest

from helpers import REPO_ROOT, make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import db, queue


class EndToEndTest(unittest.TestCase):
    """THE PROOF: a demo pack + toy workflow YAML (scan.grep_rules ->
    check.recheck -> db.upsert_item -> db.transition) running through
    the REAL queue + engine + db against a throwaway git repo — and a second
    workflow triggered purely by the transition event."""

    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)
        self.pack_dir = make_pack(self.base, self.repo)
        self.engine = make_engine(self.base, pack_dir=self.pack_dir)

    def test_full_pipeline(self):
        eng = self.engine
        # loader wired the interactions from consumes: lists alone
        self.assertEqual(eng.subscriptions,
                         {"demo.scan_requested": ["filebug"],
                          "item.triaged": ["notify"]})

        # intake: one event starts everything
        db.emit_event(eng.conn, "demo.scan_requested", {"key": "planted-1"},
                      eng.subscriptions)
        executed = eng.run_until_idle()
        self.assertEqual(executed, 2)  # filebug, then notify via the event bus

        # the item was filed and transitioned by the evidence, atomically
        item = eng.conn.execute("SELECT * FROM items").fetchone()
        self.assertEqual(item["key"], "demo-planted-1")
        self.assertEqual(item["state"], "triaged")
        trans = eng.conn.execute("SELECT * FROM transitions").fetchall()
        self.assertEqual([(t["from_state"], t["to_state"], t["event"]) for t in trans],
                         [("found", "triaged", "evidence:repro_confirmed")])

        # both tasks terminal 'done'; filebug persisted all four boundaries
        tasks = eng.conn.execute("SELECT * FROM tasks ORDER BY id").fetchall()
        self.assertEqual([(t["kind"], t["state"]) for t in tasks],
                         [("filebug", "done"), ("notify", "done")])
        steps = eng.conn.execute(
            "SELECT step, outcome FROM task_steps WHERE task_id=? ORDER BY rowid",
            (tasks[0]["id"],)).fetchall()
        self.assertEqual([(s["step"], s["outcome"]) for s in steps],
                         [("scan", "ok"), ("reproduce", "confirmed"),
                          ("file", "ok"), ("record", "ok")])

        # the notify workflow saw the item id through the event payload
        outbox = self.base / "outbox"
        self.assertEqual([p.name for p in outbox.iterdir()],
                         ["notified-%d" % item["id"]])

        # replayed intake event: no duplicate task, nothing re-runs
        db.emit_event(eng.conn, "demo.scan_requested", {"key": "planted-1"},
                      eng.subscriptions)
        self.assertEqual(eng.run_until_idle(), 0)
        n = eng.conn.execute("SELECT count(*) c FROM tasks").fetchone()["c"]
        self.assertEqual(n, 2)

    def test_refuted_path_files_nothing(self):
        main = self.repo / "src" / "main.txt"
        main.write_text("nothing planted here\n")
        # scan finds nothing, oracle refutes (grep -c writes 0, exits 1)
        db.emit_event(self.engine.conn, "demo.scan_requested", {"key": "clean-1"},
                      self.engine.subscriptions)
        self.engine.run_until_idle()
        task = self.engine.conn.execute(
            "SELECT * FROM tasks WHERE kind='filebug'").fetchone()
        self.assertEqual(task["state"], "done")
        n = self.engine.conn.execute("SELECT count(*) c FROM items").fetchone()["c"]
        self.assertEqual(n, 0)

    def test_genericity_gate_passes(self):
        out = subprocess.run(["bash", str(REPO_ROOT / "scripts" / "check_generic.sh")],
                             cwd=str(REPO_ROOT), stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        self.assertEqual(out.returncode, 0, out.stdout.decode())


if __name__ == "__main__":
    unittest.main()
