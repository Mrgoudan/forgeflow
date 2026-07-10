"""Workflow definition versioning: def_hash stability, the mid-flight
definition-change park, fresh-attempt re-run under the new definition, and
the v1 -> v2 schema migration."""
from __future__ import annotations

import sqlite3
import unittest

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import db, loader, queue

WF = (
    "workflow: vtest\n"
    "consumes: [vtest.wanted]\n"
    "steps:\n"
    "  - name: first\n"
    "    block: shell.run\n"
    "    timeout_s: %d\n"
    "    params: { cmd: [\"true\"] }\n"
    "    outcomes: { ok: second, nonzero: failed, mismatch: failed, timeout: failed }\n"
    "  - name: second\n"
    "    block: shell.run\n"
    "    timeout_s: 30\n"
    "    params: { cmd: [\"true\"] }\n"
    "    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed }\n")


class DefHashTest(unittest.TestCase):
    def _load(self, text):
        d = tmpdir()
        (d / "w.yaml").write_text(text)
        return loader.load_workflow_file(d / "w.yaml")

    def test_stable_across_identical_loads(self):
        self.assertEqual(self._load(WF % 30).def_hash(),
                         self._load(WF % 30).def_hash())

    def test_changes_when_definition_changes(self):
        self.assertNotEqual(self._load(WF % 30).def_hash(),
                            self._load(WF % 31).def_hash())


class VersioningGateTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        repo = make_target_repo(self.base)
        wf_dir = self.base / "wf"
        wf_dir.mkdir()
        (wf_dir / "vtest.yaml").write_text(WF % 30)
        pack_dir = make_pack(self.base, repo, workflows_dir=wf_dir)
        self.eng = make_engine(self.base, pack_dir=pack_dir)
        self.conn = self.eng.conn

    def _mid_flight_task(self, def_hash):
        """A task that looks crash-recovered: recorded step for the current
        attempt, state pending, stamped with the given definition hash."""
        tid = queue.enqueue(self.conn, "vtest", {"key": "v1"})
        self.conn.execute(
            "INSERT INTO task_steps(task_id, attempt, step, outcome, result)"
            " VALUES (?,0,'first','ok','{}')", (tid,))
        self.conn.execute("UPDATE tasks SET def_hash=? WHERE id=?",
                          (def_hash, tid))
        return tid

    def test_fresh_task_gets_stamped(self):
        tid = queue.enqueue(self.conn, "vtest", {"key": "v0"})
        self.eng.run_until_idle()
        row = self.conn.execute("SELECT state, def_hash FROM tasks WHERE id=?",
                                (tid,)).fetchone()
        self.assertEqual(row["state"], "done")
        self.assertEqual(row["def_hash"],
                         self.eng.workflows["vtest"].def_hash())

    def test_mid_flight_definition_change_parks(self):
        tid = self._mid_flight_task("0" * 64)   # stamped under an old def
        self.eng.run_until_idle()
        row = self.conn.execute(
            "SELECT state, park_reason FROM tasks WHERE id=?", (tid,)).fetchone()
        self.assertEqual(row["state"], "parked")
        self.assertEqual(row["park_reason"], "definition_changed")
        # unpark = fresh attempt: re-runs from step 0 under the NEW definition
        queue.unpark(self.conn, tid)
        self.eng.run_until_idle()
        row = self.conn.execute(
            "SELECT state, attempts, def_hash FROM tasks WHERE id=?",
            (tid,)).fetchone()
        self.assertEqual(row["state"], "done")
        self.assertEqual(row["attempts"], 1)
        self.assertEqual(row["def_hash"], self.eng.workflows["vtest"].def_hash())
        steps = [r["step"] for r in self.conn.execute(
            "SELECT step FROM task_steps WHERE task_id=? AND attempt=1"
            " ORDER BY rowid", (tid,))]
        self.assertEqual(steps, ["first", "second"])

    def test_mid_flight_same_definition_resumes(self):
        tid = self._mid_flight_task(self.eng.workflows["vtest"].def_hash())
        self.eng.run_until_idle()
        row = self.conn.execute("SELECT state FROM tasks WHERE id=?",
                                (tid,)).fetchone()
        self.assertEqual(row["state"], "done")
        # 'first' was replayed from its recorded row, not re-run: still one row
        n = self.conn.execute(
            "SELECT count(*) FROM task_steps WHERE task_id=? AND attempt=0"
            " AND step='first'", (tid,)).fetchone()[0]
        self.assertEqual(n, 1)

    def test_null_stamp_adopts_current_definition(self):
        """A task from a pre-versioning engine (def_hash NULL) with recorded
        steps resumes under the current definition instead of parking."""
        tid = self._mid_flight_task(None)
        self.eng.run_until_idle()
        row = self.conn.execute("SELECT state, def_hash FROM tasks WHERE id=?",
                                (tid,)).fetchone()
        self.assertEqual(row["state"], "done")
        self.assertEqual(row["def_hash"], self.eng.workflows["vtest"].def_hash())


class MigrationTest(unittest.TestCase):
    def test_v1_db_migrates_to_v2(self):
        path = tmpdir() / "old.db"
        conn = sqlite3.connect(str(path))
        conn.executescript("""
            CREATE TABLE tasks (
                id            INTEGER PRIMARY KEY,
                kind          TEXT NOT NULL,
                item_id       INTEGER,
                payload       TEXT NOT NULL,
                payload_hash  TEXT NOT NULL,
                state         TEXT NOT NULL DEFAULT 'pending',
                attempts      INTEGER NOT NULL DEFAULT 0,
                error_class   TEXT,
                park_reason   TEXT,
                next_attempt  TEXT,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );
            INSERT INTO tasks(kind, payload, payload_hash) VALUES ('k','{}','h');
            PRAGMA user_version=1;
        """)
        conn.commit()
        conn.close()

        conn = db.connect(path)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 2)
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(tasks)")}
        self.assertIn("def_hash", cols)
        tables = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertIn("join_groups", tables)
        self.assertIn("join_members", tables)
        # existing rows survived the ALTER with a NULL stamp
        row = conn.execute("SELECT kind, def_hash FROM tasks").fetchone()
        self.assertEqual(row["kind"], "k")
        self.assertIsNone(row["def_hash"])


if __name__ == "__main__":
    unittest.main()
