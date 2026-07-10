from __future__ import annotations

import unittest
from pathlib import Path

from helpers import tmpdir

from forgeflow import cli, db, gc, queue


class RetryTest(unittest.TestCase):
    def setUp(self):
        self.conn = db.connect(tmpdir() / "t.db")

    def test_retry_failed_gets_fresh_attempt(self):
        tid = queue.enqueue(self.conn, "k", {"i": 1})
        queue.claim(self.conn)
        self.assertEqual(queue.fail(self.conn, tid, "agent_noop"), "failed")  # terminal
        self.assertEqual(queue.retry(self.conn), 1)
        row = self.conn.execute("SELECT state, attempts, error_class FROM tasks"
                                " WHERE id=?", (tid,)).fetchone()
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["attempts"], 1)          # fresh attempt (0 -> 1)
        self.assertIsNone(row["error_class"])
        self.assertEqual(queue.claim(self.conn)["id"], tid)

    def test_retry_by_kind(self):
        for i, k in enumerate(("a", "a", "b")):
            t = queue.enqueue(self.conn, k, {"n": i})
            queue.claim(self.conn)
            queue.fail(self.conn, t, "agent_noop")
        self.assertEqual(queue.retry(self.conn, kind="a"), 2)
        self.assertEqual(queue.retry(self.conn, kind="a"), 0)  # none left failed

    def test_force_bypasses_dedup(self):
        a = queue.enqueue(self.conn, "k", {"x": 1})
        b = queue.enqueue(self.conn, "k", {"x": 1})              # dedup
        c = queue.enqueue(self.conn, "k", {"x": 1, "_force": 7})  # fresh
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)


class GcTest(unittest.TestCase):
    def setUp(self):
        self.root = tmpdir()
        (self.root / "state").mkdir()
        (self.root / "data" / "tasks").mkdir(parents=True)
        (self.root / "data" / "runs").mkdir(parents=True)
        (self.root / "workspaces").mkdir()
        self.conn = db.connect(self.root / "state" / "forgeflow.db")

    def _task(self, tid, state, old):
        when = "'2000-01-01 00:00:00'" if old else "datetime('now')"
        self.conn.execute(
            "INSERT INTO tasks(id,kind,payload,payload_hash,state,updated_at)"
            " VALUES(?,?,'{}',?,?,%s)" % when, (tid, "k", "h%d" % tid, state))

    def test_collect_prunes_terminal_old_only(self):
        self._task(1, "done", old=True)      # prune archives + worktree
        self._task(2, "running", old=False)  # live: keep worktree
        self._task(3, "done", old=False)     # recent: keep archive
        for tid in (1, 2, 3):
            (self.root / "data" / "tasks" / str(tid)).mkdir()
            (self.root / "workspaces" / ("task-%d-a0" % tid)).mkdir()
        self.conn.execute("INSERT INTO runs(id,task_id,model,prompt_sha,pack_rev)"
                          " VALUES(9,1,'m','s','r')")
        (self.root / "data" / "runs" / "9").mkdir()
        self.conn.execute("INSERT INTO events(name,payload,at)"
                          " VALUES('e','{}','2000-01-01 00:00:00')")
        self.conn.execute("INSERT INTO events(name,payload) VALUES('e2','{}')")
        self.conn.commit()

        st = gc.collect(self.conn, self.root, days=14)
        self.assertEqual(st, {"worktrees": 2, "task_dirs": 1, "run_dirs": 1,
                              "events": 1, "joins": 0})
        self.assertFalse((self.root / "data" / "tasks" / "1").exists())     # old done
        self.assertTrue((self.root / "data" / "tasks" / "3").exists())      # recent
        self.assertFalse((self.root / "workspaces" / "task-1-a0").exists())  # terminal
        self.assertTrue((self.root / "workspaces" / "task-2-a0").exists())   # live
        self.assertFalse((self.root / "data" / "runs" / "9").exists())
        self.assertEqual(self.conn.execute("SELECT count(*) FROM events").fetchone()[0], 1)

    def test_dry_run_removes_nothing(self):
        self._task(1, "done", old=True)
        (self.root / "data" / "tasks" / "1").mkdir()
        st = gc.collect(self.conn, self.root, days=14, dry_run=True)
        self.assertEqual(st["task_dirs"], 1)
        self.assertTrue((self.root / "data" / "tasks" / "1").exists())  # untouched


class DoctorTest(unittest.TestCase):
    def _root(self):
        root = tmpdir()
        (root / "state").mkdir()
        (root / "workspaces").mkdir()
        conn = db.connect(root / "state" / "forgeflow.db")
        return root, conn

    def _beat(self, conn, age_s):
        import time as _t
        conn.execute("INSERT INTO watermarks(scope,cursor)"
                     " VALUES('daemon.heartbeat',?)", (str(int(_t.time()) - age_s),))
        conn.commit()

    def test_healthy_fresh_heartbeat(self):
        root, conn = self._root()
        self._beat(conn, 5)
        self.assertEqual(cli.main(["--root", str(root), "doctor"]), 0)

    def test_stale_daemon_with_running_task_flags(self):
        root, conn = self._root()
        self._beat(conn, 999)
        conn.execute("INSERT INTO tasks(id,kind,payload,payload_hash,state)"
                     " VALUES(1,'k','{}','h','running')")
        conn.commit()
        self.assertEqual(cli.main(["--root", str(root), "doctor", "--stale", "120"]), 1)

    def test_leaked_worktree_flags(self):
        root, conn = self._root()
        self._beat(conn, 5)
        (root / "workspaces" / "task-1-a0").mkdir()   # no such task -> leaked
        self.assertEqual(cli.main(["--root", str(root), "doctor"]), 1)


class MigrateTest(unittest.TestCase):
    def test_fresh_db_is_stamped_latest(self):
        conn = db.connect(tmpdir() / "f.db")
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0],
                         db.SCHEMA_VERSION)

    def test_existing_db_runs_pending_migration(self):
        import sqlite3
        p = tmpdir() / "m.db"
        db.connect(p).close()                       # a real db (tasks exists)
        c = sqlite3.connect(str(p)); c.execute("PRAGMA user_version=0"); c.close()
        old_v, old_m = db.SCHEMA_VERSION, db.MIGRATIONS
        try:
            db.SCHEMA_VERSION = 99
            db.MIGRATIONS = old_m + [
                (99, lambda c: c.execute(
                    "CREATE TABLE IF NOT EXISTS mig_marker(x)"))]
            conn = db.connect(p)                     # existing -> migrate
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 99)
            self.assertTrue(conn.execute("SELECT 1 FROM sqlite_master WHERE"
                                         " name='mig_marker'").fetchone())
        finally:
            db.SCHEMA_VERSION, db.MIGRATIONS = old_v, old_m


class DiskGuardTest(unittest.TestCase):
    def test_disk_gate_pauses_then_resumes(self):
        from types import SimpleNamespace
        from forgeflow import engine as E
        stub = SimpleNamespace(pack=SimpleNamespace(min_free_disk_mb=100),
                               root=".", _lowdisk=False)
        stub._disk_ok = lambda: E.Engine._disk_ok(stub)
        orig = E.shutil.disk_usage
        try:
            E.shutil.disk_usage = lambda p: SimpleNamespace(free=50 * 1024 * 1024)
            self.assertFalse(E.Engine._disk_gate(stub))   # 50 MB < 100 floor
            self.assertTrue(stub._lowdisk)
            E.shutil.disk_usage = lambda p: SimpleNamespace(free=500 * 1024 * 1024)
            self.assertTrue(E.Engine._disk_gate(stub))    # recovered
            self.assertFalse(stub._lowdisk)
        finally:
            E.shutil.disk_usage = orig

    def test_disk_ok_disabled_when_zero(self):
        from types import SimpleNamespace
        from forgeflow import engine as E
        stub = SimpleNamespace(pack=SimpleNamespace(min_free_disk_mb=0), root=".")
        self.assertTrue(E.Engine._disk_ok(stub))

    def test_doctor_flags_low_disk(self):
        root = tmpdir()
        (root / "state").mkdir()
        conn = db.connect(root / "state" / "forgeflow.db")
        import time as _t
        conn.execute("INSERT INTO watermarks(scope,cursor)"
                     " VALUES('daemon.heartbeat',?)", (str(int(_t.time())),))
        conn.commit()
        self.assertEqual(cli.main(["--root", str(root), "doctor",
                                   "--min-free", "99999999999"]), 1)


class CliSmokeTest(unittest.TestCase):
    def test_metrics_and_gc_run(self):
        root = tmpdir()
        (root / "state").mkdir()
        db.connect(root / "state" / "forgeflow.db")
        self.assertEqual(cli.main(["--root", str(root), "metrics"]), 0)
        self.assertEqual(cli.main(["--root", str(root), "gc", "--dry-run"]), 0)


if __name__ == "__main__":
    unittest.main()
