from __future__ import annotations

import threading
import unittest

from helpers import tmpdir

from forgeflow import db, queue


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.conn = db.connect(self.dir / "t.db")

    def test_enqueue_idempotent(self):
        a = queue.enqueue(self.conn, "k", {"x": 1, "y": [1, 2]})
        b = queue.enqueue(self.conn, "k", {"y": [1, 2], "x": 1})  # same, reordered
        c = queue.enqueue(self.conn, "k", {"x": 2})
        d = queue.enqueue(self.conn, "other", {"x": 1, "y": [1, 2]})
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)
        self.assertNotEqual(a, d)
        n = self.conn.execute("SELECT count(*) c FROM tasks").fetchone()["c"]
        self.assertEqual(n, 3)

    def test_claim_atomicity(self):
        ids = [queue.enqueue(self.conn, "k", {"i": i}) for i in range(20)]
        claimed, lock = [], threading.Lock()

        def worker():
            conn = db.connect(self.dir / "t.db")
            while True:
                t = queue.claim(conn)
                if t is None:
                    return
                with lock:
                    claimed.append(t["id"])

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(claimed), sorted(ids))       # nothing lost
        self.assertEqual(len(claimed), len(set(claimed)))    # nothing doubled

    def test_claim_order_and_eligibility(self):
        first = queue.enqueue(self.conn, "k", {"i": 1})
        queue.enqueue(self.conn, "k", {"i": 2})
        t = queue.claim(self.conn)
        self.assertEqual(t["id"], first)  # oldest first
        # not eligible: next_attempt in the future
        self.conn.execute(
            "UPDATE tasks SET state='retry_wait',"
            " next_attempt=datetime('now','+1 hour') WHERE state='pending'")
        self.assertIsNone(queue.claim(self.conn))

    def test_retry_arithmetic_backoff_then_park(self):
        tid = queue.enqueue(self.conn, "k", {"i": 1})
        queue.claim(self.conn)
        # forge_server: 10 attempts, base 10s cap 600s, park on exhaust
        for attempt in range(1, 11):
            state = queue.fail(self.conn, tid, "forge_server")
            row = self.conn.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
            self.assertEqual(row["attempts"], attempt)
            self.assertEqual(state, "retry_wait")
            expected_delay = min(10 * 2 ** (attempt - 1), 600)
            gap = self.conn.execute(
                "SELECT CAST(strftime('%s', next_attempt) AS INTEGER)"
                " - CAST(strftime('%s', 'now') AS INTEGER) AS g"
                " FROM tasks WHERE id=?", (tid,)).fetchone()["g"]
            self.assertAlmostEqual(gap, expected_delay, delta=2)
        state = queue.fail(self.conn, tid, "forge_server")  # 11th failure
        self.assertEqual(state, "parked")
        row = self.conn.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
        self.assertEqual(row["park_reason"], "forge_server")

    def test_park_immediately_and_unpark(self):
        tid = queue.enqueue(self.conn, "k", {"i": 1})
        queue.claim(self.conn)
        self.assertEqual(queue.fail(self.conn, tid, "agent_limit"), "parked")
        # a parked task never blocks the loop
        self.assertIsNone(queue.claim(self.conn))
        self.assertEqual(queue.unpark(self.conn), 1)
        row = self.conn.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["attempts"], 1)  # unpark does NOT reset attempts
        t = queue.claim(self.conn)
        self.assertEqual(t["id"], tid)

    def test_consume_task_never_retries(self):
        tid = queue.enqueue(self.conn, "k", {"i": 1})
        queue.claim(self.conn)
        self.assertEqual(queue.fail(self.conn, tid, "workspace_dirty"), "failed")
        row = self.conn.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["error_class"], "workspace_dirty")

    def test_unknown_error_class_fails_terminal(self):
        tid = queue.enqueue(self.conn, "k", {"i": 1})
        queue.claim(self.conn)
        self.assertEqual(queue.fail(self.conn, tid, "no_such_class"), "failed")

    def test_reset_orphans(self):
        tid = queue.enqueue(self.conn, "k", {"i": 1})
        queue.claim(self.conn)
        self.assertEqual(queue.reset_orphans(self.conn), 1)
        row = self.conn.execute("SELECT state, attempts FROM tasks WHERE id=?",
                                (tid,)).fetchone()
        self.assertEqual((row["state"], row["attempts"]), ("pending", 0))


if __name__ == "__main__":
    unittest.main()
