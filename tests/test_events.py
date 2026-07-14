from __future__ import annotations

import unittest

from helpers import tmpdir

from forgeflow import db, queue, util


class EventsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.conn = db.connect(self.dir / "t.db")
        self.subs = {"item.triaged": ["fix", "notify"],
                     "custom.ping": ["pong"]}
        # the lifecycle is pack-declared now; tests declare a minimal one
        self.states = {"found": {"triaged", "rejected"},
                       "triaged": {"merged"}, "merged": set(),
                       "rejected": set()}

    def test_fanout_enqueues_all_subscribers_atomically(self):
        fid = db.upsert_item(self.conn, "K", "t", "test", "r")
        db.record_transition(self.conn, fid, "triaged", "evidence:x",
                             subscriptions=self.subs, states=self.states)
        tasks = self.conn.execute(
            "SELECT kind, state FROM tasks ORDER BY id").fetchall()
        self.assertEqual([(t["kind"], t["state"]) for t in tasks],
                         [("fix", "pending"), ("notify", "pending")])
        # the item, the audit row, the event, and both tasks all exist
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM transitions").fetchone()["c"], 1)
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM events WHERE name='item.triaged'"
        ).fetchone()["c"], 1)

    def test_rollback_rolls_back_fanout_too(self):
        fid = db.upsert_item(self.conn, "K", "t", "test", "r")
        try:
            with util.tx(self.conn):
                db.record_transition(self.conn, fid, "triaged", "evidence:x",
                                     subscriptions=self.subs,
                                     states=self.states)
                raise RuntimeError("boom mid-transaction")
        except RuntimeError:
            pass
        # NOTHING happened: no transition, no event, no tasks, state unchanged
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM transitions").fetchone()["c"], 0)
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM events").fetchone()["c"], 0)
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM tasks").fetchone()["c"], 0)
        self.assertEqual(self.conn.execute(
            "SELECT state FROM items WHERE id=?", (fid,)).fetchone()["state"],
            "found")

    def test_replayed_event_does_not_double_enqueue(self):
        payload = {"item_id": 1, "transition_id": 9}
        db.emit_event(self.conn, "custom.ping", payload, self.subs)
        db.emit_event(self.conn, "custom.ping", payload, self.subs)  # replay
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM tasks WHERE kind='pong'").fetchone()["c"], 1)
        # the event LOG keeps both (append-only fact log); the QUEUE dedups
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM events").fetchone()["c"], 2)

    def test_illegal_transition_refused(self):
        fid = db.upsert_item(self.conn, "K", "t", "test", "r")
        with self.assertRaises(db.TransitionError):   # not reachable from found
            db.record_transition(self.conn, fid, "merged", "nope",
                                 states=self.states)
        with self.assertRaises(db.TransitionError):   # not a declared state
            db.record_transition(self.conn, fid, "not_a_state", "nope",
                                 states=self.states)
        with self.assertRaises(db.TransitionError):   # no such item
            db.record_transition(self.conn, 999, "triaged", "nope",
                                 states=self.states)

    def test_no_declared_lifecycle_is_loud(self):
        fid = db.upsert_item(self.conn, "K2", "t", "test", "r")
        with self.assertRaisesRegex(db.TransitionError, "item_states"):
            db.record_transition(self.conn, fid, "triaged", "nope")

    def test_unsubscribed_event_is_just_a_fact(self):
        db.emit_event(self.conn, "custom.unconsumed", {"x": 1}, self.subs)
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM tasks").fetchone()["c"], 0)
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM events").fetchone()["c"], 1)


if __name__ == "__main__":
    unittest.main()
