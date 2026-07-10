"""Fan-out/join: the end-to-end barrier (fan-out -> members -> join event ->
collector), empty fan-outs, mixed member outcomes, exactly-once firing,
re-applied fan-out dedup (group reuse), and operator retry re-opening an
unfired member slot."""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import db, queue

JOIN_DEFS = Path(__file__).resolve().parent / "data" / "join"


class JoinE2ETest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        repo = make_target_repo(self.base)
        pack_dir = make_pack(self.base, repo, workflows_dir=JOIN_DEFS)
        self.eng = make_engine(self.base, pack_dir=pack_dir)
        self.conn = self.eng.conn

    def _emit(self, payload):
        db.emit_event(self.conn, "jointest.wanted", payload,
                      self.eng.subscriptions)
        self.eng.run_until_idle()

    def test_fanout_join_barrier(self):
        self._emit({"items": [0, 0, 1], "batch": "b1"})

        # one group, fired, expecting 3 members (3 items x 1 consumer)
        g = self.conn.execute("SELECT * FROM join_groups").fetchone()
        self.assertEqual(g["expect_n"], 3)
        self.assertIsNotNone(g["fired_at"])

        # identical items became distinct members (the _join index dedup key)
        members = self.conn.execute(
            "SELECT state FROM join_members WHERE group_id=?"
            " ORDER BY task_id", (g["id"],)).fetchall()
        self.assertEqual([m["state"] for m in members],
                         ["done", "done", "failed"])

        # the join event fired exactly once, with truthful counts + join data
        evs = self.conn.execute(
            "SELECT payload FROM events WHERE name='jointest.done'").fetchall()
        self.assertEqual(len(evs), 1)
        payload = json.loads(evs[0]["payload"])
        self.assertEqual(payload["total"], 3)
        self.assertEqual(payload["done"], 2)
        self.assertEqual(payload["failed"], 1)
        self.assertEqual(payload["batch"], "b1")
        self.assertEqual(payload["join_group"], g["id"])

        # the collector consumed the barrier and read the roster
        step = self.conn.execute(
            "SELECT s.result FROM task_steps s JOIN tasks t ON t.id=s.task_id"
            " WHERE t.kind='jointest_collect' AND s.step='gather'").fetchone()
        result = json.loads(step["result"])
        self.assertEqual(result["total"], 3)
        self.assertEqual(result["counts"], {"done": 2, "failed": 1})
        self.assertEqual(len(result["members"]), 3)
        self.assertFalse(result["truncated"])

    def test_empty_fanout_fires_immediately(self):
        self._emit({"items": [], "batch": "b2"})
        g = self.conn.execute("SELECT * FROM join_groups").fetchone()
        self.assertEqual(g["expect_n"], 0)
        self.assertIsNotNone(g["fired_at"])
        payload = json.loads(self.conn.execute(
            "SELECT payload FROM events WHERE name='jointest.done'"
        ).fetchone()["payload"])
        self.assertEqual(payload["total"], 0)
        # the fan step reported the 'empty' outcome
        step = self.conn.execute(
            "SELECT s.outcome FROM task_steps s JOIN tasks t ON t.id=s.task_id"
            " WHERE t.kind='jointest_fan' AND s.step='spread'").fetchone()
        self.assertEqual(step["outcome"], "empty")


class JoinQueueTest(unittest.TestCase):
    """Queue-level semantics, driven without workflows."""

    def setUp(self):
        self.conn = db.connect(tmpdir() / "t.db")
        self.subs = {"jointest.probe": ["probe_kind"]}
        self.parent = queue.enqueue(self.conn, "parent_kind", {"p": 1})
        queue.complete(self.conn, self.parent)

    def _fanout(self, payloads, join_event="jointest.done"):
        op = {"op": "fanout", "name": "jointest.probe", "payloads": payloads,
              "join_event": join_event, "join_data": {}}
        return queue.apply_fanout(self.conn, op, {"id": self.parent}, self.subs)

    def _members(self, gid):
        return self.conn.execute(
            "SELECT task_id, state FROM join_members WHERE group_id=?"
            " ORDER BY task_id", (gid,)).fetchall()

    def _join_events(self):
        return self.conn.execute(
            "SELECT count(*) FROM events WHERE name='jointest.done'"
        ).fetchone()[0]

    def test_reapplied_fanout_reuses_group(self):
        gid = self._fanout([{"v": 1}, {"v": 2}])
        gid2 = self._fanout([{"v": 1}, {"v": 2}])          # parent re-ran
        self.assertEqual(gid, gid2)
        self.assertEqual(len(self._members(gid)), 2)       # no duplicate members
        n_tasks = self.conn.execute(
            "SELECT count(*) FROM tasks WHERE kind='probe_kind'").fetchone()[0]
        self.assertEqual(n_tasks, 2)                       # no duplicate children

    def test_retry_reopens_unfired_member_then_fires_once(self):
        gid = self._fanout([{"v": 1}, {"v": 2}])
        t1, t2 = [m["task_id"] for m in self._members(gid)]

        # member 1 fails terminally; member 2 still waiting -> group open
        self.assertEqual(queue.fail(self.conn, t1, "workspace_dirty",
                                    subscriptions=self.subs), "failed")
        self.assertEqual([m["state"] for m in self._members(gid)],
                         ["failed", None])
        self.assertIsNone(self.conn.execute(
            "SELECT fired_at FROM join_groups WHERE id=?", (gid,)).fetchone()[0])

        # operator retry re-opens the member slot: the join waits for the re-run
        queue.retry(self.conn, task_id=t1)
        self.assertEqual([m["state"] for m in self._members(gid)],
                         [None, None])

        queue.complete(self.conn, t1, subscriptions=self.subs)
        self.assertEqual(self._join_events(), 0)           # still one waiting
        queue.complete(self.conn, t2, subscriptions=self.subs)
        self.assertEqual(self._join_events(), 1)           # barrier fired

        # fired means fired: nothing re-fires it
        self.assertFalse(queue.check_join_fire(self.conn, gid, self.subs))
        queue.retry(self.conn, task_id=t1)                 # no-op: t1 is done
        self.assertEqual(self._join_events(), 1)

    def test_reapply_after_fire_never_refires(self):
        gid = self._fanout([{"v": 9}])
        (t1,) = [m["task_id"] for m in self._members(gid)]
        queue.complete(self.conn, t1, subscriptions=self.subs)
        self.assertEqual(self._join_events(), 1)
        self.assertEqual(self._fanout([{"v": 9}]), gid)    # replayed boundary
        self.assertEqual(self._join_events(), 1)           # fired_at guard held

    def test_relink_onto_terminal_task_records_state(self):
        """Defense-in-depth in _join_link: if a member row is ever missing
        while its task is already terminal (e.g. after manual db surgery),
        re-applying the fan-out must record the terminal state at link time
        — a NULL member would hold the join open forever."""
        gid = self._fanout([{"v": 5}])
        (t1,) = [m["task_id"] for m in self._members(gid)]
        queue.complete(self.conn, t1, subscriptions=self.subs)
        self.conn.execute("DELETE FROM join_members WHERE group_id=?", (gid,))
        self.assertEqual(self._fanout([{"v": 5}]), gid)    # same key -> reuse
        self.assertEqual([m["state"] for m in self._members(gid)], ["done"])


if __name__ == "__main__":
    unittest.main()
