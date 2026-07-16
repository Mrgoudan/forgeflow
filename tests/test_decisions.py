from __future__ import annotations

import json
import unittest

from helpers import tmpdir

from forgeflow import blocks, db, queue


def _ctx(conn, **kw):
    kw.setdefault("_conn", conn)
    kw.setdefault("_timeout_s", 30)
    kw.setdefault("_step_dir", str(tmpdir()))
    return kw


class DecisionLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.conn = db.connect(tmpdir() / "t.db")
        self.ask = blocks.get("human.ask").fn
        self.task = {"id": 11, "payload": {"feature_key": "F"}}
        queue.enqueue(self.conn, "w", {"feature_key": "F"})   # id 1 (unused)

    def test_full_round_trip(self):
        # 1. proposing step carried a decision -> a round opens, gate waits
        prev = {"decision": {"title": "pick a design", "kind": "proposal",
                             "options": [{"title": "A", "pros": ["simple"]},
                                         {"title": "B", "cons": ["risky"]}],
                             "recommended": "A"}}
        o, r = self.ask(_ctx(self.conn, key="F/design"), self.task, prev)
        self.assertEqual(o, "awaiting_human")
        did = r["decision_id"]
        self.assertEqual(r["_staged"][0]["name"], "decision.requested")

        # 2. still unanswered -> keeps waiting (no duplicate round)
        o, r2 = self.ask(_ctx(self.conn, key="F/design"), self.task, prev)
        self.assertEqual(o, "awaiting_human")
        self.assertEqual(r2["decision_id"], did)

        # 3. human resolves -> gate consumes; the VERDICT is the outcome
        db.resolve_decision(self.conn, did, "picked", {"picked": "B"})
        o, r3 = self.ask(_ctx(self.conn, key="F/design"), self.task, prev)
        self.assertEqual(o, "picked")
        self.assertEqual(r3["answer"]["picked"], "B")

        # 4. consumed: a re-entry with a NEW decision opens round 2
        o, r4 = self.ask(_ctx(self.conn, key="F/design"), self.task,
                         {"decision": {"title": "round 2", "options": ["C"]}})
        self.assertEqual(o, "awaiting_human")
        self.assertEqual(self.conn.execute(
            "SELECT round FROM decisions WHERE id=?",
            (r4["decision_id"],)).fetchone()[0], 2)

    def test_resolve_resumes_same_attempt(self):
        prev = {"decision": {"title": "t", "options": ["x"]}}
        o, r = self.ask(_ctx(self.conn, key="F/q"), self.task, prev)
        # simulate the park the workflow mapping would cause
        self.conn.execute("UPDATE tasks SET state='parked', attempts=2 WHERE id=1")
        self.conn.execute("UPDATE decisions SET task_id=1 WHERE id=?",
                          (r["decision_id"],))
        db.resolve_decision(self.conn, r["decision_id"], "revise",
                            {"rejected": ["x"], "comment": "no"})
        row = self.conn.execute(
            "SELECT state, attempts FROM tasks WHERE id=1").fetchone()
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["attempts"], 2)      # SAME attempt — no bump

    def test_awaiting_human_policy_never_auto_unparks(self):
        self.assertIsNone(queue.POLICY["awaiting_human"].unpark_after_s)

    def test_bad_verdict_refused(self):
        prev = {"decision": {"title": "t", "options": ["x"]}}
        _, r = self.ask(_ctx(self.conn, key="F/v"), self.task, prev)
        with self.assertRaises(ValueError):
            db.resolve_decision(self.conn, r["decision_id"], "maybe")

    def test_gate_without_decision_is_loud(self):
        with self.assertRaisesRegex(RuntimeError, "no open decision"):
            self.ask(_ctx(self.conn, key="F/none"), self.task, {})


class FreshReplayTest(unittest.TestCase):
    def test_fresh_block_attribute(self):
        self.assertTrue(blocks.get("human.ask").fresh)
        self.assertFalse(blocks.get("event.emit").fresh)


class DecisionsHttpTest(unittest.TestCase):
    def test_page_and_form_resolve(self):
        import urllib.request
        from urllib.parse import urlencode
        base = tmpdir()
        conn = db.connect(base / "state" / "forgeflow.db")
        did = db.create_decision(conn, "F/design", "pick", kind="proposal",
                                 options=[{"title": "A", "pros": ["p"]}, "B"],
                                 recommended="A")
        from forgeflow import httpd
        server = httpd.serve(base, {}, pack_name="t")
        httpd.serve_in_thread(server)
        host, port = server.server_address
        try:
            page = urllib.request.urlopen(
                "http://%s:%s/decisions" % (host, port)).read().decode()
            self.assertIn("pick", page)
            self.assertIn("F/design", page)
            body = urlencode({"verdict": "picked", "picked": "A"}).encode()
            req = urllib.request.Request(
                "http://%s:%s/api/decision/%d/resolve" % (host, port, did),
                data=body)
            resp = urllib.request.urlopen(req)
            self.assertIn(resp.status, (200, 303))
            row = conn.execute("SELECT status, verdict FROM decisions"
                               " WHERE id=?", (did,)).fetchone()
            self.assertEqual((row["status"], row["verdict"]),
                             ("resolved", "picked"))
            front = urllib.request.urlopen(
                "http://%s:%s/" % (host, port)).read().decode()
            self.assertNotIn("waiting on you", front)   # resolved -> no banner
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
