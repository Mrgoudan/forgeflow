"""HTTP front door: status/metrics/task JSON, dashboard HTML, emit through
subscriptions, token enforcement, and the config guard that refuses a
non-loopback bind without a token."""
from __future__ import annotations

import json
import unittest
import urllib.error
import urllib.request

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import config, httpd


class HttpConfigTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)

    def _load(self, extra):
        return config.load_pack(make_pack(self.base, self.repo, extra=extra))

    def test_valid_http_section(self):
        pack = self._load("http: { port: 8321 }\n")
        self.assertEqual(pack.http, {"host": "127.0.0.1", "port": 8321,
                                     "token_ref": None})

    def test_non_loopback_needs_token(self):
        with self.assertRaises(SystemExit):
            self._load("http: { host: 0.0.0.0, port: 8321 }\n")
        pack = self._load("http: { host: 0.0.0.0, port: 8321,"
                          " token_ref: DASH }\n")
        self.assertEqual(pack.http["token_ref"], "DASH")

    def test_rejects_malformed(self):
        for extra in ("http: [1]\n",
                      "http: { port: notaport }\n",
                      "http: { port: 70000 }\n",
                      "http: { port: 80, nope: 1 }\n"):
            with self.assertRaises(SystemExit, msg=extra):
                self._load(extra)


class HttpServerTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        repo = make_target_repo(self.base)
        pack_dir = make_pack(self.base, repo)      # demo pack: filebug + notify
        self.eng = make_engine(self.base, pack_dir=pack_dir)
        self.server = None

    def tearDown(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()

    def _serve(self, token=None):
        self.server = httpd.serve(self.base / "ff", self.eng.subscriptions,
                                  host="127.0.0.1", port=0, token=token,
                                  pack_name="demo")
        httpd.serve_in_thread(self.server)
        return "http://127.0.0.1:%d" % self.server.server_address[1]

    def _get(self, url, token=None, expect=200):
        req = urllib.request.Request(url)
        if token:
            req.add_header("Authorization", "Bearer %s" % token)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    def _post(self, url, body, token=None):
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"},
                                     method="POST")
        if token:
            req.add_header("Authorization", "Bearer %s" % token)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_status_metrics_task_and_dashboard(self):
        base = self._serve()
        code, body = self._get(base + "/api/status")
        self.assertEqual(code, 200)
        st = json.loads(body)
        self.assertIn("tasks", st)
        self.assertIn("recent_events", st)

        code, body = self._get(base + "/api/metrics")
        self.assertEqual(code, 200)
        self.assertIn("queue_depth", json.loads(body))

        code, body = self._get(base + "/")
        self.assertEqual(code, 200)
        self.assertIn("runs", body)                  # the run-centric front page
        code, body = self._get(base + "/explore")
        self.assertEqual(code, 200)
        self.assertIn("<h2>recent tasks</h2>", body)  # ops moved to /explore

        code, body = self._get(base + "/api/task/99999")
        self.assertEqual(code, 404)
        code, body = self._get(base + "/nope")
        self.assertEqual(code, 404)

    def test_emit_runs_through_subscriptions(self):
        base = self._serve()
        code, resp = self._post(base + "/api/emit",
                                {"name": "demo.scan_requested",
                                 "data": {"key": "http1"}})
        self.assertEqual(code, 200)
        self.assertEqual(resp["consumers"], ["filebug"])
        n = self.eng.run_until_idle()               # the daemon-side claim loop
        self.assertGreaterEqual(n, 1)
        row = self.eng.conn.execute(
            "SELECT state FROM tasks WHERE kind='filebug'").fetchone()
        self.assertEqual(row["state"], "done")
        # the API can read the task it caused
        tid = self.eng.conn.execute(
            "SELECT id FROM tasks WHERE kind='filebug'").fetchone()["id"]
        code, body = self._get(base + "/api/task/%d" % tid)
        self.assertEqual(code, 200)
        self.assertEqual(json.loads(body)["state"], "done")

    def test_emit_rejects_unconsumed_and_bad_bodies(self):
        base = self._serve()
        code, resp = self._post(base + "/api/emit",
                                {"name": "nobody.listens", "data": {}})
        self.assertEqual(code, 400)
        self.assertIn("consumed_events", resp)
        code, resp = self._post(base + "/api/emit", {"data": {}})
        self.assertEqual(code, 400)
        code, resp = self._post(base + "/api/emit",
                                {"name": "demo.scan_requested", "data": [1]})
        self.assertEqual(code, 400)

    def test_token_enforced_everywhere_when_configured(self):
        base = self._serve(token="sekrit")
        self.assertEqual(self._get(base + "/api/status")[0], 401)
        self.assertEqual(self._get(base + "/")[0], 401)
        self.assertEqual(self._post(base + "/api/emit",
                                    {"name": "demo.scan_requested",
                                     "data": {}})[0], 401)
        self.assertEqual(self._get(base + "/api/status", token="wrong")[0], 401)
        self.assertEqual(self._get(base + "/api/status", token="sekrit")[0], 200)
        code, _ = self._post(base + "/api/emit",
                             {"name": "demo.scan_requested",
                              "data": {"key": "t1"}}, token="sekrit")
        self.assertEqual(code, 200)

    def test_force_bypasses_dedup(self):
        base = self._serve()
        body = {"name": "demo.scan_requested", "data": {"key": "same"}}
        self._post(base + "/api/emit", body)
        self._post(base + "/api/emit", body)                       # dedups
        n = self.eng.conn.execute("SELECT count(*) FROM tasks"
                                  " WHERE kind='filebug'").fetchone()[0]
        self.assertEqual(n, 1)
        self._post(base + "/api/emit", dict(body, force=True))     # fresh task
        n = self.eng.conn.execute("SELECT count(*) FROM tasks"
                                  " WHERE kind='filebug'").fetchone()[0]
        self.assertEqual(n, 2)


if __name__ == "__main__":
    unittest.main()


class BoardTest(unittest.TestCase):
    def test_board_parse_rejects_non_select(self):
        from helpers import make_pack, tmpdir
        from forgeflow import config
        base = tmpdir()
        pack_dir = make_pack(base, base, extra=(
            "board:\n  task_panels:\n"
            "    - { title: X, kind: table, sql: \"DELETE FROM tasks\" }\n"))
        with self.assertRaisesRegex(SystemExit, "single SELECT"):
            config.load_pack(pack_dir)

    def test_thread_key_parse(self):
        from helpers import make_pack, tmpdir
        from forgeflow import config
        base = tmpdir()
        with self.assertRaisesRegex(SystemExit, "thread_key"):
            config.load_pack(make_pack(base, base,
                                       extra="board: { thread_key: [1] }\n"))
        pack = config.load_pack(make_pack(base, base,
                                          extra="board: { thread_key: key }\n"))
        self.assertEqual(pack.board["thread_key"], "key")

    def test_runs_front_page_groups_by_thread(self):
        import urllib.request
        from helpers import make_pack, make_engine, make_target_repo, tmpdir
        from forgeflow import db, httpd
        base = tmpdir()
        repo = make_target_repo(base)
        pack_dir = make_pack(base, repo, extra="board: { thread_key: key }\n")
        eng = make_engine(base, pack_dir)
        # a LIVE run (pending task): must render as an active run card
        db.emit_event(eng.conn, "demo.scan_requested", {"key": "RUN-A"},
                      eng.subscriptions)
        # a FINISHED run: must collapse into the history strip
        db.emit_event(eng.conn, "demo.scan_requested", {"key": "RUN-B"},
                      eng.subscriptions)
        eng.conn.execute("UPDATE tasks SET state='done' WHERE"
                         " json_extract(payload,'$.key')='RUN-B'")
        server = httpd.serve(base / "ff", eng.subscriptions,
                             workflows=eng.workflows, board=eng.pack.board,
                             pack_name="demo")
        httpd.serve_in_thread(server)
        host, port = server.server_address
        try:
            page = urllib.request.urlopen(
                "http://%s:%s/" % (host, port)).read().decode()
            self.assertIn("RUN-A", page)             # the live run, named
            self.assertIn("<svg", page)              # drawn as a pipeline
            self.assertIn("item.triaged", page)      # orchestration edge label
            self.assertIn("notify", page)            # downstream workflow shown
            self.assertIn("reproduce", page)         # steps are nodes too
            self.assertIn("finished run", page)      # RUN-B collapsed
            self.assertIn("RUN-B", page)
            self.assertNotIn("recent events", page)  # ops moved off the front
            ex = urllib.request.urlopen(
                "http://%s:%s/explore" % (host, port)).read().decode()
            self.assertIn("recent events", ex)
            # the run audit page: the whole story of one thread
            audit = urllib.request.urlopen(
                "http://%s:%s/run/RUN-A" % (host, port)).read().decode()
            self.assertIn("audit trail", audit)
            self.assertIn("filebug", audit)
            self.assertIn("demo.scan_requested", audit)   # the run's events
        finally:
            server.shutdown()
            server.server_close()

    def test_views_and_launch(self):
        import urllib.request
        from urllib.parse import urlencode
        from helpers import make_pack, make_engine, make_target_repo, tmpdir
        from forgeflow import config, httpd
        base = tmpdir()
        repo = make_target_repo(base)
        req_file = base / "req.md"
        req_file.write_text("the requirement text\n")
        pack_dir = make_pack(base, repo, extra=(
            "board:\n"
            "  thread_key: key\n"
            "  views:\n"
            "    thing:\n"
            "      title: 'thing {key}'\n"
            "      panels:\n"
            "        - { title: itself, kind: table,\n"
            "            sql: \"SELECT :key AS k, 'X-9' AS 'link:other'\" }\n"
            "    other:\n"
            "      title: other\n"
            "      panels:\n"
            "        - { title: o, kind: table, sql: 'SELECT :key AS k' }\n"
            "  launch:\n"
            "    - title: start a scan\n"
            "      event: demo.scan_requested\n"
            "      fields:\n"
            "        - { name: key, required: true }\n"
            "        - { name: note, kind: path_or_text }\n"))
        eng = make_engine(base, pack_dir)
        server = httpd.serve(base / "ff", eng.subscriptions,
                             workflows=eng.workflows, board=eng.pack.board,
                             pack_name="demo")
        httpd.serve_in_thread(server)
        host, port = server.server_address
        try:
            page = urllib.request.urlopen(
                "http://%s:%s/view/thing?key=T-1" % (host, port)).read().decode()
            self.assertIn("thing T-1", page)               # title templated
            self.assertIn("T-1", page)                     # :key bound
            self.assertIn('href="/view/other?key=X-9"', page)  # cross-link
            self.assertIn(">other<", page)                 # header sans prefix
            # unknown view 404s
            try:
                urllib.request.urlopen("http://%s:%s/view/nope" % (host, port))
                self.fail("unknown view must 404")
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 404)
            # front page renders the launch form
            front = urllib.request.urlopen(
                "http://%s:%s/" % (host, port)).read().decode()
            self.assertIn("start a scan", front)
            self.assertIn("/api/launch", front)
            # launching with a PATH as the note reads the file content
            body = urlencode({"event": "demo.scan_requested", "key": "L-1",
                              "note": str(req_file)}).encode()
            resp = urllib.request.urlopen(urllib.request.Request(
                "http://%s:%s/api/launch" % (host, port), data=body))
            self.assertIn(resp.status, (200, 303))
            row = eng.conn.execute(
                "SELECT payload FROM tasks WHERE kind='filebug'").fetchone()
            payload = json.loads(row["payload"])
            self.assertEqual(payload["key"], "L-1")
            self.assertIn("the requirement text", payload["note"])
            # a missing required field is a 400, not a silent half-launch
            bad = urlencode({"event": "demo.scan_requested", "note": "x"}).encode()
            try:
                urllib.request.urlopen(urllib.request.Request(
                    "http://%s:%s/api/launch" % (host, port), data=bad))
                self.fail("missing required field must 400")
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 400)
        finally:
            server.shutdown()
            server.server_close()

    def test_task_page_and_panels(self):
        import json as _json
        import urllib.request
        from helpers import make_pack, make_engine, tmpdir
        from forgeflow import db, httpd, queue
        base = tmpdir()
        pack_dir = make_pack(base, base, extra=(
            "board:\n  task_panels:\n"
            "    - { title: Things, kind: table,\n"
            "        sql: \"SELECT 'a' AS x, 1 AS n\" }\n"))
        eng = make_engine(base, pack_dir)
        queue.enqueue(eng.conn, "filebug", {"feature_key": "F"})
        server = httpd.serve(base / "ff", eng.subscriptions,
                             workflows=eng.workflows,
                             board=eng.pack.board, pack_name="t")
        httpd.serve_in_thread(server)
        host, port = server.server_address
        try:
            page = urllib.request.urlopen(
                "http://%s:%s/task/1" % (host, port)).read().decode()
            self.assertIn("filebug", page)
            self.assertIn("Things", page)          # the pack panel rendered
            self.assertIn("walk", page)            # the step-graph section
            body = urllib.request.urlopen(
                "http://%s:%s/" % (host, port)).read().decode()
            self.assertIn("/task/1", body)         # overview links tasks
            r = urllib.request.urlopen(
                "http://%s:%s/api/run/999/prompt" % (host, port))
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 404)          # artifact 404s safely
        finally:
            server.shutdown()
            server.server_close()
