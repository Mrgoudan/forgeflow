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
        self.assertIn("<h2>tasks</h2>", body)

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
