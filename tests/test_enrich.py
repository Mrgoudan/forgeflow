"""Local model in the selection loop: LLM rerank of the top window and
model-written summaries of long rows — both through run_agent (pinned,
archived), both degrading gracefully, previews never triggering calls."""
from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import config, contract, queue
from forgeflow import select as select_mod

SELECT = contract.CONTEXT_PROVIDERS["select"]

SCHEMA_SQL = ("CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY,"
              " body TEXT, created_at TEXT, conf REAL, repo TEXT);")


class FakeLocalModel:
    """chat-completions double for a local model: answers the rerank
    contract (favoring keys containing 'gem') and the summarize contract
    (fixed marker text). Counts calls per contract."""

    def __init__(self):
        self.rerank_calls = 0
        self.summarize_calls = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(
                    int(self.headers.get("Content-Length", 0))))
                ask = body["messages"][0]["content"]
                if '"scores"' in ask and "ranking knowledge-base" in ask:
                    outer.rerank_calls += 1
                    keys = [e["key"] for e in
                            json.loads(ask.split("## context: entries\n")[1]
                                       .split("\n")[0])]
                    scores = {kk: (10 if "gem" in kk else 1) for kk in keys}
                    text = "```json\n%s\n```" % json.dumps({"scores": scores})
                else:
                    outer.summarize_calls += 1
                    text = ("```json\n%s\n```"
                            % json.dumps({"summary":
                                          "CONDENSED marker parser crash"}))
                data = json.dumps({"choices": [{"message": {
                    "role": "assistant", "content": text}}]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = "http://127.0.0.1:%d/v1" % self.httpd.server_port
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class EnrichTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        repo = make_target_repo(self.base)
        self.server = FakeLocalModel()
        self.addCleanup(self.server.stop)
        pack_dir = make_pack(self.base, repo, extra=(
            "schema: [schema.sql]\n"
            "agents:\n"
            "  cheap: { backend: openai-compat, base_url: '%s',"
            " model: local-tiny }\n"
            "corpora:\n"
            "  notes:\n"
            "    table: notes\n    key: id\n    text: body\n"
            "    ts: created_at\n    weight: conf\n"
            "    embed_with: hashing\n    summarize_with: cheap\n"
            % self.server.base_url))
        (pack_dir / "schema.sql").write_text(SCHEMA_SQL)
        self.eng = make_engine(self.base, pack_dir=pack_dir)
        self.conn = self.eng.conn

    def _row(self, id, body):
        self.conn.execute(
            "INSERT OR REPLACE INTO notes(id, body, created_at, conf, repo)"
            " VALUES (?,?,?,?,?)", (id, body, "2026-01-01", 0.5, "alpha"))

    def _task(self):
        return queue.enqueue(self.conn, "jobkind", {"n": self.conn.execute(
            "SELECT count(*) FROM tasks").fetchone()[0]})

    def _select(self, spec, task_id):
        return SELECT(self.eng.env,
                      {"id": task_id, "attempts": 0, "payload": {}}, spec)

    def test_rerank_promotes_judged_row_and_is_audited(self):
        self._row("plain_a", "parser crash in intake stage detail one")
        self._row("gem_b", "parser crash in intake stage detail two")
        out = self._select({"corpus": "notes", "query": "parser crash intake",
                            "k": 2, "diversify": 0,
                            "rerank": {"llm": "cheap"}}, self._task())
        self.assertTrue(out["reranked"])
        self.assertEqual(out["entries"][0]["key"], "gem_b")   # judge's pick
        self.assertEqual(out["entries"][0]["rerank"], 10)
        self.assertEqual(self.server.rerank_calls, 1)
        run = self.conn.execute("SELECT * FROM runs").fetchone()
        self.assertEqual(run["model"], "local-tiny")          # pinned + audited

    def test_rerank_failure_falls_back_to_fused_order(self):
        self._row("a", "parser crash one")
        self._row("b", "parser crash two")
        self.server.stop()
        out = self._select({"corpus": "notes", "query": "parser crash",
                            "k": 2, "rerank": {"llm": "cheap",
                                               "timeout_s": 5}}, self._task())
        self.assertFalse(out["reranked"])
        self.assertIn("rerank_error", out)
        self.assertEqual(len(out["entries"]), 2)              # still answered

    def test_preview_skips_rerank_and_summaries(self):
        self._row("a", "parser crash " + "x" * 5000)
        out = self._select({"corpus": "notes", "query": "parser crash",
                            "k": 1, "max_chars": 100,
                            "rerank": {"llm": "cheap"}}, task_id=999999)
        self.assertFalse(out["reranked"])
        self.assertTrue(out["entries"][0].get("truncated"))   # no model call
        self.assertEqual(self.server.rerank_calls, 0)
        self.assertEqual(self.server.summarize_calls, 0)

    def test_long_row_summarized_and_cached(self):
        self._row("long", "parser crash " + "y" * 5000)
        spec = {"corpus": "notes", "query": "parser crash", "k": 1,
                "max_chars": 200}
        out = self._select(spec, self._task())
        e = out["entries"][0]
        self.assertTrue(e.get("summarized"))
        self.assertEqual(e["text"], "CONDENSED marker parser crash")
        self.assertNotIn("truncated", e)
        self.assertEqual(self.server.summarize_calls, 1)
        self._select(spec, self._task())                      # cache hit
        self.assertEqual(self.server.summarize_calls, 1)
        row = self.conn.execute("SELECT * FROM corpus_summaries").fetchone()
        self.assertEqual((row["corpus"], row["binding"]), ("notes", "cheap"))

    def test_summary_feeds_lexical_matching(self):
        # body shares nothing with the query; the cached summary does
        from forgeflow.util import sha256_text
        body = "z " * 40
        self._row("hidden", body)
        self._row("decoy", "quarterly invoice rollup")
        self.conn.execute(
            "INSERT INTO corpus_summaries(corpus, key, binding, text_sha,"
            " summary) VALUES ('notes','hidden','cheap',?,?)",
            (sha256_text(body), "segfault deadline in tokenize path"))
        out = self._select({"corpus": "notes",
                            "query": "segfault tokenize deadline", "k": 1},
                           self._task())
        self.assertEqual(out["entries"][0]["key"], "hidden")

    def test_config_rejects_unknown_summarize_role(self):
        base2 = tmpdir()
        repo2 = make_target_repo(base2)
        pack_dir = make_pack(base2, repo2, extra=(
            "schema: [schema.sql]\n"
            "corpora:\n  n: { table: notes, text: body,"
            " summarize_with: ghost }\n"))
        (pack_dir / "schema.sql").write_text(SCHEMA_SQL)
        with self.assertRaisesRegex(SystemExit, "ghost"):
            config.load_pack(pack_dir)

    def test_check_spec_rerank(self):
        check = select_mod._check_select_spec
        pack = self.eng.pack
        self.assertIsNone(check({"corpus": "notes", "query": "q",
                                 "rerank": {"llm": "cheap", "window": 10}},
                                pack))
        self.assertIn("agents", check({"corpus": "notes", "query": "q",
                                       "rerank": {"llm": "ghost"}}, pack))
        self.assertIn("unknown keys", check({"corpus": "notes", "query": "q",
                                             "rerank": {"llm": "cheap",
                                                        "x": 1}}, pack))
        self.assertIn("positive", check({"corpus": "notes", "query": "q",
                                         "rerank": {"llm": "cheap",
                                                    "window": 0}}, pack))


if __name__ == "__main__":
    unittest.main()
