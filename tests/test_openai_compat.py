from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from helpers import tmpdir

from forgeflow import config, contract, db, queue, runner
from forgeflow.blocks import get

SCHEMA = {"type": "object", "required": ["verdict"],
          "properties": {"verdict": {"enum": ["FIXED", "NOOP"]}}}

GOOD_TEXT = "done.\n```json\n{\"verdict\": \"FIXED\"}\n```"


class FakeLLMServer:
    """chat-completions + embeddings test double. Records every request
    (path, headers, body); behavior driven by self.mode."""

    def __init__(self):
        self.requests = []
        self.mode = "good"
        self.health = {"model": "emb-model", "dim": 3,
                       "weights_sha256": "abc123"}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):     # health endpoint for model pinning
                data = json.dumps(outer.health).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                body = json.loads(self.rfile.read(
                    int(self.headers.get("Content-Length", 0))))
                outer.requests.append({
                    "path": self.path, "body": body,
                    "auth": self.headers.get("Authorization")})
                n = len(outer.requests)
                if outer.mode == "http429":
                    self.send_response(429)
                    self.end_headers()
                    self.wfile.write(b'{"error": "slow down"}')
                    return
                if self.path.endswith("/embeddings"):
                    payload = {"data": [{"embedding": [0.1, 0.2, 0.3]}]}
                else:
                    text = GOOD_TEXT
                    if outer.mode == "invalid_then_good" and n == 1:
                        text = "no fenced block, sorry"
                    payload = {"choices": [{"message":
                                            {"role": "assistant", "content": text}}]}
                data = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = "http://127.0.0.1:%d/v1" % self.httpd.server_port
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class OpenAICompatTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.conn = db.connect(self.dir / "t.db")
        self.server = FakeLLMServer()
        self.addCleanup(self.server.stop)
        self.binding = {"backend": "openai-compat",
                        "base_url": self.server.base_url,
                        "model": "test-embedding-ish-model",
                        "api_key_ref": "TESTREF"}
        self.secrets = {"LLM_API_KEY_TESTREF": "sekrit-123"}
        queue.enqueue(self.conn, "k", {"x": 1})
        self.task = queue.claim(self.conn)

    def _run(self):
        return runner.run_agent(
            self.conn, self.task, self.binding, "You decide.", SCHEMA,
            data_dir=self.dir / "data", pack_rev="r", timeout_s=30,
            context_slice={"payload": {"x": 1}}, secrets=self.secrets)

    def test_happy_path_with_bearer_key_from_secrets(self):
        verdict = self._run()
        self.assertEqual(verdict["verdict"], "FIXED")
        req = self.server.requests[0]
        self.assertTrue(req["path"].endswith("/chat/completions"))
        self.assertEqual(req["auth"], "Bearer sekrit-123")
        self.assertEqual(req["body"]["model"], "test-embedding-ish-model")
        run = self.conn.execute("SELECT * FROM runs").fetchone()
        self.assertEqual(run["verdict"], "FIXED")
        # the key never lands in the archived request snapshot
        req_file = (self.dir / "data" / "runs" / str(run["id"]) / "ask0" /
                    "request.json").read_text()
        self.assertNotIn("sekrit-123", req_file)

    def test_missing_secret_parks_as_agent_limit(self):
        with self.assertRaises(runner.RunnerError) as cm:
            runner.run_agent(
                self.conn, self.task, self.binding, "p", SCHEMA,
                data_dir=self.dir / "data", pack_rev="r", timeout_s=30,
                secrets={})   # no LLM_API_KEY_TESTREF
        self.assertEqual(cm.exception.error_class, "agent_limit")
        self.assertEqual(len(self.server.requests), 0)  # never even called

    def test_reask_carries_message_history(self):
        self.server.mode = "invalid_then_good"
        verdict = self._run()
        self.assertEqual(verdict["verdict"], "FIXED")
        self.assertEqual(len(self.server.requests), 2)
        msgs = self.server.requests[1]["body"]["messages"]
        roles = [m["role"] for m in msgs]
        self.assertEqual(roles, ["user", "assistant", "user"])  # history kept
        self.assertIn("output contract", msgs[2]["content"])
        self.assertEqual(self.conn.execute(
            "SELECT count(*) c FROM runs").fetchone()["c"], 1)  # same runs row

    def test_http_429_is_agent_limit(self):
        self.server.mode = "http429"
        with self.assertRaises(runner.RunnerError) as cm:
            self._run()
        self.assertEqual(cm.exception.error_class, "agent_limit")

    def test_unreachable_endpoint_is_agent_backend(self):
        self.binding["base_url"] = "http://127.0.0.1:1/v1"   # nothing there
        with self.assertRaises(runner.RunnerError) as cm:
            self._run()
        self.assertEqual(cm.exception.error_class, "agent_backend")

    def test_api_backed_embedding_model_through_engine(self):
        pack_dir = self.dir / "pack"
        pack_dir.mkdir()
        (pack_dir / "project.yaml").write_text(
            "name: p\nmodels:\n  bertish: { base_url: %s, model: emb-model }\n"
            % self.server.base_url)
        pack = config.load_pack(pack_dir)
        env = contract.ExecEnv(conn=self.conn, subscriptions={},
                               data_dir=self.dir / "data",
                               workspaces_dir=self.dir / "ws", pack=pack)
        wf = (contract.Workflow.define("w")
              .step("embed", get("model.embed"), timeout_s=10, params={
                  "model": "bertish", "text": "{payload.text}",
                  "object": {"repo": "r", "path": "a.c", "sha": "s"}})
              .on("embed", "ok", "done")
              .on("embed", "error", "failed")
              .on("embed", "timeout", "failed"))
        wf.validate()
        queue.enqueue(self.conn, "w", {"text": "crash in parser"})
        task = queue.claim(self.conn)
        self.assertEqual(contract.execute(env, wf, task), "done")
        emb = self.conn.execute("SELECT * FROM embeddings").fetchone()
        self.assertEqual(emb["dim"], 3)
        self.assertEqual(json.loads(emb["vector"]), [0.1, 0.2, 0.3])
        self.assertTrue(self.server.requests[-1]["path"].endswith("/embeddings"))

    def test_api_model_health_pinning(self):
        pack_dir = self.dir / "pack3"
        pack_dir.mkdir()
        cfg = ("name: p\nmodels:\n"
               "  bertish:\n"
               "    base_url: %s\n"
               "    model: emb-model\n"
               "    health_url: %s/\n"
               "    expect: { model: emb-model, weights_sha256: %s }\n")
        # matching pin loads fine
        (pack_dir / "project.yaml").write_text(
            cfg % (self.server.base_url, self.server.base_url, "abc123"))
        pack = config.load_pack(pack_dir)
        self.assertIn("bertish", pack.models)
        # drifted weights refuse to start
        (pack_dir / "project.yaml").write_text(
            cfg % (self.server.base_url, self.server.base_url, "OTHER"))
        with self.assertRaisesRegex(SystemExit, "drifted"):
            config.load_pack(pack_dir)
        # unreachable health endpoint refuses to start
        (pack_dir / "project.yaml").write_text(
            "name: p\nmodels:\n  bertish: { base_url: http://127.0.0.1:1/v1,"
            " model: m, expect: { model: m } }\n")
        with self.assertRaisesRegex(SystemExit, "health check"):
            config.load_pack(pack_dir)

    def test_api_model_config_requires_model(self):
        pack_dir = self.dir / "pack2"
        pack_dir.mkdir()
        (pack_dir / "project.yaml").write_text(
            "name: p\nmodels:\n  bad: { base_url: http://x/v1 }\n")
        with self.assertRaisesRegex(SystemExit, "needs 'model'"):
            config.load_pack(pack_dir)


if __name__ == "__main__":
    unittest.main()
