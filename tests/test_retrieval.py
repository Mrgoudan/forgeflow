from __future__ import annotations

import unittest

from helpers import tmpdir
from test_localmodel import write_model

from forgeflow import config, contract, db, queue
from forgeflow.blocks import block, get


@block("test.retrctx", "local", {"ok"}, accepts_context={"retrieval"})
def _retrctx(ctx, task, prev):
    return "ok", {}


class RetrievalProviderTest(unittest.TestCase):
    """The lesser model shapes what agents see: k-nearest stored objects by
    embedding similarity, deterministic ordering, spec checked at load."""

    def setUp(self):
        self.dir = tmpdir()
        model_path = self.dir / "m.json"
        sha = write_model(model_path)
        pack_dir = self.dir / "pack"
        pack_dir.mkdir()
        (pack_dir / "project.yaml").write_text(
            "name: p\nmodels:\n  m: { path: %s, sha256: %s }\n"
            % (model_path, sha))
        self.pack = config.load_pack(pack_dir)
        self.conn = db.connect(self.dir / "t.db")
        self.env = contract.ExecEnv(conn=self.conn, subscriptions={},
                                    data_dir=self.dir / "data",
                                    workspaces_dir=self.dir / "ws",
                                    pack=self.pack)
        # seed embeddings through the real block + engine (not hand-rolled)
        wf = (contract.Workflow.define("seed")
              .step("embed", get("model.embed"), timeout_s=10, params={
                  "model": "m", "text": "{payload.text}",
                  "object": {"repo": "r", "path": "{payload.path}", "sha": "s"}})
              .on("embed", "ok", "done").on("embed", "error", "failed")
              .on("embed", "timeout", "failed"))
        wf.validate()
        for text, path in [("crash crash leak", "bug_zone.c"),
                           ("docs readme docs", "manual.md"),
                           ("crash docs", "mixed.c")]:
            queue.enqueue(self.conn, "seed", {"text": text, "path": path})
            contract.execute(self.env, wf, queue.claim(self.conn))
        # a reading attached to the top object surfaces in the slice
        obj = self.conn.execute(
            "SELECT id FROM code_objects WHERE path='bug_zone.c'").fetchone()
        self.conn.execute(
            "INSERT INTO readings(object_id, sha, summary) VALUES (?,?,?)",
            (obj["id"], "s", "ownership bug hotspot"))

    def _retrieve(self, query, k=2):
        task = {"id": 99, "attempts": 0, "payload": {"q": query}}
        provider = contract.CONTEXT_PROVIDERS["retrieval"]
        return provider(self.env, task,
                        {"model": "m", "query": "{payload.q}", "k": k})

    def test_ranks_by_similarity_and_attaches_readings(self):
        out = self._retrieve("leak crash")
        self.assertEqual([e["path"] for e in out], ["bug_zone.c", "mixed.c"])
        self.assertEqual(out[0]["summary"], "ownership bug hotspot")
        self.assertNotIn("summary", out[1])          # no reading for mixed.c
        self.assertGreater(out[0]["score"], out[1]["score"])

    def test_deterministic_and_k_bounded(self):
        a = self._retrieve("docs readme", k=1)
        b = self._retrieve("docs readme", k=1)
        self.assertEqual(a, b)
        self.assertEqual(len(a), 1)
        self.assertEqual(a[0]["path"], "manual.md")
        self.assertEqual(len(self._retrieve("docs", k=10)), 3)  # capped by data

    def test_loader_rejects_bad_specs(self):
        from forgeflow import loader
        base = ("workflow: w\nsteps:\n"
                "  - name: a\n    block: test.retrctx\n    timeout_s: 10\n"
                "    context:\n      - retrieval: %s\n"
                "    outcomes: { ok: done }\n")
        wf_dir = self.dir / "wfs"
        wf_dir.mkdir()
        for spec, msg in [
                ("{ query: x }", "needs 'model'"),
                ("{ model: ghost, query: x }", "not in pack models"),
                ("{ model: m }", "string 'query'"),
                ("{ model: m, query: x, k: 0 }", "positive integer")]:
            (wf_dir / "w.yaml").write_text(base % spec)
            with self.assertRaisesRegex(SystemExit, msg):
                loader.load_workflow_file(wf_dir / "w.yaml", pack=self.pack)


if __name__ == "__main__":
    unittest.main()
