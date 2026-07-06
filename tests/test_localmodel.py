from __future__ import annotations

import hashlib
import json
import unittest

from helpers import tmpdir

from forgeflow import config, contract, db, localmodel, queue
from forgeflow.blocks import get

WEIGHTS = {
    "kind": "bow-embed", "dim": 2,
    "vocab": {"crash": [1.0, 0.0], "leak": [0.9, 0.1],
              "docs": [0.0, 1.0], "readme": [0.1, 0.9]},
    "buckets": [[0.5, 0.5], [0.2, 0.8]],
    "centroids": {"bug": [1.0, 0.0], "chore": [0.0, 1.0],
                  "aardvark": [0.0, 1.0]},  # exact tie with 'chore'
}


def write_model(path):
    data = json.dumps(WEIGHTS, sort_keys=True).encode()
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


class LocalModelTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.model_path = self.dir / "m.json"
        self.sha = write_model(self.model_path)

    def test_embed_deterministic_and_normalized(self):
        w, sha = localmodel.load_model(self.model_path, self.sha)
        a = localmodel.embed("crash Crash LEAK", w)
        b = localmodel.embed("crash Crash LEAK", w)
        self.assertEqual(a, b)
        self.assertAlmostEqual(sum(x * x for x in a), 1.0, places=9)
        # OOV falls into a deterministic hash bucket, empty input is zero
        self.assertEqual(localmodel.embed("zzzzz", w),
                         localmodel.embed("zzzzz", w))
        self.assertEqual(localmodel.embed("", w), [0.0, 0.0])

    def test_classify_stable_tiebreak(self):
        w, _ = localmodel.load_model(self.model_path, self.sha)
        label, score, margin = localmodel.classify("crash leak crash", w)
        self.assertEqual(label, "bug")
        self.assertGreater(margin, 0)
        # 'docs' scores identically for centroids 'chore' and 'aardvark':
        # lexicographic tie-break must pick 'aardvark', forever
        label, score, margin = localmodel.classify("docs", w)
        self.assertEqual(label, "aardvark")
        self.assertEqual(margin, 0.0)

    def test_sha_mismatch_refused_at_pack_load(self):
        pack_dir = self.dir / "pack"
        pack_dir.mkdir()
        (pack_dir / "project.yaml").write_text(
            "name: p\nmodels:\n  m: { path: %s, sha256: %s }\n"
            % (self.model_path, "0" * 64))
        with self.assertRaisesRegex(SystemExit, "sha256 mismatch"):
            config.load_pack(pack_dir)
        (pack_dir / "project.yaml").write_text(
            "name: p\nmodels:\n  m: { path: %s, sha256: %s }\n"
            % (self.model_path, self.sha))
        pack = config.load_pack(pack_dir)
        self.assertEqual(pack.models["m"]["sha256"], self.sha)

    def test_classify_block_cannot_gate(self):
        blk = get("model.classify")
        self.assertEqual(set(blk.outcomes), {"ok"})  # structurally a claim

    def test_blocks_through_engine_store_embedding(self):
        pack_dir = self.dir / "pack"
        pack_dir.mkdir()
        (pack_dir / "project.yaml").write_text(
            "name: p\nmodels:\n  m: { path: %s, sha256: %s }\n"
            % (self.model_path, self.sha))
        pack = config.load_pack(pack_dir)
        conn = db.connect(self.dir / "t.db")
        env = contract.ExecEnv(conn=conn, subscriptions={},
                               data_dir=self.dir / "data",
                               workspaces_dir=self.dir / "ws", pack=pack)
        wf = (contract.Workflow.define("w")
              .step("embed", get("model.embed"), timeout_s=10, params={
                  "model": "m", "text": "{payload.text}",
                  "object": {"repo": "r", "path": "src/a.c", "sha": "abc"}})
              .on("embed", "ok", "classify")
              .step("classify", get("model.classify"), timeout_s=10, params={
                  "model": "m", "text": "{payload.text}"})
              .on("classify", "ok", "done"))
        wf.validate()
        queue.enqueue(conn, "w", {"text": "crash leak"})
        task = queue.claim(conn)
        self.assertEqual(contract.execute(env, wf, task), "done")
        emb = conn.execute("SELECT * FROM embeddings").fetchone()
        self.assertEqual(emb["model_sha"], self.sha)
        self.assertEqual(emb["dim"], 2)
        obj = conn.execute("SELECT * FROM code_objects WHERE id=?",
                           (emb["object_id"],)).fetchone()
        self.assertEqual((obj["repo"], obj["path"]), ("r", "src/a.c"))
        steps = {r["step"]: json.loads(r["result"]) for r in conn.execute(
            "SELECT step, result FROM task_steps WHERE task_id=?", (task["id"],))}
        self.assertEqual(steps["classify"]["label"], "bug")
        self.assertEqual(steps["embed"]["object_id"], emb["object_id"])

if __name__ == "__main__":
    unittest.main()
