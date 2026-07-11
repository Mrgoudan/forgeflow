"""Frozen recall calibration for select: — a golden set with known-correct
targets across adversarial categories, asserting recall floors so ranking
regressions are caught in CI (this suite already caught two real bugs:
key-order tie-ranks letting distractors outvote matches, and full-weight
priors letting fresh-but-irrelevant rows beat relevant ones).

Honest expectations are part of the contract: the zero-setup hashing
embedder MUST ace lexical/identifier/recency/importance/scoped recall and
is EXPECTED to fail pure-paraphrase recall (it is lexical); a synonym-aware
embedding model plugged into the same corpus must recover paraphrase."""
from __future__ import annotations

import json
import random
import unittest

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import contract
from forgeflow.util import sha256_file

SELECT = contract.CONTEXT_PROVIDERS["select"]

FILLER = ("system module handler queue worker report update cycle batch "
          "record service daemon file check task result state change core").split()

CONCEPTS = [("crash", "segfault", "report"), ("delete", "erase", "update"),
            ("slow", "sluggish", "fast"), ("memory", "heap", "disk"),
            ("network", "socket", "printer"), ("login", "credential", "logout"),
            ("parse", "tokenize", "render"), ("timeout", "deadline", "retry")]

IDENTIFIERS = [("flagSkipReason", "skip reason flag"),
               ("maxRetryBudget", "retry budget max"),
               ("parseIncludePath", "include path parsing"),
               ("joinGroupKey", "join group key"),
               ("payloadHashDedup", "payload hash dedup")]

SCHEMA_SQL = ("CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY,"
              " body TEXT, created_at TEXT, conf REAL, repo TEXT);")


def build(n_distractors=300, rng=None):
    rng = rng or random.Random(42)
    rows, golden, rid = [], [], [0]

    def add(body, ts="2024-06-15 00:00:00", conf=0.5, repo="alpha"):
        rid[0] += 1
        key = "r%05d" % rid[0]
        rows.append((key, body, ts, conf, repo))
        return key

    for _ in range(n_distractors):
        add(" ".join(rng.choice(FILLER) for _ in range(rng.randint(6, 12))))
    for i, (qw, _, decoy) in enumerate(CONCEPTS):
        t = add("the %s inside the %s pipeline needs a %s fix"
                % (qw, FILLER[i], qw))
        add("the %s %s pipeline runs a %s pass" % (decoy, FILLER[i], decoy))
        golden.append(("A", "how do we fix the %s in the %s pipeline"
                       % (qw, FILLER[i]), t, {}, {}))
    for ident, plain in IDENTIFIERS:
        t = add("refactor %s handling in the checker module" % ident)
        golden.append(("B", "where is %s handled" % plain, t, {}, {}))
    for qw, syn, decoy in CONCEPTS:
        t = add("recurring %s observed in the intake stage" % syn)
        add("recurring %s observed in the intake stage" % decoy)
        golden.append(("C", "recurring %s observed in the intake stage" % qw,
                       t, {}, {}))
    for i in range(5):
        text = "flaky %s failure on the %s runner" % (FILLER[i], FILLER[i + 3])
        add(text, ts="2019-01-01 00:00:00")
        t = add(text, ts="2026-0%d-01 00:00:00" % (i + 1))
        golden.append(("D", text, t, {}, {}))
    for i in range(5):
        text = "root cause note for %s %s regression" % (FILLER[i + 4],
                                                         FILLER[i + 8])
        add(text, conf=0.2)
        t = add(text, conf=0.95)
        golden.append(("E", text, t, {}, {}))
    for i in range(5):
        text = "build broken by %s toolchain %s bump" % (FILLER[i + 2],
                                                         FILLER[i + 6])
        add(text, repo="other")
        t = add(text, repo="mine")
        golden.append(("F", text, t, {"boost": {"repo": "{payload.repo}"}},
                       {"repo": "mine"}))
    return rows, golden


def synonym_weights():
    dim = len(CONCEPTS) + 1
    vocab = {}
    for i, group in enumerate(CONCEPTS):
        vec = [0.0] * dim
        vec[i] = 1.0
        for w in group[:2]:
            vocab[w] = vec
    return {"kind": "bow-embed", "dim": dim, "vocab": vocab}


class RecallTest(unittest.TestCase):
    def _engine(self, embed_yaml):
        base = tmpdir()
        repo = make_target_repo(base)
        pack_dir = make_pack(base, repo, extra=(
            "schema: [schema.sql]\n" + embed_yaml))
        (pack_dir / "schema.sql").write_text(SCHEMA_SQL)
        eng = make_engine(base, pack_dir=pack_dir)
        rows, golden = build()
        eng.conn.executemany("INSERT OR REPLACE INTO notes VALUES (?,?,?,?,?)",
                             rows)
        eng.conn.commit()
        return eng, golden

    def _recall(self, eng, golden):
        """{category: (recall@1, recall@5)} over the golden set."""
        hits = {}
        for cat, q, target, extra, payload in golden:
            spec = {"corpus": "notes", "query": q, "k": 5}
            spec.update(extra)
            out = SELECT(eng.env, {"id": 1, "attempts": 0, "payload": payload},
                         spec)
            keys = [e["key"] for e in out["entries"]]
            r1, r5 = (keys and keys[0] == target), target in keys
            c = hits.setdefault(cat, [0, 0, 0])
            c[0] += 1
            c[1] += 1 if r1 else 0
            c[2] += 1 if r5 else 0
        return {cat: (c[1] / c[0], c[2] / c[0]) for cat, c in hits.items()}

    def test_hashing_floors(self):
        eng, golden = self._engine(
            "corpora:\n  notes: { table: notes, key: id, text: body,"
            " ts: created_at, weight: conf, embed_with: hashing }\n")
        rec = self._recall(eng, golden)
        for cat in ("A", "B", "D", "E", "F"):    # lexical-reachable: perfect
            self.assertEqual(rec[cat], (1.0, 1.0), (cat, rec))
        # paraphrase is EXPECTED to fail on a lexical embedder — if this
        # ever passes, the fixture stopped testing what it claims to test
        self.assertLess(rec["C"][0], 0.5, rec)

    def test_synonym_model_recovers_paraphrase(self):
        base_yaml = (
            "models:\n  syn: { path: syn.json, sha256: %s }\n"
            "corpora:\n  notes: { table: notes, key: id, text: body,"
            " ts: created_at, weight: conf, embed_with: syn }\n")
        # write weights into the pack before load (sha pinned)
        base = tmpdir()
        repo = make_target_repo(base)
        pack_dir = base / "pack"
        pack_dir.mkdir(exist_ok=True)
        wpath = pack_dir / "syn.json"
        wpath.write_text(json.dumps(synonym_weights()))
        pack_dir = make_pack(base, repo, extra=(
            "schema: [schema.sql]\n" + base_yaml % sha256_file(wpath)))
        (pack_dir / "schema.sql").write_text(SCHEMA_SQL)
        eng = make_engine(base, pack_dir=pack_dir)
        rows, golden = build()
        eng.conn.executemany("INSERT OR REPLACE INTO notes VALUES (?,?,?,?,?)",
                             rows)
        eng.conn.commit()
        rec = self._recall(eng, golden)
        self.assertGreaterEqual(rec["C"][0], 0.9, rec)   # paraphrase recovered
        self.assertEqual(rec["C"][1], 1.0, rec)
        for cat in ("A", "D", "E", "F"):
            self.assertEqual(rec[cat], (1.0, 1.0), (cat, rec))
        self.assertEqual(rec["B"][1], 1.0, rec)          # identifiers stay found


if __name__ == "__main__":
    unittest.main()
