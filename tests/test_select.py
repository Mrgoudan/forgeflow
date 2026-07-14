"""Generic corpus selection: the hashing embedder, corpora config +
engine-start checks, and the select: provider's channel ranking / RRF
fusion / incremental embedding semantics."""
from __future__ import annotations

import unittest

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import config, contract, localmodel
from forgeflow import select as select_mod

SELECT = contract.CONTEXT_PROVIDERS["select"]

CORPUS_SQL = """
CREATE TABLE IF NOT EXISTS notes_tbl (
    id          TEXT PRIMARY KEY,
    body        TEXT,
    created_at  TEXT,
    conf        REAL,
    repo        TEXT
);
"""

CORPUS_YAML = (
    "schema: [schema.sql]\n"
    "corpora:\n"
    "  notes:\n"
    "    table: notes_tbl\n"
    "    key: id\n"
    "    text: body\n"
    "    ts: created_at\n"
    "    weight: conf\n"
    "    embed_with: hashing\n")


class HashEmbedTest(unittest.TestCase):
    def test_deterministic_and_normalized(self):
        a = localmodel.hash_embed("parser crash on empty include")
        b = localmodel.hash_embed("parser crash on empty include")
        self.assertEqual(a, b)
        self.assertAlmostEqual(sum(x * x for x in a), 1.0, places=6)
        self.assertEqual(len(a), localmodel.HASHING_DEFAULT_DIM)

    def test_shared_tokens_score_higher(self):
        q = localmodel.hash_embed("parser crash on include")
        near = localmodel.hash_embed("crash inside the parser include path")
        far = localmodel.hash_embed("billing invoice quarterly totals")
        self.assertGreater(localmodel.cosine(q, near),
                           localmodel.cosine(q, far))

    def test_identifier_splitting(self):
        toks = localmodel.split_identifiers("flagSkipReason flag_skip_reason")
        self.assertIn("skip", toks)
        self.assertIn("reason", toks)
        self.assertIn("flag", toks)

    def test_model_sha_pins_version_and_dim(self):
        self.assertNotEqual(localmodel.hashing_model_sha(256),
                            localmodel.hashing_model_sha(128))


class CorporaConfigTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)

    def _load(self, extra):
        pack_dir = make_pack(self.base, self.repo, extra=extra)
        (pack_dir / "schema.sql").write_text(CORPUS_SQL)
        return config.load_pack(pack_dir)

    def test_valid_corpus_parses(self):
        pack = self._load(CORPUS_YAML)
        self.assertEqual(pack.corpora["notes"]["table"], "notes_tbl")

    def test_hashing_model_type(self):
        pack = self._load("models:\n  fast: { hashing: { dim: 64 } }\n")
        self.assertEqual(pack.models["fast"], {"hashing": {"dim": 64}})
        for bad in ("models:\n  fast: { hashing: { dim: 4 } }\n",
                    "models:\n  fast: { hashing: { dim: true } }\n",
                    "models:\n  fast: { hashing: { nope: 1 } }\n"):
            with self.assertRaises(SystemExit, msg=bad):
                self._load(bad)

    def test_rejections(self):
        cases = [
            "corpora: [1]\n",                                     # not a map
            "corpora:\n  n: { text: body }\n",                    # no table
            "corpora:\n  n: { table: t }\n",                      # no text
            "corpora:\n  n: { table: 't;drop', text: body }\n",   # identifier
            "corpora:\n  n: { table: t, text: body, nope: 1 }\n",  # unknown key
            "corpora:\n  n: { table: t, text: body,"
            " embed_with: ghost }\n",                             # unknown model
        ]
        for extra in cases:
            with self.assertRaises(SystemExit, msg=extra):
                self._load(extra)


class EngineCorporaCheckTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)

    def test_missing_table_fails_engine_start(self):
        pack_dir = make_pack(self.base, self.repo, extra=(
            "corpora:\n  n: { table: ghost_tbl, text: body }\n"))
        with self.assertRaisesRegex(SystemExit, "ghost_tbl"):
            make_engine(self.base, pack_dir=pack_dir)

    def test_missing_column_fails_engine_start(self):
        pack_dir = make_pack(self.base, self.repo, extra=(
            "schema: [schema.sql]\n"
            "corpora:\n  n: { table: notes_tbl, text: ghost_col }\n"))
        (pack_dir / "schema.sql").write_text(CORPUS_SQL)
        with self.assertRaisesRegex(SystemExit, "ghost_col"):
            make_engine(self.base, pack_dir=pack_dir)

    def test_valid_corpus_starts(self):
        pack_dir = make_pack(self.base, self.repo, extra=CORPUS_YAML)
        (pack_dir / "schema.sql").write_text(CORPUS_SQL)
        eng = make_engine(self.base, pack_dir=pack_dir)
        self.assertIn("notes", eng.pack.corpora)


class SelectProviderTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        repo = make_target_repo(self.base)
        pack_dir = make_pack(self.base, repo, extra=CORPUS_YAML)
        (pack_dir / "schema.sql").write_text(CORPUS_SQL)
        self.eng = make_engine(self.base, pack_dir=pack_dir)
        self.conn = self.eng.conn

    def _row(self, id, body, ts="2026-01-01 00:00:00", conf=0.5, repo="alpha"):
        self.conn.execute(
            "INSERT OR REPLACE INTO notes_tbl(id, body, created_at, conf, repo)"
            " VALUES (?,?,?,?,?)", (id, body, ts, conf, repo))

    def _select(self, spec, payload=None, task_id=1):
        task = {"id": task_id, "attempts": 0, "payload": payload or {}}
        return SELECT(self.eng.env, task, spec)

    def _keys(self, out):
        return [e["key"] for e in out["entries"]]

    def test_lexical_relevance_wins(self):
        self._row("a", "parser crashes on empty include path")
        self._row("b", "billing invoice quarterly totals report")
        self._row("c", "dashboard color palette tweaks")
        out = self._select({"corpus": "notes",
                            "query": "crash in the include parser", "k": 2})
        self.assertEqual(self._keys(out)[0], "a")
        self.assertEqual(out["considered"], 3)
        self.assertFalse(out["included_all"])
        # every entry is explainable: fused score + per-channel ranks
        self.assertIn("score", out["entries"][0])
        self.assertIn("lexical", out["entries"][0]["channels"])

    def test_identifier_split_matches_camel_case(self):
        self._row("a", "refactor flagSkipReason handling in checker")
        self._row("b", "unrelated marketing copy for the website")
        out = self._select({"corpus": "notes",
                            "query": "why is skip reason flagged", "k": 1})
        self.assertEqual(self._keys(out), ["a"])

    def test_recency_lifts_newer_among_equals(self):
        self._row("old", "parser include crash", ts="2020-01-01 00:00:00")
        self._row("new", "parser include crash", ts="2026-06-01 00:00:00")
        out = self._select({"corpus": "notes",
                            "query": "parser include crash", "k": 2})
        self.assertEqual(self._keys(out)[0], "new")

    def test_prior_weight_lifts(self):
        self._row("meh", "parser include crash", conf=0.1,
                  ts="2026-01-01 00:00:00")
        self._row("gold", "parser include crash", conf=0.9,
                  ts="2026-01-01 00:00:00")
        out = self._select({"corpus": "notes",
                            "query": "parser include crash", "k": 2})
        self.assertEqual(self._keys(out)[0], "gold")

    def test_filter_scopes_candidates(self):
        self._row("a", "parser crash", repo="alpha")
        self._row("b", "parser crash", repo="beta")
        out = self._select({"corpus": "notes", "query": "parser crash",
                            "filter": {"repo": "{payload.repo}"}, "k": 5},
                           payload={"repo": "beta"})
        self.assertEqual(out["considered"], 1)
        self.assertEqual(self._keys(out), ["b"])

    def test_query_templates_from_step_prev(self):
        # a per-item loop selects by what its cursor picked: the query
        # templates from the PREVIOUS step's result via env.step_prev.
        self._row("a", "append an element to the end of a growable array")
        self._row("b", "billing invoice quarterly totals report")
        self.eng.env.step_prev = {"summary": "append element growable array"}
        try:
            out = self._select({"corpus": "notes",
                                "query": "{prev.summary}", "k": 1})
        finally:
            self.eng.env.step_prev = None
        self.assertEqual(self._keys(out), ["a"])

    def test_prev_missing_key_fails_loud(self):
        # template semantics stay STRICT (same as payload): referencing a prev
        # key the previous step did not produce is a loud error, not a silent
        # empty query — so a select: over prev requires the feeding step to
        # always carry that field.
        self._row("a", "alpha")
        self.eng.env.step_prev = {}
        try:
            with self.assertRaises(KeyError):
                self._select({"corpus": "notes",
                              "query": "x {prev.summary}", "k": 1})
        finally:
            self.eng.env.step_prev = None

    def test_boost_lifts_linked_rows(self):
        self._row("foreign", "parser include crash", repo="other",
                  ts="2026-01-01 00:00:00")
        self._row("ours", "parser include crash", repo="alpha",
                  ts="2026-01-01 00:00:00")
        out = self._select({"corpus": "notes", "query": "parser include crash",
                            "boost": {"repo": "{payload.repo}"}, "k": 2},
                           payload={"repo": "alpha"})
        self.assertEqual(self._keys(out)[0], "ours")

    def test_weights_can_silence_a_channel(self):
        self._row("lexhit", "alpha beta gamma delta", ts="2020-01-01 00:00:00")
        self._row("fresh", "unrelated words entirely", ts="2026-06-01 00:00:00")
        spec = {"corpus": "notes", "query": "alpha beta gamma delta", "k": 1}
        self.assertEqual(self._keys(self._select(spec)), ["lexhit"])
        spec["weights"] = {"lexical": 0, "semantic": 0, "prior": 0}
        self.assertEqual(self._keys(self._select(spec)), ["fresh"])

    def test_include_all_under_skips_ranking(self):
        self._row("a", "one")
        self._row("b", "two")
        out = self._select({"corpus": "notes", "query": "anything",
                            "include_all_under": 1024, "k": 1})
        self.assertTrue(out["included_all"])
        self.assertEqual(self._keys(out), ["a", "b"])   # all, ordered by key

    def test_truncation_is_flagged_never_silent(self):
        self._row("big", "parser " + "x" * 5000)
        out = self._select({"corpus": "notes", "query": "parser",
                            "max_chars": 100, "k": 1})
        e = out["entries"][0]
        self.assertEqual(len(e["text"]), 100)
        self.assertTrue(e["truncated"])

    def test_incremental_embedding_and_reembed_on_change(self):
        self._row("a", "parser crash")
        self._row("b", "billing totals")
        spec = {"corpus": "notes", "query": "parser", "k": 2}
        self._select(spec)
        n = lambda: self.conn.execute(
            "SELECT count(*) FROM corpus_embeddings WHERE corpus='notes'"
        ).fetchone()[0]
        sha_of = lambda key: self.conn.execute(
            "SELECT text_sha FROM corpus_embeddings WHERE corpus='notes'"
            " AND key=?", (key,)).fetchone()[0]
        self.assertEqual(n(), 2)
        before = sha_of("a")
        self._select(spec)                     # second run: nothing re-embedded
        self.assertEqual(n(), 2)
        self.assertEqual(sha_of("a"), before)
        self._row("a", "parser crash NOW WITH MORE DETAIL")   # text changed
        self._select(spec)
        self.assertEqual(n(), 2)               # replaced, not duplicated
        self.assertNotEqual(sha_of("a"), before)

    def test_deterministic_across_runs(self):
        for i in range(8):
            self._row("r%d" % i, "parser include crash variant %d" % i,
                      ts="2026-01-0%d 00:00:00" % (i % 9 + 1), conf=i / 10.0)
        spec = {"corpus": "notes", "query": "parser include crash", "k": 4}
        self.assertEqual(self._keys(self._select(spec)),
                         self._keys(self._select(spec)))

    def test_bad_filter_column_is_loud(self):
        self._row("a", "x")
        with self.assertRaisesRegex(RuntimeError, "ghost"):
            self._select({"corpus": "notes", "query": "x",
                          "filter": {"ghost": "1"}})

    def test_multi_query_fusion(self):
        self._row("title_hit", "checker rejects the verdict schema gate")
        self._row("error_hit", "segmentation violation inside include parser")
        self._row("noise", "quarterly billing invoice rollup")
        out = self._select({"corpus": "notes",
                            "query": ["verdict schema gate rejected",
                                      "segmentation violation include"],
                            "k": 2})
        self.assertEqual(sorted(self._keys(out)), ["error_hit", "title_hit"])
        # a single query only reaches its own side
        out1 = self._select({"corpus": "notes",
                             "query": "verdict schema gate rejected", "k": 1})
        self.assertEqual(self._keys(out1), ["title_hit"])

    def test_dedup_identical_text_never_takes_two_slots(self):
        self._row("twin_lo", "parser crash in intake", conf=0.1)
        self._row("twin_hi", "parser crash in intake", conf=0.9)
        self._row("other", "parser crash elsewhere entirely", conf=0.5)
        out = self._select({"corpus": "notes", "query": "parser crash in intake",
                            "k": 2})
        keys = self._keys(out)
        self.assertEqual(out["deduped"], 1)
        self.assertIn("twin_hi", keys)          # the better twin won its slot
        self.assertNotIn("twin_lo", keys)
        self.assertIn("other", keys)            # the freed slot adds coverage

    def test_diversity_admits_complementary_row(self):
        # a realistic pool: noise below, four near-identical top hits, one
        # complementary relevant row. Default MMR must spend a slot on the
        # complement instead of a third copy; diversify: 0 must not.
        import random
        rng = random.Random(7)
        filler = ("table queue batch record service daemon file check "
                  "task result").split()
        for i in range(20):
            self._row("noise%02d" % i,
                      " ".join(rng.choice(filler) for _ in range(8)))
        for tag in ("one", "two", "three", "four"):
            self._row("copy_%s" % tag,
                      "parser crash in intake stage copy %s" % tag)
        self._row("complement",
                  "parser crash with memory heap spike in intake")
        spec = {"corpus": "notes", "query": "parser crash in intake stage",
                "k": 3}
        with_div = self._keys(self._select(spec))          # default 0.5
        self.assertIn("complement", with_div)
        without = self._keys(self._select(dict(spec, diversify=0)))
        self.assertNotIn("complement", without)   # pure rank = three copies

    def test_budget_drops_are_counted(self):
        self._row("a", "parser crash " + "a" * 120)
        self._row("b", "parser crash " + "b" * 120)
        self._row("c", "parser crash " + "c" * 120)
        out = self._select({"corpus": "notes", "query": "parser crash",
                            "k": 3, "max_bytes": 150})
        self.assertEqual(len(out["entries"]), 1)
        self.assertEqual(out["dropped"], 2)

    def test_utility_learns_from_outcomes(self):
        """The acceptance-signal loop: a row shown to a task that reached
        done outranks its equal shown to a task that failed — learned from
        the ledger alone, no labels."""
        from forgeflow import queue
        self._row("good", "approach one for the parser crash")
        self._row("bad", "approach two for the parser crash")
        # history: task shown 'good' succeeds, task shown 'bad' fails
        t1 = queue.enqueue(self.conn, "jobkind", {"n": 1})
        self._select({"corpus": "notes",
                      "query": "approach one parser crash", "k": 1},
                     task_id=t1)
        queue.complete(self.conn, t1)
        t2 = queue.enqueue(self.conn, "jobkind", {"n": 2})
        self._select({"corpus": "notes",
                      "query": "approach two parser crash", "k": 1},
                     task_id=t2)
        queue.fail(self.conn, t2, "workspace_dirty")     # consume -> failed
        # a fresh same-kind task with a NEUTRAL query: utility decides
        t3 = queue.enqueue(self.conn, "jobkind", {"n": 3})
        out = self._select({"corpus": "notes", "query": "parser crash",
                            "k": 2}, task_id=t3)
        self.assertEqual(self._keys(out), ["good", "bad"])
        self.assertIn("utility", out["entries"][0]["channels"])
        # the ledger recorded what t3 was shown, too
        n = self.conn.execute("SELECT count(*) FROM context_uses"
                              " WHERE task_id=?", (t3,)).fetchone()[0]
        self.assertEqual(n, 2)

    def test_track_false_disables_the_ledger_and_utility(self):
        """Data governance: a corpus with track: false never records what
        tasks were shown, and the utility channel abstains."""
        from forgeflow import queue
        base = tmpdir()
        repo = make_target_repo(base)
        pack_dir = make_pack(base, repo, extra=CORPUS_YAML.replace(
            "    embed_with: hashing\n",
            "    embed_with: hashing\n    track: false\n"))
        (pack_dir / "schema.sql").write_text(CORPUS_SQL)
        eng = make_engine(base, pack_dir=pack_dir)
        eng.conn.execute(
            "INSERT INTO notes_tbl(id, body, created_at, conf, repo)"
            " VALUES ('a','parser crash','2026-01-01',0.5,'alpha')")
        tid = queue.enqueue(eng.conn, "jobkind", {"n": 1})   # a REAL task
        out = SELECT(eng.env, {"id": tid, "attempts": 0, "payload": {}},
                     {"corpus": "notes", "query": "parser crash", "k": 1})
        self.assertEqual(len(out["entries"]), 1)
        self.assertNotIn("utility", out["entries"][0]["channels"])
        self.assertEqual(eng.conn.execute(
            "SELECT count(*) FROM context_uses").fetchone()[0], 0)

    def test_preview_tasks_never_pollute_the_ledger(self):
        self._row("a", "parser crash")
        self._select({"corpus": "notes", "query": "parser crash", "k": 1})
        n = self.conn.execute("SELECT count(*) FROM context_uses").fetchone()[0]
        self.assertEqual(n, 0)          # task id 1 has no tasks row -> no log

    def test_dedup_has_an_off_switch(self):
        self._row("twin_a", "parser crash in intake", conf=0.1)
        self._row("twin_b", "parser crash in intake", conf=0.9)
        spec = {"corpus": "notes", "query": "parser crash in intake", "k": 2}
        self.assertEqual(len(self._keys(self._select(spec))), 1)   # default on
        out = self._select(dict(spec, dedup=False))
        self.assertEqual(sorted(self._keys(out)), ["twin_a", "twin_b"])
        self.assertEqual(out["deduped"], 0)
        self.assertIn("boolean", select_mod._check_select_spec(
            {"corpus": "notes", "query": "q", "dedup": 1}, self.eng.pack))

    def test_funnel_telemetry(self):
        """Every selection reports the cascade as numbers — where a
        candidate died is a lookup, not guesswork."""
        self._row("twin_a", "parser crash in intake", conf=0.1)
        self._row("twin_b", "parser crash in intake", conf=0.9)
        self._row("c", "parser crash " + "c" * 200)
        self._row("d", "unrelated billing rollup")
        out = self._select({"corpus": "notes", "query": "parser crash",
                            "k": 3, "max_bytes": 250, "max_chars": 220})
        f = out["funnel"]
        self.assertEqual(f["gathered"], 4)
        self.assertEqual(f["deduped"], 1)          # twin collapsed
        self.assertEqual(f["pool"], 3)
        self.assertEqual(f["reranked"], 0)         # no judge configured
        self.assertEqual(f["chosen"], 3)
        self.assertEqual(f["packed"], len(out["entries"]))
        self.assertEqual(f["dropped"], out["dropped"])
        self.assertEqual(f["chosen"], f["packed"] + f["dropped"])
        # include_all path reports its (shorter) funnel too
        out2 = self._select({"corpus": "notes", "query": "x",
                             "include_all_under": 10 ** 6, "k": 1})
        self.assertEqual(out2["funnel"], {"gathered": 4, "packed": 4})

    def test_check_spec(self):
        pack = self.eng.pack
        check = select_mod._check_select_spec
        self.assertIsNone(check({"corpus": "notes", "query": "q"}, pack))
        self.assertIn("corpus", check({"query": "q"}, pack))
        self.assertIn("query", check({"corpus": "notes"}, pack))
        self.assertIn("positive", check({"corpus": "notes", "query": "q",
                                         "k": 0}, pack))
        self.assertIn("unknown channels",
                      check({"corpus": "notes", "query": "q",
                             "weights": {"ghost": 1}}, pack))
        self.assertIn("mapping", check({"corpus": "notes", "query": "q",
                                        "filter": [1]}, pack))
        self.assertIsNone(check({"corpus": "notes", "query": ["a", "b"],
                                 "diversify": 0.5, "max_bytes": 4096}, pack))
        self.assertIn("non-empty", check({"corpus": "notes", "query": []},
                                         pack))
        self.assertIn("0..1", check({"corpus": "notes", "query": "q",
                                     "diversify": 2}, pack))
        self.assertIn("positive", check({"corpus": "notes", "query": "q",
                                         "max_bytes": 0}, pack))


if __name__ == "__main__":
    unittest.main()
