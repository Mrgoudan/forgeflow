"""The select: context provider — ranked, constructed, learned context
from any pack-declared corpus. Split from contract.py (which owns the
execution contract); importing this module registers the provider.

Design grounded in production-RAG evidence (docs/LLM.md cites sources):
- hybrid beats pure-vector -> independent channels (lexical, semantic,
recency, prior, boost) fused by Reciprocal Rank Fusion. RRF because
calibrating linear score weights needs labeled queries per corpus;
rank fusion is the robust untuned default.
- recency and priors must score the FULL filtered pool, never re-rank a
narrow top-K cosine cut — at this engine's scale (thousands to low
hundreds of thousands of rows) brute force is the correct architecture.
- embeddings are OPTIONAL: the lexical channel plus the zero-setup
hashing embedder give a strong deterministic core; real embedding
models plug in per corpus when wanted.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from .contract import context_provider
from .util import tx


SELECT_CHANNELS = ("lexical", "semantic", "recency", "prior", "boost",
                   "utility")
_RRF_K = 60

# Default channel weights: RELEVANCE channels (query/task-conditioned) vote
# at full strength; PRIORS (recency, importance, learned utility) modulate
# at 0.3 — enough to decide among relevance ties (their job), not enough
# for a fresh-or-important-but-irrelevant row to outvote an actual match
# (a failure mode the recall evaluation demonstrated at equal weights).
# Override per step via weights:.
SELECT_WEIGHTS = {"lexical": 1.0, "semantic": 1.0, "boost": 1.0,
                  "recency": 0.3, "prior": 0.3, "utility": 0.3}


def _rank_desc(scored, keys):
    """Fractional (average-rank) ranking, best score first. TIES SHARE the
    mean of their positions — a block of 2000 rows tied at zero dilutes to
    a deep rank instead of handing whichever row sorts first an excellent
    one (a real bug the recall evaluation caught: key-order tie-breaks let
    arbitrary distractors outvote true matches through RRF). Keys the
    channel could not score rank behind everything it could. Returns None
    when the channel carries no signal (all scores equal) — an
    uninformative channel must not vote."""
    if not scored or len({v for v in scored.values()}) <= 1:
        return None
    counts = {}
    for v in scored.values():
        counts[v] = counts.get(v, 0) + 1
    rank_of = {}
    seen = 0
    for v in sorted(counts, reverse=True):        # values, best first
        rank_of[v] = seen + (counts[v] + 1) / 2.0  # mean of the tied block
        seen += counts[v]
    worst = float(len(keys) + 1)
    return {kk: rank_of[scored[kk]] if kk in scored else worst
            for kk in keys}


@context_provider("select")
def _ctx_select(env, task, spec):
    """Pick the most relevant/important rows of a declared corpus for THIS
    task: SQL metadata pre-filter, per-channel ranking, RRF fusion, top-k.
    Deterministic end to end; every selected entry carries its fused score
    and per-channel ranks, so 'why did the model see this' is data."""
    from . import localmodel
    from .util import sha256_text
    from .util import template as _template
    conn = env.conn
    corpus_name = spec["corpus"]
    corpus = env.pack.corpora[corpus_name]
    payload_map = {"payload": task.get("payload") or {}}
    raw_q = spec["query"]
    queries = [_template(q, payload_map)
               for q in (raw_q if isinstance(raw_q, list) else [raw_q])]
    k = int(spec.get("k", 5))
    max_chars = int(spec.get("max_chars", 2000))
    # MMR redundancy penalty; 0.5 =~ lambda 0.67 in classic MMR terms (the
    # standard relevance-leaning balance). 0 restores pure ranked order.
    diversify = float(spec.get("diversify", 0.5))

    # ---- candidates: metadata pre-filter pushed into SQL
    table = corpus["table"]
    cols = {r["name"] for r in conn.execute('PRAGMA table_info("%s")' % table)}
    key_col = corpus.get("key")
    sel = [('"%s"' % key_col if key_col else "rowid") + " AS _key",
           '"%s" AS _text' % corpus["text"]]
    if corpus.get("ts"):
        sel.append('"%s" AS _ts' % corpus["ts"])
    if corpus.get("weight"):
        sel.append('"%s" AS _weight' % corpus["weight"])
    boost_spec = spec.get("boost") or {}
    for i, col in enumerate(sorted(boost_spec)):
        if col not in cols:
            raise RuntimeError("select: boost column '%s' not in table '%s'"
                               % (col, table))
        sel.append('"%s" AS _b%d' % (col, i))
    where, args = [], []
    for col in sorted(spec.get("filter") or {}):
        if col not in cols:
            raise RuntimeError("select: filter column '%s' not in table '%s'"
                               % (col, table))
        where.append('"%s" = ?' % col)
        args.append(_template(spec["filter"][col], payload_map))
    sql = 'SELECT %s FROM "%s"' % (", ".join(sel), table)
    if where:
        sql += " WHERE " + " AND ".join(where)
    cands = []
    for r in conn.execute(sql, args):
        cands.append({
            "key": str(r["_key"]),
            "text": "" if r["_text"] is None else str(r["_text"]),
            "ts": r["_ts"] if corpus.get("ts") else None,
            "weight": r["_weight"] if corpus.get("weight") else None,
            "boost": [r["_b%d" % i] for i in range(len(boost_spec))],
        })
    cands.sort(key=lambda c: c["key"])
    keys = [c["key"] for c in cands]
    by_key = {c["key"]: c for c in cands}

    # cached model-written summaries (corpus summarize_with:) — they feed
    # the lexical match (long rows stay findable) and replace blind
    # truncation at injection. Only summaries whose text_sha still matches
    # the row count; stale ones are ignored (and rewritten on selection).
    summaries = {}
    sum_binding = corpus.get("summarize_with")
    if sum_binding and keys:
        from .util import sha256_text as _sha_t
        ph = ",".join("?" * len(keys))
        for r in conn.execute(
                "SELECT key, text_sha, summary FROM corpus_summaries"
                " WHERE corpus=? AND binding=? AND key IN (%s)" % ph,
                [corpus_name, sum_binding] + keys):
            if r["key"] in by_key and \
                    r["text_sha"] == _sha_t(by_key[r["key"]]["text"]):
                summaries[r["key"]] = r["summary"]

    def entry(c, score=None, channels=None):
        e = {"key": c["key"], "text": c["text"][:max_chars]}
        if len(c["text"]) > max_chars:
            e["truncated"] = True         # never a silent cut
        if score is not None:
            e["score"] = round(score, 6)
            e["channels"] = channels
        return e

    out = {"corpus": corpus_name,
           "query": queries if len(queries) > 1 else queries[0],
           "considered": len(cands)}

    # ---- small corpus: don't rank, include everything (declared opt-in)
    all_under = spec.get("include_all_under")
    if all_under and sum(len(c["text"].encode("utf-8", "replace"))
                         for c in cands) <= int(all_under):
        out["included_all"] = True
        out["entries"] = [entry(c) for c in cands]
        out["funnel"] = {"gathered": len(cands), "packed": len(cands)}
        if not _previewing(env) and corpus.get("track", True):
            _log_uses(conn, task, corpus_name, [c["key"] for c in cands])
        return out
    out["included_all"] = False

    # ---- channels -> voters. A channel may abstain when signal-free;
    # query-conditioned channels vote ONCE PER QUERY (multi-query fusion:
    # the task's title and its error text each get a say), each vote
    # carrying weight/nq so a channel's total influence is constant.
    voters = []                             # (channel, weight_fraction, ranks)
    weights = dict(SELECT_WEIGHTS)
    weights.update(spec.get("weights") or {})
    nq = float(len(queries))

    for q in queries:
        qtok = set(localmodel.split_identifiers(q))
        r = _rank_desc(
            {c["key"]: float(len(qtok & set(localmodel.split_identifiers(
                c["text"] + " " + summaries.get(c["key"], "")))))
             for c in cands}, keys)
        if r is not None:
            voters.append(("lexical", weights["lexical"] / nq, r))

    if corpus.get("embed_with"):
        qvecs, vectors = _corpus_vectors(env, corpus_name, corpus, cands,
                                         queries)
        for qvec in qvecs:
            r = _rank_desc({kk: localmodel.cosine(qvec, vec)
                            for kk, vec in vectors.items()}, keys)
            if r is not None:
                voters.append(("semantic", weights["semantic"] / nq, r))

    if corpus.get("ts"):
        norm = _comparable({c["key"]: c["ts"] for c in cands
                            if c["ts"] is not None})
        r = _rank_desc(norm, keys) if norm else None
        if r is not None:
            voters.append(("recency", weights["recency"], r))

    if corpus.get("weight"):
        vals = {c["key"]: float(c["weight"]) for c in cands
                if isinstance(c["weight"], (int, float))
                and not isinstance(c["weight"], bool)}
        r = _rank_desc(vals, keys) if vals else None
        if r is not None:
            voters.append(("prior", weights["prior"], r))

    if boost_spec:
        want = [_template(boost_spec[col], payload_map)
                for col in sorted(boost_spec)]
        r = _rank_desc(
            {c["key"]: float(sum(1 for got, w in zip(c["boost"], want)
                                 if got is not None and str(got) == str(w)))
             for c in cands}, keys)
        if r is not None:
            voters.append(("boost", weights["boost"], r))

    r = (_utility_ranks(conn, task, corpus_name, keys)
         if corpus.get("track", True) else None)
    if r is not None:
        voters.append(("utility", weights["utility"], r))

    # ---- Reciprocal Rank Fusion over the voters
    fused = {kk: sum(w / (_RRF_K + ranks[kk]) for _, w, ranks in voters
                     if w > 0)
             for kk in keys}

    # ---- optional LLM rerank of the top window (a local judge scoring
    # usefulness-to-THIS-task; the cookbook-verified stage). Failure of
    # any kind falls back to the fused order — the model may reduce yield,
    # never integrity. Preview tasks (no tasks row) skip it.
    ordered = sorted(keys, key=lambda kk: (-fused[kk], kk))
    rr = spec.get("rerank")
    rr_scores = {}
    if rr:
        out["reranked"] = False
        if not _previewing(env) and _task_row(conn, task) is not None:
            window = ordered[:int(rr.get("window", max(20, 2 * k)))]
            try:
                rr_scores = _rerank_scores(env, task, rr, window, by_key,
                                           summaries)
                window.sort(key=lambda kk: (-rr_scores.get(kk, -1),
                                            -fused[kk], kk))
                ordered = window + ordered[len(window):]
                out["reranked"] = True
            except Exception as e:
                out["rerank_error"] = "%s: %s" % (type(e).__name__, e)
                print("select: rerank via '%s' failed (%s) — using fused "
                      "order" % (rr.get("llm"), e), file=sys.stderr)

    # ---- construction: ranked list -> USEFUL set.
    # 1) dedup: an identical text never occupies two slots — the
    #    better-ranked twin wins (with the recency/prior channels, that IS
    #    the newer/heavier one). dedup: false switches it off.
    dedup_on = spec.get("dedup", True)
    seen_sha, pool, deduped = set(), [], 0
    from .util import sha256_text as _sha
    for kk in ordered:
        if dedup_on:
            tsha = _sha(by_key[kk]["text"])
            if tsha in seen_sha:
                deduped += 1
                continue
            seen_sha.add(tsha)
        pool.append(kk)
        if len(pool) >= max(50, 4 * k):   # MMR pool cap (cost bound)
            break
    out["deduped"] = deduped

    # 2) diversity (MMR): each next pick trades relevance against
    #    redundancy with what is already picked, so k slots cover the
    #    task's ground instead of repeating the top hit. diversify=0
    #    restores pure ranked order.
    if diversify > 0 and len(pool) > 1 and k > 1:
        lo = min(fused[kk] for kk in pool)
        hi = max(fused[kk] for kk in pool)
        rel = {kk: (fused[kk] - lo) / (hi - lo) if hi > lo else 1.0
               for kk in pool}
        dvec = {kk: localmodel.hash_embed(by_key[kk]["text"]) for kk in pool}
        picked = [pool[0]]
        remaining = pool[1:]
        while remaining and len(picked) < k:
            best = min(remaining, key=lambda kk: (
                -(rel[kk] - diversify * max(localmodel.cosine(dvec[kk], dvec[p])
                                            for p in picked)),
                -fused[kk], kk))
            picked.append(best)
            remaining.remove(best)
        chosen = picked
    else:
        chosen = pool[:k]

    # 3) injection text: a row longer than max_chars gets a model-written
    #    SUMMARY when the corpus declares summarize_with (cached by
    #    text_sha; generated lazily here — at most the k chosen rows per
    #    call, each bounded). No binding, or generation fails -> plain
    #    truncation with the explicit flag, as before.
    display = {}
    for kk in chosen:
        text = by_key[kk]["text"]
        if len(text) <= max_chars:
            display[kk] = (text, None)
            continue
        s = summaries.get(kk)
        if s is None and sum_binding and not _previewing(env) \
                and _task_row(conn, task) is not None:
            s = _gen_summary(env, task, corpus_name, sum_binding, kk, text,
                             max_chars)
            if s is not None:
                summaries[kk] = s
        if s is not None:
            display[kk] = (s[:max_chars], "summarized")
        else:
            display[kk] = (text[:max_chars], "truncated")

    # 4) budget: pack in chosen order until max_bytes; dropped is COUNTED,
    #    never silent.
    max_bytes = spec.get("max_bytes")
    final, used, dropped = [], 0, 0
    for kk in chosen:
        size = len(display[kk][0].encode("utf-8", "replace"))
        if max_bytes is not None and final and used + size > int(max_bytes):
            dropped += 1
            continue
        final.append(kk)
        used += size
    out["dropped"] = dropped

    per_channel = {}
    for ch, _, ranks_d in voters:
        cur = per_channel.setdefault(ch, {})
        for kk in final:
            cur[kk] = min(cur.get(kk, ranks_d[kk]), ranks_d[kk])
    entries = []
    for kk in final:
        text, mode = display[kk]
        e = {"key": kk, "text": text,
             "score": round(fused[kk], 6),
             "channels": {ch: round(per_channel[ch][kk], 1)
                          for ch in per_channel}}
        if mode:
            e[mode] = True
        if kk in rr_scores:
            e["rerank"] = rr_scores[kk]
        entries.append(e)
    out["entries"] = entries
    # the cascade, as numbers: where candidates died is a lookup, not
    # guesswork (gathered = SQL filter; reranked = judge window, 0 = judge
    # skipped/failed; pool = post-dedup MMR pool; chosen = post-MMR/k;
    # packed = post-budget).
    out["funnel"] = {"gathered": len(cands),
                     "reranked": len(window) if out.get("reranked") else 0,
                     "deduped": deduped, "pool": len(pool),
                     "chosen": len(chosen), "packed": len(final),
                     "dropped": dropped}
    if not _previewing(env) and corpus.get("track", True):
        _log_uses(conn, task, corpus_name, final)
    return out


def _previewing(env):
    return bool(getattr(env, "preview", False))


def _bounded(env, configured):
    """Cap a provider-side model/network timeout by the step's remaining
    assembly budget (env.provider_deadline, set by the engine while
    providers run). Floor of 1s; no deadline -> the configured value."""
    deadline = getattr(env, "provider_deadline", None)
    if deadline is None:
        return int(configured)
    return max(1, min(int(configured), int(deadline - time.monotonic())))


def _task_row(conn, task):
    """The tasks row behind this task dict, or None for PREVIEW tasks
    (llm show, ad-hoc calls) — previews never log to the utility ledger,
    never trigger model calls, never pin runs rows."""
    task_id = task.get("id")
    if task_id is None:
        return None
    return conn.execute("SELECT id, kind FROM tasks WHERE id=?",
                        (task_id,)).fetchone()


def _log_uses(conn, task, corpus_name, keys):
    """Record what this task was SHOWN (the utility ledger)."""
    row = _task_row(conn, task) if keys else None
    if row is None:
        return
    kind = task.get("kind") or row["kind"]
    with tx(conn):
        for kk in keys:
            conn.execute(
                "INSERT OR IGNORE INTO context_uses(task_id, kind, corpus, key)"
                " VALUES (?,?,?,?)", (task["id"], kind, corpus_name, kk))


_RERANK_PROMPT = (
    "You are ranking knowledge-base entries by how USEFUL they are for the "
    "task below. Score every entry key from 0 (useless) to 10 (essential). "
    "Judge usefulness for THIS task, not general quality.")

_RERANK_SCHEMA = {"type": "object", "required": ["scores"],
                  "properties": {"scores": {"type": "object"}},
                  "additionalProperties": False}


def _rerank_scores(env, task, rr, window, by_key, summaries):
    """One bounded call to the rerank binding (typically a local model):
    the task payload + the window's entries in, integer scores out. Runs
    through run_agent, so it is pinned, archived, and visible in
    `llm runs` like any agent call. Raises on any failure — the caller
    falls back to the fused order."""
    from . import runner
    binding = env.pack.agents[rr["llm"]]
    entries = [{"key": kk,
                "text": (summaries.get(kk) or by_key[kk]["text"])[:400]}
               for kk in window]
    verdict = runner.run_agent(
        env.conn, task, binding, _RERANK_PROMPT, _RERANK_SCHEMA,
        data_dir=env.data_dir, pack_rev=env.pack.rev,
        timeout_s=_bounded(env, rr.get("timeout_s", 60)),
        context_slice={"task": task.get("payload") or {},
                       "entries": entries})
    scores = {}
    for kk, v in (verdict.get("scores") or {}).items():
        if kk in by_key and isinstance(v, (int, float)) \
                and not isinstance(v, bool):
            scores[kk] = max(0, min(10, int(v)))
    return scores


_SUMMARIZE_PROMPT = (
    "Condense the RECORD below. Preserve identifiers, error strings, "
    "numbers, paths, and decisions verbatim where possible; drop "
    "boilerplate. Stay under the character limit given in the context.")

_SUMMARIZE_SCHEMA = {"type": "object", "required": ["summary"],
                     "properties": {"summary": {"type": "string"}},
                     "additionalProperties": False}


def _gen_summary(env, task, corpus_name, sum_binding, key, text, max_chars):
    """Summarize one long row via the corpus's binding and cache it by
    text_sha. Returns the summary, or None on any failure (the caller
    truncates instead — reduced yield, never a failed step)."""
    from . import runner
    from .util import sha256_text
    try:
        verdict = runner.run_agent(
            env.conn, task, env.pack.agents[sum_binding],
            _SUMMARIZE_PROMPT, _SUMMARIZE_SCHEMA,
            data_dir=env.data_dir, pack_rev=env.pack.rev,
            timeout_s=_bounded(env, 60),
            context_slice={"record": text, "limit_chars": max_chars})
        summary = verdict.get("summary")
        if not isinstance(summary, str) or not summary.strip():
            return None
    except Exception as e:
        print("select: summarize via '%s' failed for %s/%s (%s) — "
              "truncating" % (sum_binding, corpus_name, key, e),
              file=sys.stderr)
        return None
    with tx(env.conn):
        env.conn.execute(
            "INSERT OR REPLACE INTO corpus_summaries"
            " (corpus, key, binding, text_sha, summary) VALUES (?,?,?,?,?)",
            (corpus_name, key, sum_binding, sha256_text(text), summary))
    return summary


def _utility_ranks(conn, task, corpus_name, keys):
    """Outcome-learned usefulness: rows previously shown to SAME-KIND tasks
    that reached done earn rank; co-occurrence with failed loses it.
    Laplace-smoothed ((done+1)/(done+failed+2)); rows with no history sit
    at the neutral 0.5, so cold rows are neither punished nor promoted.
    Abstains until history actually differentiates. Auto-labelled from the
    engine's own ledger — no annotation, ever."""
    task_id = task.get("id")
    row = conn.execute("SELECT kind FROM tasks WHERE id=?",
                       (task_id,)).fetchone() if task_id is not None else None
    kind = task.get("kind") or (row["kind"] if row else None)
    if kind is None:
        return None
    hist = {}
    for r in conn.execute(
            "SELECT u.key,"
            " sum(CASE WHEN t.state='done' THEN 1 ELSE 0 END) d,"
            " sum(CASE WHEN t.state='failed' THEN 1 ELSE 0 END) f"
            " FROM context_uses u JOIN tasks t ON t.id = u.task_id"
            " WHERE u.corpus=? AND u.kind=? AND u.task_id != ?"
            "   AND t.state IN ('done','failed')"
            " GROUP BY u.key", (corpus_name, kind, task_id or -1)):
        hist[r["key"]] = (r["d"], r["f"])
    scored = {}
    for kk in keys:
        d, f = hist.get(kk, (0, 0))
        scored[kk] = (d + 1.0) / (d + f + 2.0)
    return _rank_desc(scored, keys)


def _comparable(vals):
    """Coerce a {key: ts} mapping to uniformly comparable values: all
    numeric if every value parses as a number, else all strings (ISO
    timestamps compare correctly lexicographically)."""
    try:
        return {kk: float(v) for kk, v in vals.items()}
    except (TypeError, ValueError):
        return {kk: str(v) for kk, v in vals.items()}


def _corpus_vectors(env, corpus_name, corpus, cands, queries):
    """Ensure every candidate row has a vector for the corpus's embedder;
    embed only rows that are new or whose text changed (text_sha pin).
    Returns ([query_vector, ...], {key: vector})."""
    from . import localmodel
    from .util import canonical_json, sha256_text
    conn = env.conn
    model_name = corpus["embed_with"]
    if model_name == "hashing":
        dim = localmodel.HASHING_DEFAULT_DIM
        model_sha = localmodel.hashing_model_sha(dim)
        embed_fn = lambda t: localmodel.hash_embed(t, dim)  # noqa: E731
    else:
        mspec = env.pack.models[model_name]
        if "hashing" in mspec:
            dim = mspec["hashing"]["dim"]
            model_sha = localmodel.hashing_model_sha(dim)
            embed_fn = lambda t: localmodel.hash_embed(t, dim)  # noqa: E731
        elif "path" in mspec:
            weights, model_sha = localmodel.load_model(
                mspec["path"], expected_sha=mspec["sha256"])
            embed_fn = lambda t: localmodel.embed(t, weights)  # noqa: E731
        else:
            from . import runner
            model_sha = sha256_text(canonical_json(
                {"base_url": mspec["base_url"], "model": mspec["model"]}))
            out_dir = Path(env.data_dir) / "corpus-embed" / corpus_name
            embed_fn = lambda t: runner.embed_api(  # noqa: E731
                mspec, t, timeout_s=_bounded(env, 60), out_dir=out_dir)
    existing = {}
    keys = [c["key"] for c in cands]
    if keys:
        ph = ",".join("?" * len(keys))
        for r in conn.execute(
                "SELECT key, text_sha, vector FROM corpus_embeddings"
                " WHERE corpus=? AND model_sha=? AND key IN (%s)" % ph,
                [corpus_name, model_sha] + keys):
            existing[r["key"]] = (r["text_sha"], json.loads(r["vector"]))
    vectors = {}
    stale = []
    for c in cands:
        tsha = sha256_text(c["text"])
        got = existing.get(c["key"])
        if got is not None and got[0] == tsha:
            vectors[c["key"]] = got[1]
        else:
            stale.append((c, tsha))
    if stale:
        with tx(conn):
            for c, tsha in stale:
                vec = embed_fn(c["text"])
                conn.execute(
                    "INSERT OR REPLACE INTO corpus_embeddings"
                    " (corpus, key, model_sha, text_sha, dim, vector)"
                    " VALUES (?,?,?,?,?,?)",
                    (corpus_name, c["key"], model_sha, tsha, len(vec),
                     json.dumps(vec)))
                vectors[c["key"]] = vec
    return [embed_fn(q) for q in queries], vectors


def _check_select_spec(spec, pack):
    corpora = getattr(pack, "corpora", None) or {}
    if not spec.get("corpus") or spec["corpus"] not in corpora:
        return ("select needs 'corpus' naming a pack corpora entry "
                "(defined: %s)" % (sorted(corpora) or "none"))
    q = spec.get("query")
    if isinstance(q, list):
        if not q or not all(isinstance(x, str) and x for x in q):
            return "select 'query' list must hold non-empty strings"
    elif not q or not isinstance(q, str):
        return "select needs a string 'query' (or a list of them)"
    for f in ("k", "max_chars", "include_all_under", "max_bytes"):
        v = spec.get(f)
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)
                              or v < 1):
            return "select '%s' must be a positive integer" % f
    dd = spec.get("dedup")
    if dd is not None and not isinstance(dd, bool):
        return "select 'dedup' must be a boolean"
    dv = spec.get("diversify")
    if dv is not None and (isinstance(dv, bool)
                           or not isinstance(dv, (int, float))
                           or not (0 <= dv <= 1)):
        return "select 'diversify' must be a number in 0..1"
    rr = spec.get("rerank")
    if rr is not None:
        if not isinstance(rr, dict):
            return "select 'rerank' must be a mapping {llm, window?, timeout_s?}"
        unknown = set(rr) - {"llm", "window", "timeout_s"}
        if unknown:
            return "select rerank: unknown keys %s" % sorted(unknown)
        agents = getattr(pack, "agents", None) or {}
        if not rr.get("llm") or rr["llm"] not in agents:
            return ("select rerank needs 'llm' naming an agents: role "
                    "(defined: %s)" % (sorted(agents) or "none"))
        for f in ("window", "timeout_s"):
            v = rr.get(f)
            if v is not None and (isinstance(v, bool) or not isinstance(v, int)
                                  or v < 1):
                return "select rerank '%s' must be a positive integer" % f
    for f in ("filter", "boost"):
        v = spec.get(f)
        if v is not None and (not isinstance(v, dict) or not all(
                isinstance(kk, str) for kk in v)):
            return "select '%s' must be a mapping of column -> value" % f
    w = spec.get("weights")
    if w is not None:
        if not isinstance(w, dict):
            return "select 'weights' must be a mapping"
        bad = set(w) - set(SELECT_CHANNELS)
        if bad:
            return ("select weights: unknown channels %s (known: %s)"
                    % (sorted(bad), list(SELECT_CHANNELS)))
        for ch, val in w.items():
            if isinstance(val, bool) or not isinstance(val, (int, float)) \
                    or val < 0:
                return "select weights.%s must be a number >= 0" % ch
    return None


_ctx_select.check_spec = _check_select_spec
