"""Step runner enforcing the total execution contract (EXECUTION.md).

The runner guarantees:
- bounded: every step has a timeout and a visit cap; the whole walk is
  bounded by the sum of visit caps — no dispatch graph can loop forever;
- total: a step's block declares a closed outcome set and the workflow maps
  EVERY outcome — checked by validate() at load, so an unmapped outcome is
  a startup error, never a runtime surprise;
- persisted: each completed step writes its task_steps row (plus anything
  the block staged) in ONE transaction before the next step starts;
- terminal: every task provably reaches done | failed | parked | deferred.

An LLM step (when one exists) is just a block whose outcome set includes
its failure classes. The runner treats it exactly like a build step that
can go red. Nothing special, nothing trusted.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

from . import db as dbmod
from . import queue
from .util import canonical_json, sha256_text, tx

TERMINAL_TASK_STATES = ("done", "failed", "parked", "deferred")

DEFAULT_MAX_VISITS = 3


class WorkflowError(SystemExit):
    """Startup validation failure — refuse to run, with a readable message."""


# ------------------------------------------------------------ definitions

@dataclass(frozen=True)
class Step:
    name: str
    block: "object"              # blocks.Block: fn, outcomes, exec_class, ...
    timeout_s: int
    params: dict = field(default_factory=dict)
    context: tuple = ()          # ((provider_name, spec_dict), ...)
    max_visits: int = DEFAULT_MAX_VISITS
    resumable: bool = False      # a new ATTEMPT may reuse the last result
    llm: str = None              # pack agent binding name (llm blocks only)
    schema: str = None           # verdict schema name (llm blocks only)
    outcomes: frozenset = None   # effective set; block.outcomes unless an
                                 # llm step extends it with schema enums
    lane: str = None             # concurrency lane (semaphore key); a step runs
                                 # only when its lane has a free slot. default =
                                 # the block's exec_class.


@dataclass
class Workflow:
    kind: str
    steps: list = field(default_factory=list)
    dispatch: dict = field(default_factory=dict)  # (step, outcome) -> target
    consumes: list = field(default_factory=list)
    emits: list = field(default_factory=list)
    _def_hash: str = None                         # def_hash() cache

    def def_hash(self) -> str:
        """Stable fingerprint of the definition: every field that changes
        execution (steps, params, context, timeouts, visit caps, resumable,
        llm/schema bindings, outcome sets, lanes, dispatch, consumes/emits).
        Tasks are stamped with it when an attempt starts executing; a
        mid-flight task never replays under a different hash (execute()
        parks it as definition_changed instead)."""
        if self._def_hash is None:
            doc = {
                "kind": self.kind,
                "steps": [{
                    "name": s.name, "block": s.block.name,
                    "timeout_s": s.timeout_s, "params": s.params,
                    "context": [[c, spec] for c, spec in s.context],
                    "max_visits": s.max_visits, "resumable": bool(s.resumable),
                    "llm": s.llm, "schema": s.schema,
                    "outcomes": sorted(s.outcomes), "lane": s.lane,
                } for s in self.steps],
                "dispatch": sorted([n, o, t] for (n, o), t in self.dispatch.items()),
                "consumes": list(self.consumes),
                "emits": list(self.emits),
            }
            self._def_hash = sha256_text(canonical_json(doc))
        return self._def_hash

    # -- builder API ---------------------------------------------------
    @classmethod
    def define(cls, kind: str) -> "Workflow":
        return cls(kind=kind)

    def step(self, name: str, block, *, timeout_s: int, params=None,
             context=(), max_visits: int = DEFAULT_MAX_VISITS,
             resumable=None, llm=None, schema=None,
             outcomes=None, lane=None) -> "Workflow":
        if any(s.name == name for s in self.steps):
            raise WorkflowError("%s: duplicate step '%s'" % (self.kind, name))
        if resumable is None:
            resumable = getattr(block, "resumable", False)
        eff = frozenset(outcomes) if outcomes else frozenset(block.outcomes)
        if not eff >= frozenset(block.outcomes):
            raise WorkflowError(
                "%s.%s: step outcome set may extend, never shrink, the "
                "block's declared set" % (self.kind, name))
        self.steps.append(Step(name, block, timeout_s, dict(params or {}),
                               tuple(context), max_visits, resumable,
                               llm, schema, eff, lane))
        self._def_hash = None
        return self

    def on(self, step_name: str, outcome: str, target: str) -> "Workflow":
        """target: another step name, or a terminal task state."""
        key = (step_name, outcome)
        if key in self.dispatch and self.dispatch[key] != target:
            raise WorkflowError("%s: conflicting dispatch for %s" % (self.kind, (key,)))
        self.dispatch[key] = target
        self._def_hash = None
        return self

    # -- startup proof ---------------------------------------------------
    def validate(self) -> None:
        """Prove totality before any work is accepted. Violations raise
        WorkflowError with the workflow/step named."""
        w = self.kind
        if not self.steps:
            raise WorkflowError("%s: workflow has no steps" % w)
        names = [s.name for s in self.steps]
        by_name = {s.name: s for s in self.steps}

        for s in self.steps:
            if s.timeout_s is None or s.timeout_s <= 0:
                raise WorkflowError("%s.%s: every step needs timeout_s > 0" % (w, s.name))
            if s.max_visits < 1:
                raise WorkflowError("%s.%s: max_visits must be >= 1" % (w, s.name))
            declared = set(s.outcomes)
            mapped = {o for (n, o) in self.dispatch if n == s.name}
            missing = declared - mapped
            phantom = mapped - declared
            if missing:
                raise WorkflowError(
                    "%s.%s: unmapped outcomes %s — block '%s' can return them, "
                    "the workflow must say where they go"
                    % (w, s.name, sorted(missing), s.block.name))
            if phantom:
                raise WorkflowError(
                    "%s.%s: phantom outcomes %s — block '%s' can never return "
                    "them (declared: %s)"
                    % (w, s.name, sorted(phantom), s.block.name,
                       sorted(declared)))
            for o in mapped:
                target = self.dispatch[(s.name, o)]
                if target not in by_name and target not in TERMINAL_TASK_STATES:
                    raise WorkflowError(
                        "%s.%s: outcome '%s' -> unknown target '%s' "
                        "(not a step, not one of %s)"
                        % (w, s.name, o, target, "|".join(TERMINAL_TASK_STATES)))

        # reachability: every step reachable from the entry step ...
        entry = names[0]
        seen = set()
        frontier = [entry]
        while frontier:
            cur = frontier.pop()
            if cur in seen or cur not in by_name:
                continue
            seen.add(cur)
            for (n, o), target in self.dispatch.items():
                if n == cur and target in by_name:
                    frontier.append(target)
        unreachable = set(names) - seen
        if unreachable:
            raise WorkflowError("%s: unreachable steps %s" % (w, sorted(unreachable)))

        # ... and a terminal state reachable from every step (no trap cycles)
        can_finish = set()
        changed = True
        while changed:
            changed = False
            for s in self.steps:
                if s.name in can_finish:
                    continue
                for o in s.outcomes:
                    target = self.dispatch[(s.name, o)]
                    if target in TERMINAL_TASK_STATES or target in can_finish:
                        can_finish.add(s.name)
                        changed = True
                        break
        stuck = set(names) - can_finish
        if stuck:
            raise WorkflowError(
                "%s: no terminal state reachable from steps %s" % (w, sorted(stuck)))


# ------------------------------------------------------------ context

# Context is declared, never ambient: a provider turns (env, task, spec)
# into the value injected under the provider's name. Content layers extend
# this registry; the engine ships only mechanical providers.
CONTEXT_PROVIDERS = {}


def context_provider(name):
    def wrap(fn):
        if name in CONTEXT_PROVIDERS:
            raise WorkflowError("context provider '%s' registered twice" % name)
        CONTEXT_PROVIDERS[name] = fn
        return fn
    return wrap


@context_provider("payload")
def _ctx_payload(env, task, spec):
    return task["payload"]


@context_provider("pack")
def _ctx_pack(env, task, spec):
    """The pack's params mapping (already path-templated at load)."""
    return env.pack.params if env.pack else {}


@context_provider("notes")
def _ctx_notes(env, task, spec):
    """Declared files injected as context: {basename: content}. Paths were
    pack-templated at load; runtime placeholders resolve against the
    payload. A missing file is a loud failure, never silently skipped —
    the step's prompt_sha pins exactly what was injected."""
    from .util import template as _template
    out = {}
    for f in spec.get("files", ()):
        f = _template(f, {"payload": task.get("payload") or {}})
        p = Path(f)
        if not p.is_file():
            raise RuntimeError("notes context: file %s does not exist" % p)
        out[p.name] = p.read_text(errors="replace")
    return out


def _check_notes_spec(spec, pack):
    files = spec.get("files")
    if not files or not isinstance(files, list):
        return "notes context needs a non-empty 'files' list"
    for f in files:
        if not isinstance(f, str):
            return "notes files must be strings, got %r" % (f,)
        if "{" not in f and not Path(f).is_file():
            return "notes file %s does not exist (fail-loud at load)" % f
    return None


_ctx_notes.check_spec = _check_notes_spec


@context_provider("retrieval")
def _ctx_retrieval(env, task, spec):
    """k-nearest stored code objects by embedding similarity — the lesser
    model shaping what the agent sees, never deciding anything. The query
    (templated from the payload) is embedded with the named pack model;
    candidates come from the embeddings table rows produced by the SAME
    model (model_sha match); ties break by object id, so identical db
    state always yields the identical context slice."""
    from . import localmodel, runner
    from .util import canonical_json, sha256_text
    from .util import template as _template
    mspec = env.pack.models[spec["model"]]
    query = _template(spec["query"], {"payload": task.get("payload") or {}})
    k = int(spec.get("k", 5))
    if "base_url" in mspec:
        out_dir = (Path(env.data_dir) / "tasks" / str(task["id"])
                   / "retrieval")
        vec = runner.embed_api(mspec, query, timeout_s=60, out_dir=out_dir)
        model_sha = sha256_text(canonical_json(
            {"base_url": mspec["base_url"], "model": mspec["model"]}))
    else:
        weights, model_sha = localmodel.load_model(
            mspec["path"], expected_sha=mspec["sha256"])
        vec = localmodel.embed(query, weights)
    scored = []
    for r in env.conn.execute(
            "SELECT e.object_id, e.vector, co.repo, co.path, co.symbol"
            " FROM embeddings e JOIN code_objects co ON co.id = e.object_id"
            " WHERE e.model_sha=?", (model_sha,)):
        score = localmodel.cosine(vec, json.loads(r["vector"]))
        scored.append((-score, r["object_id"], r))
    scored.sort(key=lambda t: (t[0], t[1]))
    out = []
    for neg_score, obj_id, r in scored[:k]:
        entry = {"repo": r["repo"], "path": r["path"], "symbol": r["symbol"],
                 "score": round(-neg_score, 6)}
        if spec.get("from", "readings") == "readings":
            reading = env.conn.execute(
                "SELECT summary FROM readings WHERE object_id=?"
                " ORDER BY id DESC LIMIT 1", (obj_id,)).fetchone()
            if reading:
                entry["summary"] = reading["summary"]
        out.append(entry)
    return out


def _check_retrieval_spec(spec, pack):
    if not spec.get("model"):
        return "retrieval context needs 'model' (a pack models entry)"
    models = getattr(pack, "models", None) or {}
    if spec["model"] not in models:
        return ("retrieval model '%s' not in pack models section (defined: %s)"
                % (spec["model"], sorted(models) or "none"))
    if not spec.get("query") or not isinstance(spec["query"], str):
        return "retrieval context needs a string 'query'"
    k = spec.get("k", 5)
    if not isinstance(k, int) or k < 1:
        return "retrieval 'k' must be a positive integer"
    return None


_ctx_retrieval.check_spec = _check_retrieval_spec


# ---- select: generic ranked selection over any pack-declared corpus ------
#
# Design grounded in production-RAG evidence (docs/LLM.md cites sources):
# - hybrid beats pure-vector -> independent channels (lexical, semantic,
#   recency, prior, boost) fused by Reciprocal Rank Fusion. RRF because
#   calibrating linear score weights needs labeled queries per corpus;
#   rank fusion is the robust untuned default.
# - recency and priors must score the FULL filtered pool, never re-rank a
#   narrow top-K cosine cut — at this engine's scale (thousands to low
#   hundreds of thousands of rows) brute force is the correct architecture.
# - embeddings are OPTIONAL: the lexical channel plus the zero-setup
#   hashing embedder give a strong deterministic core; real embedding
#   models plug in per corpus when wanted.

SELECT_CHANNELS = ("lexical", "semantic", "recency", "prior", "boost")
_RRF_K = 60

# Default channel weights: RELEVANCE channels (query/task-conditioned) vote
# at full strength; PRIORS (recency, importance) modulate at 0.3 — enough
# to decide among relevance ties (their job), not enough for a fresh-or-
# important-but-irrelevant row to outvote an actual match (a failure mode
# the recall evaluation demonstrated at equal weights). Override per step
# via weights:.
SELECT_WEIGHTS = {"lexical": 1.0, "semantic": 1.0, "boost": 1.0,
                  "recency": 0.3, "prior": 0.3}


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
    query = _template(spec["query"], payload_map)
    k = int(spec.get("k", 5))
    max_chars = int(spec.get("max_chars", 2000))

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

    def entry(c, score=None, channels=None):
        e = {"key": c["key"], "text": c["text"][:max_chars]}
        if len(c["text"]) > max_chars:
            e["truncated"] = True         # never a silent cut
        if score is not None:
            e["score"] = round(score, 6)
            e["channels"] = channels
        return e

    out = {"corpus": corpus_name, "query": query, "considered": len(cands)}

    # ---- small corpus: don't rank, include everything (declared opt-in)
    all_under = spec.get("include_all_under")
    if all_under and sum(len(c["text"].encode("utf-8", "replace"))
                         for c in cands) <= int(all_under):
        out["included_all"] = True
        out["entries"] = [entry(c) for c in cands]
        return out
    out["included_all"] = False

    # ---- channels -> rankings (each may abstain when signal-free)
    ranks = {}
    qtok = set(localmodel.split_identifiers(query))
    ranks["lexical"] = _rank_desc(
        {c["key"]: float(len(qtok & set(localmodel.split_identifiers(c["text"]))))
         for c in cands}, keys)

    if corpus.get("embed_with"):
        qvec, vectors = _corpus_vectors(env, corpus_name, corpus, cands, query)
        ranks["semantic"] = _rank_desc(
            {kk: localmodel.cosine(qvec, vec) for kk, vec in vectors.items()},
            keys)
    else:
        ranks["semantic"] = None

    if corpus.get("ts"):
        norm = _comparable({c["key"]: c["ts"] for c in cands
                            if c["ts"] is not None})
        ranks["recency"] = _rank_desc(norm, keys) if norm else None
    else:
        ranks["recency"] = None

    if corpus.get("weight"):
        vals = {c["key"]: c["weight"] for c in cands
                if isinstance(c["weight"], (int, float))
                and not isinstance(c["weight"], bool)}
        ranks["prior"] = _rank_desc({kk: float(v) for kk, v in vals.items()},
                                    keys) if vals else None
    else:
        ranks["prior"] = None

    if boost_spec:
        want = [_template(boost_spec[col], payload_map)
                for col in sorted(boost_spec)]
        ranks["boost"] = _rank_desc(
            {c["key"]: float(sum(1 for got, w in zip(c["boost"], want)
                                 if got is not None and str(got) == str(w)))
             for c in cands}, keys)
    else:
        ranks["boost"] = None

    # ---- Reciprocal Rank Fusion over the channels that voted
    weights = dict(SELECT_WEIGHTS)
    weights.update(spec.get("weights") or {})
    fused = {}
    for kk in keys:
        s = 0.0
        for ch in SELECT_CHANNELS:
            if ranks[ch] is not None and weights[ch] > 0:
                s += weights[ch] / (_RRF_K + ranks[ch][kk])
        fused[kk] = s
    top = sorted(keys, key=lambda kk: (-fused[kk], kk))[:k]
    out["entries"] = [
        entry(by_key[kk], fused[kk],
              {ch: round(ranks[ch][kk], 1) for ch in SELECT_CHANNELS
               if ranks[ch] is not None})
        for kk in top]
    return out


def _comparable(vals):
    """Coerce a {key: ts} mapping to uniformly comparable values: all
    numeric if every value parses as a number, else all strings (ISO
    timestamps compare correctly lexicographically)."""
    try:
        return {kk: float(v) for kk, v in vals.items()}
    except (TypeError, ValueError):
        return {kk: str(v) for kk, v in vals.items()}


def _corpus_vectors(env, corpus_name, corpus, cands, query):
    """Ensure every candidate row has a vector for the corpus's embedder;
    embed only rows that are new or whose text changed (text_sha pin).
    Returns (query_vector, {key: vector})."""
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
                mspec, t, timeout_s=60, out_dir=out_dir)
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
    return embed_fn(query), vectors


def _check_select_spec(spec, pack):
    corpora = getattr(pack, "corpora", None) or {}
    if not spec.get("corpus") or spec["corpus"] not in corpora:
        return ("select needs 'corpus' naming a pack corpora entry "
                "(defined: %s)" % (sorted(corpora) or "none"))
    if not spec.get("query") or not isinstance(spec["query"], str):
        return "select needs a string 'query'"
    for f in ("k", "max_chars", "include_all_under"):
        v = spec.get(f)
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)
                              or v < 1):
            return "select '%s' must be a positive integer" % f
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


@dataclass
class ExecEnv:
    conn: "object"
    subscriptions: dict = field(default_factory=dict)
    data_dir: Path = Path("data")
    workspaces_dir: Path = Path("workspaces")
    pack: "object" = None
    lanes: dict = None           # lane name -> BoundedSemaphore (parallel daemon);
                                 # None = no throttling (serial driver / tests).
    policy: dict = None          # effective retry policy (queue.build_policy with
                                 # the pack's retry: overrides); None/{} = defaults.


# ------------------------------------------------------------ execution

def execute(env: ExecEnv, workflow: Workflow, task: dict) -> str:
    """Run a claimed task through its workflow; returns the resulting task
    state. Crash-resume: task_steps rows for this attempt replay without
    re-running; loop-backs invalidate stale forward history first."""
    conn = env.conn
    task_id, attempt = task["id"], task["attempts"]
    by_name = {s.name: s for s in workflow.steps}

    # ---- definition versioning gate ------------------------------------
    # Each attempt is stamped with the definition hash when it first
    # executes. Same hash -> resume normally. Different hash while THIS
    # attempt already has recorded steps -> the YAML changed under a
    # mid-flight task: never replay old outcomes through a new dispatch
    # graph — park as definition_changed; unpark/retry starts a FRESH
    # attempt (no recorded steps) which re-stamps and runs from step 0
    # under the new definition. A NULL stamp (task predates versioning,
    # or attempt not started) adopts the current definition.
    current_hash = workflow.def_hash()
    if task.get("def_hash") != current_hash:
        started = conn.execute(
            "SELECT 1 FROM task_steps WHERE task_id=? AND attempt=? LIMIT 1",
            (task_id, attempt)).fetchone()
        if task.get("def_hash") and started:
            print("contract: task %s [%s] definition_changed: workflow '%s' "
                  "changed under a mid-flight task (stamped %.12s..., now "
                  "%.12s...) — parked; unpark/retry re-runs under the new "
                  "definition" % (task_id, task["kind"], workflow.kind,
                                  task["def_hash"], current_hash),
                  file=sys.stderr)
            queue.park(conn, task_id, reason="definition_changed")
            return "parked"
        conn.execute("UPDATE tasks SET def_hash=?, updated_at=datetime('now')"
                     " WHERE id=?", (current_hash, task_id))
        task["def_hash"] = current_hash

    rows = _load_recorded(conn, task_id, attempt, workflow)
    replayed = set()
    visits = {}
    current = workflow.steps[0].name
    prev = {}

    while True:
        step = by_name[current]
        visits[current] = visits.get(current, 0) + 1
        if visits[current] > step.max_visits:
            return _fail_loud(env, task, "step_budget_exhausted",
                              "step '%s' exceeded max_visits=%d"
                              % (current, step.max_visits))

        if current in rows and current not in replayed:
            # resume: this step already completed for this attempt
            outcome, result = rows[current]["outcome"], rows[current]["result"]
            replayed.add(current)
        else:
            # revisit (retry edge): the step's own committed row is replaced
            # by the re-execution; the rest of the path stays authoritative —
            # a later crash replays recorded outcomes wherever the walk
            # reaches them, which is deterministic and lands on this frontier.
            revisit = current in rows
            try:
                outcome, result, wall_ms = _run_block(env, step, task, prev)
            except subprocess.TimeoutExpired:
                if "timeout" in step.outcomes:
                    outcome, result, wall_ms = "timeout", {"timeout_s": step.timeout_s}, step.timeout_s * 1000
                else:
                    return _fail_loud(env, task, "framework_bug",
                                      "step '%s' timed out but block '%s' declares no "
                                      "'timeout' outcome" % (current, step.block.name))
            except Exception:
                return _fail_loud(env, task, "framework_bug",
                                  "uncaught exception in step '%s' (block '%s'):\n%s"
                                  % (current, step.block.name, traceback.format_exc()))
            if outcome not in step.outcomes:
                return _fail_loud(env, task, "framework_bug",
                                  "step '%s': block '%s' returned undeclared outcome "
                                  "%r (declared: %s)"
                                  % (current, step.block.name, outcome,
                                     sorted(step.block.outcomes)))

            target = workflow.dispatch.get((current, outcome))
            # persist the boundary: staged rows + step row + dispatch effect,
            # one transaction — only after COMMIT does anything else happen.
            with tx(conn, immediate=True):
                if revisit:
                    conn.execute(
                        "DELETE FROM task_steps WHERE task_id=? AND attempt=?"
                        " AND step=?", (task_id, attempt, current))
                staged = result.pop("_staged", None)
                if staged:
                    result.update(_apply_staged(env, staged, task))
                cur = conn.execute(
                    "INSERT INTO task_steps(task_id, attempt, step, outcome,"
                    " result, wall_ms) VALUES (?,?,?,?,?,?)",
                    (task_id, attempt, current, outcome,
                     json.dumps(result, sort_keys=True), wall_ms))
                rows[current] = {"outcome": outcome, "result": result,
                                 "rowid": cur.lastrowid}
                replayed.add(current)
                if target in TERMINAL_TASK_STATES:
                    return _apply_terminal(env, task, target, outcome)

        target = workflow.dispatch.get((current, outcome))
        if target is None:
            return _fail_loud(env, task, "framework_bug",
                              "no dispatch for (%s, %s) — recorded outcome from "
                              "an older definition?" % (current, outcome))
        if target in TERMINAL_TASK_STATES:
            # reached via replay (the terminal effect already committed with
            # the row, but the task is 'running' again — re-apply, idempotent)
            return _apply_terminal(env, task, target, outcome)
        prev = result
        current = target


def _load_recorded(conn, task_id, attempt, workflow):
    rows = {}
    for r in conn.execute(
            "SELECT rowid, step, outcome, result FROM task_steps"
            " WHERE task_id=? AND attempt=? ORDER BY rowid", (task_id, attempt)):
        rows[r["step"]] = {"outcome": r["outcome"],
                           "result": json.loads(r["result"] or "{}"),
                           "rowid": r["rowid"]}
    # resumable steps may carry their result across ATTEMPTS (e.g. an intact
    # worktree); non-resumable steps re-run on a new attempt by design.
    for s in workflow.steps:
        if s.resumable and s.name not in rows and attempt > 0:
            r = conn.execute(
                "SELECT outcome, result FROM task_steps WHERE task_id=? AND"
                " attempt<? AND step=? ORDER BY attempt DESC, rowid DESC LIMIT 1",
                (task_id, attempt, s.name)).fetchone()
            if r:
                rows[s.name] = {"outcome": r["outcome"],
                                "result": json.loads(r["result"] or "{}"),
                                "rowid": -1}
    return rows


def _run_block(env, step, task, prev):
    ctx = dict(step.params)
    for provider_name, spec in step.context:
        provider = CONTEXT_PROVIDERS.get(provider_name)
        if provider is None:
            raise RuntimeError("unknown context provider '%s'" % provider_name)
        ctx[provider_name] = provider(env, task, spec)
    step_dir = (Path(env.data_dir) / "tasks" / str(task["id"])
                / ("a%d" % task["attempts"]) / step.name)
    # the engine guarantees the step dir exists before the block runs, so a
    # block can write straight to _step_dir without defending itself.
    step_dir.mkdir(parents=True, exist_ok=True)
    ctx["_timeout_s"] = step.timeout_s
    ctx["_step_dir"] = str(step_dir)
    ctx["_workspaces_dir"] = str(env.workspaces_dir)
    ctx["_tools"] = dict(env.pack.tools) if env.pack else {}
    ctx["_data_dir"] = str(env.data_dir)
    ctx["_conn"] = env.conn      # for runner-backed blocks (runs row pinning)
    ctx["_pack"] = env.pack
    ctx["_step"] = step
    # concurrency lane: under the parallel daemon, hold the lane's semaphore for
    # the block's duration so a capped lane (e.g. build=1) serializes across
    # workers. No-op for the serial driver (env.lanes is None). The block runs
    # OUTSIDE any db transaction, so holding a lane never blocks other workers'
    # commits.
    lane = step.lane or getattr(step.block, "exec_class", None)
    sem = (env.lanes or {}).get(lane)
    started = time.monotonic()
    if sem is not None:
        with sem:
            outcome, result = step.block.fn(ctx, task, prev)
    else:
        outcome, result = step.block.fn(ctx, task, prev)
    wall_ms = int((time.monotonic() - started) * 1000)
    if wall_ms > step.timeout_s * 1000:
        print("contract: step '%s' exceeded its budget (%dms > %ds)"
              % (step.name, wall_ms, step.timeout_s), file=sys.stderr)
    if not isinstance(result, dict):
        raise RuntimeError("block '%s' returned non-dict result %r"
                           % (step.block.name, type(result)))
    return outcome, result, wall_ms


def _apply_staged(env, ops, task):
    """Apply block-staged db effects inside the boundary transaction.
    Returns ids to merge into the persisted step result."""
    out = {}
    for op in ops:
        kind = op.get("op")
        if kind == "fanout":
            out["join_group"] = queue.apply_fanout(
                env.conn, op, task, env.subscriptions)
        elif kind == "upsert_item":
            out["item_id"] = dbmod.upsert_item(
                env.conn, op["key"], op["title"], op["source"], op["repo"],
                detail=op.get("detail"), severity=op.get("severity"),
                pattern=op.get("pattern"), base_sha=op.get("base_sha"))
        elif kind == "transition":
            out["transition_id"] = dbmod.record_transition(
                env.conn, op["item_id"], op["to_state"], op["event"],
                evidence=op.get("evidence"), run_id=op.get("run_id"),
                subscriptions=env.subscriptions)
        elif kind == "emit_event":
            out["event_id"] = dbmod.emit_event(
                env.conn, op["name"], op["payload"], env.subscriptions)
        elif kind == "store_embedding":
            row = env.conn.execute(
                "SELECT id FROM code_objects WHERE repo=? AND path=?"
                " AND symbol IS ?", (op["repo"], op["path"], op["symbol"])).fetchone()
            if row:
                obj_id = row["id"]
            else:
                obj_id = env.conn.execute(
                    "INSERT INTO code_objects(repo, path, symbol, kind,"
                    " first_seen_sha, last_seen_sha) VALUES (?,?,?,?,?,?)",
                    (op["repo"], op["path"], op["symbol"],
                     "function" if op["symbol"] else "file",
                     op["sha"], op["sha"])).lastrowid
            env.conn.execute(
                "INSERT OR REPLACE INTO embeddings(object_id, model_sha, dim,"
                " vector) VALUES (?,?,?,?)",
                (obj_id, op["model_sha"], op["dim"], json.dumps(op["vector"])))
            out["object_id"] = obj_id
        else:
            raise RuntimeError("unknown staged op %r" % kind)
    return out


def _apply_terminal(env, task, target, outcome) -> str:
    if target == "done":
        queue.complete(env.conn, task["id"], subscriptions=env.subscriptions)
        return "done"
    if target == "deferred":
        queue.defer(env.conn, task["id"], subscriptions=env.subscriptions)
        return "deferred"
    if target == "parked":
        queue.park(env.conn, task["id"], reason=outcome)
        return "parked"
    # 'failed': if the outcome names a policy class (engine table or a pack's
    # retry: section), that class decides (retry_wait / park / consume);
    # otherwise it is a plain terminal failure the workflow author chose.
    pol = env.policy or queue.POLICY
    if outcome in pol:
        return queue.fail(env.conn, task["id"], outcome, policy=pol,
                          subscriptions=env.subscriptions)
    queue._set_state(env.conn, task["id"], "failed", error_class=outcome,
                     subscriptions=env.subscriptions)
    return "failed"


def _fail_loud(env, task, error_class, detail) -> str:
    print("contract: task %s [%s] %s: %s"
          % (task["id"], task["kind"], error_class, detail), file=sys.stderr)
    return queue.fail(env.conn, task["id"], error_class, detail=detail,
                      policy=env.policy, subscriptions=env.subscriptions)
