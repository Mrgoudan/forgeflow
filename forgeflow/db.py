"""SQLite state store: items state machine, task queue, audit log, run pins.

Single-writer (the daemon). WAL mode. No ORM — the schema IS the design.

Two choke points live here:
- record_transition() is the ONLY way item state changes. It enforces
  ITEM_STATES, appends to the audit log, and fans the transition event
  out to subscribed workflows — all inside the caller's transaction, so
  workflow interaction is atomic.
- emit_event() is the ONLY way any event (item or otherwise) reaches
  the queue. Idempotency lives in queue.enqueue's payload-hash unique key.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from . import queue
from .util import ensure_tx

# The item lifecycle. Transitions happen ONLY through record_transition(),
# which enforces this map and appends to the audit log.
ITEM_STATES = {
    "found":     {"triaged", "rejected"},
    "triaged":   {"fixing", "deferred"},
    "fixing":    {"verifying", "deferred", "failed"},
    "verifying": {"pr_open", "fixing", "deferred", "failed"},
    "pr_open":   {"in_review", "merged", "failed"},
    "in_review": {"fixing", "merged", "deferred"},  # fixing = review requested changes
    "merged":    set(),
    "deferred":  {"triaged"},   # human can un-defer
    "rejected":  set(),
    "failed":    {"triaged"},   # human can requeue
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id            INTEGER PRIMARY KEY,
    key           TEXT UNIQUE NOT NULL,  -- stable dedup key
    source        TEXT NOT NULL,         -- which workflow/human/import produced it
    pattern       TEXT,                  -- generalized root-cause pattern id
    title         TEXT NOT NULL,
    detail        TEXT,                  -- evidence: repro path, observed output
    state         TEXT NOT NULL DEFAULT 'found',
    severity      TEXT,
    repo          TEXT NOT NULL,
    base_sha      TEXT,
    branch        TEXT,                  -- fix branch name once fixing
    pr_number     INTEGER,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS transitions (           -- append-only audit log
    id            INTEGER PRIMARY KEY,
    item_id    INTEGER NOT NULL REFERENCES items(id),
    from_state    TEXT NOT NULL,
    to_state      TEXT NOT NULL,
    event         TEXT NOT NULL,         -- machine event, e.g. 'evidence:build_green'
    evidence      TEXT,                  -- JSON: exit codes, probe diff, shas
    run_id        INTEGER REFERENCES runs(id),
    at            TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS events (                -- append-only event log
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,         -- 'item.triaged', 'pr.opened', ...
    payload       TEXT NOT NULL,         -- canonical JSON
    at            TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tasks (
    id            INTEGER PRIMARY KEY,
    kind          TEXT NOT NULL,         -- workflow kind that handles this task
    item_id    INTEGER REFERENCES items(id),
    payload       TEXT NOT NULL,         -- JSON task input
    payload_hash  TEXT NOT NULL,         -- sha256(canonical_json(payload))
    state         TEXT NOT NULL DEFAULT 'pending',
                  -- pending | running | retry_wait | parked
                  -- | done | failed | deferred        (last three terminal)
    attempts      INTEGER NOT NULL DEFAULT 0,
    error_class   TEXT,                  -- see queue.POLICY keys
    park_reason   TEXT,                  -- set while state='parked'
    next_attempt  TEXT,                  -- ISO time; NULL = eligible now
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Enqueue idempotency: a replayed event cannot double-enqueue.
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idem ON tasks(kind, payload_hash);

CREATE TABLE IF NOT EXISTS task_steps (            -- step-boundary persistence
    task_id    INTEGER NOT NULL REFERENCES tasks(id),
    attempt    INTEGER NOT NULL,
    step       TEXT NOT NULL,
    outcome    TEXT NOT NULL,
    result     TEXT,                 -- JSON, small; big artifacts go to data/
    wall_ms    INTEGER,
    at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (task_id, attempt, step)
);

CREATE TABLE IF NOT EXISTS runs (                  -- one row per agent invocation
    id            INTEGER PRIMARY KEY,
    task_id       INTEGER NOT NULL REFERENCES tasks(id),
    model         TEXT NOT NULL,         -- pinned model id
    prompt_sha    TEXT NOT NULL,         -- sha256 of the assembled prompt
    pack_rev      TEXT NOT NULL,         -- git rev of the pack
    vault_rev     TEXT,                  -- git rev of the method vault
    probe_rev     TEXT,                  -- git rev of the probe set
    base_sha      TEXT,
    build_id      TEXT,                  -- toolchain build fingerprint
    exit_code     INTEGER,
    verdict       TEXT,                  -- schema-validated verdict enum, never prose
    output_path   TEXT,                  -- full raw output, logged not parsed
    started_at    TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at   TEXT
);

CREATE TABLE IF NOT EXISTS egress (                -- everything ever sent out
    id            INTEGER PRIMARY KEY,
    kind          TEXT NOT NULL,         -- comment | pr_create | pr_update | label
    target        TEXT NOT NULL,         -- repo#pr / repo#issue
    body_sha      TEXT NOT NULL,
    body_path     TEXT NOT NULL,         -- archived copy of what was sent
    forge_id      TEXT,                  -- remote-side id once the send succeeds
    task_id       INTEGER REFERENCES tasks(id),
    at            TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_egress_idem ON egress(kind, target, body_sha);

CREATE TABLE IF NOT EXISTS lessons (
    id            INTEGER PRIMARY KEY,
    task_kind     TEXT NOT NULL,         -- which task kinds this lesson applies to
    trigger       TEXT NOT NULL,         -- what situation activates it
    rule          TEXT NOT NULL,         -- the instruction injected into prompts
    provenance    TEXT,                  -- PR/incident it came from
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS watermarks (            -- external cursor state
    scope         TEXT PRIMARY KEY,      -- e.g. 'pr_comments:<repo>'
    cursor        TEXT NOT NULL          -- last processed id, NOT a timestamp
);

-- ---- code comprehension layer -------------------------------------------
-- What the system has looked at and learned. These rows may influence WHAT
-- an agent sees (prompt assembly, targeting); they must NEVER influence what
-- the system decides (that stays with the evidence gate). LLM-derived facts
-- are claims, not evidence — provenance is always recorded.

CREATE TABLE IF NOT EXISTS code_objects (          -- registry of code locations
    id            INTEGER PRIMARY KEY,
    repo          TEXT NOT NULL,
    path          TEXT NOT NULL,         -- file path at repo root
    symbol        TEXT,                  -- function/class; NULL = whole file
    kind          TEXT NOT NULL,         -- function | file | subsystem
    first_seen_sha TEXT NOT NULL,
    last_seen_sha TEXT NOT NULL,         -- bumped when confirmed still present
    UNIQUE(repo, path, symbol)
);

CREATE TABLE IF NOT EXISTS readings (              -- what an agent learned, pinned
    id            INTEGER PRIMARY KEY,
    object_id     INTEGER NOT NULL REFERENCES code_objects(id),
    run_id        INTEGER REFERENCES runs(id),     -- provenance: which run read it
    sha           TEXT NOT NULL,         -- validity pin; object changed => stale
    summary       TEXT NOT NULL,         -- short digest for prompt injection
    facts         TEXT,                  -- JSON claims (invariants, callers, quirks)
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS embeddings (           -- local-model vectors, pinned
    object_id     INTEGER NOT NULL REFERENCES code_objects(id),
    model_sha     TEXT NOT NULL,         -- weights fingerprint; new weights = new rows
    dim           INTEGER NOT NULL,
    vector        TEXT NOT NULL,         -- JSON float array
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (object_id, model_sha)
);

CREATE TABLE IF NOT EXISTS coverage (              -- hunt ledger: where we have looked
    object_id     INTEGER NOT NULL REFERENCES code_objects(id),
    workflow      TEXT NOT NULL,
    sha           TEXT NOT NULL,         -- tree state when swept
    probe_rev     TEXT,                  -- oracle version used
    outcome       TEXT NOT NULL,         -- clean | items:<n>
    swept_at      TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (object_id, workflow, sha)
);

CREATE TABLE IF NOT EXISTS implications (          -- item <-> code mapping
    item_id    INTEGER NOT NULL REFERENCES items(id),
    object_id     INTEGER NOT NULL REFERENCES code_objects(id),
    role          TEXT NOT NULL,         -- root_cause | touched_by_fix | witness
    PRIMARY KEY (item_id, object_id, role)
);

CREATE TABLE IF NOT EXISTS patterns (              -- graduated from items.pattern
    id            TEXT PRIMARY KEY,
    description   TEXT NOT NULL,
    review_lens   TEXT,                  -- text injected into review prompts
    grep_rule     TEXT,                  -- machine-checkable rule: a no-AI finder
    status        TEXT NOT NULL DEFAULT 'active',  -- active | retired
    escapes       INTEGER NOT NULL DEFAULT 0,      -- found later after review missed it
    catches       INTEGER NOT NULL DEFAULT 0       -- caught at review time
);

CREATE TABLE IF NOT EXISTS regions (               -- explore surface map
    id            TEXT PRIMARY KEY,      -- a source subsystem path prefix
    repo          TEXT NOT NULL,
    dry_streak    INTEGER NOT NULL DEFAULT 0,      -- consecutive no-new explores
    cooldown_until_round INTEGER,
    leased_by_task INTEGER REFERENCES tasks(id)    -- disjointness: one explorer per region
);

CREATE TABLE IF NOT EXISTS chains (                -- curated traced call-paths
    id            TEXT PRIMARY KEY,
    repo          TEXT NOT NULL,
    sha           TEXT NOT NULL,         -- validity pin; hops drift with code
    nodes         TEXT NOT NULL,         -- JSON: [{path, line, symbol}, ...]
    hop_invariants TEXT NOT NULL,        -- JSON: per-hop promise + rank
    yields        TEXT,                  -- JSON: item keys this chain produced
    status        TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS methods (               -- the oracle bench
    id            TEXT PRIMARY KEY,
    description   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'candidate', -- candidate | active | exhausted
    trials        INTEGER NOT NULL DEFAULT 0,
    verified_yield INTEGER NOT NULL DEFAULT 0,     -- items that passed the repro gate
    last_used_round INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tasks_claim
    ON tasks(state, next_attempt) WHERE state IN ('pending','retry_wait');
CREATE INDEX IF NOT EXISTS idx_items_state ON items(state);
"""


class TransitionError(ValueError):
    """A transition outside ITEM_STATES. Always a caller bug — loud."""


def connect(path) -> sqlite3.Connection:
    """Open (creating schema if needed). Autocommit mode: multi-statement
    writes must use util.tx()/ensure_tx() — that is what makes the engine's
    step-boundary transaction composition explicit."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    return conn


def upsert_item(conn, key: str, title: str, source: str, repo: str,
                   detail=None, severity=None, pattern=None, base_sha=None) -> int:
    """Insert a item in state 'found', or return the existing id for the
    same key (stable dedup). Never regresses state."""
    with ensure_tx(conn):
        row = conn.execute("SELECT id FROM items WHERE key=?", (key,)).fetchone()
        if row:
            return row["id"]
        cur = conn.execute(
            "INSERT INTO items(key, source, pattern, title, detail, severity,"
            " repo, base_sha) VALUES (?,?,?,?,?,?,?,?)",
            (key, source, pattern, title, detail, severity, repo, base_sha))
        return cur.lastrowid


def emit_event(conn, name: str, payload: dict, subscriptions=None) -> int:
    """Append to the event log and enqueue one task per subscribed workflow
    kind — inside the caller's transaction when there is one. Duplicate
    payloads are absorbed by queue.enqueue's idempotency key."""
    from .util import canonical_json  # local import keeps module deps flat
    subscriptions = subscriptions or {}
    with ensure_tx(conn):
        cur = conn.execute("INSERT INTO events(name, payload) VALUES (?,?)",
                           (name, canonical_json(payload)))
        event_id = cur.lastrowid
        task_payload = dict(payload)
        task_payload["event"] = name
        item_id = payload.get("item_id")
        if item_id is not None:
            row = conn.execute("SELECT 1 FROM items WHERE id=?",
                               (item_id,)).fetchone()
            if row is None:  # payload key, not a proven row — don't link
                item_id = None
        for kind in subscriptions.get(name, ()):
            queue.enqueue(conn, kind, task_payload, item_id=item_id)
        return event_id


def record_transition(conn, item_id: int, to_state: str, event: str,
                      evidence=None, run_id=None, subscriptions=None) -> int:
    """The ONLY way item state changes. Enforces ITEM_STATES, appends
    the audit row, and fans out 'item.<to_state>' to subscribed workflows
    in the SAME transaction. Returns the transition id."""
    if to_state not in ITEM_STATES:
        raise TransitionError("unknown item state '%s'" % to_state)
    with ensure_tx(conn):
        row = conn.execute("SELECT state FROM items WHERE id=?",
                           (item_id,)).fetchone()
        if row is None:
            raise TransitionError("item %s does not exist" % item_id)
        from_state = row["state"]
        if to_state not in ITEM_STATES[from_state]:
            raise TransitionError(
                "illegal transition %s -> %s for item %s (event %s)"
                % (from_state, to_state, item_id, event))
        conn.execute(
            "UPDATE items SET state=?, updated_at=datetime('now') WHERE id=?",
            (to_state, item_id))
        cur = conn.execute(
            "INSERT INTO transitions(item_id, from_state, to_state, event,"
            " evidence, run_id) VALUES (?,?,?,?,?,?)",
            (item_id, from_state, to_state, event,
             json.dumps(evidence) if evidence is not None else None, run_id))
        transition_id = cur.lastrowid
        # 'via' = the machine event that caused the transition; the key is
        # NOT named 'event' because emit_event reserves that for the event
        # name when building subscriber task payloads.
        emit_event(conn, "item." + to_state,
                   {"item_id": item_id, "transition_id": transition_id,
                    "from_state": from_state, "via": event},
                   subscriptions)
        return transition_id
