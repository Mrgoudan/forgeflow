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
    evidence      TEXT,                  -- JSON: exit codes, check output, shas
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
    def_hash      TEXT,                  -- workflow definition fingerprint the
                                         -- current attempt runs under (stamped
                                         -- at execution; see contract.execute)
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
    probe_rev     TEXT,                  -- git rev of the check set
    base_sha      TEXT,
    build_id      TEXT,                  -- toolchain build fingerprint
    exit_code     INTEGER,
    verdict       TEXT,                  -- schema-validated verdict enum, never prose
    output_path   TEXT,                  -- full raw output, logged not parsed
    wall_ms       INTEGER,               -- total wall time incl. re-asks
    reasks        INTEGER,               -- correction rounds used (0 = clean)
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


CREATE TABLE IF NOT EXISTS watermarks (            -- external cursor state
    scope         TEXT PRIMARY KEY,      -- e.g. 'pr_comments:<repo>'
    cursor        TEXT NOT NULL          -- last processed id, NOT a timestamp
);

-- ---- fan-out / join -------------------------------------------------------
-- A fanout.emit step creates ONE group + one member row per spawned task
-- (queue.enqueue links members via the payload's reserved _join key). When
-- every member reaches a terminal state, the group's join event fires exactly
-- once (fired_at guard) — see queue._set_state / queue.check_join_fire.

CREATE TABLE IF NOT EXISTS join_groups (
    id            INTEGER PRIMARY KEY,
    key           TEXT UNIQUE NOT NULL,  -- dedup: a re-applied identical fan-out
                                         -- (task re-attempt) reuses this group
    parent_task   INTEGER REFERENCES tasks(id),
    event         TEXT NOT NULL,         -- join event emitted at completion
    data          TEXT NOT NULL,         -- JSON merged into the join payload
    expect_n      INTEGER NOT NULL,      -- members expected (items x consumers)
    fired_at      TEXT,                  -- NULL until the join event was emitted
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS join_members (
    group_id      INTEGER NOT NULL REFERENCES join_groups(id),
    task_id       INTEGER NOT NULL REFERENCES tasks(id),
    state         TEXT,                  -- NULL until the member is terminal
    PRIMARY KEY (group_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_join_members_task ON join_members(task_id);

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

-- Vectors for corpus rows (the select: context provider). Maintained
-- incrementally at query time: a row is (re)embedded only when no vector
-- exists for (corpus, key, model_sha) or its text_sha changed. Generic:
-- 'corpus' names a pack corpora: entry over ANY table.
CREATE TABLE IF NOT EXISTS corpus_embeddings (
    corpus        TEXT NOT NULL,
    key           TEXT NOT NULL,         -- the row's stable id (declared key column)
    model_sha     TEXT NOT NULL,
    text_sha      TEXT NOT NULL,         -- sha of the text embedded (staleness pin)
    dim           INTEGER NOT NULL,
    vector        TEXT NOT NULL,         -- JSON float array
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (corpus, key, model_sha)
);







-- What each task was SHOWN from each corpus (written by the select:
-- provider). Joined against tasks.state at query time, this is the
-- outcome-learned utility signal: rows that co-occur with done tasks of
-- the same kind earn rank, rows that co-occur with failures lose it —
-- auto-labelled from the ledger, no human annotation.
CREATE TABLE IF NOT EXISTS context_uses (
    task_id       INTEGER NOT NULL REFERENCES tasks(id),
    kind          TEXT NOT NULL,         -- task kind (utility is per-kind)
    corpus        TEXT NOT NULL,
    key           TEXT NOT NULL,
    at            TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (task_id, corpus, key)
);

CREATE INDEX IF NOT EXISTS idx_context_uses_lookup
    ON context_uses(corpus, kind, key);

CREATE INDEX IF NOT EXISTS idx_tasks_claim
    ON tasks(state, next_attempt) WHERE state IN ('pending','retry_wait');
CREATE INDEX IF NOT EXISTS idx_items_state ON items(state);
"""

# SCHEMA is always the LATEST core schema. Bump SCHEMA_VERSION and append the
# upgrade to MIGRATIONS whenever it changes: a FRESH db gets SCHEMA and is just
# stamped (no migration run); an EXISTING older db runs the pending
# (version > user_version) migrations in order. Migrations are for CHANGES the
# CREATE-IF-NOT-EXISTS base can't make on an existing table (ALTER, backfill).
# (Pack tables evolve in the pack's own schema.sql.)
# EVERY migration must be IDEMPOTENT (column/table-guarded): a process can
# die between executescript(SCHEMA) and the user_version stamp, leaving a
# db whose tables are already at the latest shape but whose version says
# otherwise — the next open re-runs the migrations, and re-running must be
# a no-op, never a duplicate-column error. (Found by the chaos test.)

def _add_column(conn, table, column, decl):
    cols = {r[1] for r in conn.execute("PRAGMA table_info(%s)" % table)}
    if column not in cols:
        conn.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, column, decl))


def _mig_v2(conn):
    """v2 (0.2.0): workflow definition versioning + fan-out/join."""
    _add_column(conn, "tasks", "def_hash", "TEXT")
    conn.execute("""CREATE TABLE IF NOT EXISTS join_groups (
        id            INTEGER PRIMARY KEY,
        key           TEXT UNIQUE NOT NULL,
        parent_task   INTEGER REFERENCES tasks(id),
        event         TEXT NOT NULL,
        data          TEXT NOT NULL,
        expect_n      INTEGER NOT NULL,
        fired_at      TEXT,
        created_at    TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS join_members (
        group_id      INTEGER NOT NULL REFERENCES join_groups(id),
        task_id       INTEGER NOT NULL REFERENCES tasks(id),
        state         TEXT,
        PRIMARY KEY (group_id, task_id)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_join_members_task"
                 " ON join_members(task_id)")


def _mig_v3(conn):
    """v3 (0.3.0): agent run latency + re-ask accounting."""
    _add_column(conn, "runs", "wall_ms", "INTEGER")
    _add_column(conn, "runs", "reasks", "INTEGER")


def _mig_v4(conn):
    """v4 (0.4.0): generic corpus selection — vectors over arbitrary
    pack-declared tables."""
    conn.execute("""CREATE TABLE IF NOT EXISTS corpus_embeddings (
        corpus        TEXT NOT NULL,
        key           TEXT NOT NULL,
        model_sha     TEXT NOT NULL,
        text_sha      TEXT NOT NULL,
        dim           INTEGER NOT NULL,
        vector        TEXT NOT NULL,
        created_at    TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (corpus, key, model_sha)
    )""")


def _mig_v5(conn):
    """v5 (0.5.0): selection ledger for outcome-learned utility."""
    conn.execute("""CREATE TABLE IF NOT EXISTS context_uses (
        task_id       INTEGER NOT NULL REFERENCES tasks(id),
        kind          TEXT NOT NULL,
        corpus        TEXT NOT NULL,
        key           TEXT NOT NULL,
        at            TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (task_id, corpus, key)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_context_uses_lookup"
                 " ON context_uses(corpus, kind, key)")


SCHEMA_VERSION = 5
MIGRATIONS: list = [
    (2, _mig_v2),
    (3, _mig_v3),
    (4, _mig_v4),
    (5, _mig_v5),
]       # [(version, callable(conn))] — idempotent single statements only
        #   (they must compose into _migrate's one transaction)


def _migrate(conn, fresh):
    """Bring the core schema to SCHEMA_VERSION. A fresh db already has the
    latest SCHEMA, so it is only stamped; an existing older db runs the
    version-ordered deltas past its user_version — all deltas plus the
    stamp in ONE transaction, so a crash mid-migration rolls back to a
    state the next open handles identically."""
    if fresh:
        conn.execute("PRAGMA user_version=%d" % SCHEMA_VERSION)
        return
    have = conn.execute("PRAGMA user_version").fetchone()[0] or 1  # unversioned == v1
    if have >= SCHEMA_VERSION:
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        for version, step in MIGRATIONS:
            if have < version:
                step(conn)
                have = version
        conn.execute("PRAGMA user_version=%d" % max(have, SCHEMA_VERSION))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


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
    fresh = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table'"
                         " AND name='tasks'").fetchone() is None
    conn.executescript(SCHEMA)
    _migrate(conn, fresh)
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
