"""Task queue over db.tasks with a retry policy keyed by error CLASS.

Error classes come from exit codes, file comparisons, and process structure
ONLY. Output text is never matched — that rule is what killed the
"output contained the words 'rate limit' -> 5-hour freeze" failure mode.

State machine (tasks.state):
    pending -> running -> done | failed | deferred      (terminal)
                       -> retry_wait -> running ...     (bounded by POLICY)
                       -> parked     -> pending          (unpark; attempts kept)
A parked task never blocks the loop: claim() simply doesn't see it.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass

from .util import ensure_tx, payload_hash, tx


@dataclass(frozen=True)
class Policy:
    max_attempts: int         # failures allowed before exhaustion
    backoff_base_s: int       # delay = base * 2**(attempt-1), capped
    backoff_cap_s: int
    park_on_exhaust: bool     # park (human-visible, resumable) vs fail
    consume_task: bool = False  # True = terminal immediately, never retry
    # how long a PARKED task of this class waits before the daemon re-tries it
    # (parked_due). None = never auto-unpark: the cause can't heal on its own
    # (e.g. a bad token) and a human must unpark it. Backend-dependent classes
    # (see BACKEND_PARK_CLASSES) are additionally health-gated by the engine.
    unpark_after_s: object = 600


POLICY = {
    "forge_auth":           Policy(20, 30, 3600, park_on_exhaust=True, unpark_after_s=None),
    "forge_server":         Policy(10, 10, 600,  park_on_exhaust=True, unpark_after_s=600),
    "agent_limit":          Policy(0,  0,  0,    park_on_exhaust=True, unpark_after_s=1800),  # quota window: probe every 30 min
    "agent_backend":        Policy(3,  60, 3600, park_on_exhaust=True, unpark_after_s=300),   # transport blip: recovers fast
    "agent_invalid_output": Policy(2,  0,  0,    park_on_exhaust=False),  # re-ask twice then fail
    "verify_red":           Policy(1,  0,  0,    park_on_exhaust=False),  # one retry then fail
    "timeout":              Policy(1,  60, 3600, park_on_exhaust=False),  # one delayed retry
    "workspace_dirty":      Policy(0,  0,  0,    park_on_exhaust=False, consume_task=True),
    "agent_noop":           Policy(0,  0,  0,    park_on_exhaust=False, consume_task=True),
    "framework_bug":        Policy(0,  0,  0,    park_on_exhaust=False, consume_task=True),
    "step_budget_exhausted": Policy(0, 0,  0,    park_on_exhaust=False, consume_task=True),
}

# classes whose recovery depends on the agent backend being reachable: the
# engine health-probes before unparking these (see Engine._unpark_tick).
BACKEND_PARK_CLASSES = {"agent_limit", "agent_backend"}
UNPARK_AFTER_DEFAULT = 600


def _unpark_after(error_class):
    p = POLICY.get(error_class)
    return p.unpark_after_s if p else UNPARK_AFTER_DEFAULT

TERMINAL_TASK_STATES = {"done", "failed", "deferred"}


def enqueue(conn, kind: str, payload: dict, finding_id=None) -> int:
    """Insert a pending task, idempotent on (kind, payload-hash): replaying
    the same event yields the SAME task id and no duplicate row. Joins the
    caller's transaction (event fan-out atomicity depends on this)."""
    h = payload_hash(payload)
    with ensure_tx(conn):
        cur = conn.execute(
            "INSERT OR IGNORE INTO tasks(kind, finding_id, payload, payload_hash)"
            " VALUES (?,?,?,?)",
            (kind, finding_id, json.dumps(payload, sort_keys=True), h))
        if cur.rowcount:
            return cur.lastrowid
        row = conn.execute(
            "SELECT id FROM tasks WHERE kind=? AND payload_hash=?",
            (kind, h)).fetchone()
        return row["id"]


def claim(conn):
    """Atomically claim the oldest eligible task. BEGIN IMMEDIATE takes the
    write lock before the SELECT, so no two claimers can pick the same row.
    Returns the task as a dict (payload decoded) or None."""
    with tx(conn, immediate=True):
        row = conn.execute(
            "SELECT id FROM tasks"
            " WHERE state IN ('pending','retry_wait')"
            "   AND (next_attempt IS NULL OR next_attempt <= datetime('now'))"
            " ORDER BY id LIMIT 1").fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE tasks SET state='running', updated_at=datetime('now')"
            " WHERE id=?", (row["id"],))
        task = conn.execute("SELECT * FROM tasks WHERE id=?", (row["id"],)).fetchone()
    task = dict(task)
    task["payload"] = json.loads(task["payload"])
    return task


def complete(conn, task_id: int) -> None:
    _set_state(conn, task_id, "done")


def defer(conn, task_id: int) -> None:
    _set_state(conn, task_id, "deferred")


def park(conn, task_id: int, reason: str) -> None:
    with ensure_tx(conn):
        conn.execute(
            "UPDATE tasks SET state='parked', park_reason=?, error_class=?,"
            " next_attempt=NULL, updated_at=datetime('now') WHERE id=?",
            (reason, reason, task_id))


def unpark(conn, task_id=None, ids=None) -> int:
    """parked -> pending, attempts unchanged. Three modes:
      - ids=[...]   : exactly these (the daemon's cadence/health tick).
      - task_id=N   : one (targeted operator/board override).
      - both None   : ALL parked (operator 'release everything').
    Returns how many tasks became eligible."""
    with ensure_tx(conn):
        if ids is not None:
            ids = list(ids)
            if not ids:
                return 0
            ph = ",".join("?" * len(ids))
            cur = conn.execute(
                "UPDATE tasks SET state='pending', next_attempt=NULL,"
                " updated_at=datetime('now')"
                " WHERE state='parked' AND id IN (%s)" % ph, ids)
        elif task_id is None:
            cur = conn.execute(
                "UPDATE tasks SET state='pending', next_attempt=NULL,"
                " updated_at=datetime('now') WHERE state='parked'")
        else:
            cur = conn.execute(
                "UPDATE tasks SET state='pending', next_attempt=NULL,"
                " updated_at=datetime('now') WHERE id=? AND state='parked'",
                (task_id,))
        return cur.rowcount


def parked_due(conn, classes=None):
    """Parked tasks whose per-class cadence (POLICY.unpark_after_s) has elapsed
    since they parked. Returns [(id, error_class), ...]. Classes with
    unpark_after_s=None never come due (human-only). `classes` optionally
    restricts to a subset."""
    rows = conn.execute(
        "SELECT id, error_class,"
        " CAST(strftime('%s','now') - strftime('%s', updated_at) AS INTEGER) age"
        " FROM tasks WHERE state='parked'").fetchall()
    due = []
    for r in rows:
        cls = r["error_class"]
        if classes is not None and cls not in classes:
            continue
        after = _unpark_after(cls)
        if after is not None and r["age"] is not None and r["age"] >= after:
            due.append((r["id"], cls))
    return due


def rearm(conn, ids) -> None:
    """Reset the park clock on these tasks. A failed health probe re-arms the
    cadence so the next probe is a FULL cadence away (probe every 30 min, not
    every daemon tick)."""
    ids = list(ids)
    if not ids:
        return
    with ensure_tx(conn):
        ph = ",".join("?" * len(ids))
        conn.execute("UPDATE tasks SET updated_at=datetime('now')"
                     " WHERE state='parked' AND id IN (%s)" % ph, ids)


def fail(conn, task_id: int, error_class: str, detail=None) -> str:
    """Apply POLICY[error_class]. Returns the resulting task state.

    Unknown error classes never guess a retry policy: the task terminates
    as 'failed' and the anomaly is logged loudly.
    """
    policy = POLICY.get(error_class)
    with ensure_tx(conn):
        row = conn.execute("SELECT attempts FROM tasks WHERE id=?",
                           (task_id,)).fetchone()
        if row is None:
            raise ValueError("fail(): task %s does not exist" % task_id)
        attempts = row["attempts"]
        if policy is None:
            print("queue.fail: UNKNOWN error class '%s' on task %s — "
                  "terminal failure, no retry (%s)"
                  % (error_class, task_id, detail), file=sys.stderr)
            _set_state(conn, task_id, "failed", error_class)
            return "failed"
        if policy.consume_task:
            _set_state(conn, task_id, "failed", error_class)
            return "failed"
        new_attempts = attempts + 1
        if new_attempts <= policy.max_attempts:
            delay = min(policy.backoff_base_s * (2 ** (new_attempts - 1)),
                        policy.backoff_cap_s)
            conn.execute(
                "UPDATE tasks SET state='retry_wait', attempts=?, error_class=?,"
                " next_attempt=datetime('now', '+' || ? || ' seconds'),"
                " updated_at=datetime('now') WHERE id=?",
                (new_attempts, error_class, int(delay), task_id))
            return "retry_wait"
        # exhausted
        conn.execute("UPDATE tasks SET attempts=? WHERE id=?",
                     (new_attempts, task_id))
        if policy.park_on_exhaust:
            park(conn, task_id, error_class)
            return "parked"
        _set_state(conn, task_id, "failed", error_class)
        return "failed"


def reset_orphans(conn) -> int:
    """Crash recovery at daemon start: tasks left 'running' belong to a dead
    process (single-daemon lock proves it). Reset to 'pending', attempts
    unchanged — task_steps rows make re-execution resume-aware."""
    with ensure_tx(conn):
        cur = conn.execute(
            "UPDATE tasks SET state='pending', updated_at=datetime('now')"
            " WHERE state='running'")
        return cur.rowcount


def _set_state(conn, task_id: int, state: str, error_class=None) -> None:
    with ensure_tx(conn):
        conn.execute(
            "UPDATE tasks SET state=?, error_class=COALESCE(?, error_class),"
            " next_attempt=NULL, updated_at=datetime('now') WHERE id=?",
            (state, error_class, task_id))
