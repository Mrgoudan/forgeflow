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
import re
import sys
from dataclasses import dataclass, replace

from .util import canonical_json, ensure_tx, payload_hash, sha256_text, tx


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
    # a workflow definition changed under a mid-flight task: park for the
    # operator (unpark/retry = fresh attempt under the NEW definition). Never
    # auto-unparks by default; packs may set unpark_after_s to opt into
    # automatic re-runs after a definition change.
    "definition_changed":   Policy(0,  0,  0,    park_on_exhaust=True, unpark_after_s=None),
    # a human gate: parked until a person answers (resolve_decision resumes
    # the SAME attempt — the park is a wait, not a failure).
    "awaiting_human":       Policy(0,  0,  0,    park_on_exhaust=True, unpark_after_s=None),
    "workspace_dirty":      Policy(0,  0,  0,    park_on_exhaust=False, consume_task=True),
    "agent_noop":           Policy(0,  0,  0,    park_on_exhaust=False, consume_task=True),
    "framework_bug":        Policy(0,  0,  0,    park_on_exhaust=False, consume_task=True),
    "step_budget_exhausted": Policy(0, 0,  0,    park_on_exhaust=False, consume_task=True),
}

# classes whose recovery depends on the agent backend being reachable: the
# engine health-probes before unparking these (see Engine._unpark_tick).
BACKEND_PARK_CLASSES = {"agent_limit", "agent_backend"}
UNPARK_AFTER_DEFAULT = 600

# What a pack's retry: section may tune. consume_task is structural (the
# engine's own invariants — framework_bug MUST terminate) and is not exposed.
_TUNABLE_FIELDS = {"max_attempts", "backoff_base_s", "backoff_cap_s",
                   "park_on_exhaust", "unpark_after_s"}
_CLASS_RE = re.compile(r"^[a-z0-9_]+$")


def build_policy(overrides) -> dict:
    """The effective retry policy: POLICY plus a pack's retry: overrides.
    Packs may retune engine classes (except consume_task classes) and define
    NEW classes for their own block outcomes — an outcome mapped to 'failed'
    whose name is a policy class gets that class's retry arithmetic instead
    of failing terminally. Raises ValueError on any malformed entry; the
    result is a plain dict consulted per-engine (never global mutation)."""
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict):
        raise ValueError("retry section must be a mapping of class -> fields")
    policy = dict(POLICY)
    for cls, fields in overrides.items():
        if not isinstance(cls, str) or not _CLASS_RE.match(cls):
            raise ValueError("bad error class name %r (want [a-z0-9_]+)" % (cls,))
        if not isinstance(fields, dict) or not fields:
            raise ValueError("retry.%s: must be a non-empty mapping" % cls)
        unknown = set(fields) - _TUNABLE_FIELDS
        if unknown:
            raise ValueError("retry.%s: unknown fields %s (tunable: %s)"
                             % (cls, sorted(unknown), sorted(_TUNABLE_FIELDS)))
        base = policy.get(cls)
        if base is not None and base.consume_task:
            raise ValueError(
                "retry.%s: this class terminates immediately by engine "
                "design and cannot be reconfigured" % cls)
        kw = {}
        for f, v in fields.items():
            if f == "park_on_exhaust":
                if not isinstance(v, bool):
                    raise ValueError("retry.%s.park_on_exhaust: want bool, got %r"
                                     % (cls, v))
            elif f == "unpark_after_s":
                if v is not None and (isinstance(v, bool)
                                      or not isinstance(v, int) or v < 0):
                    raise ValueError("retry.%s.unpark_after_s: want null or "
                                     "int >= 0, got %r" % (cls, v))
            else:
                if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                    raise ValueError("retry.%s.%s: want int >= 0, got %r"
                                     % (cls, f, v))
            kw[f] = v
        if base is None:  # a NEW pack-defined class
            missing = {"max_attempts", "park_on_exhaust"} - set(kw)
            if missing:
                raise ValueError("retry.%s: new class needs at least %s"
                                 % (cls, sorted(missing)))
            kw.setdefault("backoff_base_s", 0)
            kw.setdefault("backoff_cap_s", 0)
            kw.setdefault("unpark_after_s", UNPARK_AFTER_DEFAULT)
            policy[cls] = Policy(**kw)
        else:
            policy[cls] = replace(base, **kw)
    return policy


def _unpark_after(error_class, policy=None):
    p = (policy or POLICY).get(error_class)
    return p.unpark_after_s if p else UNPARK_AFTER_DEFAULT

TERMINAL_TASK_STATES = {"done", "failed", "deferred"}


def enqueue(conn, kind: str, payload: dict, item_id=None) -> int:
    """Insert a pending task, idempotent on (kind, payload-hash): replaying
    the same event yields the SAME task id and no duplicate row. Joins the
    caller's transaction (event fan-out atomicity depends on this).

    A payload carrying the reserved _join key ({"group": id, "index": i},
    written only by fanout.emit) additionally links the task as a member of
    that join group — the group's join event fires when every member is
    terminal (see _note_terminal / check_join_fire)."""
    h = payload_hash(payload)
    with ensure_tx(conn):
        cur = conn.execute(
            "INSERT OR IGNORE INTO tasks(kind, item_id, payload, payload_hash)"
            " VALUES (?,?,?,?)",
            (kind, item_id, json.dumps(payload, sort_keys=True), h))
        if cur.rowcount:
            task_id = cur.lastrowid
        else:
            task_id = conn.execute(
                "SELECT id FROM tasks WHERE kind=? AND payload_hash=?",
                (kind, h)).fetchone()["id"]
        j = payload.get("_join")
        if isinstance(j, dict) and "group" in j:
            _join_link(conn, int(j["group"]), task_id)
        return task_id


def _join_link(conn, group_id: int, task_id: int) -> None:
    """Register a task as a join-group member. Idempotent (re-applied
    fan-outs). A dedup hit on an ALREADY-TERMINAL task must record that
    state immediately — the task will never transition again, and a NULL
    member would hold the join open forever."""
    conn.execute("INSERT OR IGNORE INTO join_members(group_id, task_id, state)"
                 " VALUES (?,?,NULL)", (group_id, task_id))
    row = conn.execute("SELECT state FROM tasks WHERE id=?", (task_id,)).fetchone()
    if row and row["state"] in TERMINAL_TASK_STATES:
        conn.execute("UPDATE join_members SET state=? WHERE group_id=?"
                     " AND task_id=? AND state IS NULL",
                     (row["state"], group_id, task_id))


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


def complete(conn, task_id: int, subscriptions=None) -> None:
    _set_state(conn, task_id, "done", subscriptions=subscriptions)


def defer(conn, task_id: int, subscriptions=None) -> None:
    _set_state(conn, task_id, "deferred", subscriptions=subscriptions)


def park(conn, task_id: int, reason: str) -> None:
    with ensure_tx(conn):
        conn.execute(
            "UPDATE tasks SET state='parked', park_reason=?, error_class=?,"
            " next_attempt=NULL, updated_at=datetime('now') WHERE id=?",
            (reason, reason, task_id))


def unpark(conn, task_id=None, ids=None) -> int:
    """parked -> pending, STARTING A FRESH ATTEMPT (attempts += 1). Three modes:
      - ids=[...]   : exactly these (the daemon's cadence/health tick).
      - task_id=N   : one (targeted operator/board override).
      - both None   : ALL parked (operator 'release everything').
    Returns how many tasks became eligible.

    Why bump attempts: the contract replays a step's RECORDED outcome for the
    SAME attempt (crash-resume). A task parked by a workflow outcome->parked
    mapping (e.g. explore's agent_limit->parked) still has that failing step
    recorded for the current attempt, so resuming WITHOUT a new attempt would
    just replay the stored failure and re-park — the agent would never re-run.
    A fresh attempt has no recorded steps, so the workflow runs from scratch."""
    bump = ("UPDATE tasks SET state='pending', attempts=attempts+1,"
            " next_attempt=NULL, updated_at=datetime('now')")
    with ensure_tx(conn):
        if ids is not None:
            ids = list(ids)
            if not ids:
                return 0
            ph = ",".join("?" * len(ids))
            cur = conn.execute(
                bump + " WHERE state='parked' AND id IN (%s)" % ph, ids)
        elif task_id is None:
            cur = conn.execute(bump + " WHERE state='parked'")
        else:
            cur = conn.execute(bump + " WHERE id=? AND state='parked'", (task_id,))
        return cur.rowcount


def retry(conn, task_id=None, kind=None) -> int:
    """failed -> pending with a FRESH attempt (attempts+1, so the contract
    re-runs from step 0 instead of replaying the recorded failure). Targeted
    (task_id), by kind, or all failed. Returns how many became eligible.

    A retried task that is a member of a join group whose join event has NOT
    fired yet goes back to waiting (member state -> NULL): the join reflects
    the re-run's outcome, not the superseded failure. Groups that already
    fired are untouched (the join event is history, never rewritten)."""
    sel = "SELECT id FROM tasks WHERE state='failed'"
    args = []
    if task_id is not None:
        sel += " AND id=?"
        args.append(task_id)
    elif kind is not None:
        sel += " AND kind=?"
        args.append(kind)
    with ensure_tx(conn):
        ids = [r["id"] for r in conn.execute(sel, args)]
        if not ids:
            return 0
        ph = ",".join("?" * len(ids))
        n = conn.execute(
            "UPDATE tasks SET state='pending', attempts=attempts+1,"
            " next_attempt=NULL, error_class=NULL, updated_at=datetime('now')"
            " WHERE state='failed' AND id IN (%s)" % ph, ids).rowcount
        conn.execute(
            "UPDATE join_members SET state=NULL WHERE task_id IN (%s)"
            " AND group_id IN (SELECT id FROM join_groups WHERE fired_at IS NULL)"
            % ph, ids)
        return n


def parked_due(conn, classes=None, policy=None):
    """Parked tasks whose per-class cadence (POLICY.unpark_after_s) has elapsed
    since they parked. Returns [(id, error_class), ...]. Classes with
    unpark_after_s=None never come due (human-only). `classes` optionally
    restricts to a subset; `policy` is the engine's effective policy dict
    (build_policy), default the engine table."""
    rows = conn.execute(
        "SELECT id, error_class,"
        " CAST(strftime('%s','now') - strftime('%s', updated_at) AS INTEGER) age"
        " FROM tasks WHERE state='parked'").fetchall()
    due = []
    for r in rows:
        cls = r["error_class"]
        if classes is not None and cls not in classes:
            continue
        after = _unpark_after(cls, policy)
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


def fail(conn, task_id: int, error_class: str, detail=None, policy=None,
         subscriptions=None) -> str:
    """Apply policy[error_class] (the engine's effective policy — build_policy
    with pack overrides — or the default table). Returns the resulting task
    state.

    Unknown error classes never guess a retry policy: the task terminates
    as 'failed' and the anomaly is logged loudly.
    """
    policy = (policy or POLICY).get(error_class)
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
            _set_state(conn, task_id, "failed", error_class,
                       subscriptions=subscriptions)
            return "failed"
        if policy.consume_task:
            _set_state(conn, task_id, "failed", error_class,
                       subscriptions=subscriptions)
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
        _set_state(conn, task_id, "failed", error_class,
                   subscriptions=subscriptions)
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


def _set_state(conn, task_id: int, state: str, error_class=None,
               subscriptions=None) -> None:
    with ensure_tx(conn):
        conn.execute(
            "UPDATE tasks SET state=?, error_class=COALESCE(?, error_class),"
            " next_attempt=NULL, updated_at=datetime('now') WHERE id=?",
            (state, error_class, task_id))
        if state in TERMINAL_TASK_STATES:
            _note_terminal(conn, task_id, state, subscriptions)


# ---------------------------------------------------------- fan-out / join

def _note_terminal(conn, task_id: int, state: str, subscriptions) -> None:
    """Record a member task's terminal state on every join group it belongs
    to, then fire any group that just became complete — atomically with the
    state change (we are inside the caller's transaction). A task that
    re-terminates (operator retry) updates its recorded state; a group that
    already fired never fires again (fired_at guard)."""
    groups = [r["group_id"] for r in conn.execute(
        "SELECT group_id FROM join_members WHERE task_id=?", (task_id,))]
    if not groups:
        return
    conn.execute("UPDATE join_members SET state=? WHERE task_id=?",
                 (state, task_id))
    for gid in groups:
        check_join_fire(conn, gid, subscriptions)


def check_join_fire(conn, group_id: int, subscriptions=None) -> bool:
    """Fire the group's join event iff every expected member is terminal and
    it has not fired yet. Exactly-once: the fired_at stamp is claimed with a
    guarded UPDATE inside the caller's transaction. Returns True if this
    call fired the event."""
    g = conn.execute("SELECT * FROM join_groups WHERE id=?",
                     (group_id,)).fetchone()
    if g is None or g["fired_at"] is not None:
        return False
    n, waiting = conn.execute(
        "SELECT count(*), count(*) - count(state) FROM join_members"
        " WHERE group_id=?", (group_id,)).fetchone()
    if n < g["expect_n"] or waiting:
        return False
    with ensure_tx(conn):
        claimed = conn.execute(
            "UPDATE join_groups SET fired_at=datetime('now')"
            " WHERE id=? AND fired_at IS NULL", (group_id,)).rowcount
        if not claimed:
            return False
        counts = {"done": 0, "failed": 0, "deferred": 0}
        for r in conn.execute("SELECT state, count(*) c FROM join_members"
                              " WHERE group_id=? GROUP BY state", (group_id,)):
            counts[r["state"]] = r["c"]
        payload = json.loads(g["data"] or "{}")
        payload.update({"join_group": group_id, "total": n,
                        "done": counts["done"], "failed": counts["failed"],
                        "deferred": counts["deferred"]})
        from . import db  # lazy: db imports queue at module level
        db.emit_event(conn, g["event"], payload, subscriptions or {})
    return True


def apply_fanout(conn, op: dict, task: dict, subscriptions) -> int:
    """Apply a fanout.emit staged op inside the step-boundary transaction:
    create (or reuse) the join group, emit one event per item payload with
    the reserved _join key injected, and fire immediately if the group is
    already complete (zero items / zero consumers / all members already
    terminal via dedup). The group key is deterministic over (task, event
    names, payloads), so a re-executed attempt reuses the SAME group and the
    payload-hash dedup absorbs the re-emitted children — a parent re-run
    never doubles the fan-out."""
    key = sha256_text(canonical_json(
        {"task": task["id"], "name": op["name"], "join_event": op["join_event"],
         "payloads": op["payloads"]}))
    with ensure_tx(conn):
        row = conn.execute("SELECT id FROM join_groups WHERE key=?",
                           (key,)).fetchone()
        if row is not None:
            gid = row["id"]
        else:
            expect = len(op["payloads"]) * len(subscriptions.get(op["name"], ()))
            gid = conn.execute(
                "INSERT INTO join_groups(key, parent_task, event, data, expect_n)"
                " VALUES (?,?,?,?,?)",
                (key, task["id"], op["join_event"],
                 canonical_json(op.get("join_data") or {}), expect)).lastrowid
        from . import db  # lazy: db imports queue at module level
        for i, p in enumerate(op["payloads"]):
            payload = dict(p)
            payload["_join"] = {"group": gid, "index": i}
            db.emit_event(conn, op["name"], payload, subscriptions)
        check_join_fire(conn, gid, subscriptions)
    return gid


def clamp_clock_skew(conn, horizon_s=172800, to_s=3600):
    """Wall-clock guard: next_attempt is wall-clock arithmetic (durable across
    restarts — the right base for a queue), but a backward clock jump (NTP
    correction) can leave timestamps absurdly far in the future, stranding
    parked/backoff tasks. Any next_attempt beyond `horizon_s` from now is
    treated as skew and clamped to now + `to_s`. Returns rows clamped."""
    cur = conn.execute(
        "UPDATE tasks SET next_attempt = datetime('now', '+' || ? || ' seconds')"
        " WHERE next_attempt IS NOT NULL"
        "   AND next_attempt > datetime('now', '+' || ? || ' seconds')",
        (int(to_s), int(horizon_s)))
    return cur.rowcount


def resume_decision(conn, task_id) -> int:
    """parked(awaiting_human) -> pending on the SAME attempt. Unlike unpark(),
    no attempts bump: nothing failed, so completed steps replay instantly and
    only the (fresh) gate step re-executes to consume the verdict."""
    cur = conn.execute(
        "UPDATE tasks SET state='pending', next_attempt=NULL,"
        " updated_at=datetime('now') WHERE id=? AND state='parked'", (task_id,))
    return cur.rowcount
