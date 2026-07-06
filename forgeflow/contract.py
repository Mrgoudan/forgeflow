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
from .util import tx

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


@dataclass
class Workflow:
    kind: str
    steps: list = field(default_factory=list)
    dispatch: dict = field(default_factory=dict)  # (step, outcome) -> target
    consumes: list = field(default_factory=list)
    emits: list = field(default_factory=list)

    # -- builder API ---------------------------------------------------
    @classmethod
    def define(cls, kind: str) -> "Workflow":
        return cls(kind=kind)

    def step(self, name: str, block, *, timeout_s: int, params=None,
             context=(), max_visits: int = DEFAULT_MAX_VISITS,
             resumable=None, llm=None, schema=None,
             outcomes=None) -> "Workflow":
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
                               llm, schema, eff))
        return self

    def on(self, step_name: str, outcome: str, target: str) -> "Workflow":
        """target: another step name, or a terminal task state."""
        key = (step_name, outcome)
        if key in self.dispatch and self.dispatch[key] != target:
            raise WorkflowError("%s: conflicting dispatch for %s" % (self.kind, (key,)))
        self.dispatch[key] = target
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


@dataclass
class ExecEnv:
    conn: "object"
    subscriptions: dict = field(default_factory=dict)
    data_dir: Path = Path("data")
    workspaces_dir: Path = Path("workspaces")
    pack: "object" = None


# ------------------------------------------------------------ execution

def execute(env: ExecEnv, workflow: Workflow, task: dict) -> str:
    """Run a claimed task through its workflow; returns the resulting task
    state. Crash-resume: task_steps rows for this attempt replay without
    re-running; loop-backs invalidate stale forward history first."""
    conn = env.conn
    task_id, attempt = task["id"], task["attempts"]
    by_name = {s.name: s for s in workflow.steps}

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
                    result.update(_apply_staged(env, staged))
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
    ctx["_timeout_s"] = step.timeout_s
    ctx["_step_dir"] = str(step_dir)
    ctx["_workspaces_dir"] = str(env.workspaces_dir)
    ctx["_tools"] = dict(env.pack.tools) if env.pack else {}
    ctx["_data_dir"] = str(env.data_dir)
    ctx["_conn"] = env.conn      # for runner-backed blocks (runs row pinning)
    ctx["_pack"] = env.pack
    ctx["_step"] = step
    started = time.monotonic()
    outcome, result = step.block.fn(ctx, task, prev)
    wall_ms = int((time.monotonic() - started) * 1000)
    if wall_ms > step.timeout_s * 1000:
        print("contract: step '%s' exceeded its budget (%dms > %ds)"
              % (step.name, wall_ms, step.timeout_s), file=sys.stderr)
    if not isinstance(result, dict):
        raise RuntimeError("block '%s' returned non-dict result %r"
                           % (step.block.name, type(result)))
    return outcome, result, wall_ms


def _apply_staged(env, ops):
    """Apply block-staged db effects inside the boundary transaction.
    Returns ids to merge into the persisted step result."""
    out = {}
    for op in ops:
        kind = op.get("op")
        if kind == "upsert_finding":
            out["finding_id"] = dbmod.upsert_finding(
                env.conn, op["key"], op["title"], op["source"], op["repo"],
                detail=op.get("detail"), severity=op.get("severity"),
                pattern=op.get("pattern"), base_sha=op.get("base_sha"))
        elif kind == "transition":
            out["transition_id"] = dbmod.record_transition(
                env.conn, op["finding_id"], op["to_state"], op["event"],
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
        queue.complete(env.conn, task["id"])
        return "done"
    if target == "deferred":
        queue.defer(env.conn, task["id"])
        return "deferred"
    if target == "parked":
        queue.park(env.conn, task["id"], reason=outcome)
        return "parked"
    # 'failed': if the outcome names a known error class, POLICY decides
    # (retry_wait / park / consume); otherwise it is a plain terminal failure
    # the workflow author chose.
    if outcome in queue.POLICY:
        return queue.fail(env.conn, task["id"], outcome)
    queue._set_state(env.conn, task["id"], "failed", error_class=outcome)
    return "failed"


def _fail_loud(env, task, error_class, detail) -> str:
    print("contract: task %s [%s] %s: %s"
          % (task["id"], task["kind"], error_class, detail), file=sys.stderr)
    return queue.fail(env.conn, task["id"], error_class, detail=detail)
