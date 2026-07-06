# Engine runtime semantics

The seventh contract: how the machinery actually runs. WORKFLOWS.md defines
what a workflow *is*; EXECUTION.md defines what must always hold; this defines
the concrete mechanics an implementation must follow. SQLite is the single
source of truth; one daemon process is the single writer.

## Process model

- One daemon per state dir, enforced by `flock` on `state/daemon.lock` at
  startup (second `run` exits 0 with a message). One-shot CLI commands
  (`hunt`/`fix --finding`/`review --pr`) run WITHOUT the lock: they enqueue
  and then drive the claim loop until their own task tree is terminal, using
  the same code paths. SIGTERM: finish the current step, persist, exit;
  never kill mid-transaction.
- SQLite: WAL mode, `busy_timeout >= 5000ms`, `foreign_keys=ON`. The board
  and one-shot commands open their own connections; WAL makes concurrent
  reads safe; writes serialize via `BEGIN IMMEDIATE`.

## Main loop

```
loop:
  intake()      # poll forge watermarks (and later: drain webhook queue)
                # -> emit events -> enqueue via subscriptions
  unpark()      # parked tasks whose park condition may have cleared
                # (agent_limit: recheck every unpark_interval; operator
                #  unparks are immediate via board -> db)
  task = claim()
  if task is None: sleep(idle_interval); continue
  execute(task) # contract.execute — see below
```

`idle_interval` and `unpark_interval` come from the pack; defaults 15s / 600s.

## Claim (atomic, no double-claim)

```
BEGIN IMMEDIATE;
  SELECT id FROM tasks
   WHERE state IN ('pending','retry_wait')
     AND (next_attempt IS NULL OR next_attempt <= now)
   ORDER BY id LIMIT 1;
  UPDATE tasks SET state='running', updated_at=now WHERE id=?;
COMMIT;
```

Oldest-first (`ORDER BY id`) is the fairness policy. Concurrency within one
daemon = sequential by default; if worker threads are ever added, claim
stays the serialization point — but do not add them in v0.

## Step execution (contract.execute)

Per step, in order:

1. **Resume check**: `task_steps` rows tell which steps already completed
   for this task attempt — skip them (crash resume lands here).
2. **Run** the block fn. Timeout enforcement: blocks that spawn processes
   MUST pass `timeout_s` to subprocess and let `TimeoutExpired` escape;
   the engine maps it to the step's `timeout` outcome. Pure-Python blocks
   are expected to be fast; the engine additionally records wall time and
   flags (log, board) any step exceeding its budget.
3. **Classify**: the returned outcome must be in the step's declared set —
   anything else, and any uncaught exception, fails the task loudly with
   error_class 'framework_bug' (this is a bug in a block, not in a model).
4. **Persist the boundary** (one transaction): INSERT task_steps(task_id,
   attempt, step, outcome, result_json, wall_ms) + any rows the block
   staged (findings, readings, egress, runs updates) + the dispatch
   decision. Only after COMMIT does the next step start.
5. **Dispatch**: `dispatch[(step, outcome)]` → next step, or a terminal
   task state. Terminal → finalize (below).

### task_steps (add to schema)

```
CREATE TABLE IF NOT EXISTS task_steps (
    task_id    INTEGER NOT NULL REFERENCES tasks(id),
    attempt    INTEGER NOT NULL,
    step       TEXT NOT NULL,
    outcome    TEXT NOT NULL,
    result     TEXT,                 -- JSON, small; big artifacts go to data/
    wall_ms    INTEGER,
    at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (task_id, attempt, step)
);
```

## Failure & retry

`queue.fail(task, error_class)` applies POLICY (queue.py table):
- retryable → state 'retry_wait', attempts+1, `next_attempt = now +
  min(backoff_base * 2^attempts, backoff_cap)`; task_steps rows for the
  FAILED attempt are kept (audit) but the next attempt re-runs all steps
  (attempt number increments) unless the step is marked resumable
  (worktree.create is; agent.run is NOT — a new attempt is a new run row).
- exhausted → 'parked' (park_on_exhaust) else 'failed'.
- consume_task classes → terminal immediately, no retry ever.
- 'parked' stores park_reason = error_class; unpark() resets to 'pending'
  without incrementing attempts.

## Crash recovery (daemon start)

Tasks in 'running' are orphans (we are the only daemon; the lock proves the
old process died): reset to 'pending' with attempts unchanged. Their
task_steps rows make re-execution resume-aware. Orphaned runs rows
(started_at set, finished_at NULL) stay as the audit record of the crash.
Orphaned worktrees under workspaces/ whose task is terminal are pruned at
startup; a task in 'pending'/'retry_wait' keeps its worktree.

## Events & subscriptions

- `record_transition(finding, to_state, event, evidence, run_id)` — inside
  its transaction: UPDATE finding, INSERT transitions row, then fan-out:
  for each workflow whose `consumes:` includes `finding.<to_state>`,
  enqueue a task (same transaction — interaction is atomic).
- Non-finding events (`pr.opened`, `comment.fix_request`) are emitted by
  intake() through the same `emit_event(conn, name, payload)` helper.
- Enqueue idempotency key: `(kind, sha256(canonical_json(payload)))` —
  UNIQUE index; a replayed event cannot double-enqueue. Canonical JSON =
  sorted keys, no whitespace.

## Worktree lifecycle binding

- Path: `workspaces/task-<id>-a<attempt>/`, created by worktree.create,
  recorded in task_steps result.
- Removed when the task reaches done/failed/deferred. KEPT for 'parked'
  (resume may need it) and pruned only when the parked task terminates.
- `git worktree add` from the pack repo path; branch per task; `git
  worktree remove --force` + branch delete on cleanup. Never operate on
  the pack's main checkout.

## Agent step mechanics (runner.run_agent)

Order is load-bearing:
1. assemble context slice (declared context only) → build prompt → sha256.
2. INSERT runs row (task_id, model, prompt_sha, pack_rev, vault_rev,
   probe_rev, base_sha, build_id) — COMMIT before exec.
3. exec backend (claude-cli: argv fixed, prompt via stdin, cwd=worktree,
   env minus secrets except what the backend needs; timeout).
4. stdout/stderr → data/runs/<run_id>/ verbatim.
5. extract LAST ```json fenced block; validate against the step schema;
   on failure re-ask (append a correction message) at most twice — each
   re-ask is the SAME runs row (re-asks are not new runs).
6. UPDATE runs (exit_code, verdict, finished_at). Return verdict or raise
   RunnerError(error_class) for the engine to map.

## Egress mechanics

`egress.post()` order: leak_scan → INSERT egress row + write body to
data/egress/<id>.md (transaction) → forge call → UPDATE egress row with
forge-side id. If the forge call fails the egress row stays with a null
forge id (audit: we intended to send) and the error class propagates to
the step. Idempotency: before sending, look up (kind, target, body_sha) —
a match means it was already sent; return the recorded id (safe replay).
`FORGE_WRITE=1` env gates real sends; without it, archive-only + log.

## Determinism inventory (engine level)

| decision | source |
|---|---|
| which task runs next | ORDER BY id over eligible set |
| retry timing | POLICY table arithmetic |
| resume point | task_steps rows |
| who reacts to an event | consumes: lists (loader subscriptions) |
| double-send / double-enqueue | body_sha / payload-hash unique keys |
| clock | single `now` per transaction, from the db (`datetime('now')`) |
```
