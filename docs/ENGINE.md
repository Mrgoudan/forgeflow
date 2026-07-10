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
  beat()          # heartbeat watermark (doctor reads it)
  unpark()        # parked tasks whose park condition may have cleared
                  # (agent_limit: recheck every unpark_interval; operator
                  #  unparks are immediate via cli -> db)
  schedule_tick() # timed triggers: emit each schedule entry's event once
                  # per every_s window (see "Timed triggers" below)
  disk_gate()     # pause claiming while free disk < min_free_disk_mb
  task = claim()
  if task is None: sleep(idle_interval); continue
  execute(task)   # contract.execute — see below
```

`idle_interval` and `unpark_interval` come from the pack; defaults 15s / 600s.
If the pack declares an `http:` section, the daemon also serves the
dashboard/JSON API from a thread (see "HTTP front door" below).

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

`queue.fail(task, error_class)` applies the effective policy — the engine's
POLICY table merged with the pack's `retry:` overrides (`queue.build_policy`,
carried on the ExecEnv, never global mutation):
- retryable → state 'retry_wait', attempts+1, `next_attempt = now +
  min(backoff_base * 2^attempts, backoff_cap)`; task_steps rows for the
  FAILED attempt are kept (audit) but the next attempt re-runs all steps
  (attempt number increments) unless the step is marked resumable
  (worktree.create is; agent.run is NOT — a new attempt is a new run row).
- exhausted → 'parked' (park_on_exhaust) else 'failed'.
- consume_task classes → terminal immediately, no retry ever. These are
  structural and cannot be reconfigured by packs.
- 'parked' stores park_reason = error_class; unpark() resets to 'pending'
  WITH attempts+1 — a fresh attempt, because the contract replays recorded
  outcomes per attempt and a parked task may carry the failing step's row.
- packs may define NEW classes in `retry:`; a block outcome whose name is a
  policy class, mapped to 'failed', gets that class's arithmetic instead of
  terminal failure.

## Definition versioning

Every workflow definition has a stable fingerprint (`Workflow.def_hash()`:
steps, params, context, timeouts, visit caps, resumable flags, llm/schema
bindings, outcome sets, lanes, dispatch, consumes/emits). When an attempt
first executes, the task is stamped with it (`tasks.def_hash`).

At execute():
- stamp == current hash → resume normally (recorded steps replay);
- stamp differs and THIS attempt has recorded steps → the YAML changed under
  a mid-flight task: **park as `definition_changed`** — recorded outcomes are
  never replayed through a changed dispatch graph. `unpark`/`retry` starts a
  fresh attempt that re-stamps and runs from step 0 under the new definition.
  (`definition_changed` never auto-unparks by default; a pack may opt into
  timed re-runs via `retry: { definition_changed: { unpark_after_s: N } }`.)
- stamp is NULL (task predates versioning / attempt not started) → adopt the
  current definition and proceed.

## Fan-out / join

`fanout.emit` stages a `fanout` op; the engine applies it inside the step
boundary transaction: create the join group + emit one event per item with
the reserved `_join {group, index}` payload key. `queue.enqueue` links each
spawned task as a group member. Every terminal transition
(`queue._set_state`) updates the member's recorded state and checks the
group: all members terminal → the join event fires **exactly once**
(guarded `fired_at` claim), atomically with the member's own transition.

Determinism rules:
- group key = sha256(task id, event names, item payloads): a re-executed
  parent reuses the group; payload-hash dedup absorbs re-emitted children.
- `_join.index` makes identical items distinct members.
- parked members hold the join open (parked is not terminal).
- operator `retry` of a failed member of an UNFIRED group re-opens its slot
  (state → NULL); fired groups are history and never change or re-fire.
- expect_n = items × consumers at creation; zero (empty list or no
  consumers) fires the join immediately with total 0.
- gc prunes groups fired more than `--days` ago (members with them);
  unfired groups are live state, never collected.

## Timed triggers (schedule)

Per entry `{event, every_s, data}`: occurrence = `now - (now % every_s)`
(wall clock). Fire iff occurrence > the persisted cursor
(`watermarks scope='schedule.<event>'`); emitting the event and advancing
the cursor is one transaction. Consequences: exactly-once per window across
restarts; a daemon down N windows fires only the current one (missed windows
skip — no catch-up storm); first sight fires immediately; resolution is the
daemon loop cadence. Startup refuses a schedule whose event nobody consumes.
The window start rides in the payload as `schedule_occurrence` (also making
distinct windows distinct payloads for the dedup key).

## HTTP front door (httpd.py)

Optional, inside the daemon. GET `/` (dashboard), `/api/status`,
`/api/metrics`, `/api/task/<id>`; POST `/api/emit {name, data, force?}`.
Rules: config refuses non-loopback bind without `token_ref`; when a token is
configured EVERY request must present it (`Authorization: Bearer`); the
token lives in the secrets file (`HTTP_TOKEN_<REF>`), never in pack files;
emits accept only consumed events; each request thread opens its own
connection (WAL + busy_timeout serialize writes next to the workers).

## Record / replay

`runner._finish` archives every schema-valid verdict as
`data/runs/<id>/verdict.json` (canonical JSON, before the `_run_id`
annotation). The `replay` backend answers an ask by sha256(assembled prompt)
lookup in the source root's runs table and returns the recorded verdict as a
fenced block — revalidated against the step schema like any live answer. A
miss (changed prompt/context/schema, or gc'd archive) is
`agent_invalid_output`: bounded fast retries then loud failure, never a
stale answer and never a park-stall in CI. `--replay-from ROOT` rebinds
every pack agent to the replay backend; a per-agent binding
(`backend: replay, source: <root>`) is validated at pack load (the
recording must exist).

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
6. UPDATE runs (exit_code, verdict, wall_ms, reasks, finished_at). Return
   verdict or raise RunnerError(error_class) for the engine to map.

Binding validation is split across three moments, each fail-loud:
- pack load (config._check_agent): STRUCTURE — known backend, per-backend
  key allowlist, required fields, types;
- engine start (runner.check_binding, AFTER any --replay-from wrap):
  ENVIRONMENT — cli resolvable (pinned to an absolute path like pack
  tools), api_key_ref secret present, replay source recorded;
- `llm check` (runner.probe_binding / probe_model): LIVE — one probe
  round-trip per binding proving reachability, auth, model load, and that
  the model can follow the fenced-JSON output contract.

Backend knobs: openai-compat forwards `params:` verbatim into the request
body (temperature, max_tokens, response_format, ... — model/messages stay
authoritative); claude-cli accepts `max_turns` and `extra_args` (static,
from the verified pack file).

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
| retry timing | effective policy arithmetic (POLICY + pack retry:) |
| resume point | task_steps rows, gated by tasks.def_hash |
| who reacts to an event | consumes: lists (loader subscriptions) |
| double-send / double-enqueue | body_sha / payload-hash unique keys |
| schedule firing | window arithmetic vs the persisted cursor watermark |
| join firing | member terminal states + guarded fired_at claim |
| agent answer under replay | prompt_sha lookup in the recorded runs table |
| clock | single `now` per transaction, from the db (`datetime('now')`) |
```
