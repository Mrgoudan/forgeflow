# Workflows as data: building blocks, declared context, declared interaction

A workflow is not code. It is a YAML document: step → step → step, where every
step names a **block** (reusable building block), declares what **context** it
needs, which **llm binding** it uses (or none), and where every outcome goes.
Python only implements blocks; workflows are assembled, validated, and
inspected as data.

## Why not LangChain / LangGraph / Temporal

| | what it gives | why not here |
|---|---|---|
| LangChain/Graph | chains/graphs around LLM **API calls**, memory, tools | our LLM step is an agentic CLI in a worktree; the hard parts (evidence gate, pinning, egress, worktrees) aren't in the framework; fast-moving dep in a months-running daemon |
| Temporal | durable replay-deterministic workflows, retries, timers | correct model, heavy runtime (server cluster); our SQLite queue + step persistence is the same shape sized for one box. If we ever outgrow a box, port the *loader*, keep the YAML. |
| Airflow/Prefect | batch DAG scheduling | wrong shape: we're event-driven, long-running, agentic |

The YAML layer is also insurance: workflow definitions are engine-agnostic.

## Anatomy of a workflow definition

```yaml
# forgeflow/workflows/defs/fix.yaml  (built-in; packs may ship their own)
workflow: fix
consumes:                       # what starts me (events = item/task facts)
  - item.triaged
  - comment.fix_request
emits:                          # what I may cause (checked: no undeclared emits)
  - item.pr_open
  - item.deferred
  - item.failed

steps:
  - name: workspace
    block: worktree.create      # building block from blocks.py
    outcomes: { ok: candidate, dirty: failed }   # dirty consumes task (POLICY)

  - name: candidate
    block: agent.run            # THE llm block
    llm: fix                    # -> pack agent.fix {backend, model, api_key_ref}
    context:                    # declared, injected mechanically, all pinned
      - payload
      - lessons: { task_kind: fix }
      - readings: { scope: touched_objects, fresh: prefer }
      - chains:   { scope: touching_region }
      - notes:    { map: pack.knowledge.subsystem_map }
    schema: agent_verdict       # schema gate contract for this step
    timeout_s: 3600
    outcomes:
      FIXED: verify
      DEFERRED: deferred
      BLOCKED: deferred
      NOOP: done
      agent_limit: parked
      agent_invalid: failed
      timeout: failed

  - name: verify                # evidence gate — no llm, no context, no trust
    block: check.suite          # verify commands in order, exit codes only
    params: { checks: [{ name: build, cmd: [...] }, { name: probes, cmd: [...] }] }
    outcomes: { green: publish, red_retryable: candidate, red: deferred, timeout: failed }

  - name: publish
    block: forge.publish_pr     # PACK-SUPPLIED egress block (leak scan +
    outcomes: { ok: done, forge_auth: parked, forge_server: parked }   # body-sha idempotency per ENGINE.md)
```

Loader guarantees (all at startup, none at runtime):
- every `block` exists in the blocks registry and its declared outcome set
  matches the YAML's mapped outcomes exactly (no unmapped, no phantom);
- every `llm:` binding resolves in the pack's `agent:` section, and the
  block's execution class matches the backend class (agentic blocks reject
  text-only backends — the openai-compat/fix rejection generalized);
- every `context:` source exists in the context-provider registry;
- every `consumes`/`emits` event name is a declared transition event;
- the whole graph passes contract totality validation (bounded, terminal).

## Context is declared, never ambient

A step gets EXACTLY what it declares — the runner assembles it, pins it
(prompt_sha covers the assembled result), and archives it. No block reads
the db ad hoc; a block's function signature is `(ctx_slice, task, prev)`.
That makes steps testable in isolation (feed a dict, assert the outcome)
and makes "what did this step know?" a db lookup instead of archaeology.

The engine ships these providers (packs register more, like blocks):

| provider | injects |
|---|---|
| `payload` | the task's event payload |
| `pack` | the pack's params mapping |
| `notes` | declared files, verbatim (missing file = loud failure) |
| `select` | ranked useful rows from a declared corpus — hybrid channels, RRF, dedup/MMR/budget construction, outcome-learned utility (docs/LLM.md) |
| `retrieval` | k-nearest code objects by embedding (the code-comprehension layer) |

## Fan-out / join in YAML

One-per-item parallelism with a barrier is a block, not orchestration code:

```yaml
- name: spread
  block: fanout.emit
  timeout_s: 30
  params:
    name: probe.wanted              # per-item event; must be in emits:
    items: "{prev.candidates}"      # a list
    data: { candidate: "{item}" }   # per-item payload; {item}/{index} resolve
    join:
      event: probe.all_done         # must be in emits:; fires exactly once
      data: { batch: "{payload.batch}" }
  outcomes: { ok: done, empty: done }
```

The join event's payload is `{join_group, total, done, failed, deferred}`
plus `join.data`. The consumer may use `join.collect` to read the member
roster. Semantics live in ENGINE.md ("Fan-out / join").

## Reserved payload keys

The engine owns keys starting with `_` plus the names it injects; workflows
read named keys only and must not fabricate these:

| key | written by | meaning |
|---|---|---|
| `event` | emit_event | the event name that created the task |
| `_force` | emit --force / POST /api/emit | dedup-bypass nonce |
| `_join` | fanout.emit | `{group, index}` join membership |
| `join_group`, `total`, `done`, `failed`, `deferred` | join firing | barrier results |
| `schedule_occurrence` | schedule tick | window start (epoch seconds) |

## How workflows interact: events, not calls

Workflows never invoke each other. `record_transition()` is the event bus:
a transition event (e.g. `item.merged`) is matched against every enabled
workflow's `consumes:` list; matches enqueue tasks — in the same db
transaction, so interaction is atomic, replayable, and visible as data.

The cross-triggers become pure configuration:

```yaml
# hunt.yaml
consumes: [item.merged]           # variant-hunt on every merged fix
# review.yaml
consumes: [pr.opened, pr.updated]
# fix.yaml
consumes: [item.triaged, comment.fix_request]
```

Adding an interaction = adding one line to a `consumes:` list. Removing a
workflow can't strand others — unconsumed events are simply facts in the log.

## The blocks library (blocks.py) — the only Python a workflow touches

| block | class | outcomes |
|---|---|---|
| `shell.run` | local | ok, nonzero, mismatch, timeout |
| `worktree.create` / `worktree.drop` | local | ok, dirty/error, timeout |
| `git.branch` / `git.fold_commit` / `git.branch_advanced` | local | ok/…, error, timeout |
| `agent.run` | llm (agentic or text by binding) | schema enums + agent_limit/agent_invalid/agent_backend/timeout |
| `check.suite` | local | green, red_retryable, red, timeout |
| `check.recheck` | local | confirmed, refuted, timeout |
| `db.upsert_item` / `db.transition` | state | ok |
| `event.emit` | state | ok |
| `fanout.emit` | state | ok, empty |
| `join.collect` | state | ok |
| `scan.grep_rules` | local, no-AI | ok, timeout |
| `model.embed` / `model.classify` | local | ok(/error) |

(Forge egress blocks — PR/comment posting with leak scanning and body-sha
idempotency — are pack-supplied; the ENGINE.md egress mechanics section
defines the contract they must follow.)

New capability = new block (Python, tested once) → immediately available to
every workflow YAML. New process = new YAML → no Python at all.
