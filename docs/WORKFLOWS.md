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
consumes:                       # what starts me (events = finding/task facts)
  - finding.triaged
  - comment.fix_request
emits:                          # what I may cause (checked: no undeclared emits)
  - finding.pr_open
  - finding.deferred
  - finding.failed

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
    block: evidence.suite       # branch_advanced + build + probes + corpus
    params: { checks: [branch_advanced, build, probe_sweep, corpus, path_allowlist] }
    outcomes: { green: publish, red_retryable: candidate, red: deferred }

  - name: publish
    block: publish.pr           # fold-one-commit, push --force-with-lease,
    outcomes: { ok: done, forge_auth: parked, forge_server: parked }
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

## How workflows interact: events, not calls

Workflows never invoke each other. `record_transition()` is the event bus:
a transition event (e.g. `finding.merged`) is matched against every enabled
workflow's `consumes:` list; matches enqueue tasks — in the same db
transaction, so interaction is atomic, replayable, and visible as data.

The cross-triggers become pure configuration:

```yaml
# bughunt.yaml
consumes: [finding.merged]        # variant-hunt on every merged fix
# review.yaml
consumes: [pr.opened, pr.updated]
# autofix.yaml
consumes: [finding.triaged, comment.fix_request]
```

Adding an interaction = adding one line to a `consumes:` list. Removing a
workflow can't strand others — unconsumed events are simply facts in the log.

## The blocks library (blocks.py) — the only Python a workflow touches

| block | class | outcomes |
|---|---|---|
| `worktree.create` / `worktree.drop` | local | ok, dirty |
| `agent.run` | llm (agentic or text by binding) | schema enums + agent_limit/agent_invalid/timeout |
| `evidence.suite` / `evidence.check` | local | green, red_retryable, red |
| `publish.pr` / `publish.comment` | egress | ok, forge_auth, forge_server, leak_blocked |
| `db.upsert_finding` / `db.transition` | state | ok |
| `scan.grep_rules` | local, no-AI | ok(candidates) |
| `oracle.reproduce` | local | confirmed, refuted |

New capability = new block (Python, tested once) → immediately available to
every workflow YAML. New process = new YAML → no Python at all.
