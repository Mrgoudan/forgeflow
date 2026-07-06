# How the system is invoked, and how it invokes the AI

Two separate questions with two separate answers:

1. **What wakes the system up** — events and schedules (never the AI).
2. **When and how the AI runs** — exactly one place in the codebase executes
   the `claude` CLI: `runner.run_agent()`. Everything else is deterministic
   machinery around it.

---

## 1. System entry points

```
                       ┌─────────────────────────────────────────────┐
  GitCode webhook ────▶│                                             │
  (PR opened/updated,  │  events.py: normalize → enqueue task rows   │
   comment posted)     │                                             │
                       │  every event becomes a tasks row; nothing   │
  poll fallback ──────▶│  is acted on inline                         │
  (watermark cursor,   │                                             │
   poll_interval_s)    └──────────────────┬──────────────────────────┘
                                          │
  schedule ──────────────▶ enqueue hunt tasks (e.g. nightly sweep)
  (daemon timer table)                    │
                                          ▼
  operator CLI ─────────────▶ ┌────────────────────────┐
  forgeflow hunt              │  state/forgeflow.db     │
  forgeflow fix --finding F40 │  tasks: pending ──────┐ │
  forgeflow review --pr 17    └───────────────────────┼─┘
  (one-shot: enqueue + run                            │
   that one task, exit)                               ▼
                                     ┌────────────────────────────┐
  board action ──────────────────▶   │ daemon loop:               │
  (retry-now / defer / kill)         │   task = queue.claim()     │
                                     │   dispatch by task.kind    │
                                     └────────────────────────────┘
```

Rules:

- **Everything is a task.** A webhook, a human CLI call, a schedule tick, and a
  cross-trigger all just insert `tasks` rows. There is one execution path.
- **Webhook bodies are verified** (HMAC, `forge.verify_webhook`) or dropped —
  the poll fallback guarantees nothing is lost by dropping.
- **The daemon is optional.** `forgeflow review --pr 17` claims and runs that
  task in-process against the same db — identical code path, no daemon needed.
- Single-flight: one daemon per state dir (flock). One-shot CLIs may run
  alongside; SQLite transactions arbitrate.

## 2. Task dispatch → where the AI does (and does not) run

```
queue.claim() → task
  │
  ├─ kind=hunt    → bughunt.run_once()
  │     probes + generators against base compiler   ← NO AI
  │     outcome diffs → findings rows               ← NO AI
  │     new raw finding needs severity/pattern?     ← AI (triage)
  │
  ├─ kind=fix     → autofix.run_once()
  │     worktree setup, base checkout               ← NO AI
  │     produce candidate branch                    ← AI (the fix itself)
  │     branch_advanced? build? probes? corpus?     ← NO AI (evidence gate)
  │     fold commit, push --force-with-lease        ← NO AI
  │     PR create / success comment via egress      ← NO AI
  │
  ├─ kind=review  → review.run_once()
  │     worktree checkout of PR head, build         ← NO AI
  │     probe sweep head vs base → machine findings ← NO AI
  │     read diff, propose findings w/ repro cmds   ← AI (review)
  │     execute each repro_cmd, drop non-reproducing← NO AI (evidence gate)
  │     refutation pass per surviving finding       ← AI (skeptic)
  │     compose + post ONE comment via egress       ← NO AI
  │
  └─ kind=learn   → lessons append from human instruction ← AI (distill)
```

The AI generates candidates (a fix, findings, a triage, a lesson). It never
pushes, posts, transitions state, or decides retries.

## 3. Inside `runner.run_agent()` — the only AI call site

```
┌─ 1. ASSEMBLE PROMPT (deterministic) ─────────────────────────────────┐
│  pack prompt file (vault/bsc/prompts/<kind>.md)          [sha256'd]  │
│  + lessons rows WHERE task_kind = <kind>                             │
│  + readings digests for code_objects the task touches (sha-fresh    │
│    ones as facts, stale ones marked "re-verify")                     │
│  + code_notes mapped from touched path prefixes (pack subsystem_map) │
│  + task payload (finding detail, repro, instruction)                 │
│  + output contract: "final message MUST be a ```json block valid     │
│    against <schema>"                                                 │
└──────────────────────────────────────────────────────────────────────┘
┌─ 2. PIN (before anything runs) ──────────────────────────────────────┐
│  INSERT runs(model, prompt_sha, pack_rev, vault/probe_rev,           │
│              base_sha, build_id)                                     │
│  a crash now still leaves an attributable record                     │
└──────────────────────────────────────────────────────────────────────┘
┌─ 3. EXECUTE ─────────────────────────────────────────────────────────┐
│  cd workspaces/<task-id>   (dedicated git worktree)                  │
│  claude -p <promptfile --via stdin> \                                │
│         --permission-mode bypassPermissions \                        │
│         --model <pack.agent.model>                                   │
│  timeout from pack; stdout/stderr → data/runs/<run_id>/              │
└──────────────────────────────────────────────────────────────────────┘
┌─ 4. CLASSIFY (exit code + CLI stderr structure, never model prose) ──┐
│  CLI auth/usage-limit exit      → error_class=agent_limit → park     │
│  nonzero exit otherwise         → retry per policy                   │
└──────────────────────────────────────────────────────────────────────┘
┌─ 5. SCHEMA GATE ─────────────────────────────────────────────────────┐
│  extract LAST ```json block → jsonschema validate                    │
│  invalid/missing → re-ask (≤2) → agent_invalid_output                │
│  valid → UPDATE runs SET verdict, exit_code                          │
│  return verdict dict to the workflow                                 │
└──────────────────────────────────────────────────────────────────────┘
```

The verdict dict is a *claim*. The workflow hands it to the evidence gate:

```
verdict.verdict == "FIXED"
      │
      ▼
evidence.branch_advanced(base_sha, branch)     git rev-list, exit code
evidence.build(pack, workspace)                ninja, exit code
evidence.probe_sweep + probe_diff              oracle files, no grep heuristics
evidence.corpus(pack, workspace)               check-clang-bsc, exit code
evidence.commit_path_allowlist                 diff paths vs pack allowlist
      │
      ├─ all green → record_transition(finding, 'verifying'→'pr_open',
      │              event='evidence:all_green', evidence={...}, run_id)
      └─ any red   → transition to fixing-retry / deferred per policy
```

Only `record_transition()` moves a finding. Only `egress.post()` talks to the
forge. Only `run_agent()` talks to the model. Three choke points; everything
audited.

## 4. Worked example: `#fix`-style request end to end

```
human posts structured fix-request comment on PR #17
  → webhook POST (or poll) → verify HMAC → watermark check (comment id new?)
  → allowlist check (author may request fixes?)                    NO AI
  → INSERT tasks(kind=fix, payload={pr:17, instruction, target_sha})
  → daemon claims task → worktree on PR branch                     NO AI
  → run_agent(fix): assemble → pin → claude -p → schema gate       AI
  → verdict FIXED, branch advanced → evidence gate all green       NO AI
  → fold to one commit → push --force-with-lease
  → confirm remote head == local head                              NO AI
  → egress.post(): leak scan → archive → ONE success comment
    with protocol block {v:1, kind:fix-done, target_sha:...}       NO AI
  → record_transition(... 'in_review', event='egress:fix_done')
  every other outcome: logged, silent, retried/parked per POLICY table
```
