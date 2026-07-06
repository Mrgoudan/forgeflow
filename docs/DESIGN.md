# forgeflow — design

One long-running daemon that integrates **bug hunt, auto fix, and PR review** for
any project hosted on **GitCode**, with all method assets (probes, code notes,
lessons) carried over as versioned, machine-injected inputs.

Project-specific behavior lives entirely in a **pack** (`packs/<name>/`); the BSC
compiler is the first pack, not a special case. A pack declares its capabilities
(build? corpus? probes?) and workflows run the applicable subset — a plain library
or service repo gets evidence-gated review and hunt without the compiler stages.
Every workflow is callable **one-shot from the CLI** against the same state store;
the daemon only adds scheduling, event intake, and retry timers (see daemon.py).

## The problem with the predecessors

`autofix` (bash pipeline), `pr_monitor` (python pollers), and `autotest` (hunt corpus)
work, but decisions ride on nondeterministic surfaces:

- Agent **prose parsed as control flow** (defer-verdict grep, `#fix` magic tokens).
- **Substring error detection** ("rate limit" in review text froze a monitor for 5 h).
- **Nothing pinned** — no model/prompt/knowledge revision recorded per run.
- **Silent config fallback** (issue monitor ran months with the PR prompt).
- **Hand-maintained knowledge** that drifts (TRIAGE.md, ai_prs.txt parsed 4 ways).

## Doctrine: the system is deterministic, the LLM is not

The LLM has exactly one role: **generate a candidate** (a fix, a review finding, a
triage). Its output can influence control flow only through two gates:

1. **Schema gate** (`runner.py`) — every agent invocation must return JSON valid
   against a schema in `schemas/`. Invalid → bounded re-ask → clean task failure.
   Prose is never parsed. Ever.
2. **Evidence gate** (`evidence.py`) — no agent claim transitions state without a
   deterministic check confirming it: build exit code, probe outcome diff, corpus
   result, `git rev-parse`. "The agent said FIXED" is not an event; "ninja exited 0
   and probes flipped" is.

Everything around the gates is boring machinery:

- **State machine per finding** (`db.py`):
  `found → triaged → fixing → verifying → pr_open → in_review → merged | deferred | failed`.
  Transitions only via defined events. Every transition appended to an audit log.
- **Pinned tuple** recorded on every agent run: model id, prompt sha256, pack
  revision, base sha, compiler build id, probe-set revision. Replays are comparable;
  divergence is attributable.
- **Error classification by exit code / HTTP status only** (`queue.py` policy table).
  Output text is logged, never matched.
- **Fail-loud config** (`config.py`) — schema-validated at startup; a missing prompt
  file kills the daemon with a message, it does not borrow another file.
- **Single egress choke point** (`egress.py`) — every byte leaving for the forge
  (comment, PR body, title) passes one `post()` with an audit log and an asset-leak
  scan. Workflows cannot call the forge write-API directly.

## Retry policy (queue.py)

| error class            | detection            | policy                                   |
|------------------------|----------------------|------------------------------------------|
| forge_4xx_auth         | HTTP 401/403         | exp backoff, max 20 attempts, then park   |
| forge_5xx / network    | HTTP 5xx / timeout   | exp backoff, max 10                       |
| agent_limit            | CLI exit code + stderr class from runner | park task until window reset; task NOT consumed |
| agent_invalid_output   | schema validation    | re-ask ×2, then fail task                 |
| verify_red             | evidence gate        | 1 retry, then defer with reason           |
| workspace_dirty        | git status           | consume task, alert (the "#883 looped 5000×" lesson) |
| agent_noop             | schema verdict NOOP  | done, post nothing (no-false-comment invariant) |

A parked task never blocks the daemon; the queue is per-task.

## Workflows (plugins over the same state machine)

- **bughunt** — scheduled probe sweeps + generator runs against the base compiler;
  outcome diffs become `found` rows. Deterministic core; agent only for triage.
- **autofix** — consumes `triaged` rows or fix-request comments; agent generates a
  branch; evidence gate verifies; egress opens the PR. One commit per fix,
  `--force-with-lease`, remote-head confirmation before any comment.
- **review** — PR event → build PR head in a worktree → **probe sweep diff vs base**
  (machine findings, zero LLM) → agent findings, each requiring a runnable repro →
  refutation pass → only survivors post. Silence is a valid outcome.

Cross-triggers (the 1+1>2) are rows-begetting-rows rules:
merged fix → variant-hunt task; confirmed bug class → review lens; review-confirmed
defect on an AI PR → autofix task.

## Method assets

| asset | destination |
|---|---|
| `sem_tests/probes/` (192 cases) | the oracle; versioned probe-set, used by all three workflows |
| `sem_tests/code_notes/` | per-subsystem agent context, injected mechanically by touched-path mapping |
| `KNOWLEDGE.md` lessons table | `lessons.jsonl` (schema'd), injected per task type, appended by learn events |
| `TRIAGE.md` | deleted as a file; generated view over the findings DB |
| `bug_log.md`, `findings.jsonl` | one-time import into `patterns.db` (`scripts/import_legacy.py`) |
| `gen*.py`, `glm_hunt/` | hunt plugins scheduled by the daemon |
| prompts / agent defs | pack files, sha256-pinned per run |
| verify/rebuild/repro scripts | wrapped as-is by the evidence gate |

## Forge

GitCode only for now (`forge/gitcode.py`): API base `https://api.gitcode.com/api/v5`,
auth header `PRIVATE-TOKEN: <pat>` (NOT gitee's `access_token` param), webhook
signature `X-GitCode-Signature-256: sha256=…` (GitHub-compatible HMAC).
The `Forge` interface is 9 methods; a second forge is a second file, no core changes.

## Non-goals (for the skeleton)

Multi-forge, multi-tenant auth, distributed workers, web-exposed dashboard.
The board binds 127.0.0.1 and the daemon is one process with SQLite. Scale problems
are good problems for later.
