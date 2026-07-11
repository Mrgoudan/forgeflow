# Total execution contract

**The script is the framework. The LLM is a fallible subroutine inside it.**
Every workflow must run to a defined terminal state regardless of LLM
nondeterminism — wrong output, garbage output, refusals, hangs, rate limits,
or the model being entirely unreachable. LLM behavior may reduce *yield*
(fewer findings, fewer fixes); it may never reduce *integrity* (hung tasks,
undefined states, corrupted rows, skipped audit records).

## The four totality rules

Enforced by `forgeflow/contract.py` — workflows are written as steps; the
step runner refuses anything that can't satisfy these:

1. **Bounded** — every step has a timeout and an attempt cap, and the
   budget covers the step END TO END: context assembly (providers may
   call models) is deducted from the block's budget, and assembly alone
   overrunning it is the step's `timeout` outcome. Agent re-asks
   are bounded (≤2). Rounds are bounded (K dry). Retries are bounded
   (queue.POLICY). There is no unbounded loop anywhere whose exit condition
   depends on LLM output.

2. **Typed outcomes, total dispatch** — a step returns exactly one of a
   declared outcome set (e.g. `ok | agent_limit | agent_invalid | red |
   timeout`), and the workflow declares a transition for EVERY outcome.
   An outcome without a mapped transition is a startup error, not a runtime
   surprise. "The model said something weird" is not an outcome — it maps to
   `agent_invalid` at the schema gate, always.

3. **Persisted at step boundaries** — each completed step writes its result
   row before the next begins. Process death (OOM, reboot, kill -9) resumes
   at the last boundary via the queue; a crashed agent leaves its pinned
   `runs` row as evidence. Restart is replay-safe because every step is
   idempotent (plan_id / watermark / findings.key / egress body-sha dedup).
   Replay is additionally gated by the workflow definition fingerprint
   (`tasks.def_hash`): recorded outcomes never replay through a CHANGED
   dispatch graph — a mid-flight task whose YAML changed parks as
   `definition_changed` and re-runs fresh under the new definition on
   unpark (ENGINE.md "Definition versioning").

4. **Terminal in bounded steps** — every task provably reaches
   `done | failed | parked | deferred`. `parked` is a *defined* state
   (human-visible on the board, resumable), not a hang. Nothing waits on the
   LLM in a blocking loop — a parked task frees the worker immediately.

## Degraded mode: what runs with the LLM completely down

Each workflow has a NO-AI core that keeps running and producing value when
every agent call parks. This is the difference between "the AI pipeline is
down" and "the system is down" — the system is never down.

| workflow | no-AI core (always runs)                                    | AI stages (degrade to park) |
|----------|-------------------------------------------------------------|------------------------------|
| bughunt  | probe sweeps + generator runs vs base; outcome diffs become findings; coverage ledger updates | lens agents, triage of raw findings |
| review   | PR head build (red build = posted finding via egress); probe sweep head-vs-base (flips = machine findings) | diff-reading findings, refutation pass |
| autofix  | queue intake, dedup, watermarks; branch/PR state reconciliation from forge | the fix candidate itself |
| learn    | instruction capture into `.../learn` records                | distillation into lessons rows |
| board    | fully functional (reads db only)                            | — |

When the LLM returns: parked tasks are re-eligible in claim order; nothing
was lost, nothing must be reconstructed, no human intervention required.

## What this forbids (learned the hard way in the predecessors)

- A 5-hour `time.sleep` inside a polling loop (blocks everything; here a
  parked task blocks nothing).
- Retrying a task forever because a checkout fails every time (bounded +
  `consume_task` for permanent errors — the "#883 looped 5000×" incident).
- A workflow whose next action depends on grepping agent prose (outcome sets
  are closed; prose is archived, not routed on).
- Distinguishing "agent chose to do nothing" from "agent never ran" by
  guessing: NOOP is a schema verdict; agent-never-ran is an exit-code class.
  Different outcomes, different transitions, both defined.
