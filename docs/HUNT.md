# Deterministic multi-agent bug hunt

Faithful port of the proven execution model in
`legacy/autotest-sem_tests/design/SCHEME.md` (Conductor / Explorer / Exploiter,
explore-vs-exploit, two modes, oracle bench, region saturation) — re-hosted on
the platform's totality rules so the dynamism survives without the
nondeterminism.

## Role mapping

| SCHEME.md role | forgeflow implementation |
|---|---|
| **Conductor** (main thread: spawn, validate, distribute) | not an agent — it is the daemon + dispatch rules. Validation = the merge oracle; distribution = pure functions of db state; tracking-docs = db rows |
| **Explorer** (find NEW root cause, ≤8 probes, disjoint region) | `hunt_explore` task. Region lease in `regions` table enforces disjointness. Budget in payload. Outcome enum: `CONFIRMED_NEW \| NO_NEW_PATTERN \| SATURATED` |
| **Exploiter** (blast radius of one confirmed pattern, ≤8 variants) | `hunt_exploit` task, spawned by cross-trigger on a verified CONFIRMED_NEW. Per-variant enum: `FOLDED \| SHAPE_REJECTED \| INCONCLUSIVE \| DISTINCT` |
| **Oracle-Scout / Oracle-Trial** (invent + sandbox-score new detection methods) | `oracle_scout` / `oracle_trial` task kinds writing to the `methods` bench; trial runs are report-only (never create findings directly) |

## Dynamic dispatch, deterministically

SCHEME.md's conductor is event-driven (auto-swap on every return, spawn
Exploiter on every confirm). That dynamism is kept — what changes is *how
decisions are made*:

- **Every dispatch decision is a pure function of db state at claim time.**
  When an explorer task completes, its completion transaction enqueues the
  replacement: next region = first by (never-explored, then oldest-explored;
  cooldown regions excluded; ORDER BY stable keys). Same db state ⇒ same
  choice. No round barrier — completion IS the trigger (auto-swap).
- **Concurrency cap = 6 explorers** (pack), enforced by counting leased
  regions. Exploiters don't count against it (SCHEME.md rule) but have their
  own cap.
- **Decisions are serialized** through the single-writer queue, so there is a
  total order of dispatch decisions; replaying the event log reproduces them.

```
explorer returns CONFIRMED_NEW ──▶ merge oracle re-runs repro (validate)
    │ pass: findings row + patterns row (+grep_rule if class-shaped)
    │       + enqueue hunt_exploit(pattern)          ← spawn-exploiter rule
    │       + enqueue hunt_explore(next region)      ← auto-swap rule
    └ fail: archived; enqueue hunt_explore(next region)   ← auto-swap anyway

explorer returns NO_NEW_PATTERN / SATURATED
    ──▶ regions.dry_streak += 1
        streak >= 3 ⇒ cooldown_until_round = round + C   ← shift-subsystem rule
        enqueue hunt_explore(next region outside cooldown)
```

## Explore vs Exploit stance

Institutionalized exactly as SCHEME.md: Explorer is the home of explore
(new region, low yield, high value — new classes), Exploiter the home of
exploit (same-source sites, high yield, mostly FOLDED variants that map
breadth before one canonical filing). The stance balance is not a knob —
it's emergent from the two rules above: every confirm spawns one exploiter;
every return spawns one replacement explorer.

## Two modes, both fed to every Explorer

- **Mode 1 — function reading.** The explorer must write function notes
  BEFORE probing (SCHEME.md hard rule): `readings` rows whose `facts` follow
  the note schema — invariant (one sentence), peers (must-agree functions),
  3 ranked candidates under the three lenses (reachability / symmetry /
  composition). Enforced by the hunt_explore output schema: probes without
  accompanying notes are rejected at the schema gate.
- **Mode 2 — chain tracing.** `chains` table = the curated `_chains.md`
  ported: node list, per-hop invariants, ranked hops, sha-pinned. NOT a call
  graph and NOT auto-indexed (that stays rejected): chains are added only by
  the two SCHEME.md triggers — a filed bug whose root spans ≥2 functions, or
  a peer relationship surfaced by a function read.
- **Cross-feeding:** a function read surfacing a peer relation emits a
  chain-candidate row; a chain hop surfacing an unread function emits it
  into `code_objects` for the coverage ledger. Both are merge-step outputs
  (db writes), not agent side-effects.
- Prompt assembly for an explorer therefore injects: its region's sha-fresh
  readings + active chains touching the region + applicable lessons +
  the defect-class playbook (patterns with grep_rule).

## The oracle: gate + bench (two different things)

1. **The verification oracle (invariant, non-negotiable):** a candidate
   becomes a finding only if its repro reproduces against the base compiler,
   classified by exit code + expected files. This gate never rotates.
2. **The detection-method bench (`methods` table):** how candidates get
   *generated* — invariant-probe, metamorphic verdict-flip, cross-version
   diff, coverage-gap, … Rotation is a deterministic bandit: each round the
   conductor computes a UCB-style index from (verified_yield, trials,
   current_round) and picks argmax, ties by id. `oracle_scout` proposes new
   bench entries (open-ended); `oracle_trial` scores them sandboxed and
   report-only. Scores update ONLY from gate-verified findings — a method
   cannot bluff its way up the bench.

## Saturation heuristics (layered, all counted in db)

| level | signal | response |
|---|---|---|
| region | `dry_streak >= 3` | cooldown; dispatch shifts to fresh subsystem |
| mode | mode's candidates all folding | widen the set (new chains / new functions) — SCHEME.md: saturation is not retirement |
| method | bench index decays with trials at flat verified_yield | bandit rotates to next method |
| campaign | all regions cooling + bench flat (K dry rounds) | hunt task not re-enqueued; board shows "phase-3 saturated" |

## No-AI finders (run before any agent, always)

- probe regression sweep (oracle files, exit codes)
- generator corpus runs (`gen*.py`)
- **playbook grep scan**: every `patterns.grep_rule` over new/changed code —
  the C1–C4 defect classes are grep-checkable by design; hits become
  candidates that still pass the verification oracle before becoming findings.

Everything above obeys EXECUTION.md: every task kind has a closed outcome
set, bounded budgets (≤8 probes per explorer/exploiter), persisted step
boundaries, and terminal states. The LLM decides only *what to propose*;
never what counts, where next, or when to stop.
