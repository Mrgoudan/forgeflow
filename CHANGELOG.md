# Changelog

## 0.2.0 — 2026-07-10

Schema: v2 (`tasks.def_hash`, `join_groups`, `join_members`) — existing
roots migrate automatically on first open (`PRAGMA user_version`).

### Added
- **Fan-out/join**: `fanout.emit` block (one event per list item + a join
  event that fires exactly once when every spawned task is terminal) and
  `join.collect` (member roster). Crash/retry-safe: re-run parents reuse
  their join group; `retry` of a failed member re-opens its slot in unfired
  groups; `gc` prunes fired groups; `status` shows open joins.
- **Workflow definition versioning**: every task attempt is stamped with the
  definition fingerprint it executes under; a YAML edit under a mid-flight
  task parks it as `definition_changed` (never replays old outcomes through
  a new graph); unpark/retry re-runs fresh under the new definition.
- **Timed triggers**: `schedule:` in project.yaml — events fired once per
  interval window, restart-safe cursor, no catch-up storms, startup error if
  nobody consumes the event.
- **HTTP front door**: `http:` in project.yaml — read-only dashboard at `/`,
  JSON `/api/status`, `/api/metrics`, `/api/task/<id>`, and
  `POST /api/emit`. Bearer-token auth; non-loopback binds require a token
  (from the secrets file, `HTTP_TOKEN_<REF>`).
- **Record/replay for agent steps**: every schema-valid verdict is archived
  as `verdict.json`; the `replay` backend (or `--replay-from ROOT` on any
  command) answers by assembled-prompt hash — deterministic CI without
  models, loud misses on prompt/schema drift.
- **Pack-configurable retry policy**: `retry:` in project.yaml retunes
  engine error classes or defines new ones for pack block outcomes;
  structural (consume) classes are locked.
- **Chaos test**: randomized `kill -9` schedule proving convergence, bounded
  side-effect duplication, and ledger consistency.
- CI (GitHub Actions, Linux + macOS × 3.9/3.11/3.13), MIT LICENSE,
  packaging metadata, `docs/COMING_FROM.md` migration guides.

### Changed
- `unpark`/`retry` semantics documented precisely (both start a fresh
  attempt); `queue.fail`/`parked_due` accept the pack's effective policy;
  Python floor raised to 3.9.

## 0.1.0

Initial engine: YAML workflows over a SQLite queue — totality-validated
loading, atomic claims, step-boundary persistence with crash resume,
policy-driven retries with parking, event-bus orchestration, verified pack
config, agent steps with schema-gated verdicts (claude-cli / openai-compat
backends), local pinned models, worktree blocks, and the ops surface
(status/trace/metrics/doctor/gc/retry/unpark).
