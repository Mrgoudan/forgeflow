# Changelog

## 0.3.1 — 2026-07-11

### Fixed
- **Init crash window (found by the chaos test in CI)**: a process killed
  between schema creation and the `user_version` stamp left latest-shape
  tables with version 0; the next open re-ran the v2 migration and died on
  a duplicate-column ALTER, bricking the root. Migrations are now
  idempotent (column/table-guarded callables) and run — with the stamp —
  in one transaction, so a `kill -9` anywhere during first start
  self-heals on the next open. Two regression tests pin the exact window.

## 0.3.0 — 2026-07-11

The LLM-experience release. Schema: v3 (`runs.wall_ms`, `runs.reasks`) —
existing roots migrate automatically.

### Added
- **`llm check`** — one live probe per agent binding and per pack model:
  endpoint reachable, auth valid, model loaded, and the model follows the
  fenced-JSON output contract (a model that can't would fail every step as
  `agent_invalid`, so the probe says exactly that). Exit 1 on failure —
  croneable. Replay bindings report available recordings; embedding models
  (API and pinned local weights) are probed too.
- **`llm show ROLE --data '{...}'`** — render the EXACT assembled prompt a
  step would send (base prompt + context sections + output contract) plus
  the `prompt_sha` the runs table would pin.
- **`llm runs`** — recent agent runs: model, verdict, wall time, correction
  rounds. `metrics` (and `/api/metrics`) gain a per-model section: runs,
  no-verdict rate, avg/max latency, re-ask totals.
- **Binding validation in three fail-loud stages**: structure at pack load
  (known backend, per-backend key allowlist, required fields, types);
  environment at engine start after any `--replay-from` wrap (cli resolved
  and pinned like pack tools, `api_key_ref` secret present); live chain via
  `llm check`.
- **openai-compat `params:` passthrough** — temperature, max_tokens,
  response_format, anything: forwarded verbatim (model/messages stay
  authoritative). Covers Ollama, vLLM, llama.cpp, LM Studio, gateways.
- **claude-cli `max_turns` + `extra_args`** — bound the agentic loop and
  pass verbatim CLI flags from the verified pack file.
- **docs/LLM.md** — recipes (Ollama, vLLM, llama.cpp, LM Studio, gateways,
  Claude CLI, record/replay) and the error-class troubleshooting table.

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
