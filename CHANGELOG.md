# Changelog

## 0.8.0 — 2026-07-16

### Added
- **Runs**: `board.thread_key` names the payload field that correlates one
  raw request across every workflow. The front page is rebuilt around it:
  active runs render as live SVG pipeline graphs (nodes = workflow kinds
  from the emits→consumes orchestration map, per-node state + step
  progress, needs-you badge, flowing edge), finished runs collapse, ops
  tables move to `/explore`.
- **Humane decisions**: `decisions.context` column (schema v7) — a
  human-readable situation report rendered AT the decision point
  (markdown-lite, escaped). `/decisions` rework: click-to-choose option
  cards (one confirm), a single "Reject all & regenerate" action with an
  optional message (= revise); reframe/abandon demoted to quiet links. No
  raw JSON on user surfaces.
- **Entity views**: `board.views` — pack-declared parameterized pages
  (`/view/<name>?key=...`); panel SQL binds query-string args; a column
  aliased `link:<view>` renders as a cross-link (tables + status grids).
- **Run audit**: `/run/<thread>` — the complete story of one request:
  every task oldest-first, every attempt/step with block name, in/out and
  run artifacts, every decision round, every event.
- **Launch forms**: `board.launch` — pack-declared start forms
  (`POST /api/launch`); `path_or_text` fields read the file when handed a
  readable path; `on_view` attaches a form to an entity view with `{key}`
  prefilled (the "change this function" hook); `hidden` field kind.

## 0.7.3 — 2026-07-12

### Fixed
- **JSON-mode endpoint compatibility** (found driving a local qwen2.5-coder
  via Ollama): `extract_verdict` now accepts a raw-JSON response body, not
  only a ```json fenced block. Models run with
  `response_format: {type: json_object}` return bare JSON — the reliable
  way to get structured output from small local models — which previously
  failed as `agent_invalid`. Fenced blocks still win when present (agentic
  CLIs narrate around them); a bare ``` wrapper is also tolerated.


## 0.7.1 — 2026-07-11

Individually configurable, verified. Two stages lacked an off switch:

### Added
- `dedup: false` on a select step (identical texts may then occupy
  multiple slots; default stays on).
- `track: false` on a corpus — selection history is never recorded for
  that table (`context_uses` untouched, utility channel abstains): the
  data-governance switch.
- docs/LLM.md: the kill-switch table — every selection stage and how to
  disable or tune it independently.

## 0.7.0 — 2026-07-11

Payload assembly, governed and reviewable. No schema change.

### Added
- **Steps are bounded end to end**: context assembly (providers may call
  models) is deducted from the block's budget; assembly alone overrunning
  `timeout_s` is the step's `timeout` outcome; provider-side model calls
  (rerank, summaries, API embeddings) cap themselves against the step's
  remaining budget.
- **Context manifest**: every agent run writes `context.json` beside its
  archived prompt — per-section provider, spec, bytes, sha.
- **Total context budget**: `params: { max_context_bytes: N }` on llm
  steps; breach fails loudly with per-section sizes, before any model
  call; validated at load.
- **`llm show --task ID [--step NAME]`**: full-fidelity payload preview —
  every declared provider resolved against live db state, manifest and
  budget check printed, in preview mode (`env.preview`: no ledger writes,
  no model calls — real tasks stay clean).

### Changed
- The selection cascade moved to its own module (`forgeflow/select.py`);
  `contract.py` again holds only the execution contract.

## 0.6.0 — 2026-07-11

A local model in the selection loop (the Anthropic-cookbook patterns:
summary-indexed retrieval, contextual enrichment, rerank). Schema: v6
(`corpus_summaries`) — existing roots migrate automatically.

### Added
- **`summarize_with:` on a corpus** — an agents: role (typically a local
  Ollama/llama.cpp model) condenses rows longer than the step's
  `max_chars` instead of blind truncation (`summarized: true`); cached by
  `text_sha`, generated lazily for selected rows only, and fed into the
  lexical channel so long rows stay findable. Engine-supplied contract —
  no pack prompt needed.
- **`rerank: {llm, window?, timeout_s?}` on a select step** — a bounded
  judge call scores the top window for usefulness-to-this-task and
  reorders it before dedup/MMR/budget; per-entry scores ride in the
  output. Falls back to fused order on any failure (`reranked: false` +
  `rerank_error`), never fails the step.
- Both paths run through `run_agent`: pinned, archived, visible in
  `llm runs`/`metrics`. Previews (`llm show`) never trigger model calls.

## 0.5.0 — 2026-07-11

From relevant to USEFUL: selection now runs the construction pipeline the
coding-agent industry converged on (select → prioritize → filter →
assemble). Schema: v5 (`context_uses`) — existing roots migrate
automatically.

### Added
- **Multi-query fusion**: `query:` accepts a list; each query votes per
  relevance channel at weight/n (RAG-fusion pattern) — the task's title
  and its error text each get a say.
- **Dedup**: identical texts never occupy two slots; the better-ranked
  twin wins; collapsed count reported (`deduped`).
- **Diversity (MMR)**: `diversify:` (default 0.5 ≈ classic λ 0.67) trades
  relevance against redundancy so k slots cover the task's ground instead
  of repeating the top hit; 0 restores pure ranked order.
- **Budget packing**: `max_bytes:` packs entries in final order;
  `dropped:` counted, never silent.
- **Outcome-learned utility channel**: the engine records what each task
  was shown (`context_uses`); rows co-occurring with `done` tasks of the
  same kind outrank rows co-occurring with `failed` (Laplace-smoothed,
  neutral cold start, abstains without history, previews never pollute
  the ledger). The acceptance-signal loop, auto-labelled from the engine's
  own audit trail.

## 0.4.1 — 2026-07-11

### Fixed (found by the new recall calibration suite)
- **Tie ranking**: equal scores within a channel now share a fractional
  (average) rank. Previously ties broke by key order, letting arbitrary
  distractors inherit strong ranks and outvote true matches through RRF.
- **Prior weighting**: recency/importance channels default to weight 0.3
  (relevance channels stay 1.0) — priors decide among relevance ties but
  can no longer lift fresh-or-important-but-irrelevant rows over matches.

### Added
- `tests/test_recall.py`: frozen golden-set recall calibration (six
  adversarial categories, distractor sea) asserting recall floors in CI —
  including the honest expectation that the hashing embedder fails pure
  paraphrase and that a semantic model plugged into the same corpus
  recovers it. Measured results documented in docs/LLM.md.

## 0.4.0 — 2026-07-11

Context selection over your own data. Schema: v4 (`corpus_embeddings`) —
existing roots migrate automatically.

### Added
- **Corpora**: declare any table/view as selectable
  (`corpora: {name: {table, text, key?, ts?, weight?, embed_with?}}`);
  table/column existence checked at engine start.
- **`select:` context provider** — for each task, pick the most
  relevant/important corpus rows: SQL metadata pre-filter (`filter:`),
  independent ranking channels (identifier-aware lexical, optional
  semantic, recency, importance prior, `boost:` link matching) fused with
  Reciprocal Rank Fusion; channels with no signal abstain. Deterministic,
  explainable (per-entry fused score + channel ranks), `include_all_under`
  bypasses ranking for small corpora, oversized entries truncate with an
  explicit flag. Design choices are grounded in published production-RAG
  results — see docs/LLM.md.
- **`hashing` embedder** (`models: {fast: {hashing: {dim: 256}}}` or
  `embed_with: hashing`): deterministic, stdlib-only, zero-setup — rows
  never leave the machine to become searchable. Real weights or an
  `/embeddings` endpoint remain pluggable per corpus.
- **Incremental vector maintenance**: rows are (re)embedded at query time
  only when new or changed (`text_sha` pin), stored in `corpus_embeddings`.

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
