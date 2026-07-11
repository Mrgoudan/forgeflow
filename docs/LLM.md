# LLM steps: setup, recipes, operations

Everything about getting a model behind `agent.run` — CLI agents, hosted
APIs, locally-served models — and running it like infrastructure instead of
a demo. The engine's stance everywhere: *the model may reduce yield, never
integrity* (EXECUTION.md).

## The three checks, in order

```bash
forgeflow --pack P --root R validate     # structure: bindings well-formed,
                                         # prompts/schemas resolve, outcomes total
forgeflow --pack P --root R llm check    # LIVE: one probe round-trip per agent
                                         # binding and per embedding model
forgeflow --pack P --root R llm show fix --data '{"bug": "demo"}'
                                         # the EXACT prompt a step would send + its sha
```

`validate` (and engine start) catches structure and environment: unknown
backend, wrong keys for a backend, missing `base_url`/`model`, a `cli:` not
on PATH, an `api_key_ref` with no secret behind it. `llm check` proves the
live chain: endpoint reachable, auth accepted, model loaded, **and the model
can follow the fenced-JSON output contract** — a model that can't will fail
every agent step as `agent_invalid`, so the probe reports it as a failure
with that exact explanation. Exit code 1 on any failure (croneable).

## Recipe: Ollama (local)

```bash
ollama serve && ollama pull qwen3   # any chat model you like
```

```yaml
agents:
  triage:
    backend: openai-compat
    base_url: "http://127.0.0.1:11434/v1"
    model: qwen3
    params: { temperature: 0 }      # passed through to the request verbatim
```

No API key needed. `llm check` should answer in well under a second once
the model is warm; the first probe may be slow (model load) — that's worth
knowing before the daemon hits it at 3am, which is the point of the probe.

## Recipe: vLLM / llama.cpp server / LM Studio (local)

All three speak the same protocol on `/v1`:

```yaml
agents:
  fix:
    backend: openai-compat
    base_url: "http://127.0.0.1:8000/v1"      # vLLM default
    # base_url: "http://127.0.0.1:8080/v1"    # llama.cpp server
    # base_url: "http://127.0.0.1:1234/v1"    # LM Studio
    model: whatever-you-served
    params: { max_tokens: 2048 }
```

Small local models often fumble the fenced-JSON contract. Two mitigations,
in order of preference: pick a model that follows instructions (run
`llm check` — it tells you immediately), and remember the engine already
re-asks twice with a correction message before failing the step as
`agent_invalid`. If your server supports constrained output,
`params: { response_format: { type: json_object } }` passes straight
through.

## Recipe: hosted API / gateway (OpenRouter, together, any proxy)

```yaml
agents:
  fix:
    backend: openai-compat
    base_url: "https://your-gateway.example/v1"
    model: some-model-id
    api_key_ref: GATEWAY          # -> LLM_API_KEY_GATEWAY in the secrets file
```

```bash
chmod 600 ~/.config/forgeflow/secrets.env
echo 'LLM_API_KEY_GATEWAY=sk-...' >> ~/.config/forgeflow/secrets.env
```

The key reaches only the HTTP header — never pack files, argv, logs, or the
archived request snapshot. A missing secret refuses engine start (never
"starts, then parks at 2am"); HTTP 401/403/429 at runtime park the task as
`agent_limit` and the daemon health-probes before retrying.

## Recipe: agentic CLI (tools, worktree cwd)

For steps that need an agent that *does things* (edit files in a task's
worktree, run commands) rather than answer text:

```yaml
agents:
  fix:
    backend: claude-cli
    model: your-model-id
    max_turns: 40                  # bound the agentic loop
    extra_args: ["--allowedTools", "Bash,Edit"]   # verbatim CLI flags
    env_keys: [ANTHROPIC_BASE_URL] # extra env vars the CLI may read
```

The engine execs the CLI with a fixed argv (prompt via stdin — argv leaks
into process listings), `cwd` = the task's worktree, and a minimal
environment (PATH/HOME/proxy vars + your `env_keys`, never the daemon's
secrets). The binary is resolved and pinned at engine start, exactly like
the pack `tools:` section. Re-asks resume the same CLI session.

## Recipe: deterministic CI (record/replay)

```bash
forgeflow --root live --pack P emit fix.wanted --data '{...}' --drive   # real model, recorded
forgeflow --root ci --pack P --replay-from live emit fix.wanted --data '{...}' --drive
```

Replay answers each agent step from the recording by the sha of the
assembled prompt; a changed prompt/context/schema is a **loud miss**, never
a stale answer. `llm check` on a replay binding reports how many recorded
verdicts the source holds. Per-role binding form:
`agents: { fix: { backend: replay, source: /path/to/live-root } }`.

## Selecting context from your own tables (corpora + select)

You have a database of related knowledge — findings, lessons, docs, past
results, any tables your pack ships. For each task, the engine can pick the
most relevant/important rows and hand exactly those to the model:

```yaml
# project.yaml — map ANY table to the standard shape
corpora:
  lessons:
    table: lessons          # any table or view (pack schema: files)
    key: id                 # stable row id (default: rowid)
    text: summary           # what gets matched
    ts: created_at          # optional: recency signal
    weight: confidence      # optional: your own importance column
    embed_with: hashing     # zero-setup embedder; or a models: entry
```

```yaml
# any agent step
context:
  - payload
  - select:
      corpus: lessons
      query: "{payload.title}"
      k: 5
      filter: { repo: "{payload.repo}" }     # hard scope, pushed into SQL
      boost:  { component: "{payload.component}" }   # soft: same-component rows rise
```

How ranking works — and why it is built this way (each choice is grounded
in published production results):

- **Independent channels, fused by Reciprocal Rank Fusion.** Lexical
  (identifier-aware token overlap — `flagSkipReason` matches "skip
  reason"), semantic (if `embed_with`), recency (if `ts`), prior (if
  `weight`), boost. Hybrid lexical+vector consistently beats either alone
  in production (Anthropic's contextual-retrieval benchmarks; Uber's Genie
  runs BM25 beside dense search; Elastic measured RRF +18% NDCG over BM25
  alone). RRF specifically because Elastic also showed *calibrated linear
  score weights need ~40 labeled queries per dataset to beat it* — rank
  fusion is the robust default when you have no labels, which is every
  fresh corpus. A channel with no signal (all scores equal) abstains
  rather than voting noise.
- **The full filtered pool is scored, not a top-K-then-rerank.** Recency
  and priors only work when applied over the whole candidate set (a
  documented failure mode of narrow cosine cuts). Brute force is the
  *correct* architecture at this scale: benchmarks put exhaustive SQLite
  vector scan well under 100ms up to ~100K vectors, and embedded apps live
  in the thousands-to-hundreds-of-thousands regime. No ANN index, no
  index maintenance, perfect recall.
- **Embeddings are optional — and local by default.** The industry's
  coding agents moved *away* from mandatory embeddings (Sourcegraph
  dropped them for Cody citing third-party data exposure, index upkeep,
  and scaling; Copilot's biggest context win was deterministic
  neighboring-tabs matching). forgeflow's default `hashing` embedder is
  deterministic, dependency-free, and **your rows never leave the
  machine** to be indexed. Point `embed_with` at pinned weights or an
  `/embeddings` endpoint per corpus when you want true semantics.
- **Small corpus? Don't rank.** `include_all_under: 65536` includes the
  whole filtered corpus verbatim when it fits — below a real threshold,
  retrieval infrastructure is pure overhead (Anthropic's guidance:
  small knowledge bases belong in the prompt directly).
- **Priors modulate, relevance decides.** Default weights: lexical /
  semantic / boost 1.0, recency / prior **0.3** — enough to decide among
  relevance ties (their job), not enough for a fresh-or-important-but-
  irrelevant row to outvote an actual match. Ties within a channel share
  a fractional (average) rank, so a block of thousands of equal-score
  rows dilutes itself instead of handing arbitrary rows strong votes.
  Both rules exist because the recall calibration below caught the
  opposite behaviors as real ranking bugs. Override per step: `weights:`.
- **Explainable and deterministic.** Every selected entry carries its
  fused score and per-channel ranks in the injected context; identical db
  state yields identical selection forever (fixed tie-breaks), so replay
  and regression tests stay honest. Oversized entries are truncated with
  a `truncated: true` flag — never silently.

### Measured recall (the calibration suite)

`tests/test_recall.py` freezes a golden set — 56-query version run against
2,000 distractors, six adversarial categories — and CI asserts the floors.
Zero-setup (`hashing`): **100% recall@1** on exact-lexical, identifier
(camelCase↔words), recency, importance, and scoped categories; **paraphrase
0%** — hashing is lexical, synonyms are invisible to it, and that is
stated as a contract, not hidden. Plugging a synonym-aware embedding model
into the *same corpus* (one `embed_with:` line): paraphrase **100% @1**,
overall **98% @1 / 100% @5**. Latency (pure-stdlib brute
force, warm): ~0.04s/query at 1k rows, ~0.19s at 5k, ~0.75s at 20k — the
practical comfort zone is corpora up to a few tens of thousands of rows;
use `filter:` to pre-scope large tables.

### From relevant to useful (the construction stage)

Ranking finds what is *similar*; the model needs what is *useful for this
task*. After fusion, selection runs the construction pipeline the
coding-agent industry converged on (Copilot's documented flow is exactly
select → prioritize → filter → assemble):

- **Multi-query fusion** — `query:` accepts a list
  (`["{payload.title}", "{payload.error}"]`); each query votes per
  relevance channel at weight/n, so the task's different facets each get
  a say (the RAG-fusion pattern).
- **Dedup** — an identical text never occupies two of the k slots; the
  better-ranked twin wins (with recency/prior channels, that IS the
  newer/heavier copy). Collapsed count reported as `deduped`.
- **Diversity (MMR)** — each next pick trades relevance against
  redundancy with what is already picked (`diversify:` default 0.5 ≈ the
  classic λ 0.67 relevance-leaning balance; 0 = pure ranked order), so k
  slots cover the task's ground instead of repeating the top hit five
  ways.
- **Budget packing** — `max_bytes:` packs entries in final order and
  reports `dropped:` — a slot that didn't fit is counted, never silent.
- **Outcome-learned utility** — the engine records what every task was
  shown (`context_uses`); a `utility` channel then ranks rows by how
  often they co-occurred with *done* vs *failed* tasks of the same kind
  (Laplace-smoothed, neutral for cold rows, abstains without history).
  This is the acceptance-signal feedback loop production systems use —
  auto-labelled from the engine's own ledger, no annotation, and preview
  calls (`llm show`, ad-hoc) never pollute it.

Vectors live in the engine's `corpus_embeddings` table and are maintained
incrementally at query time: a row is (re)embedded only when new or when
its text changed (`text_sha` pin). Startup checks the whole chain: a
corpus naming a missing table/column, or an `embed_with` that doesn't
resolve, refuses to start with the exact field named.

## Operating it

```bash
forgeflow --root R llm runs             # recent runs: model, verdict, wall ms, re-asks
forgeflow --root R metrics              # per-model: runs, no-verdict rate, avg/max ms, re-asks
forgeflow --root R status               # parked agent tasks + reasons
forgeflow --root R unpark               # release them after fixing the cause
```

Every run is pinned before exec (the runs row exists even if the process
dies mid-call), archives its prompt and raw answer under `data/runs/<id>/`,
and now records total wall time and correction rounds. Rising `reasks` is
your early signal that a model update started fumbling the contract —
visible in `metrics` long before yield drops.

### Throughput and rate limits

```yaml
concurrency:
  workers: 6
  lanes: { llm: 2 }        # at most 2 concurrent model calls, engine-wide
retry:
  agent_limit: { unpark_after_s: 3600 }   # probe quota hourly instead of every 30 min
```

### Troubleshooting: what each error class means

| you see | it means | do |
|---|---|---|
| task parked `agent_limit` | 401/403/429, quota, or missing key at call time | fix key/quota; daemon auto-probes and unparks, or `unpark` |
| task parked/failed `agent_backend` | endpoint down, non-JSON reply, transport | `llm check` the binding; check the server; bounded retries already ran |
| step failed `agent_invalid` | model answered, but never matched your schema (after 2 re-asks) | `llm show` the prompt; check `data/runs/<id>/ask*/`; pick a stronger model or loosen the schema |
| step outcome `timeout` | the call exceeded the step's `timeout_s` | raise `timeout_s`, shrink context, or serve a faster model |
| `llm check`: "did not follow the output contract" | transport fine, model can't do fenced JSON | different model, or `params.response_format` if the server supports it |
| engine refuses to start | structural/environmental config problem | the message names the exact binding and field — fix that |

Every error class above comes from exit codes, HTTP status, and schema
validation — never from matching the model's prose. That's the invariant
that makes these tables trustworthy.
