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
