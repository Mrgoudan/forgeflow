# forgeflow

forgeflow runs **workflows you write as YAML files**. A workflow is a list
of steps; each step runs a "block" (a small reusable action like *run this
command* or *ask an LLM*); every step result is saved in SQLite, so if the
machine dies mid-run, it resumes where it left off. Workflows trigger each
other through **events** — that's the whole orchestration model.

You never write orchestration code. You write:
1. a **pack** — one folder that says *what exists on this machine* (paths, tools, models),
2. **workflow YAMLs** — *what to do and where every outcome goes*,
3. (only if you need something new) a **block** — a small Python function.

---

## 1. Install

```bash
git clone git@github.com:Mrgoudan/forgeflow.git
cd forgeflow
pip install -e .        # or: no install — use PYTHONPATH=. python3 -m forgeflow
```

Needs Python ≥ 3.8 and `pyyaml`. Nothing else.

## 2. Your first workflow (copy-paste, 2 minutes)

Make a pack — a folder with a `project.yaml` and your workflows:

```bash
mkdir -p hello/workflows
cat > hello/project.yaml <<'EOF'
name: hello
workflows: [workflows]
EOF

cat > hello/workflows/greet.yaml <<'EOF'
workflow: greet
consumes: [hello.wanted]      # the event that starts me
emits: []

steps:
  - name: say
    block: shell.run
    timeout_s: 30
    params: { cmd: ["echo", "hello {payload.who}"] }
    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed }
EOF
```

Check it loads (this catches every mistake BEFORE anything runs):

```bash
python3 -m forgeflow --root myrun --pack hello validate
```

```
pack: hello
workflows (1):
  greet            steps: say
                   consumes: hello.wanted
orchestration map (event -> consumers):
  hello.wanted                 -> greet
OK: every workflow is total, every reference resolves
```

Now fire the event and let it run:

```bash
python3 -m forgeflow --root myrun --pack hello emit hello.wanted \
    --data '{"who": "world"}' --drive
```

```
event 1: hello.wanted -> greet
executed 1 task(s)
```

See what happened:

```bash
python3 -m forgeflow --root myrun status
cat myrun/data/tasks/1/a0/say/cmd/stdout     # -> hello world
```

That's the whole loop: **event in → workflow runs → every step's result
and output archived under `--root`**.

What the flags mean:
- `--pack hello` — which pack (configuration folder) to load.
- `--root myrun` — where forgeflow keeps its state: `myrun/state/forgeflow.db`
  (the database), `myrun/data/` (every command's stdout/stderr, every LLM
  answer), `myrun/workspaces/` (temporary git worktrees).
- `--drive` — run the queued work right now, in this process, then exit.
  For production you'd instead run `python3 -m forgeflow ... run`, the
  always-on daemon, and `emit` events from anywhere.

## 3. Reading a workflow file

```yaml
workflow: greet             # unique name; tasks of this kind use this workflow
consumes: [hello.wanted]    # events that create a task for me
emits: []                   # events I'm allowed to send (checked!)

steps:
  - name: say               # first step in the list = entry step
    block: shell.run        # which building block to run
    timeout_s: 30           # REQUIRED: hard time limit
    params: { ... }         # the block's configuration
    outcomes:               # where EVERY possible result goes:
      ok: done              #   a terminal state (done/failed/parked/deferred)
      nonzero: failed       #   ...or the name of another step
      mismatch: failed
      timeout: failed
```

The rule that makes this reliable: **every outcome a block can produce
must be mapped, and you can't map outcomes it can't produce.** If you
forget one, `validate` refuses to load and tells you exactly which line.
Nothing surprises you at 3am.

`{payload.who}` pulls from the event data. `{prev.xyz}` pulls from the
previous step's result. `{paths.xyz}` pulls from your pack (next section).

## 4. Chaining workflows (orchestration)

Workflows never call each other. One **emits an event**, the other
**consumes it**. Add a second workflow:

```bash
cat > hello/workflows/greet.yaml <<'EOF'
workflow: greet
consumes: [hello.wanted]
emits: [hello.done]

steps:
  - name: say
    block: shell.run
    timeout_s: 30
    params: { cmd: ["echo", "hello {payload.who}"] }
    outcomes: { ok: tell, nonzero: failed, mismatch: failed, timeout: failed }

  - name: tell
    block: event.emit
    timeout_s: 10
    params: { name: hello.done, data: { who: "{payload.who}" } }
    outcomes: { ok: done }
EOF

cat > hello/workflows/cheer.yaml <<'EOF'
workflow: cheer
consumes: [hello.done]

steps:
  - name: hooray
    block: shell.run
    timeout_s: 30
    params: { cmd: ["echo", "{payload.who} was greeted!"] }
    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed }
EOF
```

```bash
python3 -m forgeflow --root myrun --pack hello emit hello.wanted \
    --data '{"who": "again"}' --drive
# executed 2 task(s)        <- greet ran, its event triggered cheer
```

Adding an interaction = adding one line to a `consumes:` list. Removing a
workflow never breaks the others — an event nobody consumes is just a log
entry. Sending the same event twice does NOT run things twice (payloads
are deduplicated by content hash).

## 5. Paths, tools, and machine-specific stuff

Workflow YAMLs must stay portable, so anything machine-specific lives in
the pack's `project.yaml` and is **verified when forgeflow starts** — a
missing path or tool stops the daemon with a clear error instead of
failing mid-run:

```yaml
name: mypack
paths:                              # every path must exist
  repo: /home/me/code/myproject
tools:                              # every tool must exist (never installed)
  git: { path: git, version_cmd: ["--version"] }
workflows: [workflows]
```

Then in any workflow: `cmd: ["git", "-C", "{paths.repo}", "status"]` —
`git` resolves to the verified binary, `{paths.repo}` to the verified path.

## 6. Writing your own block (only when you need one)

A block is one Python function. Put it in your pack:

```bash
mkdir hello/blocks
cat > hello/blocks/mine.py <<'EOF'
from forgeflow.blocks import block

@block("count.chars", "local", {"ok"})
def count_chars(ctx, task, prev):
    text = (task.get("payload") or {}).get("who", "")
    return "ok", {"length": len(text)}
EOF
```

Register it in `project.yaml`:

```yaml
blocks: [blocks/mine.py]
```

Now any workflow can use `block: count.chars`, and the next step can read
`{prev.length}`. Rules for blocks: return one outcome from your declared
set + a small result dict; run commands only through the provided helpers
(they enforce the timeout and archive output); never parse tool output
text to make decisions — use exit codes and file comparisons.

Most workflows need **zero** custom blocks — see the standard library below.

## 7. Using an LLM in a step

Three pieces in the pack, then one step in the workflow.

```yaml
# project.yaml
agents:                             # role -> which backend/model (per machine)
  fix:    { backend: claude-cli, model: your-model-id }   # agentic CLI (tools, cwd)
  triage: { backend: openai-compat,                       # ANY chat-completions API:
            base_url: "http://127.0.0.1:11434/v1",        # local runtime, gateway, cloud
            model: some-chat-model, api_key_ref: LOCAL }  # key ref, never the key
prompts:
  fix: prompts/fix.md               # the base prompt for that role
schemas:
  verdict: schemas/verdict.yaml     # what a valid answer looks like
```

API keys live in ONE place: `~/.config/forgeflow/secrets.env` (must be
`chmod 600`), as `LLM_API_KEY_<REF>=...` — pack files only name the REF.
Embedding models (BERT-style) are configured the same way:

```yaml
models:
  bertish: { base_url: "http://127.0.0.1:11434/v1", model: some-embed-model }
  tiny:    { path: models/tiny.json, sha256: <pinned> }   # or local pinned weights
```

Both work through the same `model.embed` block; vectors land in the
`embeddings` table and are hints for retrieval/dedup — never decisions.

```yaml
# schemas/verdict.yaml — the LLM must answer with JSON matching this
type: object
required: [verdict]
properties:
  verdict: { enum: [FIXED, NOOP] }
```

```yaml
# in a workflow
- name: candidate
  block: agent.run
  llm: fix                    # which agent role (each step can use a different one)
  schema: verdict
  timeout_s: 3600
  context: [payload]          # exactly what the LLM gets to see — nothing else
  outcomes:
    FIXED: verify             # the schema's enum values become outcomes
    NOOP: done
    agent_limit: parked       # model rate-limited -> park, retry later
    agent_invalid: failed     # model answered garbage (after 2 retries)
    agent_backend: parked     # CLI/transport broke
    timeout: failed
```

What forgeflow guarantees around that step: the exact prompt is recorded
(hashed) *before* the model runs; the raw answer is archived; the answer
must match your schema or it is re-asked at most twice and then fails as
`agent_invalid`; a rate-limited model **parks** the task (visible in
`status`, released with `unpark`) instead of blocking anything. The model
can only ever produce one of the outcomes you mapped. It cannot invent a
state.

## 8. Everyday commands

```bash
python3 -m forgeflow --root R --pack P validate    # load + print orchestration map
python3 -m forgeflow --root R --pack P run         # the daemon (one per root)
python3 -m forgeflow --root R --pack P once        # drain queued work, exit
python3 -m forgeflow --root R --pack P emit NAME --data '{...}' [--drive]
python3 -m forgeflow --root R           status     # tasks/findings/parked/events
python3 -m forgeflow --root R           unpark [ID]
```

(Installed via pip? `forgeflow ...` works instead of `python3 -m forgeflow ...`.)

## 9. The standard blocks

| block | outcomes | what it does |
|---|---|---|
| `shell.run` | ok, nonzero, mismatch, timeout | run any command; optionally compare its output to an expected file |
| `evidence.suite` | green, red_retryable, red, timeout | run verify commands in order; classify by exit codes only |
| `oracle.reproduce` | confirmed, refuted, timeout | "does this bug still reproduce?" — exit code + file comparison |
| `scan.grep_rules` | ok, timeout | run regex rules over a tree, collect hits as candidates |
| `worktree.create` / `drop` | ok, dirty/error, timeout | isolated git worktree per task (never touches your checkout) |
| `git.branch` / `git.fold_commit` / `git.branch_advanced` | ok/…, error, timeout | branch mechanics |
| `db.upsert_finding` / `db.transition` | ok | record/advance a tracked finding (this is what fires `finding.*` events) |
| `event.emit` | ok | send a declared event to other workflows |
| `agent.run` | your schema's enums + agent_limit/agent_invalid/agent_backend/timeout | the LLM step |
| `model.embed` / `model.classify` | ok | tiny local models (pinned weight files); outputs are hints, never decisions |

## 10. What protects you (the short version)

- **Everything checked at startup** — unmapped outcome, missing file,
  missing tool, wrong-hash model, bad event name: `validate` refuses with
  the exact file and step. If it loads, it runs.
- **Crash-safe** — `kill -9` mid-workflow; on restart it resumes at the
  last completed step. Finished steps don't re-run.
- **No duplicate work** — the same event/payload enqueues once, ever.
- **Nothing hangs** — every step has a timeout, retries are bounded, and a
  stuck dependency (rate-limited LLM, dead server) *parks* the task where
  you can see it instead of blocking the queue.
- **Full audit trail** — every step result, every command's output, every
  prompt and every model answer is on disk under `--root`, addressed by
  task/run id.

## Tests & docs

```bash
python3 -m unittest discover -s tests    # 92 tests, stdlib only
./scripts/check_generic.sh               # proves the engine has no domain leaks
```

Deep-dive contracts in [docs/](docs/): `ENGINE.md` (runtime semantics),
`EXECUTION.md` (totality rules), `WORKFLOWS.md` (YAML format), `DATA.md`
(what lives where on disk).
