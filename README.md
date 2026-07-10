# forgeflow

**Durable, auditable workflow automation in a single SQLite file — with LLM
steps that cannot go off the rails.**

[![ci](https://github.com/Mrgoudan/forgeflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Mrgoudan/forgeflow/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![python ≥3.9](https://img.shields.io/badge/python-3.9%2B-blue.svg)](pyproject.toml)
[![dependencies: pyyaml](https://img.shields.io/badge/dependencies-pyyaml_only-brightgreen.svg)](pyproject.toml)

You have real work to automate on one machine — nightly scans, build+verify
loops, agent pipelines that call an LLM and must not trust its answer. The
usual choices are cron+bash (no resume, no audit, no retries) or a workflow
cluster (a broker, a scheduler, a metadata DB… for one box). forgeflow is the
missing middle:

```
pip install → write YAML → emit an event → kill -9 it mid-run → it resumes. Every byte audited.
```

Workflows are **YAML files**: a list of steps, each running a "block" (run a
command, ask an LLM, fan out over a list…), each with a hard timeout, and
**every possible outcome explicitly mapped** — the loader refuses to start
otherwise. Every step result is persisted in SQLite at the step boundary, so
a crash resumes exactly where it left off. Workflows trigger each other only
through **events**. You never write orchestration code.

## Why people star this

- **Zero infrastructure.** Python ≥3.9 + pyyaml. State is one SQLite file
  you can back up with `cp`. No broker, no server, no docker-compose.
- **Crash-safe, provably.** `kill -9` mid-workflow; on restart it resumes at
  the last completed step. The test suite proves it at a fixed kill point
  *and* under a randomized kill schedule (chaos test).
- **The LLM can't invent a state.** An agent step's answer must match *your*
  JSON schema; the schema's enum **is** the outcome set the workflow must
  map. Rate limits park the task instead of blocking the queue. The exact
  prompt is hashed *before* the call; every answer is archived.
- **Deterministic CI for agent pipelines.** Record a run once against the
  real model, then `--replay-from` answers every agent step from the
  recording — token-free tests that fail loudly on prompt drift. Built into
  the engine, not bolted on: every run already archives its verdict.
- **Edit workflows without fear.** Every task is stamped with the hash of
  the definition it runs under; a YAML edit under a mid-flight task parks it
  for a clean re-run — never replays old state through a new graph. (The
  problem Temporal solves with `patched()` calls, solved structurally.)
- **Fan-out/join without a DSL.** One event per list item, one join event
  when *all* spawned tasks finish. Crash a parent and re-run it — the
  fan-out never doubles.
- **Everything fails loudly at startup.** Unmapped outcome, missing tool,
  missing file, wrong-hash model weights, schedule nobody consumes:
  `validate` names the exact file and step. If it loads, it runs.
- **Operable at 3am.** `status`, `trace <id>` (a task's full story),
  `metrics`, `doctor`, `gc`, park/unpark/retry — plus an optional built-in
  web dashboard. Error classification uses exit codes and file comparisons,
  **never** parsing output text.

## How it compares

| | forgeflow | cron+bash | Celery | Airflow | Temporal | LangGraph |
|---|---|---|---|---|---|---|
| infra needed | **none** | none | broker | scheduler+DB+workers | server cluster | none |
| crash resume mid-workflow | **yes** | no | no | task-level | yes | checkpointer add-on |
| every outcome must be handled | **checked at load** | no | no | no | no | no |
| LLM answer constrained by schema | **yes** | — | — | — | — | manual |
| deterministic replay of LLM runs in CI | **yes** | — | — | — | — | no |
| edit definitions with work in flight | **park + clean re-run** | n/a | n/a | undefined | `patched()` calls | undefined |
| multi-machine scale-out | no | no | yes | yes | **yes** | no |
| full on-disk audit of every step | **yes** | no | no | partial | event history | no |

forgeflow is deliberately **one machine, one daemon**. If your workload
outgrows a box, use Temporal — and take your block decomposition with you.
Honest per-tool migration notes: [docs/COMING_FROM.md](docs/COMING_FROM.md).

---

## 1. Install

```bash
git clone https://github.com/Mrgoudan/forgeflow.git
cd forgeflow
pip install -e .        # or skip install and use: PYTHONPATH=. python3 -m forgeflow …
```

Needs Python ≥ 3.9 and `pyyaml`. Nothing else — no broker, no server, no
cluster. Linux and macOS (the daemon lock uses `flock`; Windows is not
supported). MIT licensed. (Prefer no install? Every command below also works
as `PYTHONPATH=. python3 -m forgeflow …` from the repo.)

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

### Fan-out and join (barriers)

"Run this for every item in a list, then continue when ALL of them are
done" is one block:

```yaml
# in some workflow that computed {prev.candidates} (a list)
- name: spread
  block: fanout.emit
  timeout_s: 30
  params:
    name: probe.wanted            # one event per item (declared in emits:)
    items: "{prev.candidates}"
    data: { candidate: "{item}" } # per-item payload; {item}/{index} resolve
    join:
      event: probe.all_done       # fires ONCE when every spawned task is terminal
      data: { batch: "{payload.batch}" }
  outcomes: { ok: done, empty: done }
```

Whatever consumes `probe.wanted` runs once per item, in parallel if the
daemon has workers. When every one of those tasks reaches a terminal state,
`probe.all_done` fires exactly once with truthful counts:
`{join_group, total, done, failed, deferred, batch}`. The consuming workflow
can use the `join.collect` block to read the full member roster. An empty
list is the `empty` outcome and the join fires immediately; a parked member
holds the join open until it finishes — the barrier never guesses. Re-running
the parent (crash, retry) reuses the same join group, so fan-outs never
double. `status` shows open joins and their progress.

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

### Deterministic tests: record & replay

Every successful agent step already archives its schema-valid answer. Point
a later run at that recording and NO model is called — the same workflow
replays byte-identical answers, keyed by the hash of the assembled prompt:

```bash
python3 -m forgeflow --root live --pack P emit fix.wanted --data '{...}' --drive  # record (real model)
python3 -m forgeflow --root ci --pack P --replay-from live emit fix.wanted --data '{...}' --drive
```

`--replay-from` rebinds every agent to the recording. If a prompt, context,
or schema changed since the recording, the lookup misses and the step fails
loudly (`agent_invalid`) — a replay never serves a stale answer to a changed
question. Per-agent replay is also a normal binding:
`agents: { fix: { backend: replay, source: /path/to/live-root } }`.

### Tuning retries per pack

The engine ships a retry table (attempts, exponential backoff, park-vs-fail
per error class). Packs may retune it — or define classes for their own
block outcomes — in `project.yaml`:

```yaml
retry:
  agent_limit: { unpark_after_s: 3600 }       # probe quota hourly, not every 30 min
  flaky_net:   { max_attempts: 3, backoff_base_s: 5, park_on_exhaust: true }
```

A custom block outcome named `flaky_net`, mapped to `failed` in a workflow,
now retries three times with backoff and then parks instead of failing.
Structural classes (`framework_bug` and friends) cannot be reconfigured —
`validate` refuses.

## 8. Everyday commands

```bash
python3 -m forgeflow --root R --pack P validate    # load + print orchestration map
python3 -m forgeflow --root R --pack P run         # the daemon (one per root)
python3 -m forgeflow --root R --pack P once        # drain queued work, exit
python3 -m forgeflow --root R --pack P emit NAME --data '{...}' [--drive] [--force]
python3 -m forgeflow --root R           status     # tasks / items / parked / events
python3 -m forgeflow --root R           unpark [ID] # parked  -> pending (all, or one id)
python3 -m forgeflow --root R           retry [ID] [--kind K]  # failed -> pending, fresh attempt
python3 -m forgeflow --root R           trace ID    # one task's full story: steps, outcomes, timings
python3 -m forgeflow --root R           metrics     # throughput / park-rate / queue-depth
python3 -m forgeflow --root R           doctor      # health check (daemon alive? work stuck? disk?)
python3 -m forgeflow --root R           gc [--days N] [--dry-run]  # reclaim disk: old archives + worktrees
```

`emit --force` re-triggers a repeat event (bypasses payload-hash dedup).
`--replay-from ROOT` (any command) answers agent steps from that root's
recordings instead of calling a model. Daemon knobs live in `project.yaml`:
`concurrency: { workers, lanes }` (parallel workers + per-lane caps, e.g.
`lanes: { build: 1 }` serializes a rebuild) and `min_free_disk_mb` (pause
claiming when disk runs low).

(Installed via pip? `forgeflow ...` works instead of `python3 -m forgeflow ...`.)

## 9. Running in parallel (optional)

By default the daemon runs one task at a time. Opt into parallelism in the pack:

```yaml
# project.yaml
concurrency:
  workers: 6                 # up to 6 tasks executing at once
  lanes:                     # per-lane concurrency caps (a shared semaphore)
    build: 1                 # only ONE build runs, across all workers
    llm:   4                 # ≤4 agent calls at a time (rate-limit friendly)
```

A step runs in a **lane** — its `lane:` if set, else the block's exec-class
(`local`/`llm`/`state`). A capped lane admits at most that many concurrent step
runs across *all* workers; an uncapped lane is bounded only by `workers`. So you
get throughput while still forcing specific steps to be serial:

```yaml
- name: build
  block: shell.run
  lane: build          # shares the build lane (cap 1): never two builds at once
  timeout_s: 3600
  params: { cmd: [ ... ] }
  outcomes: { ok: ok, nonzero: failed, mismatch: failed, timeout: failed }
```

Integrity holds: each worker has its own SQLite connection, claims are atomic
(`BEGIN IMMEDIATE`), a block runs *outside* any transaction (so a slow step
never blocks another worker's commit), and every task's steps stay sequential +
resume-on-crash. Omit `concurrency` (or set `workers: 1`) for the classic
one-at-a-time daemon.

## 10. Running unattended: schedules and the HTTP door

Two more `project.yaml` sections turn the daemon into a self-contained
service — timed triggers in, events in over HTTP, state out over HTTP:

```yaml
schedule:                          # timed triggers (replaces the crontab)
  - { event: nightly.scan_wanted, every: 1d, data: { scope: full } }
  - { event: queue.sweep_wanted,  every: 30m }

http:                              # optional dashboard + JSON API in the daemon
  host: 127.0.0.1                  # beyond loopback REQUIRES token_ref
  port: 8321
  # token_ref: DASH                # -> HTTP_TOKEN_DASH in the secrets file
```

Schedules fire each event **once per window** — the window start rides in
the payload and a persisted cursor survives restarts, so a daemon that was
down for three windows fires only the current one (no catch-up storm), and
restarting never double-fires. A scheduled event nobody consumes refuses to
start (`validate` catches it). Missed windows are skipped by design.

The HTTP door serves a **read-only dashboard** at `/` (tasks, parked, open
joins, recent events, auto-refresh), JSON at `/api/status`, `/api/metrics`,
`/api/task/<id>`, and one write endpoint:

```bash
curl -XPOST localhost:8321/api/emit -d '{"name": "hello.wanted", "data": {"who": "http"}}'
```

Emits accept only events some workflow consumes (400 otherwise). If a token
is configured, every request needs `Authorization: Bearer <token>`; binding
beyond loopback without one is refused at startup.

## 11. The standard blocks

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
| `fanout.emit` | ok, empty | one event per list item + a join event when ALL spawned tasks finish |
| `join.collect` | ok | read a join group's member roster (ids, states, counts) |
| `agent.run` | your schema's enums + agent_limit/agent_invalid/agent_backend/timeout | the LLM step |
| `model.embed` / `model.classify` | ok | tiny local models (pinned weight files); outputs are hints, never decisions |

Passing data between steps: `{prev.x}` carries **small** values (ids, paths,
counts — the step result row). Big artifacts (logs, diffs, model output)
belong on disk: every block gets a private, archived `_step_dir`; write the
file there and pass its **path** through the result. That keeps the db lean
and every byte audit-addressable under `data/tasks/<id>/`.

## 12. What protects you (the short version)

- **Everything checked at startup** — unmapped outcome, missing file,
  missing tool, wrong-hash model, bad event name, schedule nobody consumes:
  `validate` refuses with the exact file and step. If it loads, it runs.
- **Crash-safe** — `kill -9` mid-workflow; on restart it resumes at the
  last completed step. Finished steps don't re-run. (The test suite proves
  this at a fixed kill point AND under a randomized kill schedule.)
- **Safe workflow edits** — every task is stamped with a fingerprint of the
  workflow definition it runs under. Edit the YAML while tasks are mid-flight
  and they **park** as `definition_changed` instead of replaying old outcomes
  through a new graph; `unpark` re-runs them from step 0 under the new
  definition. Finished and not-yet-started tasks are unaffected.
- **No duplicate work** — the same event/payload enqueues once, ever; a
  re-run fan-out reuses its join group.
- **Nothing hangs** — every step has a timeout, retries are bounded, and a
  stuck dependency (rate-limited LLM, dead server) *parks* the task where
  you can see it instead of blocking the queue.
- **Full audit trail** — every step result, every command's output, every
  prompt and every model answer is on disk under `--root`, addressed by
  task/run id — and replayable (`--replay-from`) for deterministic tests.

## Proof it scales past hello-world

This engine wasn't built as a demo. [**forgeflow-packs**](https://github.com/Mrgoudan/forgeflow-packs)
is a full production deployment on it: an automated **compiler reviewer +
differential-probe bug-hunter + auto-fixer** for the BiSheng C compiler —
many chained workflows, custom blocks, LLM steps that file and fix real
bugs, a live control-room dashboard, and a git-versioned knowledge store.
Every feature documented above is composed at scale in its `packs/bsc/`.

## Tests & docs

```bash
python3 -m unittest discover -s tests    # 169 tests, stdlib only — incl. kill -9 chaos runs
./scripts/check_generic.sh               # proves the engine has no domain leaks
```

Deep-dive contracts in [docs/](docs/): `ENGINE.md` (runtime semantics),
`EXECUTION.md` (totality rules), `WORKFLOWS.md` (YAML format), `DATA.md`
(what lives where on disk). These four are the engine's law; design docs
for systems BUILT on the engine live with their packs, not here. Coming
from Celery, cron, Airflow, LangGraph, or Temporal? See
[docs/COMING_FROM.md](docs/COMING_FROM.md) for the concept mapping.

---

**If forgeflow replaced a broker, a scheduler cluster, or a pile of cron
scripts for you — [a star](https://github.com/Mrgoudan/forgeflow/stargazers)
helps the next person find it.** Issues and PRs welcome; new standard blocks
are a great first contribution (the block contract makes them small and
self-contained). MIT licensed.
