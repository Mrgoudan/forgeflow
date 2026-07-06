# forgeflow

A YAML-configurable, pluggable, **deterministic** workflow engine.
SQLite-backed, totality-checked at load time, crash-resumable.

forgeflow assumes nothing about your domain. It provides the machinery —
a durable task queue, a step runner with provable termination, an atomic
event bus, verified configuration, and one audited path to subprocesses
and one to LLMs. What the workflows *do* is entirely yours.

**The identity test:** someone who knows nothing about your project must be
able to take this engine, write three blocks and one YAML file, and run a
completely different process (docs sync, release pipeline, anything). If
they can't, domain knowledge leaked into the engine — that's a bug, and
`scripts/check_generic.sh` exists to catch it.

## The three layers

```
workflow YAMLs        ← your processes, written in your vocabulary
pack (project.yaml)   ← your customization: bindings, tools, models, blocks
forgeflow/            ← the engine: assumes nothing, verifies everything
```

- **Engine** (this repo): queue, step contract, loader, runner, standard
  blocks. Contains zero domain nouns — no model names, no repo names, no
  machine paths.
- **Pack**: a directory with a `project.yaml` binding abstract names to
  your world — `agents.fix` → which backend+model, `tools.compiler` →
  which binary, `paths.repo` → which tree — plus your workflow YAMLs and
  (optionally) your own block modules. Everything referenced is verified
  at startup; the daemon refuses to boot on a missing path, tool, or
  drifted model weight.
- **Workflows**: YAML documents. Steps name blocks, declare context,
  declare an llm binding role, and map EVERY outcome. Workflows interact
  only via events (`consumes:` / `emits:`) — never by calling each other.

## Quick start: three blocks and one YAML

A pack is just a directory:

```
mypack/
  project.yaml
  blocks/custom.py        # only for genuinely new capabilities
  workflows/process.yaml
```

`project.yaml` — everything verified at startup, fail-loud:

```yaml
name: mypack
paths:   { repo: /abs/path/to/tree }        # must exist
tools:   { git: { path: git, version_cmd: ["--version"] } }  # verified, never installed
workflows: [workflows]
blocks:    [blocks/custom.py]               # pack-shipped block modules
```

`workflows/process.yaml` — every outcome mapped, or it refuses to load:

```yaml
workflow: process
consumes: [demo.requested]
emits: []

steps:
  - name: check
    block: shell.run
    timeout_s: 300
    params: { cmd: ["git", "-C", "{paths.repo}", "status", "--porcelain"] }
    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed }
```

Run it:

```python
from forgeflow import config, engine, db

eng = engine.Engine("/var/lib/myproc", pack=config.load_pack("mypack"))
db.emit_event(eng.conn, "demo.requested", {"key": "x1"}, eng.subscriptions)
eng.run_until_idle()            # or eng.run() for the flock'd daemon loop
```

## The hard rules (the product IS these rules)

- **Determinism** — decisions come from exit codes, file comparisons, and
  db state. Same db state ⇒ same dispatch decision. No prose is ever
  parsed to decide anything.
- **Totality** — every step is bounded (timeout + visit cap), returns one
  of a closed outcome set, is persisted at its boundary in one
  transaction, and every task provably reaches
  `done | failed | parked | deferred`. An unmapped outcome is a startup
  error, never a runtime surprise.
- **Three choke points** — findings move only through
  `db.record_transition()` (which fans out events in the same
  transaction); subprocesses spawn only in `util.run_cmd()`; models are
  reached only through `runner.run_agent()` (runs row pinned *before*
  exec, schema-gated output, bounded re-asks).
- **Crash resume** — kill -9 mid-workflow, restart, and execution resumes
  at the last committed step boundary; completed side effects happen
  exactly once. Replayed events cannot double-enqueue (payload-hash
  idempotency).
- **LLMs may reduce yield, never integrity** — an agent step is just a
  block whose failure classes are outcomes (`agent_limit` parks, freeing
  the worker). Every workflow keeps a no-AI core that runs with the model
  entirely down.

## What ships in the standard library

| block | outcomes | job |
|---|---|---|
| `shell.run` | ok, nonzero, mismatch, timeout | any command; exit code + optional expected-file comparison |
| `evidence.suite` | green, red_retryable, red, timeout | ordered verify commands, exit codes only |
| `oracle.reproduce` | confirmed, refuted, timeout | deterministic repro classification |
| `scan.grep_rules` | ok, timeout | no-AI pattern finder over a tree |
| `worktree.create/drop` | ok, dirty/error, timeout | isolated git worktree per task attempt |
| `git.branch / fold_commit / branch_advanced` | ok/…, error, timeout | branch mechanics on exit codes |
| `db.upsert_finding / db.transition` | ok | staged state changes, applied atomically at the step boundary |
| `event.emit` | ok | hand work to other workflows via declared events |
| `agent.run` | schema enums + agent_limit/agent_invalid/agent_backend/timeout | THE llm block; verdict enums route the workflow |
| `model.embed / model.classify` | ok | pinned local weights; outputs are claims, structurally unable to gate |

## Tests

Stdlib unittest only:

```
python3 -m unittest discover -s tests     # 88 tests
./scripts/check_generic.sh                # genericity gate
```

The suite includes the proofs that matter: claim atomicity under
concurrency, retry/park arithmetic, SIGKILL crash-resume, atomic event
fan-out with rollback, enqueue idempotency, loader rejection of every
malformed workflow shape, and an end-to-end demo pack.

## Contracts

The binding design documents live in [docs/](docs/): `ENGINE.md` (runtime
semantics), `EXECUTION.md` (totality rules), `WORKFLOWS.md` (the YAML
format and loader guarantees), `DATA.md` (data zones). The remaining docs
record the design history of the first consumer built on this engine.

Python ≥ 3.8, one dependency (`pyyaml`).
