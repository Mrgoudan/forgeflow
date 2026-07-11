# Coming from … (concept mapping)

How forgeflow's pieces line up with the tool you're leaving, what you gain,
and what you honestly give up. The short version of the positioning: **one
machine, zero infrastructure, everything audited, LLM steps that cannot go
off the rails**. If you need a multi-node cluster, forgeflow is the wrong
tool — see the Temporal section.

## From cron + shell scripts

Keep your scripts; wrap each in a workflow step.

| you have | you get |
|---|---|
| crontab entry | `schedule:` entry in project.yaml (once-per-window, restart-safe, no catch-up storm) |
| script exits non-zero, nobody notices | mapped `nonzero` outcome: retry policy, park, or fail — visible in `status`/`doctor` |
| half-finished run after a reboot | crash resume at the last completed step |
| grep through /var/log to reconstruct what happened | `trace <task>`: every step, outcome, timing, archived output |

Gotcha: forgeflow's schedule is interval-based (`every: 30s/5m/6h/1d`), not
cron expressions — no "at 09:30 on weekdays" yet.

## From Celery

| Celery | forgeflow |
|---|---|
| broker (Redis/RabbitMQ) + result backend | one SQLite file under `--root` |
| `@task` functions | blocks (reusable, declared outcome sets) |
| chains / chords / groups | events chain workflows; `fanout.emit` + join event = group/chord with a durable barrier |
| retry args scattered across decorators | one `retry:` table in project.yaml, per error class |
| lost tasks / visibility timeouts | claims are `BEGIN IMMEDIATE` transactions; orphans reset on restart |
| Flower | built-in `http:` dashboard + `status`/`metrics`/`doctor` |

You give up: multi-machine workers and sub-second task latency at high
volume. The daemon polls (default 30ms between claims when busy); this is a
throughput engine for real work units, not a microsecond job bus.

## From Airflow

| Airflow | forgeflow |
|---|---|
| DAG parsed from Python by a scheduler | workflow YAML validated at startup (`validate` = your CI check) |
| XCom for inter-task data | `{prev.*}` for small values, archived `_step_dir` files for artifacts |
| schedule-first, sensors bolt on events | event-first, schedules are just timed events |
| scheduler + webserver + metadata DB + workers | one process, one SQLite file |
| backfills | none — missed schedule windows skip by design; re-emit explicitly if you want history |

You give up: the ecosystem of provider operators and true distributed
execution. You gain: local dev that is just `emit --drive`, and diffs of
your pipelines in code review.

## From LangGraph / agent frameworks

The gap agent frameworks leave is ops, and that's exactly the part
forgeflow is: durable state, bounded retries, parking, audit.

| LangGraph et al. | forgeflow |
|---|---|
| graph nodes calling models | `agent.run` steps; the schema's enum IS the edge set — the model cannot invent a transition |
| checkpointer add-ons | every step boundary persisted in SQLite, crash-resume built in |
| prompt assembled somewhere in code | `context:` declares exactly what the model sees; the assembled prompt is hashed BEFORE the call and archived |
| rate limits = your try/except | `agent_limit` parks the task; health-gated auto-unpark |
| "works in the demo" tests | record once against the real model, `--replay-from` in CI — deterministic, token-free, loud on prompt drift |
| RAG glue you assemble yourself (splitter + vector store + retriever + reranker) | declare any table as a corpus; `select:` does hybrid ranking (RRF), dedup/MMR/budget construction, and outcome-learned utility — zero-setup local embedder, recall floors frozen in CI |
| framework API churn | workflows are YAML; the engine's four contracts are documented and versioned |

You give up: in-process streaming/token-level control and Python-native
graph composition. forgeflow treats a model call as a sealed step with a
schema-validated result — that constraint is the feature.

## From Temporal

Honest comparison, because the models are genuinely close (durable
execution, replay-safe state):

- Temporal replays **code**, so workflow code must stay deterministic
  forever and versioning mid-flight executions needs `patched()` calls.
  forgeflow replays **recorded outcomes** and stamps every task with the
  definition hash — edit the YAML freely; mid-flight tasks park as
  `definition_changed` and re-run fresh instead of crashing on
  non-determinism.
- Temporal scales horizontally across a cluster and offers signals, timers,
  and child workflows with rich SDKs. forgeflow is one box, one daemon, by
  design. If your workload outgrows a box, use Temporal — and take your
  YAML's block decomposition with you as the activity inventory.

Rule of thumb: fewer than one machine's worth of work and you want to read
every byte the system touched → forgeflow. More than one machine → Temporal.
