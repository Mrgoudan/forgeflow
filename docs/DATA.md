# Data governance

Everything lives under this folder. Every byte belongs to exactly one **zone**,
every zone has exactly one **writer**, and components never collaborate through
files — they collaborate through rows in `state/forgeflow.db`. A file that no
db row references is garbage by definition (collectable).

## Zones

| zone | tracked? | writer | lifecycle |
|---|---|---|---|
| `forgeflow/`, `schemas/`, `packs/`, `scripts/`, `systemd/`, `*.md` | git (gitcode) | humans | the platform; versioned, reviewed |
| `vault/` | own private git repo (ignored by parent) | humans + `learn` workflow (lessons append only) | method assets: probes, code_notes, prompts, knowledge. Every run pins `vault_rev`. |
| `state/` | no | `forgeflow/db.py` ONLY | `forgeflow.db` (WAL) — the single source of truth. Backed up. Never hand-edited. Includes the engine's derived stores: `corpus_embeddings` (text_sha-pinned vectors over declared corpora; rebuildable at query time, loss ≠ broken) and `context_uses` (what each task was shown — the learned-utility ledger; loss resets learning, nothing else). |
| `data/` | no | runner/egress/evidence helpers ONLY | generated artifacts, addressed by id: `data/runs/<run_id>/` (prompt snapshot, raw agent output), `data/egress/<id>.md` (exact posted bodies), `data/evidence/<transition_id>/` (build logs, probe outcomes). Append-only; referenced by db rows. |
| `workspaces/` | no | runner | ephemeral git worktrees, one per task; deleted on task completion. Always safe to `rm -rf`. |
| `legacy/` | no | nobody (frozen) | read-only snapshots of the predecessor systems (autofix, pr_monitor, autotest sem_tests) kept for reference and as `import_legacy.py` input. Never executed, never edited. |
| `logs/` | no | daemon | operational logs, rotated, disposable. |

## Passing data between steps

Two channels, chosen by size:

- **Small values** (ids, paths, counts, verdict fields) ride the step result
  row — the next step reads `{prev.x}`. Result JSON is meant to stay under a
  few KB; it is replayed on resume and shown in `trace`.
- **Big artifacts** (build logs, diffs, model output, generated files) go to
  disk. Every block receives a private `_step_dir`
  (`data/tasks/<task>/a<attempt>/<step>/`) that the engine creates before the
  block runs and archives forever (until gc). Write the artifact there and
  pass its **path** through the result; the consuming step (or a human, or
  gc) finds it by task/attempt/step address. `shell.run` already does this
  for stdout/stderr.

Never pass content through the event payload — payloads are identity (the
dedup hash); artifacts are evidence.

## Collaboration contract

- **Workflows never read each other's files.** bughunt → autofix → review hand
  off exclusively via `findings`/`tasks` rows and their state transitions.
  Cross-triggers (variant hunt on merge, review lens on confirmed pattern,
  auto-fix on review defect) are rules over transitions — db-level, not file-level.
- **`vault/` is read-only input** to all workflows, injected by the runner
  (prompts, code_notes by subsystem map, lessons by task kind). The only
  writer-path back into the vault is the `learn` workflow appending to
  `lessons.jsonl` — method assets grow, they don't mutate.
- **Artifacts are evidence, not messages.** A run's raw output in `data/runs/`
  exists so a human can audit why a decision happened; no code path reads it
  back. If a workflow needs information, it belongs in a db column.
- **`state/` + `vault/` are the backup set.** `data/` is audit trail (keep, but
  loss ≠ broken), `workspaces/`/`logs/` are disposable, `legacy/` is recoverable
  from the original repos.

## Retention

- `workspaces/<task>`: removed when the task reaches done/failed (parked tasks keep theirs).
- join groups: fired groups (and their member rows) are pruned by `gc` after
  the window; UNFIRED groups are live barrier state and are never collected.
- `data/runs/`: keep forever for findings that reached `pr_open`+; 90 days otherwise.
- `data/egress/`: keep forever (it is the record of everything we ever said publicly).
- `logs/`: rotate at 50 MB, keep 10.
- `state/forgeflow.db`: daily snapshot into `state/backups/`, keep 14.

## Secrets

Never under this folder. `~/.config/forgeflow/secrets.env` (0600) is the only
location. `legacy/` snapshots may contain historical config files — legacy is
gitignored precisely so those never reach the forge; treat any token found in
legacy as compromised and rotate it.
