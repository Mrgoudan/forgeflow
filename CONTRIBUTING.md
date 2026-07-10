# Contributing

Thanks for looking! forgeflow is small on purpose (~4k lines, stdlib +
pyyaml) and guards that smallness aggressively. The bar for a change is:
it keeps every startup guarantee, it ships with tests, and the generic
core stays generic.

## Setup

```bash
git clone https://github.com/Mrgoudan/forgeflow.git && cd forgeflow
pip install -e .
python3 -m unittest discover -s tests    # must be green before AND after
./scripts/check_generic.sh               # the core never names a domain
```

No other tooling required. CI runs exactly those two commands on
Linux/macOS × Python 3.9–3.13.

## The best first contribution: a standard block

Blocks are self-contained by design — one function, a declared outcome set,
no framework knowledge needed. Ideas: `http.request` (status-code classified),
`file.compare` variants, an `archive.extract`. Rules a block must follow
(see the header of `forgeflow/blocks.py`):

- classify from **exit codes and file comparisons only** — never parse
  output prose to make a decision;
- spawn subprocesses only through `util.run_cmd` and let `TimeoutExpired`
  escape;
- stage db writes via `result["_staged"]` — a block never commits;
- declare every outcome it can return; return nothing outside that set.

Test it with `blocks.run_isolated(...)` (no engine, no db) — see
`tests/test_blocks.py` for the pattern.

## Ground rules for engine changes

- **Fail loud at startup, never at runtime.** New config = validated in
  `config.py`/`loader.py` with the exact file and field in the error.
- **Totality is law.** Anything that adds outcomes or dispatch must keep
  `validate` able to prove every task terminates.
- **The core stays generic.** `check_generic.sh` gates project names,
  model names, machine paths. Domain logic belongs in packs.
- **Schema changes** bump `SCHEMA_VERSION` and append a migration —
  existing roots must upgrade in place (see `MigrationTest`).
- **Docs are contracts.** If you change runtime semantics, update
  `docs/ENGINE.md` in the same PR.

## Reporting bugs

Include `forgeflow --root R doctor` output, the `trace <task-id>` of an
affected task, and your workflow YAML if you can share it. Crash-resume
bugs are the highest-value reports — `tests/chaos_driver.py` shows how to
reproduce kill-schedules deterministically.
