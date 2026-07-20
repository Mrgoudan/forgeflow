#!/usr/bin/env python3
"""forgeflow export — turn ANY pack into a portable, self-contained bundle
that runs WITHOUT a separately-installed engine.

Why a bundle and not a "no-engine" script: a pack's blocks are ordinary
Python written against the engine's API (ctx, staged ops, context providers,
the agent runner). You cannot run them without that API — so the honest
generic export VENDORS the engine core into the bundle. The result needs
no `pip install forgeflow`, no daemon, no board: one `run.py` drives the
one-shot claim loop. It is generic — it works for any pack, because it
copies the pack verbatim and vendors the same engine it already runs on.

What the bundle drops (deliberately): the long-running daemon, the HTTP
board, and multi-worker parallelism. What it keeps: the full workflow
orchestration (graph walk, outcomes, retries, parking, visit caps),
the blocks, context providers, the agent runner, and the SQLite state —
so a bundled run behaves like a single-worker `run_until_idle`.

  export_bundle.py --pack ~/bsd/bsc-sdd --out /tmp/bsc-sdd-bundle
  # then, anywhere Python 3 + the pack's tools exist:
  cd /tmp/bsc-sdd-bundle
  FORGEFLOW_SECRETS=... python3 run.py --event spec.requested \\
      --data '{"feature_key":"F","requirement":"...","base":"main"}'
"""
import argparse
import shutil
import sys
from pathlib import Path

ENGINE = Path(__file__).resolve().parent.parent          # the forgeflow repo
# engine modules to vendor. httpd/board/gc/cli are daemon/UI surface — the
# bundle is one-shot, so they are omitted (run.py never imports them).
_VENDOR = ["__init__.py", "blocks.py", "config.py", "contract.py", "db.py",
           "engine.py", "loader.py", "localmodel.py", "queue.py", "runner.py",
           "select.py", "util.py"]

_RUN_PY = '''#!/usr/bin/env python3
"""Standalone one-shot runner for this bundled pack. No daemon, no board —
loads the pack against the vendored engine, emits a start event, and drives
the claim loop until the work tree is idle. Same orchestration a single
engine worker gives, minus the long-running process."""
import argparse, json, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "_vendor"))          # the vendored engine

from forgeflow import config, engine, db            # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", required=True, help="the start event to emit")
    ap.add_argument("--data", default="{}", help="JSON payload for the event")
    ap.add_argument("--root", default=str(HERE / "run"),
                    help="state dir (SQLite + worktrees + artifacts)")
    ap.add_argument("--grace", type=float, default=2.0)
    a = ap.parse_args()

    pack = config.load_pack(str(HERE / "pack"))
    eng = engine.Engine(a.root, pack=pack)
    payload = json.loads(a.data)
    if a.event not in eng.subscriptions:
        sys.exit("no workflow in this pack consumes %r (consumed: %s)"
                 % (a.event, ", ".join(sorted(eng.subscriptions)) or "none"))
    ev = db.emit_event(eng.conn, a.event, payload, eng.subscriptions)
    print("emitted %s (event %d); driving one-shot..." % (a.event, ev))
    n = eng.run_until_idle(grace_s=a.grace)
    print("executed %d task(s). State in %s" % (n, a.root))


if __name__ == "__main__":
    main()
'''


def export(pack_dir, out_dir):
    pack_dir = Path(pack_dir).resolve()
    out = Path(out_dir).resolve()
    if not (pack_dir / "project.yaml").is_file():
        sys.exit("not a pack (no project.yaml): %s" % pack_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1. copy the pack verbatim (minus its own runtime state)
    dst_pack = out / "pack"
    if dst_pack.exists():
        shutil.rmtree(dst_pack)
    ignore = ["run", ".git", "__pycache__", "*.pyc", "config"]
    # if the bundle is being written INSIDE the pack, don't copy it into itself
    try:
        rel = out.relative_to(pack_dir)
        ignore.append(rel.parts[0])
    except ValueError:
        pass
    shutil.copytree(pack_dir, dst_pack,
                    ignore=shutil.ignore_patterns(*ignore))
    # the pack's data_root anchor (e.g. `run`) must exist at load — recreate
    # any pack-local path that lives INSIDE the pack dir but was ignored.
    import yaml as _yaml
    doc = _yaml.safe_load((pack_dir / "project.yaml").read_text()) or {}
    for _k, v in (doc.get("paths") or {}).items():
        pth = Path(str(v)).expanduser()
        if not pth.is_absolute():
            (dst_pack / pth).mkdir(parents=True, exist_ok=True)
    print("copied pack -> pack/ (pack-local path anchors recreated)")

    # 2. vendor the engine core
    vend = out / "_vendor" / "forgeflow"
    vend.mkdir(parents=True, exist_ok=True)
    for mod in _VENDOR:
        src = ENGINE / "forgeflow" / mod
        if src.is_file():
            shutil.copy2(src, vend / mod)
    print("vendored engine core -> _vendor/forgeflow/ (%d modules)" % len(_VENDOR))

    # 3. the one-shot entry + a README
    (out / "run.py").write_text(_RUN_PY)
    (out / "README.md").write_text(_readme(pack_dir.name))
    print("wrote run.py + README.md")
    print("\nbundle ready: %s\n  cd %s && python3 run.py --event <E> --data '{...}'"
          % (out, out))


def _readme(name):
    return ("# %s — portable bundle\n\n"
            "Self-contained export of the `%s` forgeflow pack. Runs with no\n"
            "engine install (the engine core is vendored under `_vendor/`),\n"
            "no daemon, and no board.\n\n"
            "```\n"
            "FORGEFLOW_SECRETS=~/.config/forgeflow/secrets.env \\\n"
            "  python3 run.py --event <start-event> --data '{...json...}'\n"
            "```\n\n"
            "- `pack/` — the pack, verbatim (workflows, prompts, schemas, blocks).\n"
            "- `_vendor/forgeflow/` — the vendored engine core (orchestration,\n"
            "  blocks, agent runner, SQLite state).\n"
            "- `run/` — created on first run: the state DB + worktrees.\n\n"
            "**Kept:** the full workflow orchestration (graph walk, outcomes,\n"
            "retries, parking, visit caps), blocks, context providers, the\n"
            "agent runner. **Dropped:** the long-running daemon, the HTTP board,\n"
            "and multi-worker parallelism — a run behaves like a single-worker\n"
            "one-shot. Machine prerequisites the pack declares (compilers,\n"
            "model CLI, secrets) still apply.\n" % (name, name))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pack", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    export(a.pack, a.out)


if __name__ == "__main__":
    main()
