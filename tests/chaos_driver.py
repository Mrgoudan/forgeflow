"""Subprocess driver for the chaos test: builds/reuses an engine on the
given base dir, enqueues the chaos_demo task once (idempotent), and drives
the claim loop to idle. The parent SIGKILLs this process at random points
and restarts it until it survives to completion."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from helpers import REPO_ROOT, make_pack  # noqa: E402

sys.path.insert(0, str(REPO_ROOT))

from forgeflow import config, engine, queue  # noqa: E402


def main(base):
    base = Path(base)
    repo = base / "repo"
    pack_dir = make_pack(base, repo,
                         workflows_dir=Path(__file__).resolve().parent
                         / "data" / "chaos")
    pack = config.load_pack(pack_dir)
    eng = engine.Engine(base / "ff", pack=pack)
    queue.enqueue(eng.conn, "chaos_demo", {"key": "c1"})
    eng.run_until_idle()
    row = eng.conn.execute(
        "SELECT state FROM tasks WHERE kind='chaos_demo'").fetchone()
    print("final:%s" % row["state"])


if __name__ == "__main__":
    main(sys.argv[1])
