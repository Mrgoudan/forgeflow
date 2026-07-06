"""Subprocess driver for the crash-resume test: builds/reuses an engine on
the given base dir, enqueues the resume_demo task once (idempotent), and
drives the claim loop to idle. The parent test SIGKILLs this process
mid-'slow' and then runs it again to prove resume."""
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
                         workflows_dir=Path(__file__).resolve().parent / "data" / "resume")
    pack = config.load_pack(pack_dir)
    eng = engine.Engine(base / "ff", pack=pack)
    queue.enqueue(eng.conn, "resume_demo", {"key": "r1"})
    eng.run_until_idle()
    row = eng.conn.execute("SELECT state FROM tasks WHERE kind='resume_demo'").fetchone()
    print("final:%s" % row["state"])


if __name__ == "__main__":
    main(sys.argv[1])
