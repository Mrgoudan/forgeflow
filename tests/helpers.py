"""Shared test scaffolding: throwaway git repo with a planted bug, a demo
pack pointing at the tracked workflow defs, and an Engine on a tmp root.
Everything is built fresh per test under tempfile — nothing touches the
developer's zones."""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def make_target_repo(base: Path) -> Path:
    """A throwaway git repo containing a planted marker, a deterministic
    repro script, and the expected (buggy) output it should produce."""
    repo = base / "target-repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "main.txt").write_text(
        "fine line\nthis one has PLANTED_BUG in it\nfine again\n")
    (repo / "repro.sh").write_text(
        "#!/bin/sh\ngrep -c PLANTED_BUG src/main.txt > out.txt\n")
    (repo / "expected.txt").write_text("1\n")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@test.invalid")
    _git(repo, "config", "user.name", "test")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "seed")
    return repo


def _git(cwd, *args):
    subprocess.run(["git", "-C", str(cwd)] + list(args), check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def make_pack(base: Path, repo: Path, workflows_dir=None, extra="") -> Path:
    """Write a machine-local project.yaml for the demo pack."""
    pack_dir = base / "pack"
    pack_dir.mkdir(parents=True, exist_ok=True)
    outbox = base / "outbox"
    outbox.mkdir(exist_ok=True)
    wf = workflows_dir or (REPO_ROOT / "packs" / "demo" / "workflows")
    (pack_dir / "project.yaml").write_text(
        "name: demo\n"
        "paths:\n"
        "  repo: %s\n"
        "  outbox: %s\n"
        "workflows:\n"
        "  - %s\n"
        "tools:\n"
        "  git: { path: git, version_cmd: ['--version'] }\n"
        "%s" % (repo, outbox, wf, extra))
    return pack_dir


def make_engine(base: Path, pack_dir=None, extra_defs_dirs=()):
    from forgeflow import config, engine
    pack = config.load_pack(pack_dir) if pack_dir else None
    return engine.Engine(base / "ff", pack=pack, extra_defs_dirs=extra_defs_dirs)


def tmpdir() -> Path:
    return Path(tempfile.mkdtemp(prefix="forgeflow-test-"))
