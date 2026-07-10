"""Retention / garbage collection: reclaim disk from finished work without
touching live state. Safe next to a running daemon — it removes only archives
of TERMINAL tasks past a window and worktrees with no live task, and trims the
append-only event log (the idempotency ledger is tasks.payload_hash, NOT the
events table, so trimming events changes no behavior — only old audit trail).
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

_WS_RE = re.compile(r"^task-(\d+)-a(\d+)$")
TERMINAL = ("done", "failed", "deferred")


def _drop_worktree(path):
    try:
        subprocess.run(["git", "worktree", "remove", "--force", str(path)],
                       cwd=str(path), stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=60)
    except Exception:
        pass
    if Path(path).exists():
        shutil.rmtree(str(path), ignore_errors=True)


def collect(conn, root, days: int = 14, dry_run: bool = False) -> dict:
    """Reclaim disk. Returns counts of what was (or would be) removed.

    - worktrees:  run/workspaces/task-<id>-a* whose task is terminal or gone
    - task_dirs:  run/data/tasks/<id> for tasks terminal AND older than `days`
    - run_dirs:   run/data/runs/<id> whose task is terminal AND older than `days`
    - events:     rows in the event log older than `days`
    """
    root = Path(root)
    ws, data = root / "workspaces", root / "data"
    win = ("datetime('now','-'||?||' days')",)  # bound with (days,)
    st = {"worktrees": 0, "task_dirs": 0, "run_dirs": 0, "events": 0}

    states = {r["id"]: r["state"] for r in conn.execute("SELECT id, state FROM tasks")}
    old_tasks = {r["id"] for r in conn.execute(
        "SELECT id FROM tasks WHERE state IN ('done','failed','deferred')"
        " AND updated_at < %s" % win[0], (days,))}
    old_runs = {r["id"] for r in conn.execute(
        "SELECT id FROM runs WHERE task_id IN (SELECT id FROM tasks WHERE"
        " state IN ('done','failed','deferred') AND updated_at < %s)" % win[0],
        (days,))}

    if ws.is_dir():
        for e in ws.iterdir():
            m = _WS_RE.match(e.name)
            if not m or not e.is_dir():
                continue
            s = states.get(int(m.group(1)))
            if s is None or s in TERMINAL:
                if not dry_run:
                    _drop_worktree(e)
                st["worktrees"] += 1

    for sub, key, keep in (("tasks", "task_dirs", old_tasks),
                           ("runs", "run_dirs", old_runs)):
        d = data / sub
        if not d.is_dir():
            continue
        for e in d.iterdir():
            if e.name.isdigit() and int(e.name) in keep and e.is_dir():
                if not dry_run:
                    shutil.rmtree(str(e), ignore_errors=True)
                st[key] += 1

    st["events"] = conn.execute(
        "SELECT count(*) FROM events WHERE at < %s" % win[0], (days,)).fetchone()[0]
    if not dry_run and st["events"]:
        conn.execute("DELETE FROM events WHERE at < %s" % win[0], (days,))
        conn.commit()
    return st
