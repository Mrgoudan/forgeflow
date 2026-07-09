"""The daemon shell around claim -> execute.

One daemon per state dir, enforced by flock on state/daemon.lock. One-shot
drivers (tests, CLI commands) use run_until_idle() WITHOUT the lock: they
enqueue and then drive the same claim loop until their task tree is
terminal — same code paths, no parallel machinery.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

from . import contract, db, loader, queue

_WS_RE = re.compile(r"^task-(\d+)-a(\d+)$")


class Engine:
    def __init__(self, root, pack=None, extra_defs_dirs=()):
        self.root = Path(root)
        self.pack = pack
        self.state_dir = self.root / "state"
        self.data_dir = self.root / "data"
        self.workspaces_dir = (pack.workspace_root if pack and pack.workspace_root
                               else self.root / "workspaces")
        for d in (self.state_dir, self.data_dir, self.workspaces_dir):
            Path(d).mkdir(parents=True, exist_ok=True)
        self.conn = db.connect(self.state_dir / "forgeflow.db")
        if pack and pack.block_files:
            # pack code registers its blocks BEFORE workflows compile
            from . import blocks as blocks_mod
            blocks_mod.load_files(pack.block_files)
        dirs = list(pack.workflow_dirs) if pack else []
        dirs += list(extra_defs_dirs)
        self.workflows = loader.load_defs(dirs, pack=pack)
        self.subscriptions = loader.subscriptions(self.workflows)
        self.env = contract.ExecEnv(
            conn=self.conn, subscriptions=self.subscriptions,
            data_dir=self.data_dir, workspaces_dir=self.workspaces_dir,
            pack=pack)
        self._recover()

    # ------------------------------------------------------------ startup

    def _recover(self):
        """Crash recovery: orphaned 'running' tasks -> pending (their
        task_steps rows make re-execution resume-aware); orphaned worktrees
        of terminal tasks are pruned, live ones kept."""
        n = queue.reset_orphans(self.conn)
        if n:
            print("engine: reset %d orphaned running task(s)" % n, file=sys.stderr)
        for entry in sorted(Path(self.workspaces_dir).iterdir()):
            m = _WS_RE.match(entry.name)
            if not m or not entry.is_dir():
                continue
            row = self.conn.execute("SELECT state FROM tasks WHERE id=?",
                                    (int(m.group(1)),)).fetchone()
            if row is None or row["state"] in queue.TERMINAL_TASK_STATES:
                self._drop_worktree(entry)

    def _drop_worktree(self, path):
        try:
            subprocess.run(["git", "worktree", "remove", "--force", str(path)],
                           cwd=str(path), stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=60)
        except Exception:
            pass
        if Path(path).exists():
            shutil.rmtree(str(path), ignore_errors=True)

    # ------------------------------------------------------------ loop

    def execute_one(self, task) -> str:
        wf = self.workflows.get(task["kind"])
        if wf is None:
            return queue.fail(self.conn, task["id"], "framework_bug",
                              detail="no workflow handles kind '%s'" % task["kind"])
        state = contract.execute(self.env, wf, task)
        if state in queue.TERMINAL_TASK_STATES:
            self._cleanup_task_workspace(task["id"])
        return state

    def _cleanup_task_workspace(self, task_id):
        for entry in Path(self.workspaces_dir).glob("task-%d-a*" % task_id):
            self._drop_worktree(entry)

    def run_until_idle(self, grace_s: float = 2.0) -> int:
        """Drive the claim loop until nothing is eligible and no retry_wait
        comes due within grace_s. Returns how many tasks were executed."""
        executed = 0
        while True:
            task = queue.claim(self.conn)
            if task is not None:
                self.execute_one(task)
                executed += 1
                continue
            due = self.conn.execute(
                "SELECT 1 FROM tasks WHERE state='retry_wait' AND"
                " next_attempt <= datetime('now', '+' || ? || ' seconds')"
                " LIMIT 1", (int(grace_s),)).fetchone()
            if due is None:
                return executed
            time.sleep(0.05)

    def run(self):
        """The long-running daemon loop. flock enforces one per state dir;
        a second start exits 0 with a message. SIGTERM finishes the current
        step boundary before exiting (contract persists per boundary)."""
        import fcntl
        lock_path = self.state_dir / "daemon.lock"
        lock = open(lock_path, "w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            print("engine: another daemon holds %s — exiting" % lock_path)
            return 0
        idle = self.pack.idle_interval_s if self.pack else 15
        unpark_every = self.pack.unpark_interval_s if self.pack else 600
        last_unpark = 0.0
        while True:
            now = time.monotonic()
            if now - last_unpark >= unpark_every:
                self._unpark_tick()
                last_unpark = now
            task = queue.claim(self.conn)
            if task is None:
                time.sleep(idle)
                continue
            self.execute_one(task)

    def _unpark_tick(self):
        """Recover parked tasks by per-class cadence. Backend-dependent classes
        (agent_limit / agent_backend) are additionally health-gated: they only
        restart when the agent endpoint answers. If it's still down, re-arm the
        clock so the next probe is a full cadence away."""
        due = queue.parked_due(self.conn)
        if not due:
            return
        backend = [i for i, c in due if c in queue.BACKEND_PARK_CLASSES]
        ready = [i for i, c in due if c not in queue.BACKEND_PARK_CLASSES]
        if backend:
            if self._agent_online():
                ready += backend
            else:
                queue.rearm(self.conn, backend)   # probe again in one cadence
        n = queue.unpark(self.conn, ids=ready) if ready else 0
        if n:
            print("engine: unparked %d task(s) by cadence/health" % n)

    def _agent_online(self) -> bool:
        """Health probe for the agent backend. GET the configured URL (an
        'env:VAR' value reads that env var, so the endpoint isn't duplicated
        into the pack file). Any HTTP answer < 500 means reachable; a network
        error / timeout / 5xx means down. No URL configured -> not gated
        (recover by cadence alone), keeping the engine backend-agnostic."""
        import os
        import urllib.error
        import urllib.request
        url = getattr(self.pack, "agent_health_url", None) if self.pack else None
        if url and url.startswith("env:"):
            url = os.environ.get(url[4:])
        if not url:
            return True
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(url, method="GET"), timeout=10) as r:
                return r.status < 500
        except urllib.error.HTTPError as e:
            return e.code < 500
        except Exception:
            return False
