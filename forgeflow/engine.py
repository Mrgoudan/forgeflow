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
import threading
import time
from pathlib import Path

from . import contract, db, loader, queue

_WS_RE = re.compile(r"^task-(\d+)-a(\d+)$")


def _replay_pack(pack, replay_from):
    """--replay-from ROOT: rebind EVERY agent to the replay backend reading
    that root's recordings — deterministic CI runs with zero pack edits.
    The recording must exist (fail loud, never silently run live models)."""
    import dataclasses
    replay_from = Path(replay_from)
    if not (replay_from / "state" / "forgeflow.db").is_file():
        from .config import ConfigError
        raise ConfigError("--replay-from %s: no recording there "
                          "(state/forgeflow.db missing)" % replay_from)
    agents = {name: {"backend": "replay", "source": str(replay_from)}
              for name in pack.agents}
    return dataclasses.replace(pack, agents=agents)


class Engine:
    def __init__(self, root, pack=None, extra_defs_dirs=(), replay_from=None):
        self.root = Path(root)
        if replay_from is not None and pack is not None:
            pack = _replay_pack(pack, replay_from)
        self.pack = pack
        self.state_dir = self.root / "state"
        self.data_dir = self.root / "data"
        self.workspaces_dir = (pack.workspace_root if pack and pack.workspace_root
                               else self.root / "workspaces")
        for d in (self.state_dir, self.data_dir, self.workspaces_dir):
            Path(d).mkdir(parents=True, exist_ok=True)
        self.conn = db.connect(self.state_dir / "forgeflow.db")
        # apply pack-declared schema after the generic core (idempotent CREATE
        # IF NOT EXISTS). Written once to the db file here; worker connections
        # open the core schema and see these tables already present.
        for sf in (pack.schema_files if pack else ()):
            self.conn.executescript(Path(sf).read_text())
        if pack and pack.block_files:
            # pack code registers its blocks BEFORE workflows compile
            from . import blocks as blocks_mod
            blocks_mod.load_files(pack.block_files)
        dirs = list(pack.workflow_dirs) if pack else []
        dirs += list(extra_defs_dirs)
        self.workflows = loader.load_defs(dirs, pack=pack)
        self.subscriptions = loader.subscriptions(self.workflows)
        self.policy = (pack.policy if pack else None) or None
        self.env = contract.ExecEnv(
            conn=self.conn, subscriptions=self.subscriptions,
            data_dir=self.data_dir, workspaces_dir=self.workspaces_dir,
            pack=pack, policy=self.policy)
        self._lowdisk = False           # resource guard: pause claiming when set
        self._check_schedule()
        self._check_agents()
        self._recover()

    # ------------------------------------------------------------ startup

    def _check_schedule(self):
        """Startup guard: every scheduled event must have at least one
        consumer. A schedule feeding nobody is a config bug (fail loud at
        start), not a stream of ignored log entries at 3am."""
        for entry in (self.pack.schedule if self.pack else ()):
            if entry["event"] not in self.subscriptions:
                from .config import ConfigError
                raise ConfigError(
                    "schedule: no workflow consumes '%s' (consumed events: %s)"
                    % (entry["event"], ", ".join(sorted(self.subscriptions)) or "none"))

    def _check_agents(self):
        """Environment checks for agent bindings (cli resolvable, secrets
        present) — at engine start, AFTER any --replay-from wrap, so replay
        runs never demand the live backend's environment. Structure was
        already checked at pack load; live round-trips are `llm check`."""
        if not self.pack or not self.pack.agents:
            return
        from . import runner
        from .config import ConfigError, load_secrets
        secrets = load_secrets()
        for name, binding in self.pack.agents.items():
            err = runner.check_binding(name, binding, secrets)
            if err:
                raise ConfigError("agents.%s: %s" % (name, err))

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

    def _exec(self, env, task) -> str:
        """Run one task through its workflow on the given exec env (its own db
        connection under the parallel daemon). Worktree cleanup on terminal."""
        wf = self.workflows.get(task["kind"])
        if wf is None:
            return queue.fail(env.conn, task["id"], "framework_bug",
                              detail="no workflow handles kind '%s'" % task["kind"],
                              policy=self.policy, subscriptions=self.subscriptions)
        state = contract.execute(env, wf, task)
        if state in queue.TERMINAL_TASK_STATES:
            self._cleanup_task_workspace(task["id"])
        return state

    def execute_one(self, task) -> str:
        return self._exec(self.env, task)

    def _cleanup_task_workspace(self, task_id):
        for entry in Path(self.workspaces_dir).glob("task-%d-a*" % task_id):
            self._drop_worktree(entry)

    # ------------------------------------------------------- concurrency

    def _lanes(self) -> dict:
        """Build the lane semaphores from pack.concurrency.lanes. Shared across
        workers — that's what makes a capped lane (e.g. build=1) serialize."""
        cfg = ((self.pack.concurrency or {}).get("lanes") or {}) if self.pack else {}
        return {name: threading.BoundedSemaphore(max(1, int(cap)))
                for name, cap in cfg.items()}

    def _workers(self) -> int:
        return max(1, int((self.pack.concurrency or {}).get("workers", 1))) \
            if self.pack else 1

    def _worker_env(self, lanes):
        """A worker gets its OWN sqlite connection (handles aren't shareable
        across threads); WAL + busy_timeout + BEGIN IMMEDIATE (claim/apply)
        keep writes safe. The lane semaphores are shared."""
        conn = db.connect(self.state_dir / "forgeflow.db")
        env = contract.ExecEnv(
            conn=conn, subscriptions=self.subscriptions, data_dir=self.data_dir,
            workspaces_dir=self.workspaces_dir, pack=self.pack, lanes=lanes,
            policy=self.policy)
        return conn, env

    def _worker_loop(self, lanes, stop, executed=None):
        conn, env = self._worker_env(lanes)
        try:
            while not stop.is_set():
                if self._lowdisk:                        # resource guard
                    time.sleep(0.5)
                    continue
                try:
                    task = queue.claim(conn)
                except Exception as e:                       # keep the worker alive
                    print("worker: claim: %s" % e, file=sys.stderr)
                    time.sleep(0.1)
                    continue
                if task is None:
                    time.sleep(0.03)
                    continue
                try:
                    self._exec(env, task)
                    if executed is not None:
                        executed[0] += 1
                except Exception as e:
                    print("worker: task %s: %s" % (task["id"], e), file=sys.stderr)
        finally:
            conn.close()

    def run_until_idle(self, grace_s: float = 2.0, workers: int = 1) -> int:
        """Drive the claim loop until nothing is eligible and no retry_wait
        comes due within grace_s. workers>1 runs a bounded parallel pool (same
        semantics, lane caps enforced). Returns how many tasks were executed."""
        if workers <= 1:
            executed = 0
            while True:
                task = queue.claim(self.conn)
                if task is not None:
                    self._exec(self.env, task)
                    executed += 1
                    continue
                due = self.conn.execute(
                    "SELECT 1 FROM tasks WHERE state='retry_wait' AND"
                    " next_attempt <= datetime('now', '+' || ? || ' seconds')"
                    " LIMIT 1", (int(grace_s),)).fetchone()
                if due is None:
                    return executed
                time.sleep(0.05)
        # parallel: pool + a supervisor that stops when no work remains. A
        # claimed task is 'running' atomically, so polling states is race-free.
        lanes = self._lanes()
        stop = threading.Event()
        executed = [0]
        threads = [threading.Thread(target=self._worker_loop,
                                    args=(lanes, stop, executed), daemon=True)
                   for _ in range(workers)]
        for t in threads:
            t.start()
        try:
            while True:
                time.sleep(0.05)
                left = self.conn.execute(
                    "SELECT 1 FROM tasks WHERE state IN ('pending','running','retry_wait')"
                    " AND (next_attempt IS NULL OR"
                    "      next_attempt <= datetime('now','+'||?||' seconds')) LIMIT 1",
                    (int(grace_s),)).fetchone()
                if left is None:
                    break
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=10)
        return executed[0]

    def run(self):
        """The long-running daemon loop. flock enforces one per state dir; a
        second start exits 0 with a message. With concurrency.workers>1, a pool
        of workers claims in parallel (per-worker connections, lane-throttled)
        while this thread runs the park-recovery tick. Crash-safe either way:
        an interrupted step re-runs on resume (reset_orphans)."""
        import fcntl
        lock_path = self.state_dir / "daemon.lock"
        lock = open(lock_path, "w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            print("engine: another daemon holds %s — exiting" % lock_path)
            return 0
        self._start_http()
        idle = self.pack.idle_interval_s if self.pack else 15
        unpark_every = self.pack.unpark_interval_s if self.pack else 600
        workers = self._workers()

        if workers > 1:
            lanes = self._lanes()
            stop = threading.Event()
            pool = [threading.Thread(target=self._worker_loop,
                                     args=(lanes, stop), daemon=True)
                    for _ in range(workers)]
            for t in pool:
                t.start()
            print("engine: %d workers, lane caps=%s"
                  % (workers, {k: v._initial_value for k, v in lanes.items()}))
            last_unpark = last_beat = 0.0
            try:
                while True:
                    now = time.monotonic()
                    self._disk_gate()                    # sets _lowdisk for workers
                    if now - last_beat >= 10:
                        self._beat()
                        last_beat = now
                    if now - last_unpark >= unpark_every:
                        self._unpark_tick()
                        last_unpark = now
                    self._schedule_tick()
                    time.sleep(min(idle, unpark_every))
            finally:
                stop.set()
                for t in pool:
                    t.join(timeout=10)
            return

        last_unpark = last_beat = 0.0
        while True:                                          # single-worker path
            now = time.monotonic()
            if now - last_beat >= 10:
                self._beat()
                last_beat = now
            if now - last_unpark >= unpark_every:
                self._unpark_tick()
                last_unpark = now
            self._schedule_tick()
            if not self._disk_gate():                        # resource guard
                time.sleep(idle)
                continue
            task = queue.claim(self.conn)
            if task is None:
                time.sleep(idle)
                continue
            self._exec(self.env, task)

    def _start_http(self):
        """Serve the dashboard/API inside the daemon when the pack asks for
        it. Fail loud at start if the configured token secret is missing —
        never fall back to serving unauthenticated."""
        spec = self.pack.http if self.pack else None
        if not spec:
            return None
        token = None
        if spec["token_ref"]:
            from .config import ConfigError, load_secrets
            token = load_secrets().get("HTTP_TOKEN_%s" % spec["token_ref"])
            if not token:
                raise ConfigError("http: secret HTTP_TOKEN_%s not in the "
                                  "secrets file — refuse to serve" % spec["token_ref"])
        from . import httpd
        server = httpd.serve(self.root, self.subscriptions, host=spec["host"],
                             port=spec["port"], token=token,
                             pack_name=self.pack.name)
        httpd.serve_in_thread(server)
        print("engine: http dashboard/api on %s:%d%s"
              % (server.server_address[0], server.server_address[1],
                 " (token required)" if token else ""))
        return server

    def _disk_ok(self) -> bool:
        mb = getattr(self.pack, "min_free_disk_mb", 0) if self.pack else 0
        if not mb:
            return True
        try:
            return shutil.disk_usage(str(self.root)).free >= mb * 1024 * 1024
        except OSError:
            return True

    def _disk_gate(self) -> bool:
        """Resource guard: don't take new work while free disk is below the
        pack's floor (prevents a fill-the-disk outage). Logs the transitions;
        `_lowdisk` is read by the workers/serial loop before claiming."""
        ok = self._disk_ok()
        if not ok and not self._lowdisk:
            print("engine: LOW DISK (< %d MB free) — pausing new work"
                  % self.pack.min_free_disk_mb, file=sys.stderr)
            self._lowdisk = True
        elif ok and self._lowdisk:
            print("engine: disk recovered — resuming", file=sys.stderr)
            self._lowdisk = False
        return ok

    def _beat(self):
        """Daemon liveness: stamp a heartbeat (wall epoch) in watermarks so
        `doctor` can tell a running daemon from a dead/stuck one."""
        self.conn.execute(
            "INSERT INTO watermarks(scope, cursor) VALUES('daemon.heartbeat', ?)"
            " ON CONFLICT(scope) DO UPDATE SET cursor=excluded.cursor",
            (str(int(time.time())),))

    def _schedule_tick(self, now=None) -> int:
        """Timed triggers: emit each schedule entry's event once per every_s
        window. The window start (epoch) rides in the payload as
        'schedule_occurrence' AND a watermark cursor records the last window
        emitted — together: exactly-once per window across restarts, and no
        catch-up storm (a daemon down for three windows fires only the
        current one; missed windows are skipped by design). A schedule seen
        for the first time fires immediately. Effective resolution is the
        daemon loop cadence (idle_interval_s)."""
        entries = self.pack.schedule if self.pack else ()
        if not entries:
            return 0
        from .util import tx
        now = int(time.time() if now is None else now)
        fired = 0
        for e in entries:
            occurrence = now - (now % e["every_s"])
            scope = "schedule.%s" % e["event"]
            row = self.conn.execute(
                "SELECT cursor FROM watermarks WHERE scope=?", (scope,)).fetchone()
            if row is not None and occurrence <= int(row["cursor"]):
                continue
            payload = dict(e["data"])
            payload["schedule_occurrence"] = occurrence
            with tx(self.conn):
                db.emit_event(self.conn, e["event"], payload, self.subscriptions)
                self.conn.execute(
                    "INSERT INTO watermarks(scope, cursor) VALUES (?,?)"
                    " ON CONFLICT(scope) DO UPDATE SET cursor=excluded.cursor",
                    (scope, str(occurrence)))
            fired += 1
            print("engine: schedule fired %s (window %d, every %ds)"
                  % (e["event"], occurrence, e["every_s"]))
        return fired

    def _unpark_tick(self):
        """Recover parked tasks by per-class cadence. Backend-dependent classes
        (agent_limit / agent_backend) are additionally health-gated: they only
        restart when the agent endpoint answers. If it's still down, re-arm the
        clock so the next probe is a full cadence away."""
        due = queue.parked_due(self.conn, policy=self.policy)
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
