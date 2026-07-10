"""The optional HTTP front door: a read-only dashboard + JSON API + one
write endpoint (POST /api/emit), served from inside the daemon.

Stdlib only (http.server), same trust rules as the rest of the engine:
- Config refuses to bind beyond loopback without a token (config._parse_http).
- If a token is configured, EVERY request must carry it
  (Authorization: Bearer <token>); without one, loopback requests are as
  trusted as the CLI on the same machine.
- The token comes from the secrets file (HTTP_TOKEN_<REF>) — never from
  pack files or argv.
- POST /api/emit accepts only events some workflow consumes — an HTTP
  emit that nobody would react to is a caller bug (400), not a log entry.
- Every handler thread opens its own SQLite connection (WAL + busy_timeout
  make that safe next to the daemon's workers).
"""
from __future__ import annotations

import html
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import db


def serve(root, subscriptions, host="127.0.0.1", port=0, token=None,
          pack_name=""):
    """Start the server on host:port (port 0 = ephemeral, for tests).
    Returns the ThreadingHTTPServer; the caller owns shutdown()."""
    db_path = Path(root) / "state" / "forgeflow.db"

    class Handler(_Handler):
        pass

    Handler.db_path = db_path
    Handler.subscriptions = subscriptions
    Handler.token = token
    Handler.pack_name = pack_name
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server


def serve_in_thread(server):
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return t


class _Handler(BaseHTTPRequestHandler):
    db_path = None
    subscriptions = {}
    token = None
    pack_name = ""

    # -------------------------------------------------------------- plumbing
    def log_message(self, fmt, *args):   # compact daemon-style log line
        print("httpd: %s %s" % (self.address_string(), fmt % args))

    def _send(self, code, body, content_type="application/json"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _json(self, code, obj):
        self._send(code, json.dumps(obj, sort_keys=True))

    def _authorized(self) -> bool:
        if not self.token:
            return True              # loopback-only bind (enforced by config)
        got = self.headers.get("Authorization", "")
        return got == "Bearer %s" % self.token

    def _conn(self):
        return db.connect(self.db_path)

    # -------------------------------------------------------------- routes
    def do_GET(self):
        if not self._authorized():
            return self._json(401, {"error": "missing or bad bearer token"})
        try:
            if self.path == "/" or self.path == "/index.html":
                return self._send(200, _dashboard(self._conn(), self.pack_name),
                                  content_type="text/html")
            if self.path == "/api/status":
                return self._json(200, _status(self._conn()))
            if self.path == "/api/metrics":
                return self._json(200, _metrics(self._conn()))
            if self.path.startswith("/api/task/"):
                raw = self.path[len("/api/task/"):]
                if not raw.isdigit():
                    return self._json(400, {"error": "task id must be an integer"})
                obj = _task(self._conn(), int(raw))
                if obj is None:
                    return self._json(404, {"error": "no task %s" % raw})
                return self._json(200, obj)
            return self._json(404, {"error": "unknown path %s" % self.path})
        except Exception as e:                       # never kill the thread
            return self._json(500, {"error": str(e)})

    def do_POST(self):
        if not self._authorized():
            return self._json(401, {"error": "missing or bad bearer token"})
        if self.path != "/api/emit":
            return self._json(404, {"error": "unknown path %s" % self.path})
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > 1 << 20:
                return self._json(413, {"error": "body too large"})
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            except ValueError:
                return self._json(400, {"error": "body must be JSON"})
            name = body.get("name")
            data = body.get("data") or {}
            if not isinstance(name, str) or not name:
                return self._json(400, {"error": "need 'name' (event to emit)"})
            if not isinstance(data, dict):
                return self._json(400, {"error": "'data' must be a JSON object"})
            if name not in self.subscriptions:
                return self._json(400, {
                    "error": "no workflow consumes '%s'" % name,
                    "consumed_events": sorted(self.subscriptions)})
            if body.get("force"):
                import time
                data["_force"] = time.time_ns()
            conn = self._conn()
            event_id = db.emit_event(conn, name, data, self.subscriptions)
            return self._json(200, {"event_id": event_id, "event": name,
                                    "consumers": self.subscriptions[name]})
        except Exception as e:
            return self._json(500, {"error": str(e)})


# ------------------------------------------------------------------ queries

def _status(conn):
    tasks = {}
    for r in conn.execute("SELECT state, kind, count(*) c FROM tasks"
                          " GROUP BY state, kind ORDER BY state, kind"):
        tasks.setdefault(r["state"], {})[r["kind"]] = r["c"]
    parked = [{"id": r["id"], "kind": r["kind"], "reason": r["park_reason"],
               "attempts": r["attempts"]}
              for r in conn.execute("SELECT id, kind, park_reason, attempts"
                                    " FROM tasks WHERE state='parked' ORDER BY id")]
    joins = [{"group": r["id"], "event": r["event"], "expect": r["expect_n"],
              "terminal": r["done_n"]}
             for r in conn.execute(
                 "SELECT g.id, g.event, g.expect_n, (SELECT count(state) FROM"
                 " join_members m WHERE m.group_id=g.id) done_n"
                 " FROM join_groups g WHERE g.fired_at IS NULL ORDER BY g.id")]
    events = [{"id": r["id"], "name": r["name"], "at": r["at"]}
              for r in conn.execute("SELECT id, name, at FROM events"
                                    " ORDER BY id DESC LIMIT 20")]
    hb = conn.execute("SELECT cursor FROM watermarks"
                      " WHERE scope='daemon.heartbeat'").fetchone()
    return {"tasks": tasks, "parked": parked, "open_joins": joins,
            "recent_events": events,
            "daemon_heartbeat_epoch": int(hb["cursor"]) if hb else None}


def _metrics(conn):
    q = lambda s, *a: conn.execute(s, a).fetchone()[0]
    depth = {st: q("SELECT count(*) FROM tasks WHERE state=?", st)
             for st in ("pending", "running", "retry_wait", "parked")}
    out = {"queue_depth": depth,
           "done_last_1h": q("SELECT count(*) FROM tasks WHERE state='done'"
                             " AND updated_at > datetime('now','-1 hours')"),
           "done_last_24h": q("SELECT count(*) FROM tasks WHERE state='done'"
                              " AND updated_at > datetime('now','-1 days')"),
           "done": q("SELECT count(*) FROM tasks WHERE state='done'"),
           "failed": q("SELECT count(*) FROM tasks WHERE state='failed'"),
           "parked_by_class": {r["error_class"]: r["c"] for r in conn.execute(
               "SELECT error_class, count(*) c FROM tasks WHERE state='parked'"
               " GROUP BY error_class")}}
    runs = q("SELECT count(*) FROM runs")
    out["agent_runs"] = runs
    if runs:
        out["agent_errors"] = q("SELECT count(*) FROM runs WHERE exit_code!=0"
                                " OR verdict='error'")
    return out


def _task(conn, task_id):
    t = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if t is None:
        return None
    steps = [{"attempt": r["attempt"], "step": r["step"],
              "outcome": r["outcome"], "wall_ms": r["wall_ms"], "at": r["at"],
              "result": json.loads(r["result"] or "{}")}
             for r in conn.execute("SELECT * FROM task_steps WHERE task_id=?"
                                   " ORDER BY rowid", (task_id,))]
    runs = [{"id": r["id"], "model": r["model"], "verdict": r["verdict"],
             "exit_code": r["exit_code"], "prompt_sha": r["prompt_sha"]}
            for r in conn.execute("SELECT * FROM runs WHERE task_id=?",
                                  (task_id,))]
    return {"id": t["id"], "kind": t["kind"], "state": t["state"],
            "attempts": t["attempts"], "error_class": t["error_class"],
            "park_reason": t["park_reason"], "def_hash": t["def_hash"],
            "created_at": t["created_at"], "updated_at": t["updated_at"],
            "payload": json.loads(t["payload"]), "steps": steps, "runs": runs}


# ---------------------------------------------------------------- dashboard

_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><meta http-equiv="refresh" content="5">
<title>forgeflow%(title)s</title>
<style>
 body { font: 14px/1.45 -apple-system, "Segoe UI", sans-serif; margin: 2rem;
        color: #222; background: #fafafa; }
 h1 { font-size: 1.2rem; } h2 { font-size: 1rem; margin-top: 1.6rem; }
 table { border-collapse: collapse; min-width: 24rem; }
 th, td { text-align: left; padding: .25rem .8rem .25rem 0;
          border-bottom: 1px solid #e3e3e3; }
 .state-done { color: #1a7f37; } .state-failed { color: #c0392b; }
 .state-parked { color: #b7791f; } .state-running { color: #1f6feb; }
 .muted { color: #888; } code { background: #f0f0f0; padding: 0 .3em; }
</style></head><body>
<h1>forgeflow%(title)s <span class="muted">%(beat)s</span></h1>
%(sections)s
<p class="muted">auto-refreshes every 5s · JSON: <code>/api/status</code>
<code>/api/metrics</code> <code>/api/task/&lt;id&gt;</code> ·
emit: <code>POST /api/emit {"name": ..., "data": {...}}</code></p>
</body></html>"""


def _dashboard(conn, pack_name):
    st = _status(conn)
    esc = html.escape
    parts = []

    rows = []
    for state in sorted(st["tasks"]):
        for kind, n in sorted(st["tasks"][state].items()):
            rows.append("<tr><td class='state-%s'>%s</td><td>%s</td>"
                        "<td>%d</td></tr>" % (esc(state), esc(state), esc(kind), n))
    parts.append("<h2>tasks</h2><table><tr><th>state</th><th>kind</th>"
                 "<th>count</th></tr>%s</table>"
                 % ("".join(rows) or "<tr><td colspan=3 class=muted>none</td></tr>"))

    if st["parked"]:
        rows = ["<tr><td>#%d</td><td>%s</td><td>%s</td><td>%d</td></tr>"
                % (p["id"], esc(p["kind"]), esc(str(p["reason"])), p["attempts"])
                for p in st["parked"]]
        parts.append("<h2>parked</h2><table><tr><th>task</th><th>kind</th>"
                     "<th>reason</th><th>attempts</th></tr>%s</table>" % "".join(rows))

    if st["open_joins"]:
        rows = ["<tr><td>%d</td><td>%s</td><td>%d/%d</td></tr>"
                % (j["group"], esc(j["event"]), j["terminal"], j["expect"])
                for j in st["open_joins"]]
        parts.append("<h2>open joins</h2><table><tr><th>group</th><th>fires</th>"
                     "<th>terminal</th></tr>%s</table>" % "".join(rows))

    rows = ["<tr><td>%d</td><td class=muted>%s</td><td>%s</td></tr>"
            % (e["id"], esc(e["at"]), esc(e["name"]))
            for e in st["recent_events"]]
    parts.append("<h2>recent events</h2><table><tr><th>id</th><th>at</th>"
                 "<th>event</th></tr>%s</table>"
                 % ("".join(rows) or "<tr><td colspan=3 class=muted>none</td></tr>"))

    import time
    beat = ""
    if st["daemon_heartbeat_epoch"]:
        age = int(time.time()) - st["daemon_heartbeat_epoch"]
        beat = ("daemon heartbeat %ds ago" % age) if age < 3600 else \
               "daemon heartbeat stale"
    return _PAGE % {"title": esc(" · " + pack_name if pack_name else ""),
                    "beat": esc(beat), "sections": "\n".join(parts)}
