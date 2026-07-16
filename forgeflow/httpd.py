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
          pack_name="", workflows=None, board=None):
    """Start the server on host:port (port 0 = ephemeral, for tests).
    Returns the ThreadingHTTPServer; the caller owns shutdown()."""
    db_path = Path(root) / "state" / "forgeflow.db"

    class Handler(_Handler):
        pass

    Handler.db_path = db_path
    Handler.subscriptions = subscriptions
    Handler.token = token
    Handler.pack_name = pack_name
    Handler.workflows = workflows or {}
    Handler.board = board or {}
    Handler.data_dir = Path(root) / "data"
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
    workflows = {}
    board = {}
    data_dir = None

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
                return self._send(200, _dashboard(self._conn(), self.pack_name, self.board),
                                  content_type="text/html")
            if self.path == "/api/status":
                return self._json(200, _status(self._conn()))
            if self.path == "/api/metrics":
                return self._json(200, _metrics(self._conn()))
            if self.path == "/decisions":
                return self._send(200, _decisions_page(self._conn(), self.pack_name),
                                  content_type="text/html")
            if self.path.startswith("/task/"):
                raw = self.path[len("/task/"):]
                if not raw.isdigit():
                    return self._json(400, {"error": "task id must be an integer"})
                page = _task_page(self._conn(), int(raw), self.workflows,
                                  self.board, self.pack_name)
                if page is None:
                    return self._json(404, {"error": "no task %s" % raw})
                return self._send(200, page, content_type="text/html")
            if self.path.startswith("/api/run/"):
                parts = self.path[len("/api/run/"):].split("/", 1)
                if len(parts) != 2 or not parts[0].isdigit():
                    return self._json(400, {"error": "want /api/run/<id>/<artifact>"})
                body = _run_artifact(self.data_dir, int(parts[0]), parts[1])
                if body is None:
                    return self._json(404, {"error": "no such artifact"})
                return self._send(200, body, content_type="text/plain")
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
        if self.path.startswith("/api/decision/") and self.path.endswith("/resolve"):
            raw = self.path[len("/api/decision/"):-len("/resolve")]
            if not raw.isdigit():
                return self._json(400, {"error": "decision id must be an integer"})
            return self._resolve_decision(int(raw))
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


    def _resolve_decision(self, decision_id):
        from urllib.parse import parse_qs
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > 1 << 20:
                return self._json(413, {"error": "body too large"})
            raw = self.rfile.read(length).decode("utf-8")
            ctype = self.headers.get("Content-Type", "")
            if "json" in ctype:
                body = json.loads(raw or "{}")
                verdict = body.get("verdict")
                answer = {k: v for k, v in body.items()
                          if k in ("picked", "rejected", "comment") and v}
            else:
                q = parse_qs(raw)
                verdict = (q.get("verdict") or [None])[0]
                answer = {}
                if q.get("picked"):
                    answer["picked"] = q["picked"][0]
                if q.get("rejected"):
                    answer["rejected"] = q["rejected"]
                if q.get("comment") and q["comment"][0].strip():
                    answer["comment"] = q["comment"][0].strip()
            if verdict == "picked" and not answer.get("picked"):
                return self._json(400, {"error": "picked needs an option"})
            conn = self._conn()
            tid = db.resolve_decision(conn, decision_id, verdict, answer,
                                      answered_by="board")
            if "json" in ctype:
                return self._json(200, {"decision": decision_id,
                                        "verdict": verdict, "resumed_task": tid})
            self.send_response(303)
            self.send_header("Location", "/decisions")
            self.send_header("Content-Length", "0")
            self.end_headers()
        except ValueError as e:
            return self._json(400, {"error": str(e)})
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
        out["llm_per_model"] = [
            {"model": r["model"], "runs": r["n"], "no_verdict": r["noverdict"],
             "avg_wall_ms": r["avg_ms"], "max_wall_ms": r["max_ms"],
             "reasks": r["reasks"] or 0}
            for r in conn.execute(
                "SELECT model, count(*) n,"
                " sum(CASE WHEN verdict IS NULL THEN 1 ELSE 0 END) noverdict,"
                " CAST(avg(wall_ms) AS INT) avg_ms, max(wall_ms) max_ms,"
                " sum(COALESCE(reasks,0)) reasks"
                " FROM runs WHERE finished_at IS NOT NULL"
                " GROUP BY model ORDER BY n DESC")]
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




# ---------------------------------------------------------- task page

_ARTIFACTS = {"prompt": "ask0/prompt", "stdout": "ask0/stdout",
              "stderr": "ask0/stderr", "verdict": "verdict.json",
              "context": "context.json"}
_BAD_OUTCOMES = {"red", "fail", "FAIL", "invalid", "broken", "error",
                 "timeout", "empty", "giveup", "waiting"}


def _run_artifact(data_dir, run_id, name):
    """Stream a run artifact (tail-limited). Whitelisted names only — no
    path input ever touches the filesystem."""
    rel = _ARTIFACTS.get(name)
    if rel is None or data_dir is None:
        return None
    f = Path(data_dir) / "runs" / str(run_id) / rel
    if not f.is_file():
        return None
    data = f.read_bytes()
    return data[-262144:]                       # last 256 KB is plenty


def _panel_html(conn, panel, payload):
    """Render one pack-declared board panel (SELECT-only, enforced at load).
    A broken panel renders its error — it must never take the page down."""
    esc = html.escape
    try:
        args = {name: (payload or {}).get(key)
                for name, key in (panel.get("params") or {}).items()}
        cur = conn.execute(panel["sql"], args)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(500)
    except Exception as e:
        return "<h2>%s</h2><p class=muted>panel error: %s</p>" % (
            esc(panel["title"]), esc(str(e)))
    out = ["<h2>%s</h2>" % esc(panel["title"])]
    if panel["kind"] == "status_grid":
        cells = []
        for r in rows:
            d = dict(zip(cols, r))
            status = str(d.get("status", ""))
            cls = ("ok" if status in ("done", "green", "pass", "resolved")
                   else "cur" if status in ("active", "running")
                   else "warn" if status not in ("pending", "")
                   else "off")
            label = esc(str(d.get("label", "?")))
            extra = d.get("attempts")
            badge = ("<sup>%s</sup>" % extra) if extra else ""
            cells.append('<span class="cell %s" title="%s: %s">%s%s</span>'
                         % (cls, label, esc(status), label, badge))
        out.append('<div class="grid">%s</div>'
                   % ("".join(cells) or "<span class=muted>none</span>"))
    elif panel["kind"] == "kv":
        out.append("<table>%s</table>" % "".join(
            "<tr><th>%s</th><td>%s</td></tr>"
            % (esc(str(r[0])), esc(str(r[1] if len(r) > 1 else "")))
            for r in rows))
    else:                                        # table
        head = "".join("<th>%s</th>" % esc(c) for c in cols)
        body = "".join(
            "<tr>%s</tr>" % "".join("<td>%s</td>" % esc(str(v)) for v in r)
            for r in rows)
        out.append("<table><tr>%s</tr>%s</table>"
                   % (head, body or "<tr><td class=muted>none</td></tr>"))
    return "".join(out)


def _task_page(conn, task_id, workflows, board, pack_name):
    esc = html.escape
    t = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if t is None:
        return None
    payload = json.loads(t["payload"] or "{}")
    attempt = t["attempts"]
    rows = conn.execute(
        "SELECT step, outcome, wall_ms, at, result FROM task_steps"
        " WHERE task_id=? AND attempt=? ORDER BY rowid",
        (task_id, attempt)).fetchall()
    by_step = {r["step"]: r for r in rows}
    current = rows[-1]["step"] if rows and t["state"] == "running" else None

    parts = []
    parts.append('<p><a href="/">&larr; overview</a></p>')
    parts.append("<h2>%s #%d · attempt %d · <span class='state-%s'>%s</span>"
                 " %s</h2>" % (esc(t["kind"]), task_id, attempt,
                               esc(t["state"]), esc(t["state"]),
                               esc(t["error_class"] or "")))

    # the walk, as chips in definition order (the workflow def is the graph)
    wf = workflows.get(t["kind"]) if workflows else None
    if wf is not None:
        chips = []
        for step in wf.steps:
            r = by_step.get(step.name)
            if step.name == current:
                cls = "cur"
            elif r is None:
                cls = "off"
            elif r["outcome"] in _BAD_OUTCOMES:
                cls = "warn"
            else:
                cls = "ok"
            label = esc(step.name)
            if r is not None:
                label += " <small>%s</small>" % esc(str(r["outcome"]))
            chips.append('<span class="cell %s">%s</span>' % (cls, label))
        parts.append('<h2>walk</h2><div class="grid">%s</div>'
                     % " <span class=muted>&rarr;</span> ".join(chips))

    # pack panels (task-scoped)
    for panel in (board or {}).get("task_panels", []):
        parts.append(_panel_html(conn, panel, payload))

    # step trail with expandable results
    trail = []
    for r in reversed(rows):
        res = json.loads(r["result"] or "{}")
        run_id = res.get("_run_id")
        link = (' · <a href="/api/run/%d/prompt">prompt</a>'
                ' <a href="/api/run/%d/stdout">stdout</a>'
                ' <a href="/api/run/%d/verdict">verdict</a>'
                ' <a href="/api/run/%d/context">context</a>'
                % (run_id, run_id, run_id, run_id)) if run_id else ""
        cls = "warn" if r["outcome"] in _BAD_OUTCOMES else "ok"
        trail.append(
            "<tr><td class=muted>%s</td><td>%s</td>"
            "<td class='cell %s'>%s</td><td>%.1fs</td>"
            "<td><details><summary>result%s</summary><pre>%s</pre></details>"
            "</td></tr>"
            % (esc(r["at"]), esc(r["step"]), cls, esc(str(r["outcome"])),
               (r["wall_ms"] or 0) / 1000.0, link,
               esc(json.dumps(res, indent=1, sort_keys=True)[:4000])))
    parts.append("<h2>step trail (attempt %d)</h2><table><tr><th>at</th>"
                 "<th>step</th><th>outcome</th><th>wall</th><th></th></tr>"
                 "%s</table>" % (attempt, "".join(trail) or
                                 "<tr><td colspan=5 class=muted>none</td></tr>"))

    # payload for reference
    parts.append("<h2>payload</h2><pre>%s</pre>"
                 % esc(json.dumps(payload, indent=1, sort_keys=True)[:4000]))

    return _PAGE % {"title": esc(" · %s · task %d" % (pack_name, task_id)),
                    "beat": esc(t["state"]), "sections": _frame(parts)}


# ---------------------------------------------------------- decisions page

def _decisions_page(conn, pack_name):
    esc = html.escape
    parts = ['<p><a href="/">&larr; overview</a></p>']
    rows = conn.execute("SELECT * FROM decisions WHERE status='open'"
                        " ORDER BY id").fetchall()
    if not rows:
        parts.append("<h2>decisions</h2><p class=muted>nothing waiting on you.</p>")
    for r in rows:
        opts = json.loads(r["options"] or "[]")
        cards = []
        for o in opts:
            rich = isinstance(o, dict)
            name = (o.get("title") if rich else o) or "?"
            star = " &#9733;" if name == r["recommended"] else ""
            inner = ["<strong>%s%s</strong>" % (esc(str(name)), star)]
            if rich:
                if o.get("summary"):
                    inner.append("<p>%s</p>" % esc(str(o["summary"])))
                for label, key, cls in (("+", "pros", "ok"), ("&minus;", "cons", "warn")):
                    for item in (o.get(key) or []):
                        inner.append('<div class="pc %s">%s %s</div>'
                                     % (cls, label, esc(str(item))))
                if o.get("risks"):
                    inner.append('<div class="pc off">risk: %s</div>'
                                 % esc(str(o["risks"])))
                if o.get("sketch"):
                    inner.append("<details><summary>sketch</summary>"
                                 "<pre>%s</pre></details>" % esc(str(o["sketch"])[:2000]))
            inner.append(
                '<div class="pick"><label><input type="radio" name="picked"'
                ' value="%s"%s> pick</label> <label><input type="checkbox"'
                ' name="rejected" value="%s"> reject</label></div>'
                % (esc(str(name)), " checked" if name == r["recommended"] else "",
                   esc(str(name))))
            cards.append('<div class="opt">%s</div>' % "".join(inner))
        parts.append(
            '<h2>%s · round %d · %s</h2>'
            '<form method="post" action="/api/decision/%d/resolve">'
            '<p>%s</p>%s'
            '<div class="opts">%s</div>'
            '<p><input name="comment" placeholder="comment / discussion&hellip;"'
            ' style="width:60%%"></p>'
            '<p><button name="verdict" value="picked">Pick selected</button> '
            '<button name="verdict" value="revise">Revise (send rejections'
            ' + comment)</button> <button name="verdict" value="reframe">'
            'Reframe</button> <button name="verdict" value="abandon">Abandon'
            '</button></p></form>'
            % (esc(r["key"]), r["round"], esc(r["kind"]), r["id"],
               esc(r["title"]),
               ("<p class=muted>%s</p>" % esc(r["body"])) if r["body"] else "",
               "".join(cards)))
    hist = conn.execute("SELECT key, round, verdict, answered_by, resolved_at"
                        " FROM decisions WHERE status='resolved'"
                        " ORDER BY resolved_at DESC LIMIT 10").fetchall()
    if hist:
        parts.append("<h2>recently decided</h2><table><tr><th>key</th>"
                     "<th>round</th><th>verdict</th><th>by</th><th>at</th></tr>%s</table>"
                     % "".join("<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td>"
                               "<td class=muted>%s</td></tr>"
                               % (esc(h["key"]), h["round"], esc(str(h["verdict"])),
                                  esc(str(h["answered_by"])), esc(str(h["resolved_at"])))
                               for h in hist))
    return _PAGE % {"title": esc(" · %s · decisions" % pack_name),
                    "beat": "", "sections": _frame(parts)}


# ---------------------------------------------------------------- dashboard

_PAGE = """<!doctype html>
<html><head><meta charset="utf-8">
<title>forgeflow%(title)s</title>
<style>
 :root {
   --bg: #101418; --card: #171d24; --card-edge: #232c36;
   --ink: #d7dee6; --dim: #7d8a97; --faint: #4a545f;
   --ember: #e8963a; --ok: #4cc38a; --bad: #e5534b; --run: #539bf5;
   --wait: #d4a72c; --mono: ui-monospace, "SF Mono", "Cascadia Code",
   Menlo, Consolas, monospace;
 }
 @media (prefers-color-scheme: light) {
   :root { --bg:#f3f4f6; --card:#ffffff; --card-edge:#dfe3e8; --ink:#1f262e;
           --dim:#5c6773; --faint:#9aa4af; --ember:#c26d10; --ok:#1a7f37;
           --bad:#c73e36; --run:#0f62d6; --wait:#9a6700; }
 }
 * { box-sizing: border-box; }
 body { margin: 0; background: var(--bg); color: var(--ink);
        font: 14px/1.5 -apple-system, "Segoe UI", system-ui, sans-serif; }
 a { color: var(--run); text-decoration: none; }
 a:hover { text-decoration: underline; }
 header { display: flex; align-items: baseline; gap: .8rem;
          padding: .9rem 1.4rem; border-bottom: 1px solid var(--card-edge); }
 header .name { font-family: var(--mono); font-weight: 700; font-size: 1rem;
                color: var(--ember); letter-spacing: .02em; }
 header .name a { color: inherit; }
 header .beat { margin-left: auto; font-family: var(--mono); font-size: .75rem;
                color: var(--dim); border: 1px solid var(--card-edge);
                border-radius: 99px; padding: .15rem .7rem; }
 main { max-width: 68rem; margin: 0 auto; padding: 1.1rem 1.4rem 3rem;
        display: flex; flex-direction: column; gap: .9rem; }
 section.card { background: var(--card); border: 1px solid var(--card-edge);
                border-radius: 10px; padding: .9rem 1.1rem 1rem; }
 h2 { margin: 0 0 .55rem; font-size: .72rem; font-weight: 600;
      letter-spacing: .14em; text-transform: uppercase; color: var(--dim); }
 table { border-collapse: collapse; width: 100%%; font-family: var(--mono);
         font-size: .82rem; font-variant-numeric: tabular-nums; }
 th { text-align: left; font: 600 .68rem/1.6 -apple-system, sans-serif;
      letter-spacing: .1em; text-transform: uppercase; color: var(--faint);
      padding: 0 .9rem .3rem 0; border-bottom: 1px solid var(--card-edge); }
 td { padding: .34rem .9rem .34rem 0; border-bottom: 1px solid
      color-mix(in srgb, var(--card-edge) 55%%, transparent); vertical-align: top; }
 tr:last-child td { border-bottom: 0; }
 tr:hover td { background: color-mix(in srgb, var(--run) 4%%, transparent); }
 .muted { color: var(--dim); } code { font-family: var(--mono);
   background: color-mix(in srgb, var(--card-edge) 60%%, transparent);
   padding: .05em .4em; border-radius: 4px; }
 .state-done, .state-pass { color: var(--ok); }
 .state-failed { color: var(--bad); }
 .state-parked, .state-retry_wait { color: var(--wait); }
 .state-running, .state-pending { color: var(--run); }
 .state-done::before, .state-failed::before, .state-parked::before,
 .state-running::before, .state-pending::before, .state-retry_wait::before {
   content: "\25CF\00A0"; font-size: .7em; vertical-align: .15em; }
 .grid { display: flex; flex-wrap: wrap; gap: .3rem .35rem; align-items: center;
         font-family: var(--mono); font-size: .78rem; }
 .cell { padding: .16rem .55rem; border-radius: 6px; white-space: nowrap;
         border: 1px solid var(--card-edge);
         background: color-mix(in srgb, var(--card-edge) 35%%, transparent); }
 .cell small { color: var(--dim); font-size: .85em; margin-left: .35em; }
 .cell.ok   { border-color: color-mix(in srgb, var(--ok) 45%%, transparent);
              color: var(--ok); }
 .cell.warn { border-color: color-mix(in srgb, var(--bad) 50%%, transparent);
              color: var(--bad); }
 .cell.cur  { border-color: var(--ember); color: var(--ember);
              box-shadow: 0 0 9px color-mix(in srgb, var(--ember) 35%%, transparent); }
 .cell.off  { color: var(--faint); }
 .cell sup  { color: var(--wait); }
 .arrow { color: var(--faint); font-size: .8rem; }
 .opts { display: flex; flex-wrap: wrap; gap: .7rem; }
 .opt { flex: 1 1 16rem; max-width: 22rem; border: 1px solid var(--card-edge);
        border-radius: 8px; padding: .7rem .8rem; font-size: .85rem;
        background: color-mix(in srgb, var(--card-edge) 20%%, transparent); }
 .opt p { margin: .3rem 0; color: var(--dim); }
 .pc { font-family: var(--mono); font-size: .78rem; margin: .12rem 0; }
 .pc.ok { color: var(--ok); } .pc.warn { color: var(--bad); }
 .pc.off { color: var(--wait); }
 .pick { margin-top: .5rem; font-size: .8rem; color: var(--dim); }
 button { background: var(--card); color: var(--ink); cursor: pointer;
          border: 1px solid var(--card-edge); border-radius: 6px;
          padding: .3rem .8rem; font: inherit; font-size: .8rem; }
 button:hover { border-color: var(--ember); color: var(--ember); }
 input[name=comment] { background: var(--bg); border: 1px solid var(--card-edge);
          border-radius: 6px; color: var(--ink); padding: .3rem .6rem; font: inherit; }
 pre { font-family: var(--mono); font-size: .78rem; line-height: 1.45;
       background: color-mix(in srgb, var(--bg) 70%%, black 8%%);
       border: 1px solid var(--card-edge); border-radius: 8px;
       padding: .7rem .8rem; overflow-x: auto; max-width: 100%%; margin: .4rem 0 0; }
 details summary { cursor: pointer; color: var(--run); font-family: var(--mono);
                   font-size: .78rem; }
 details summary a { margin-left: .4em; }
</style></head><body>
<header><span class="name"><a href="/">forgeflow</a></span>
<span class="muted">%(title)s</span>
<span class="beat">%(beat)s</span></header>
<main>
%(sections)s
<p class="muted" style="font-size:.75rem">JSON <code>/api/status</code>
<code>/api/metrics</code> <code>/api/task/&lt;id&gt;</code> · emit
<code>POST /api/emit</code></p>
</main>
<script>
setInterval(async () => {
  if (document.querySelector("details[open]") || String(getSelection())) return;
  try {
    const r = await fetch(location.href, {cache: "no-store"});
    const d = new DOMParser().parseFromString(await r.text(), "text/html");
    document.querySelector("main").innerHTML = d.querySelector("main").innerHTML;
    document.querySelector("header").innerHTML = d.querySelector("header").innerHTML;
  } catch (e) {}
}, 4000);
</script>
</body></html>"""


def _frame(parts):
    """Wrap each h2-led block in a card; pass bare paragraphs through."""
    out = []
    for part in parts:
        if part.lstrip().startswith("<h2"):
            out.append('<section class="card">%s</section>' % part)
        else:
            out.append(part)
    return "\n".join(out)


def _dashboard(conn, pack_name, board=None):
    st = _status(conn)
    esc = html.escape
    parts = []

    n_open = conn.execute("SELECT count(*) FROM decisions"
                          " WHERE status='open'").fetchone()[0]
    if n_open:
        parts.append('<h2>&#9888; waiting on you</h2><p><a href="/decisions">'
                     '%d open decision%s &rarr;</a></p>'
                     % (n_open, "s" if n_open != 1 else ""))
    rows = []
    for r in conn.execute("SELECT id, kind, state, updated_at FROM tasks"
                          " ORDER BY updated_at DESC, id DESC LIMIT 15"):
        rows.append("<tr><td><a href='/task/%d'>#%d</a></td><td>%s</td>"
                    "<td class='state-%s'>%s</td><td class=muted>%s</td></tr>"
                    % (r["id"], r["id"], esc(r["kind"]), esc(r["state"]),
                       esc(r["state"]), esc(r["updated_at"])))
    parts.append("<h2>recent tasks</h2><table><tr><th>task</th><th>kind</th>"
                 "<th>state</th><th>updated</th></tr>%s</table>"
                 % ("".join(rows) or "<tr><td colspan=4 class=muted>none</td></tr>"))
    for panel in (board or {}).get("overview_panels", []):
        parts.append(_panel_html(conn, panel, {}))

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
                    "beat": esc(beat or "no heartbeat"),
                    "sections": _frame(parts)}
