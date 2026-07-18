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
            if self.path == "/" or self.path == "/index.html" \
                    or self.path.startswith("/?"):
                from urllib.parse import parse_qs, urlparse
                q = {k: v[0] for k, v in
                     parse_qs(urlparse(self.path).query).items()}
                prefill = {k[len("launch_"):]: v for k, v in q.items()
                           if k.startswith("launch_")}
                return self._send(200, _dashboard(self._conn(), self.pack_name,
                                                  self.board, self.workflows,
                                                  prefill=prefill),
                                  content_type="text/html")
            if self.path == "/explore":
                return self._send(200, _explore_page(self._conn(), self.pack_name,
                                                     self.board),
                                  content_type="text/html")
            if self.path == "/api/status":
                return self._json(200, _status(self._conn()))
            if self.path == "/api/metrics":
                return self._json(200, _metrics(self._conn()))
            if self.path == "/decisions":
                return self._send(200, _decisions_page(self._conn(), self.pack_name),
                                  content_type="text/html")
            if self.path.startswith("/view/"):
                from urllib.parse import parse_qs, unquote, urlparse
                u = urlparse(self.path)
                qs = {k: v[0] for k, v in parse_qs(u.query).items()}
                page = _view_page(self._conn(), unquote(u.path[len("/view/"):]),
                                  qs, self.board, self.pack_name)
                if page is None:
                    return self._json(404, {"error": "no such view"})
                return self._send(200, page, content_type="text/html")
            if self.path.startswith("/step/"):
                from urllib.parse import unquote
                parts = self.path[len("/step/"):].split("/", 1)
                if len(parts) != 2:
                    return self._json(400, {"error": "want /step/<workflow>/<step>"})
                page = _step_page(unquote(parts[0]), unquote(parts[1]),
                                  self.workflows, self.pack_name)
                if page is None:
                    return self._json(404, {"error": "no such step"})
                return self._send(200, page, content_type="text/html")
            if self.path.startswith("/run/"):
                from urllib.parse import unquote
                key = unquote(self.path[len("/run/"):])
                page = _run_audit_page(self._conn(), key, self.workflows,
                                       self.board, self.pack_name)
                if page is None:
                    return self._json(404, {"error": "no run '%s'" % key})
                return self._send(200, page, content_type="text/html")
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
        if self.path == "/api/launch":
            return self._launch()
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


    def _launch(self):
        """POST /api/launch: submit a pack-declared launch form. Builds the
        payload from the form fields (path_or_text fields read the file when
        the value is a readable path — same trust as the local CLI) and
        emits the declared event; the run appears on the front page."""
        from urllib.parse import parse_qs
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > 1 << 20:
                return self._json(413, {"error": "body too large"})
            raw = self.rfile.read(length).decode("utf-8")
            if "json" in self.headers.get("Content-Type", ""):
                vals = json.loads(raw or "{}")
            else:
                vals = {k: v[0] for k, v in parse_qs(raw).items()}
            event = vals.get("event")
            spec = next((l for l in (self.board or {}).get("launch", [])
                         if l["event"] == event), None)
            if spec is None:
                return self._json(400, {"error": "no launch form emits %r" % event})
            if event not in self.subscriptions:
                return self._json(400, {"error": "no workflow consumes '%s'" % event})
            data = {}
            for f in spec["fields"]:
                v = str(vals.get(f["name"]) or "").strip() or f["default"]
                if f["required"] and not v:
                    return self._json(400, {"error": "field '%s' is required"
                                            % f["name"]})
                if v and f["kind"] == "path_or_text" and "\n" not in v \
                        and len(v) < 4096:
                    p = Path(v).expanduser()
                    if p.is_file():
                        v = p.read_text(errors="replace")[:1 << 20]
                if v:
                    data[f["name"]] = v
            conn = self._conn()
            event_id = db.emit_event(conn, event, data, self.subscriptions)
            if "json" in self.headers.get("Content-Type", ""):
                return self._json(200, {"event_id": event_id, "event": event})
            self.send_response(303)
            self.send_header("Location", "/")
            self.send_header("Content-Length", "0")
            self.end_headers()
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


def _kv_html(obj, doc_cap=200000):
    """Humane rendering for a payload/result dict: each key its own row;
    a long or multi-line string value renders as the FULL document (wrapped,
    real newlines — never JSON-escaped soup); nested structures stay compact
    JSON. The 4000-char JSON dump this replaces made a requirement doc
    unreadable."""
    esc = html.escape
    rows = []
    for k in sorted(obj, key=lambda s: (s.startswith("_"), s)):
        v = obj[k]
        if isinstance(v, str) and ("\n" in v or len(v) > 120):
            val = ('<pre class="doc">%s</pre>' % esc(v[:doc_cap])) + \
                  ("<p class=muted>… truncated</p>" if len(v) > doc_cap else "")
        elif isinstance(v, str):
            val = "<code>%s</code>" % esc(v)
        else:
            s = json.dumps(v, sort_keys=True)
            if len(s) > 300:
                val = ('<details><summary>%s&hellip;</summary><pre>%s</pre>'
                       '</details>' % (esc(s[:80]),
                                       esc(json.dumps(v, indent=1,
                                                      sort_keys=True)[:doc_cap])))
            else:
                val = "<code>%s</code>" % esc(s)
        cls = " class=muted" if k.startswith("_") else ""
        rows.append("<tr><th%s>%s</th><td>%s</td></tr>" % (cls, esc(k), val))
    return "<table class=kvp>%s</table>" % "".join(rows) if rows \
        else "<p class=muted>empty</p>"


def _view_link(view, value):
    from urllib.parse import quote
    return '<a href="/view/%s?key=%s">%s</a>' % (
        quote(str(view)), quote(str(value)), html.escape(str(value)))


def _sql_args(sql, supplied):
    """Bind exactly the named params the SQL mentions (missing -> None), so
    a view panel can use :key or any other query-string arg."""
    import re
    names = set(re.findall(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)", sql))
    return {n: (supplied or {}).get(n) for n in names}


def _panel_html(conn, panel, payload, args=None):
    """Render one pack-declared board panel (SELECT-only, enforced at load).
    A broken panel renders its error — it must never take the page down.
    Cross-linking convention: a column aliased 'link:<view>' renders each
    cell as a link to /view/<view>?key=<cell> (header shows just <view>)."""
    esc = html.escape
    try:
        if args is None:
            args = {name: (payload or {}).get(key)
                    for name, key in (panel.get("params") or {}).items()}
        cur = conn.execute(panel["sql"], args)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(500)
    except Exception as e:
        return "<h2>%s</h2><p class=muted>panel error: %s</p>" % (
            esc(panel["title"]), esc(str(e)))
    links = {c: c.split(":", 1)[1] for c in cols if c.startswith("link:")}

    def _cell(col, v):
        if v is None:
            return "<span class=muted>&mdash;</span>"
        if col in links:
            return _view_link(links[col], v)
        s = str(v)
        if "\n" in s:                       # multi-line values (code) stay code
            return "<pre>%s</pre>" % esc(s[:4000])
        return esc(s)

    out = ["<h2>%s</h2>" % esc(panel["title"])]
    if panel["kind"] == "status_grid":
        link_col = next(iter(links), None)
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
            cell = ('<span class="cell %s" title="%s: %s">%s%s</span>'
                    % (cls, label, esc(status), label, badge))
            if link_col and d.get(link_col) is not None:
                from urllib.parse import quote
                cell = '<a href="/view/%s?key=%s">%s</a>' % (
                    quote(links[link_col]), quote(str(d[link_col])), cell)
            cells.append(cell)
        out.append('<div class="grid">%s</div>'
                   % ("".join(cells) or "<span class=muted>none</span>"))
    elif panel["kind"] == "kv":
        out.append("<table>%s</table>" % "".join(
            "<tr><th>%s</th><td>%s</td></tr>"
            % (esc(str(r[0])),
               _cell(cols[1] if len(cols) > 1 else "", r[1]) if len(r) > 1 else "")
            for r in rows))
    else:                                        # table
        head = "".join("<th>%s</th>" % esc(links.get(c, c)) for c in cols)
        body = "".join(
            "<tr>%s</tr>" % "".join("<td>%s</td>" % _cell(c, v)
                                    for c, v in zip(cols, r))
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
            "<td><details><summary>result%s</summary>%s</details>"
            "</td></tr>"
            % (esc(r["at"]), esc(r["step"]), cls, esc(str(r["outcome"])),
               (r["wall_ms"] or 0) / 1000.0, link, _kv_html(res)))
    parts.append("<h2>step trail (attempt %d)</h2><table><tr><th>at</th>"
                 "<th>step</th><th>outcome</th><th>wall</th><th></th></tr>"
                 "%s</table>" % (attempt, "".join(trail) or
                                 "<tr><td colspan=5 class=muted>none</td></tr>"))

    # payload for reference — long fields (the requirement doc) in full
    parts.append("<h2>payload</h2>%s" % _kv_html(payload))

    return _PAGE % {"title": esc(" · %s · task %d" % (pack_name, task_id)),
                    "beat": esc(t["state"]), "sections": _frame(parts)}


# ---------------------------------------------------------- decisions page

def _rich(text, limit=6000):
    """Markdown-lite -> HTML for human-facing decision prose: paragraphs,
    `- ` bullets, `inline code`, ```fenced blocks```. Escapes first —
    model/pack text is never trusted as HTML."""
    import re
    esc = html.escape
    text = str(text or "")[:limit]
    out, para, bullets, fence = [], [], [], None

    def _inline(s):
        return re.sub(r"`([^`]+)`", r"<code>\1</code>", esc(s))

    def _flush():
        if para:
            out.append("<p>%s</p>" % "<br>".join(para))
            para.clear()
        if bullets:
            out.append("<ul>%s</ul>" % "".join("<li>%s</li>" % b for b in bullets))
            bullets.clear()

    for line in text.splitlines():
        if fence is not None:
            if line.strip().startswith("```"):
                out.append("<pre>%s</pre>" % esc("\n".join(fence)))
                fence = None
            else:
                fence.append(line)
            continue
        if line.strip().startswith("```"):
            _flush()
            fence = []
            continue
        s = line.strip()
        if not s:
            _flush()
        elif s.startswith(("- ", "* ")):
            if para:
                _flush()
            bullets.append(_inline(s[2:]))
        else:
            if bullets:
                _flush()
            para.append(_inline(s))
    if fence is not None:
        out.append("<pre>%s</pre>" % esc("\n".join(fence)))
    _flush()
    return "".join(out)


def _decision_card(r):
    """One open decision round: readable situation + clickable option cards
    (click = choose, one confirm) + ONE reject-all-and-regenerate action.
    Reframe/abandon are demoted to quiet links. No raw JSON anywhere —
    that lives on the audit surfaces."""
    esc = html.escape
    opts = json.loads(r["options"] or "[]")
    action = "/api/decision/%d/resolve" % r["id"]
    cards, names = [], []
    for o in opts:
        rich = isinstance(o, dict)
        name = (o.get("title") if rich else o) or "?"
        names.append(str(name))
        rec = str(name) == r["recommended"]
        inner = ['<div class="opthead"><strong>%s</strong>%s</div>'
                 % (esc(str(name)),
                    '<span class="chip ok">recommended</span>' if rec else "")]
        if rich:
            if o.get("summary"):
                inner.append('<p>%s</p>' % esc(str(o["summary"])))
            for sign, key, cls in (("+", "pros", "ok"), ("&minus;", "cons", "warn")):
                for item in (o.get(key) or []):
                    inner.append('<div class="pc %s">%s %s</div>'
                                 % (cls, sign, esc(str(item))))
            if o.get("risks"):
                inner.append('<div class="pc off">risk: %s</div>'
                             % esc(str(o["risks"])))
            if o.get("sketch"):
                inner.append('<details class="lift"><summary>sketch</summary>'
                             '<pre>%s</pre></details>' % esc(str(o["sketch"])[:2000]))
        inner.append('<button class="choose" name="verdict" value="picked">'
                     'choose this &rarr;</button>')
        cards.append(
            '<form class="opt%s" method="post" action="%s"'
            ' onsubmit="return confirm(\'Go with: %s?\')">'
            '<input type="hidden" name="picked" value="%s">%s</form>'
            % (" rec" if rec else "", action,
               esc(str(name)).replace("'", "&#39;"), esc(str(name)),
               "".join(inner)))

    rejected_all = "".join('<input type="hidden" name="rejected" value="%s">'
                           % esc(n) for n in names)
    head = ('<div class="runhead"><span class="rname">%s</span>'
            '<span class="chip wait">round %d</span>'
            '<span class="when">%s</span></div>'
            '<p class="dtitle">%s</p>'
            % (esc(r["key"]), r["round"], esc(_ago(r["created_at"])),
               esc(r["title"])))
    body = ('<div class="muted">%s</div>' % _rich(r["body"])) if r["body"] else ""
    context = ""
    if "context" in r.keys() and r["context"]:
        context = ('<div class="ctx"><div class="ctxlabel">the situation'
                   '</div>%s</div>' % _rich(r["context"]))
    reject = (
        '<form class="rejects" method="post" action="%s">%s'
        '<input name="comment" placeholder="what\'s wrong / what you want'
        ' instead (optional)&hellip;">'
        '<button name="verdict" value="revise">Reject all &amp; regenerate'
        '</button></form>' % (action, rejected_all)) if names else (
        '<form class="rejects" method="post" action="%s">'
        '<input name="comment" placeholder="your answer&hellip;">'
        '<button name="verdict" value="revise">Send</button></form>' % action)
    quiet = ('<div class="quiet">or: '
             '<form method="post" action="%s" onsubmit="return'
             ' confirm(\'Reframe: go back and rethink the question itself?\')">'
             '<button class="linkish" name="verdict" value="reframe">reframe'
             ' the question</button></form> &middot; '
             '<form method="post" action="%s" onsubmit="return'
             ' confirm(\'Abandon this entirely?\')">'
             '<button class="linkish danger" name="verdict" value="abandon">'
             'abandon</button></form></div>' % (action, action))
    return ('<section class="card run">%s%s%s<div class="opts">%s</div>'
            '%s%s</section>'
            % (head, body, context, "".join(cards), reject, quiet))


def _decisions_page(conn, pack_name):
    esc = html.escape
    parts = ['<p><a href="/">&larr; runs</a></p>']
    rows = conn.execute("SELECT * FROM decisions WHERE status='open'"
                        " ORDER BY id").fetchall()
    if not rows:
        parts.append("<h2>decisions</h2><p class=muted>nothing waiting"
                     " on you.</p>")
    for r in rows:
        parts.append(_decision_card(r))
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
   --wait: #d4a72c; --llm: #a78bfa; --mono: ui-monospace, "SF Mono",
   "Cascadia Code", Menlo, Consolas, monospace;
 }
 @media (prefers-color-scheme: light) {
   :root { --bg:#f3f4f6; --card:#ffffff; --card-edge:#dfe3e8; --ink:#1f262e;
           --dim:#5c6773; --faint:#9aa4af; --ember:#c26d10; --ok:#1a7f37;
           --bad:#c73e36; --run:#0f62d6; --wait:#9a6700; --llm:#7c3aed; }
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
   content: "● "; font-size: .7em; vertical-align: .15em; }
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
 .dtitle { margin: .1rem 0 .6rem; font-size: 1.02rem; font-weight: 600; }
 .ctx { border-left: 3px solid color-mix(in srgb, var(--ember) 55%%, transparent);
   background: color-mix(in srgb, var(--card-edge) 22%%, transparent);
   border-radius: 0 8px 8px 0; padding: .55rem .9rem; margin: .6rem 0 .8rem;
   font-size: .86rem; }
 .ctx p { margin: .25rem 0; } .ctx ul { margin: .25rem 0 .25rem 1.1rem;
   padding: 0; } .ctx li { margin: .12rem 0; }
 .ctxlabel { font: 600 .66rem/1.6 -apple-system, sans-serif;
   letter-spacing: .13em; text-transform: uppercase; color: var(--ember);
   margin-bottom: .15rem; }
 form.opt { position: relative; margin: 0; transition: border-color .15s,
   box-shadow .15s, transform .15s; }
 form.opt:hover { border-color: var(--ember); transform: translateY(-1px);
   box-shadow: 0 3px 14px color-mix(in srgb, var(--ember) 18%%, transparent); }
 form.opt.rec { border-color: color-mix(in srgb, var(--ok) 45%%, transparent); }
 .opthead { display: flex; align-items: baseline; gap: .5rem;
   justify-content: space-between; margin-bottom: .2rem; }
 .choose { display: block; width: 100%%; margin-top: .6rem; text-align: center;
   color: var(--dim); border-style: dashed; }
 .choose::after { content: ""; position: absolute; inset: 0; cursor: pointer; }
 form.opt:hover .choose { color: var(--ember); border-color: var(--ember); }
 details.lift { position: relative; z-index: 2; }
 .rejects { display: flex; gap: .6rem; margin-top: .9rem; align-items: center; }
 .rejects input[name=comment] { flex: 1; }
 .rejects button { flex: none; border-color:
   color-mix(in srgb, var(--bad) 45%%, transparent); color: var(--bad); }
 .rejects button:hover { border-color: var(--bad); color: var(--bad); }
 .quiet { margin-top: .55rem; font-size: .76rem; color: var(--faint); }
 .quiet form { display: inline; }
 .linkish { background: none; border: none; padding: 0; color: var(--dim);
   text-decoration: underline; font-size: .76rem; cursor: pointer; }
 .linkish:hover { color: var(--ink); border: none; }
 .linkish.danger:hover { color: var(--bad); }
 nav { margin-left: 1rem; display: flex; gap: .9rem; font-size: .8rem; }
 nav a { color: var(--dim); } nav a:hover { color: var(--ink);
   text-decoration: none; }
 a.rname { color: var(--ink); } a.rname:hover { color: var(--ember);
   text-decoration: none; }
 .launch { display: flex; flex-direction: column; gap: .55rem;
   margin-top: .6rem; }
 .lf { display: flex; flex-direction: column; gap: .2rem; font-size: .78rem;
   color: var(--dim); }
 .launch input, .launch textarea { background: var(--bg); color: var(--ink);
   border: 1px solid var(--card-edge); border-radius: 6px; font: inherit;
   font-size: .84rem; padding: .35rem .6rem; }
 .launch textarea { font-family: var(--mono); resize: vertical; }
 .launch input:focus, .launch textarea:focus { outline: none;
   border-color: var(--ember); }
 .go { align-self: flex-start; border-color:
   color-mix(in srgb, var(--ember) 55%%, transparent); color: var(--ember); }
 .alert { display: flex; align-items: center; gap: .7rem;
   border: 1px solid color-mix(in srgb, var(--wait) 55%%, transparent);
   background: color-mix(in srgb, var(--wait) 9%%, transparent);
   border-radius: 10px; padding: .65rem 1rem; }
 .alert a { color: var(--wait); font-weight: 600; }
 .alert .dot { width: .55rem; height: .55rem; border-radius: 50%%;
   background: var(--wait); animation: blink 1.4s ease-in-out infinite;
   flex: none; }
 @keyframes blink { 0%%,100%% { opacity: .35 } 50%% { opacity: 1 } }
 .runhead { display: flex; align-items: baseline; gap: .7rem;
   margin-bottom: .35rem; }
 .runhead .rname { font-family: var(--mono); font-weight: 700;
   font-size: .95rem; letter-spacing: .02em; }
 .runhead .chip { font-family: var(--mono); font-size: .72rem;
   border-radius: 99px; padding: .1rem .6rem; border: 1px solid; }
 .chip.ok   { color: var(--ok);   border-color: color-mix(in srgb, var(--ok) 45%%, transparent); }
 .chip.bad  { color: var(--bad);  border-color: color-mix(in srgb, var(--bad) 45%%, transparent); }
 .chip.run  { color: var(--run);  border-color: color-mix(in srgb, var(--run) 45%%, transparent); }
 .chip.wait { color: var(--wait); border-color: color-mix(in srgb, var(--wait) 45%%, transparent);
              animation: blink 1.4s ease-in-out infinite; }
 .runhead .when { margin-left: auto; color: var(--faint); font-size: .75rem; }
 .pipe { overflow-x: auto; padding: .2rem 0 .1rem; }
 .pipe svg { display: block; }
 .pipe .nrect { fill: color-mix(in srgb, var(--card-edge) 30%%, transparent);
   stroke: var(--card-edge); stroke-width: 1.2; rx: 9; }
 .pipe a { text-decoration: none; }
 .pipe .ntitle { font: 600 12.5px var(--mono); fill: var(--ink); }
 .pipe .nsub { font: 10.5px var(--mono); fill: var(--dim); }
 .pipe .n-ok  .nrect { stroke: color-mix(in srgb, var(--ok) 60%%, transparent);
   fill: color-mix(in srgb, var(--ok) 7%%, transparent); }
 .pipe .n-ok  .ntitle { fill: var(--ok); }
 .pipe .n-bad .nrect { stroke: color-mix(in srgb, var(--bad) 60%%, transparent);
   fill: color-mix(in srgb, var(--bad) 8%%, transparent); }
 .pipe .n-bad .ntitle { fill: var(--bad); }
 .pipe .n-run .nrect { stroke: var(--ember);
   fill: color-mix(in srgb, var(--ember) 9%%, transparent);
   filter: drop-shadow(0 0 6px color-mix(in srgb, var(--ember) 45%%, transparent));
   animation: pulseglow 1.8s ease-in-out infinite; }
 .pipe .n-run .ntitle { fill: var(--ember); }
 .pipe .n-cur .nrect { stroke: color-mix(in srgb, var(--run) 60%%, transparent); }
 .pipe .n-cur .ntitle { fill: var(--run); }
 .pipe .n-need .nrect { stroke: var(--wait);
   fill: color-mix(in srgb, var(--wait) 10%%, transparent);
   animation: pulseglow 1.4s ease-in-out infinite; }
 .pipe .n-need .ntitle { fill: var(--wait); }
 .pipe .n-off .ntitle { fill: var(--faint); }
 .pipe .n-off .nsub { fill: var(--faint); }
 @keyframes pulseglow { 0%%,100%% { stroke-opacity: .5 } 50%% { stroke-opacity: 1 } }
 .pipe .e { stroke: var(--faint); stroke-width: 1.3; fill: none; opacity: .8; }
 .pipe .e.on { stroke: var(--ember); stroke-dasharray: 5 4;
   animation: flow 1.1s linear infinite; opacity: 1; }
 @keyframes flow { to { stroke-dashoffset: -9; } }
 .pipe .e.fb { stroke-dasharray: 3 4; opacity: .55; }
 .pipe .elabel { font: 9.5px var(--mono); fill: var(--faint); }
 .pipe .badge { font: 700 11px var(--mono); fill: var(--wait); }
 .pipe .nrule { stroke: var(--card-edge); stroke-width: 1; }
 .pipe .sname { font: 10.5px var(--mono); fill: var(--dim); }
 .pipe .sdot { fill: var(--faint); }
 .pipe .sname.ok { fill: var(--ok); } .pipe .sdot.ok { fill: var(--ok); }
 .pipe .sname.warn { fill: var(--bad); } .pipe .sdot.warn { fill: var(--bad); }
 .pipe .sname.cur { fill: var(--ember); font-weight: 700; }
 .pipe .sdot.cur { fill: var(--ember);
   animation: blink 1.2s ease-in-out infinite; }
 .pipe .sname.off, .pipe .sdot.off { opacity: .5; }
 /* graphviz-rendered pipeline: dot owns geometry, these rules own colour
    (svg presentation attributes lose to CSS). */
 .dotpipe svg { max-width: 100%%; height: auto; }
 .dotpipe text { font-family: var(--mono); }
 .dotpipe g.wf > polygon, .dotpipe g.wf > path {
   fill: color-mix(in srgb, var(--card-edge) 14%%, transparent);
   stroke: var(--card-edge); }
 .dotpipe g.wf > text { fill: var(--dim); font-weight: 700; }
 .dotpipe g.wf.n-ok > polygon, .dotpipe g.wf.n-ok > path {
   stroke: color-mix(in srgb, var(--ok) 50%%, transparent); }
 .dotpipe g.wf.n-ok > text { fill: var(--ok); }
 .dotpipe g.wf.n-bad > polygon, .dotpipe g.wf.n-bad > path {
   stroke: color-mix(in srgb, var(--bad) 55%%, transparent); }
 .dotpipe g.wf.n-bad > text { fill: var(--bad); }
 .dotpipe g.wf.n-run > polygon, .dotpipe g.wf.n-run > path {
   stroke: var(--ember); animation: pulseglow 1.8s ease-in-out infinite; }
 .dotpipe g.wf.n-run > text { fill: var(--ember); }
 .dotpipe g.wf.n-need > polygon, .dotpipe g.wf.n-need > path {
   stroke: var(--wait); animation: pulseglow 1.4s ease-in-out infinite; }
 .dotpipe g.wf.n-need > text { fill: var(--wait); }
 /* WHO does the step = fill hue (machinery steel, llm violet, human
    amber); HOW it went = border + text (ok green, warn red, cur ember). */
 .dotpipe g.st polygon, .dotpipe g.st path {
   fill: color-mix(in srgb, var(--card-edge) 32%%, transparent);
   stroke: color-mix(in srgb, var(--card-edge) 80%%, transparent); }
 .dotpipe g.st.t-llm polygon, .dotpipe g.st.t-llm path {
   fill: color-mix(in srgb, var(--llm) 14%%, transparent);
   stroke: color-mix(in srgb, var(--llm) 45%%, transparent); }
 .dotpipe g.st.t-human polygon, .dotpipe g.st.t-human path {
   fill: color-mix(in srgb, var(--wait) 15%%, transparent);
   stroke: color-mix(in srgb, var(--wait) 55%%, transparent); }
 .dotpipe g.st text { fill: var(--dim); }
 .dotpipe g.st.t-llm text { fill: color-mix(in srgb, var(--llm) 75%%, var(--ink)); }
 .dotpipe g.st.t-human text { fill: color-mix(in srgb, var(--wait) 80%%, var(--ink)); }
 .dotpipe g.st.ok polygon, .dotpipe g.st.ok path {
   stroke: color-mix(in srgb, var(--ok) 60%%, transparent); }
 .dotpipe g.st.ok text { fill: var(--ok); }
 .dotpipe g.st.warn polygon, .dotpipe g.st.warn path {
   stroke: color-mix(in srgb, var(--bad) 60%%, transparent); }
 .dotpipe g.st.warn text { fill: var(--bad); }
 .dotpipe g.st.cur polygon, .dotpipe g.st.cur path {
   fill: color-mix(in srgb, var(--ember) 14%%, transparent);
   stroke: var(--ember); animation: pulseglow 1.2s ease-in-out infinite; }
 .dotpipe g.st.cur text { fill: var(--ember); font-weight: 700; }
 .dotpipe g.st.off polygon, .dotpipe g.st.off path { opacity: .75; }
 .dotpipe g.st.off text { opacity: .8; }
 .legend { display: flex; gap: 1.1rem; margin-top: .45rem; font-family:
   var(--mono); font-size: .72rem; color: var(--faint); align-items: center; }
 .legend .sw { display: inline-block; width: .72rem; height: .72rem;
   border-radius: 3px; margin-right: .35rem; vertical-align: -.08rem;
   border: 1px solid; }
 .legend .sw.mech { background: color-mix(in srgb, var(--card-edge) 32%%,
   transparent); border-color: var(--card-edge); }
 .legend .sw.llm { background: color-mix(in srgb, var(--llm) 14%%,
   transparent); border-color: color-mix(in srgb, var(--llm) 45%%, transparent); }
 .legend .sw.human { background: color-mix(in srgb, var(--wait) 15%%,
   transparent); border-color: color-mix(in srgb, var(--wait) 55%%, transparent); }
 .dotpipe g.st.sink polygon, .dotpipe g.st.sink path { opacity: .4;
   stroke-dasharray: 3 3; }
 .dotpipe g.st.sink text { opacity: .45; }
 .dotpipe g.se.fail path { stroke: var(--faint); opacity: .3; }
 .dotpipe g.se.fail polygon { fill: var(--faint); stroke: var(--faint);
   opacity: .3; }
 .stepsx { margin-top: .5rem; display: flex; flex-direction: column;
   gap: .3rem; }
 #sidepanel { position: fixed; left: 0; top: 0; bottom: 0;
   width: min(30rem, 88vw); background: var(--card);
   border-right: 1px solid var(--card-edge);
   box-shadow: 8px 0 32px color-mix(in srgb, black 45%%, transparent);
   overflow-y: auto; padding: .9rem 1.1rem 2rem; z-index: 50;
   animation: slidein .16s ease-out; }
 @keyframes slidein { from { transform: translateX(-30%%); opacity: 0; }
   to { transform: none; opacity: 1; } }
 #sidepanel section.card { border: 0; padding: .4rem 0 .6rem;
   border-bottom: 1px solid color-mix(in srgb, var(--card-edge) 60%%,
   transparent); border-radius: 0; }
 #sp-close { float: right; font-size: 1rem; line-height: 1;
   padding: .15rem .55rem; margin: 0 0 .4rem .6rem; }
 .stepsx details { border-top: 1px solid
   color-mix(in srgb, var(--card-edge) 60%%, transparent); padding-top: .3rem; }
 .dotpipe g.se path { stroke: var(--faint); }
 .dotpipe g.se polygon { fill: var(--faint); stroke: var(--faint); }
 .dotpipe g.se text { fill: var(--faint); }
 .dotpipe g.we path { stroke: var(--dim); }
 .dotpipe g.we polygon { fill: var(--dim); stroke: var(--dim); }
 .dotpipe g.we text { fill: var(--ember); }
 .dotpipe g.we.fb path { stroke: var(--wait); }
 .dotpipe g.we.fb text { fill: var(--wait); }
 .dotpipe g.we.fb polygon { fill: var(--wait); stroke: var(--wait); }
 .exec { display: flex; flex-wrap: wrap; gap: .35rem .5rem; margin-top: .45rem;
   font-family: var(--mono); font-size: .78rem; color: var(--dim);
   align-items: center; }
 details.hist summary { cursor: pointer; color: var(--dim);
   font-size: .8rem; list-style: none; }
 details.hist summary::before { content: "▸ "; color: var(--faint); }
 details.hist[open] summary::before { content: "▾ "; }
 button { background: var(--card); color: var(--ink); cursor: pointer;
          border: 1px solid var(--card-edge); border-radius: 6px;
          padding: .3rem .8rem; font: inherit; font-size: .8rem; }
 button:hover { border-color: var(--ember); color: var(--ember); }
 input[name=comment] { background: var(--bg); border: 1px solid var(--card-edge);
          border-radius: 6px; color: var(--ink); padding: .3rem .6rem; font: inherit; }
 table.kvp th { width: 1%%; white-space: nowrap; font-family: var(--mono);
   text-transform: none; letter-spacing: 0; font-size: .78rem;
   color: var(--dim); padding-right: 1rem; }
 table.kvp td { font-family: inherit; }
 pre.doc { white-space: pre-wrap; word-break: break-word; max-width: 60rem; }
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
<nav><a href="/">runs</a> <a href="/decisions">decisions</a>
<a href="/explore">explore</a></nav>
<span class="beat">%(beat)s</span></header>
<main>
%(sections)s
<p class="muted" style="font-size:.75rem">JSON <code>/api/status</code>
<code>/api/metrics</code> <code>/api/task/&lt;id&gt;</code> · emit
<code>POST /api/emit</code></p>
</main>
<script>
// clicking a step block opens its "what does this do" page in a small
// panel on the LEFT instead of leaving the page (fallback: navigate).
document.addEventListener("click", async (e) => {
  const a = e.target.closest("a");
  // graphviz SVG anchors carry xlink:href, not href
  const href = a && (a.getAttribute("href") || a.getAttribute("xlink:href"));
  if (!href || !href.startsWith("/step/")) return;
  e.preventDefault();
  try {
    const r = await fetch(href, {cache: "no-store"});
    const d = new DOMParser().parseFromString(await r.text(), "text/html");
    let p = document.getElementById("sidepanel");
    if (!p) {
      p = document.createElement("aside");
      p.id = "sidepanel";
      document.body.appendChild(p);
    }
    p.innerHTML = '<button id="sp-close" title="close (Esc)">&times;</button>'
                  + d.querySelector("main").innerHTML;
    p.querySelector("#sp-close").onclick = () => p.remove();
    p.scrollTop = 0;
  } catch (err) { location.href = href; }
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    const p = document.getElementById("sidepanel");
    if (p) p.remove();
  }
});
setInterval(async () => {
  if (document.querySelector("details[open]") || String(getSelection())
      || (document.activeElement
          && document.activeElement.matches("input,textarea"))) return;
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


# ------------------------------------------------- runs (thread grouping)
#
# One raw request = one thread value = one RUN. The pack declares WHICH
# payload field correlates (board.thread_key); the board groups every task
# and open decision under it and draws the run as a pipeline: nodes are the
# workflow kinds, edges are the emits->consumes orchestration map. The
# engine attaches no meaning to the value — pure mechanism.

_TERMINAL = {"done", "failed", "deferred"}


def _ago(ts):
    """'YYYY-MM-DD HH:MM:SS' (sqlite UTC) -> compact age string."""
    import calendar
    import time
    try:
        then = calendar.timegm(time.strptime(str(ts), "%Y-%m-%d %H:%M:%S"))
    except (ValueError, TypeError):
        return str(ts or "")
    s = max(0, int(time.time()) - then)
    if s < 90:
        return "%ds ago" % s
    if s < 5400:
        return "%dm ago" % (s // 60)
    if s < 172800:
        return "%dh ago" % (s // 3600)
    return "%dd ago" % (s // 86400)


def _wf_graph(workflows, roots=()):
    """The orchestration map as a drawable DAG: an edge A->B for every event
    A emits that B consumes. `roots` (ordered: launch events first) pin
    their consumers at depth 0 and mark edges INTO them as FEEDBACK — an
    internal re-emit of a launchable event (fn_edit escalating back to the
    planner) is a loop-back, and must not push the normal pipeline's first
    workflow to the right. Computed from the loaded defs — nothing
    hardcoded."""
    kinds = sorted(workflows or {})
    emitters, consumers = {}, {}
    for k in kinds:
        for ev in workflows[k].emits:
            emitters.setdefault(ev, []).append(k)
        for ev in workflows[k].consumes:
            consumers.setdefault(ev, []).append(k)
    roots = list(roots)
    pinned = {}                          # kind -> its root's rank (layer order)
    for k in kinds:
        for ev in workflows[k].consumes:
            if ev in roots:
                pinned.setdefault(k, roots.index(ev))
    edges = []
    for ev in sorted(emitters):
        for s in emitters[ev]:
            for d in consumers.get(ev, []):
                fb = d in pinned and s != d
                if (s, d, ev, fb) not in edges:
                    edges.append((s, d, ev, fb))
    depth = {k: 0 for k in kinds}
    for _ in range(len(kinds) + 1):
        changed = False
        for s, d, ev, fb in edges:
            if fb or s == d:
                continue
            if depth[s] + 1 > depth[d] and depth[s] + 1 <= len(kinds):
                depth[d] = depth[s] + 1
                changed = True
        if not changed:
            break
    entry = {k: [ev for ev in workflows[k].consumes if ev not in emitters]
             for k in kinds}
    return {"kinds": kinds, "edges": edges, "depth": depth, "entry": entry,
            "pinned": pinned}


def _threads(conn, thread_key, limit=400):
    """Group recent tasks by payload[thread_key]. Returns (threads, loose):
    threads newest-first, each {key, tasks (oldest-first), latest {kind: row},
    updated}; loose = tasks with no thread value."""
    rows = conn.execute(
        "SELECT id, kind, state, error_class, park_reason, attempts, payload,"
        " created_at, updated_at FROM tasks ORDER BY id DESC LIMIT ?",
        (limit,)).fetchall()
    threads, loose = {}, []
    for r in rows:
        try:
            payload = json.loads(r["payload"] or "{}")
        except ValueError:
            payload = {}
        tv = payload.get(thread_key) if thread_key else None
        if not isinstance(tv, str) or not tv:
            loose.append(r)
            continue
        th = threads.setdefault(tv, {"key": tv, "tasks": [], "latest": {},
                                     "updated": r["updated_at"]})
        th["tasks"].append(r)
        th["updated"] = max(th["updated"], r["updated_at"])
        prev = th["latest"].get(r["kind"])
        if prev is None or r["id"] > prev["id"]:
            th["latest"][r["kind"]] = r
    ordered = sorted(threads.values(), key=lambda t: t["updated"], reverse=True)
    for th in ordered:
        th["tasks"].reverse()                      # oldest first (the story)
    return ordered, loose


def _open_decisions(conn):
    """Open decision rounds, plus a task_id -> [decision] index so runs can
    badge the exact node that is waiting on the human."""
    rows = conn.execute("SELECT id, key, round, title, task_id FROM decisions"
                        " WHERE status='open' ORDER BY id").fetchall()
    by_task = {}
    for r in rows:
        if r["task_id"] is not None:
            by_task.setdefault(r["task_id"], []).append(r)
    return rows, by_task


def _node_info(conn, th, workflows, dec_by_task):
    """Per workflow kind in one thread: latest task + step progress + per-step
    outcome classes (the expanded graph colours every step node)."""
    info = {}
    for kind, t in th["latest"].items():
        wf = (workflows or {}).get(kind)
        total = len(wf.steps) if wf else 0
        rows = conn.execute(
            "SELECT step, outcome FROM task_steps WHERE task_id=? AND attempt=?"
            " ORDER BY rowid", (t["id"], t["attempts"])).fetchall()
        done_n = len({r["step"] for r in rows})
        last = rows[-1] if rows else None
        steps_cls = {}
        for r in rows:
            steps_cls[r["step"]] = ("warn" if str(r["outcome"]) in _BAD_OUTCOMES
                                    else "ok")
        if last is not None and t["state"] == "running":
            steps_cls[last["step"]] = "cur"
        decisions = dec_by_task.get(t["id"], [])
        needs_human = bool(decisions) or (
            t["state"] == "parked" and t["error_class"] == "awaiting_human")
        if needs_human:
            sub = "needs you"
        elif t["state"] == "running":
            sub = "%s · %d/%d" % (last["step"] if last else "…", done_n, total)
        elif t["state"] == "done":
            sub = "done · %d steps" % done_n
        elif t["state"] == "failed":
            sub = "failed at %s" % (last["step"] if last else "?")
        elif t["state"] == "parked":
            sub = "parked · %s" % (t["error_class"] or "")
        elif t["state"] == "retry_wait":
            sub = "retrying %s" % (last["step"] if last else "")
        else:
            sub = t["state"]
        info[kind] = {"task": t, "sub": sub, "needs_human": needs_human,
                      "decisions": decisions, "step": last["step"] if last else "",
                      "done_n": done_n, "total": total, "steps_cls": steps_cls}
    return info


_NODE_CLS = {"done": "n-ok", "failed": "n-bad", "running": "n-run",
             "pending": "n-cur", "retry_wait": "n-cur", "parked": "n-wait",
             "deferred": "n-off"}

_DOT_CACHE = {}                       # dot-source sha -> svg (small, capped)


def _dot_q(s):                                  # dot double-quoted id
    return '"%s"' % str(s).replace("\\", "\\\\").replace('"', '\\"')


def _dot_render(source):
    """dot source -> inner <svg>, cached by source hash; None on any
    failure (missing binary, error, timeout) so callers can fall back."""
    import hashlib
    import shutil
    import subprocess
    if shutil.which("dot") is None:
        return None
    key = hashlib.sha256(source.encode()).hexdigest()
    svg = _DOT_CACHE.get(key)
    if svg is not None:
        return svg
    try:
        out = subprocess.run(["dot", "-Tsvg"], input=source.encode(),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, timeout=5)
        if out.returncode != 0:
            return None
        raw = out.stdout.decode("utf-8", "replace")
        svg = raw[raw.find("<svg"):]
    except Exception:
        return None
    if len(_DOT_CACHE) > 32:
        _DOT_CACHE.clear()
    _DOT_CACHE[key] = svg
    return svg


_DOT_STYLE = [
    'graph [fontname="Courier", fontsize=10, bgcolor="transparent"];',
    'node [shape=box, style="rounded,filled", fontname="Courier",'
    ' fontsize=9, height=0.22, margin="0.10,0.035",'
    ' color="#555555", fillcolor="#333333", fontcolor="#aaaaaa"];',
    'edge [fontname="Courier", fontsize=7, arrowsize=0.5,'
    ' color="#666666", fontcolor="#888888"];']


def _dot_overview(graph, info, entries=None):
    """The WORKFLOW-LEVEL graph (the original compact view): one node per
    workflow, event edges between them, feedback dashed. This is what a run
    card shows; the step detail lives in per-workflow expanders."""
    q = _dot_q
    src = (["digraph overview {",
            "rankdir=LR; ranksep=0.5; nodesep=0.3; splines=line;"]
           + _DOT_STYLE)
    for k in graph["kinds"]:
        nfo = info.get(k)
        state = nfo["task"]["state"] if nfo else None
        wcls = "n-need" if (nfo and nfo["needs_human"]) \
            else _NODE_CLS.get(state, "n-off")
        sub = nfo["sub"] if nfo else (
            entries.get(k, "") if entries is not None else "")
        href = ("/decisions" if nfo and nfo["needs_human"]
                else "/task/%d" % nfo["task"]["id"] if nfo else None)
        src.append('%s [label=%s, class="wf %s", fontsize=10,'
                   ' margin="0.16,0.09"%s];'
                   % (q(k), q(k + ("\\n" + sub if sub else "")), wcls,
                      (", URL=%s" % q(href)) if href else ""))
    for s, d, ev, fb in graph["edges"]:
        if s == d or s not in graph["kinds"] or d not in graph["kinds"]:
            continue
        src.append('%s -> %s [label=%s, class="we%s", penwidth=1.3%s];'
                   % (q(s), q(d), q(ev + (" ↩" if fb else "")),
                      " fb" if fb else "",
                      ", style=dashed, constraint=false" if fb else ""))
    src.append("}")
    svg = _dot_render("\n".join(src))
    return ('<div class="pipe dotpipe">%s</div>' % svg) if svg else None


def _dot_steps(kind, wf, nfo):
    """ONE workflow's step graph (shown in an expander). Failure handling
    is kept out of the layout's way: a FAILURE SINK (a step every road can
    bail to — terminal-only outcomes, several in-edges) sits detached at
    the side, its in-edges faint unlabeled dashes that do not constrain
    ranks. The happy path plus real decision loops form the visible spine."""
    q = _dot_q
    steps_cls = (nfo or {}).get("steps_cls") or {}
    names = {s.name for s in wf.steps}
    by_target, indeg = {}, {}
    for (sname, outcome), target in wf.dispatch.items():
        if sname in names and target in names:
            by_target.setdefault((sname, target), []).append(outcome)
            indeg[target] = indeg.get(target, 0) + 1
    step_by_name = {s.name: s for s in wf.steps}
    sinks = set()
    for name, s in step_by_name.items():
        targets = {t for (sn, _o), t in wf.dispatch.items() if sn == name}
        if indeg.get(name, 0) >= 3 and not (targets & names):
            sinks.add(name)                     # bail-out collector, not path
    fanout = {}
    for (sname, target) in by_target:
        if target not in sinks:
            fanout[sname] = fanout.get(sname, 0) + 1
    # THE MAIN STEM: walk from the first step, always taking the nearest
    # FORWARD target (definition order; sinks excluded). The pack lists its
    # happy path first and exception steps at the tail, so this walk IS the
    # narrative spine — drawn dead straight; loops arc left, repair forks
    # hang right, sinks stay detached.
    order = {s.name: i for i, s in enumerate(wf.steps)}
    stem, seen_stem = [], set()
    cur = wf.steps[0].name if wf.steps else None
    while cur and cur not in seen_stem:
        seen_stem.add(cur)
        stem.append(cur)
        fwd = [t for (sn, _o), t in wf.dispatch.items()
               if sn == cur and t in names and t not in sinks
               and order[t] > order[cur]]
        cur = min(fwd, key=lambda t: order[t]) if fwd else None
    stem_pos = {n: i for i, n in enumerate(stem)}
    src = (["digraph steps {",
            "rankdir=TB; ranksep=0.3; nodesep=0.25; splines=line;"]
           + _DOT_STYLE)
    for s in wf.steps:
        scls = steps_cls.get(s.name, "off")
        # WHO does this step: a human, a model, or plain machinery — each
        # its own colour channel (state rides on border/text).
        tcls = ("t-human" if s.block.name == "human.ask"
                else "t-llm" if s.block.exec_class == "llm" else "t-mech")
        branch = fanout.get(s.name, 0) >= 2 or s.block.name == "human.ask"
        doc = (s.block.fn.__doc__ or "").strip().split("\n")[0].strip()
        tip = "%s — %s" % (s.block.name, doc) if doc else s.block.name
        extra = ""
        if s.name in sinks:
            scls += " sink"
        elif branch:
            scls += " branch"
            extra = ', shape=diamond, margin="0.06,0.02"'
        group = ', group="stem"' if s.name in stem_pos else ""
        src.append('%s [label=%s, class="st %s %s", URL=%s, tooltip=%s%s%s];'
                   % (q(s.name), q(s.name), tcls, scls,
                      q("/step/%s/%s" % (kind, s.name)), q(tip[:200]), extra,
                      group))
    drawn = set()
    for (sname, target), outs in sorted(by_target.items()):
        if target in sinks:
            src.append('%s -> %s [class="se fail", style=dashed,'
                       ' constraint=false, arrowsize=0.4];'
                       % (q(sname), q(target)))
            continue
        label = ",".join(sorted(outs))
        if len(label) > 16:
            label = label[:14] + "…"
        attrs = ['label=%s' % q(label), 'class="se"']
        consecutive = (sname in stem_pos and target in stem_pos
                       and stem_pos[target] == stem_pos[sname] + 1)
        if consecutive:
            attrs.append("weight=80")           # the stem stays dead straight
            drawn.add((sname, target))
        elif order.get(target, 0) < order.get(sname, 0):
            attrs.append("constraint=false")    # loop-backs arc, never warp ranks
        src.append('%s -> %s [%s];' % (q(sname), q(target), ", ".join(attrs)))
    for a, b in zip(stem, stem[1:]):            # stem gaps: invisible rail
        if (a, b) not in drawn:
            src.append('%s -> %s [style=invis, weight=80];' % (q(a), q(b)))
    src.append("}")
    svg = _dot_render("\n".join(src))
    return ('<div class="pipe dotpipe">%s</div>' % svg) if svg else None


def _render_pipeline(graph, info, workflows, entries=None):
    """Run-card pipeline: the compact workflow-level graph, plus one
    click-to-expand step graph per workflow underneath."""
    esc = html.escape
    over = _dot_overview(graph, info, entries)
    if over is None:                            # no dot: builtin, all-in-one
        return _pipeline_svg(graph, info, workflows, entries)
    panels = []
    for k in graph["kinds"]:
        wf = (workflows or {}).get(k)
        if wf is None:
            continue
        nfo = info.get(k)
        svg = _dot_steps(k, wf, nfo)
        if svg is None:
            continue
        label = "%s · %d steps" % (k, len(wf.steps))
        if nfo:
            label += " · " + nfo["sub"]
        panels.append('<details class="hist"><summary>%s</summary>%s'
                      '</details>' % (esc(label), svg))
    legend = ('<div class="legend"><span><span class="sw human"></span>'
              'human decides</span><span><span class="sw llm"></span>'
              'model works</span><span><span class="sw mech"></span>'
              'machinery (deterministic)</span></div>')
    return over + ('<div class="stepsx">%s</div>%s' % ("".join(panels), legend)
                   if panels else "")


def _pipeline_svg(graph, info, workflows=None, entries=None):
    """One run as an SVG pipeline, EXPANDED: each workflow is a column
    listing every step (the walk, in definition order), each step dotted by
    its outcome this run — green ok, red bad, ember pulsing = executing
    now, faint = not reached. Feedback edges (a re-emitted launch event)
    curve back over the top, dashed. With `entries` (kind -> entry-event
    label), a node with no task shows what starts it — the static map."""
    esc = html.escape
    kinds, depth = graph["kinds"], graph["depth"]
    if not kinds:
        return ""
    HDR, ROW, PADB, XGAP, VGAP = 34, 15, 7, 62, 26
    has_fb = any(fb for _s, _d, _e, fb in graph["edges"])
    top = 30 if has_fb else 16

    geom = {}                       # kind -> (w, h, [step names])
    for k in kinds:
        wf = (workflows or {}).get(k)
        steps = [s.name for s in wf.steps] if wf else []
        longest = max([len(k) + 2] + [len(n) + 3 for n in steps])
        geom[k] = (max(108, int(longest * 6.9) + 26),
                   HDR + len(steps) * ROW + (PADB if steps else 0), steps)

    layers = {}
    for k in kinds:
        layers.setdefault(depth[k], []).append(k)
    order = sorted(layers)
    pinned = graph.get("pinned") or {}
    colw, xs, cx = {}, {}, 16
    for d in order:
        layers[d].sort(key=lambda k: (pinned.get(k, 99), k))
        colw[d] = max(geom[k][0] for k in layers[d])
        xs[d] = cx
        cx += colw[d] + XGAP
    width = cx - XGAP + 16
    pos, height = {}, 0
    for d in order:
        y = top
        for k in layers[d]:
            pos[k] = (xs[d], y, colw[d], geom[k][1])
            y += geom[k][1] + VGAP
        height = max(height, y - VGAP + 12)

    out = ['<svg width="%d" height="%d" viewBox="0 0 %d %d"'
           ' xmlns="http://www.w3.org/2000/svg" role="img">'
           % (width, height, width, height),
           '<defs><marker id="arr" viewBox="0 0 8 8" refX="7" refY="4"'
           ' markerWidth="7" markerHeight="7" orient="auto">'
           '<path d="M0 0 L8 4 L0 8 z" fill="currentColor" opacity=".55"/>'
           '</marker></defs>']
    # edges under nodes — normal ones join header midlines; feedback loops
    # back over the top, dashed
    for s, d, ev, fb in graph["edges"]:
        if s == d or s not in pos or d not in pos:
            continue
        x1, y1, w1, _h1 = pos[s]
        x2, y2, w2, _h2 = pos[d]
        src, dst = info.get(s), info.get(d)
        live = (src and src["task"]["state"] == "done" and dst
                and dst["task"]["state"] not in _TERMINAL)
        if fb:
            sx, sy = x1 + w1 / 2.0, y1 - 2
            tx, ty = x2 + w2 / 2.0, y2 - 2
            out.append('<path class="e fb" d="M%.0f %.0f C %.0f %.0f, %.0f'
                       ' %.0f, %.0f %.0f" marker-end="url(#arr)"/>'
                       % (sx, sy, sx, 8, tx, 8, tx, ty))
            out.append('<text class="elabel" x="%.0f" y="%.0f"'
                       ' text-anchor="middle">%s ↩</text>'
                       % ((sx + tx) / 2.0, 12, esc(ev)))
            continue
        sx, sy = x1 + w1, y1 + HDR / 2.0
        tx, ty = x2 - 7, y2 + HDR / 2.0
        mid = (sx + tx) / 2.0
        out.append('<path class="e%s" d="M%.0f %.0f C %.0f %.0f, %.0f %.0f,'
                   ' %.0f %.0f" marker-end="url(#arr)"/>'
                   % (" on" if live else "", sx, sy, mid, sy, mid, ty, tx, ty))
        out.append('<text class="elabel" x="%.0f" y="%.0f"'
                   ' text-anchor="middle">%s</text>'
                   % (mid, min(sy, ty) - 7, esc(ev)))
    # nodes over edges: header + every step as a dotted row
    for k in kinds:
        x, y, w, h = pos[k]
        steps = geom[k][2]
        nfo = info.get(k)
        state = nfo["task"]["state"] if nfo else None
        cls = "n-need" if (nfo and nfo["needs_human"]) \
            else _NODE_CLS.get(state, "n-off")
        sub = nfo["sub"] if nfo else (
            entries.get(k, "") if entries is not None else "not started")
        steps_cls = (nfo or {}).get("steps_cls") or {}
        rows = []
        for i, name in enumerate(steps):
            sy = y + HDR + (i + 0.5) * ROW
            scls = steps_cls.get(name, "off")
            rows.append('<circle class="sdot %s" cx="%d" cy="%.1f" r="3"/>'
                        '<text class="sname %s" x="%d" y="%.1f">%s</text>'
                        % (scls, x + 13, sy, scls, x + 22, sy + 3.5,
                           esc(name)))
        body = ('<g class="%s"><rect class="nrect" x="%d" y="%d" width="%d"'
                ' height="%d" rx="10"/><text class="ntitle" x="%d" y="%d"'
                ' text-anchor="middle">%s</text><text class="nsub" x="%d"'
                ' y="%d" text-anchor="middle">%s</text>'
                '<line class="nrule" x1="%d" y1="%d" x2="%d" y2="%d"/>%s%s</g>'
                % (cls, x, y, w, h, x + w / 2, y + 14, esc(k),
                   x + w / 2, y + 27, esc(sub),
                   x + 8, y + HDR - 1, x + w - 8, y + HDR - 1, "".join(rows),
                   ('<text class="badge" x="%d" y="%d" text-anchor="middle">'
                    '&#9670;</text>' % (x + w - 2, y - 1))
                   if nfo and nfo["needs_human"] else ""))
        if nfo:
            href = ("/decisions" if nfo["needs_human"]
                    else "/task/%d" % nfo["task"]["id"])
            body = '<a href="%s">%s</a>' % (href, body)
        out.append(body)
    out.append("</svg>")
    return '<div class="pipe">%s</div>' % "".join(out)


def _run_card(conn, th, graph, workflows, dec_by_task):
    """One run: name + status chip + pipeline + executing-now strip."""
    esc = html.escape
    info = _node_info(conn, th, workflows, dec_by_task)
    states = [t["state"] for t in th["latest"].values()]
    n_dec = sum(len(n["decisions"]) for n in info.values())
    needs = n_dec or any(n["needs_human"] for n in info.values())
    deepest_done = all(s in _TERMINAL for s in states) and states
    if needs:
        chip = '<span class="chip wait">&#9670; needs your decision</span>'
    elif "running" in states:
        k = next(k for k, t in th["latest"].items() if t["state"] == "running")
        chip = ('<span class="chip run">&#9654; running · %s · %s</span>'
                % (esc(k), esc(info[k]["step"])))
    elif "failed" in states:
        chip = '<span class="chip bad">&#10007; failed</span>'
    elif any(s in ("parked", "retry_wait") for s in states):
        chip = '<span class="chip wait">&#10074;&#10074; parked</span>'
    elif deepest_done and "failed" not in states:
        chip = '<span class="chip ok">&#10003; complete</span>'
    else:
        chip = '<span class="chip run">queued</span>'
    execing = ["<span class='cell cur'>%s · %s</span>"
               % (esc(k), esc(n["step"] or "starting"))
               for k, n in sorted(info.items())
               if n["task"]["state"] == "running"]
    from urllib.parse import quote
    head = ('<div class="runhead"><a class="rname" href="/run/%s"'
            ' title="full audit trail">%s</a>%s'
            '<span class="when">updated %s</span></div>'
            % (quote(th["key"]), esc(th["key"]), chip,
               esc(_ago(th["updated"]))))
    strip = ('<div class="exec">executing now: %s</div>' % " ".join(execing)) \
        if execing else ""
    return '<section class="card run">%s%s%s</section>' \
        % (head, _render_pipeline(graph, info, workflows), strip)


def _dashboard(conn, pack_name, board=None, workflows=None, prefill=None):
    """The front page: what is this system doing for me RIGHT NOW.
    Decision alert -> active runs (pipeline graphs) -> finished runs
    (collapsed) -> loose tasks. Ops tables live on /explore."""
    esc = html.escape
    board = board or {}
    parts = []

    open_dec, dec_by_task = _open_decisions(conn)
    graph = _wf_graph(workflows or {},
                      roots=[l["event"] for l in board.get("launch") or []])
    thread_key = board.get("thread_key")
    threads, loose = _threads(conn, thread_key)
    active, finished = [], []
    for th in threads:
        live = any(t["state"] not in _TERMINAL for t in th["latest"].values()) \
            or any(dec_by_task.get(t["id"]) for t in th["tasks"])
        (active if live else finished).append(th)

    if open_dec:
        items = ", ".join(esc(r["key"]) for r in open_dec[:4])
        parts.append('<div class="alert"><span class="dot"></span>'
                     '<span>%d decision%s waiting on you (%s)</span>'
                     '<a href="/decisions">decide &rarr;</a></div>'
                     % (len(open_dec), "s" if len(open_dec) != 1 else "", items))

    parts.extend(_launch_forms(board, any_active=bool(active),
                               prefill=prefill))

    for th in active:
        parts.append(_run_card(conn, th, graph, workflows, dec_by_task))

    if not active and graph["kinds"]:
        # the workflow is visible RUNNING OR NOT: with no live run, show the
        # system's pipeline itself. An entry node is labelled by what starts
        # it: a true entry event, or a launch-form event (an event can be
        # both launchable AND re-emitted internally — fn_edit escalating to
        # spec.requested — and it is still a way in).
        launchable = {l["event"] for l in board.get("launch") or []}
        entries = {}
        for k in graph["kinds"]:
            evs = [ev for ev in (workflows or {})[k].consumes
                   if ev in launchable or ev in graph["entry"].get(k, [])]
            entries[k] = ("◂ " + evs[0]) if evs else ""
        parts.append('<section class="card run"><div class="runhead">'
                     '<span class="rname">the pipeline</span>'
                     '<span class="chip run">idle — start a run above</span>'
                     '</div>%s</section>'
                     % _render_pipeline(graph, {}, workflows, entries))

    if finished:
        from urllib.parse import quote
        rows = "".join(
            "<tr><td><a href='/run/%s'>%s</a></td>"
            "<td class='state-%s'>%s</td><td>%s</td>"
            "<td class=muted>%s</td>"
            "<td><a href='/?launch_%s=%s' title='reopen the start form with"
            " this run prefilled — an edited requirement re-runs only what"
            " changed'>revise &rarr;</a></td></tr>"
            % (quote(th["key"]), esc(th["key"]),
               "done" if all(t["state"] == "done" for t in th["latest"].values())
               else "failed",
               "complete" if all(t["state"] == "done"
                                 for t in th["latest"].values()) else "ended",
               " ".join("<a href='/task/%d'>%s</a>" % (t["id"], esc(k))
                        for k, t in sorted(th["latest"].items())),
               esc(_ago(th["updated"])),
               quote(thread_key or ""), quote(th["key"]))
            for th in finished[:20])
        parts.append("<h2>finished runs</h2><details class=hist><summary>"
                     "%d finished run%s</summary><table><tr><th>run</th>"
                     "<th>state</th><th>tasks</th><th>updated</th><th></th>"
                     "</tr>%s</table></details>"
                     % (len(finished), "s" if len(finished) != 1 else "", rows))

    if not threads and not loose:
        parts.append("<h2>runs</h2><p class=muted>nothing yet — emit an event"
                     " (<code>POST /api/emit</code>) to start a run.</p>")

    if loose:
        rows = "".join(
            "<tr><td><a href='/task/%d'>#%d</a></td><td>%s</td>"
            "<td class='state-%s'>%s</td><td class=muted>%s</td></tr>"
            % (r["id"], r["id"], esc(r["kind"]), esc(r["state"]),
               esc(r["state"]), esc(_ago(r["updated_at"])))
            for r in loose[:10])
        parts.append("<h2>other tasks</h2><table><tr><th>task</th><th>kind</th>"
                     "<th>state</th><th>updated</th></tr>%s</table>" % rows)

    import time
    hb = conn.execute("SELECT cursor FROM watermarks"
                      " WHERE scope='daemon.heartbeat'").fetchone()
    beat = "no heartbeat"
    if hb:
        age = int(time.time()) - int(hb["cursor"])
        beat = ("daemon &middot; %ds ago" % age) if age < 3600 \
            else "daemon heartbeat stale"
    return _PAGE % {"title": esc(" · " + pack_name if pack_name else ""),
                    "beat": beat, "sections": _frame(parts)}


def _explore_page(conn, pack_name, board=None):
    """The ops surface that used to crowd the front page: pack panels,
    parked tasks, open joins, the event stream."""
    st = _status(conn)
    esc = html.escape
    parts = ['<p><a href="/">&larr; runs</a></p>']

    for panel in (board or {}).get("overview_panels", []):
        parts.append(_panel_html(conn, panel, {}))

    if st["parked"]:
        rows = ["<tr><td><a href='/task/%d'>#%d</a></td><td>%s</td>"
                "<td>%s</td><td>%d</td></tr>"
                % (p["id"], p["id"], esc(p["kind"]), esc(str(p["reason"])),
                   p["attempts"])
                for p in st["parked"]]
        parts.append("<h2>parked</h2><table><tr><th>task</th><th>kind</th>"
                     "<th>reason</th><th>attempts</th></tr>%s</table>"
                     % "".join(rows))

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

    tasks = "".join(
        "<tr><td><a href='/task/%d'>#%d</a></td><td>%s</td>"
        "<td class='state-%s'>%s</td><td class=muted>%s</td></tr>"
        % (r["id"], r["id"], esc(r["kind"]), esc(r["state"]), esc(r["state"]),
           esc(_ago(r["updated_at"])))
        for r in conn.execute("SELECT id, kind, state, updated_at FROM tasks"
                              " ORDER BY updated_at DESC, id DESC LIMIT 25"))
    parts.append("<h2>recent tasks</h2><table><tr><th>task</th><th>kind</th>"
                 "<th>state</th><th>updated</th></tr>%s</table>"
                 % (tasks or "<tr><td colspan=4 class=muted>none</td></tr>"))

    return _PAGE % {"title": esc(" · %s · explore" % pack_name),
                    "beat": "", "sections": _frame(parts)}


# ------------------------------------------------------------- step page

def _step_page(kind, step_name, workflows, pack_name):
    """What does this block do? Clicked from a pipeline node: the step's
    block (name + its docstring — the code IS the documentation), where
    each outcome routes, its params, context providers, and bounds. Pure
    reflection over the loaded defs — nothing is written twice."""
    esc = html.escape
    wf = (workflows or {}).get(kind)
    if wf is None:
        return None
    step = next((s for s in wf.steps if s.name == step_name), None)
    if step is None:
        return None
    blk = step.block
    doc = (blk.fn.__doc__ or "").strip()
    parts = ['<p><a href="/">&larr; runs</a></p>',
             '<div class="runhead"><span class="rname">%s · %s</span>'
             '<span class="chip run">%s</span></div>'
             % (esc(kind), esc(step_name), esc(blk.name))]
    parts.append('<h2>what this block does</h2><div class="ctx">%s</div>'
                 % _rich(doc or "(no docstring)"))
    rows = "".join(
        "<tr><td>%s</td><td>&rarr;</td><td>%s</td></tr>"
        % (esc(outcome),
           ("<a href='/step/%s/%s'>%s</a>" % (esc(kind), esc(target),
                                              esc(target))
            if any(s.name == target for s in wf.steps)
            else "<span class='state-%s'>%s</span>" % (esc(target), esc(target))))
        for (sname, outcome), target in sorted(wf.dispatch.items())
        if sname == step_name)
    parts.append("<h2>where each outcome goes</h2><table><tr><th>outcome</th>"
                 "<th></th><th>next</th></tr>%s</table>" % rows)
    if step.params:
        parts.append("<h2>params</h2>%s" % _kv_html(dict(step.params)))
    meta = {"exec class": blk.exec_class, "timeout": "%ds" % step.timeout_s,
            "max visits": step.max_visits}
    if step.lane:
        meta["lane"] = step.lane
    if step.llm:
        meta["agent binding"] = step.llm
        meta["verdict schema"] = step.schema or ""
    if step.context:
        meta["context served"] = ", ".join(c for c, _spec in step.context)
    parts.append("<h2>bounds &amp; wiring</h2>%s" % _kv_html(meta))
    return _PAGE % {"title": esc(" · %s · %s/%s" % (pack_name, kind, step_name)),
                    "beat": "", "sections": _frame(parts)}


# ------------------------------------------------------------- entity views

def _view_page(conn, name, qs, board, pack_name):
    """A pack-declared entity page (/view/<name>?key=...): the pack's panels
    with the query string bound as SQL params. Everything an entity touches,
    one click deep, via the link:<view> column convention."""
    esc = html.escape
    spec = ((board or {}).get("views") or {}).get(name)
    if spec is None:
        return None
    key = qs.get("key", "")
    title = spec["title"].replace("{key}", key)
    parts = ['<p><a href="/">&larr; runs</a></p>',
             '<div class="runhead"><span class="rname">%s</span></div>'
             % esc(title)]
    for pn in spec["panels"]:
        parts.append(_panel_html(conn, pn, {}, args=_sql_args(pn["sql"], qs)))
    for ln in (board or {}).get("launch") or []:
        if ln.get("on_view") == name:
            parts.append(_launch_form(ln, collapsed=True, key=key))
    return _PAGE % {"title": esc(" · %s · %s" % (pack_name, title)),
                    "beat": "", "sections": _frame(parts)}


# ---------------------------------------------------------- run audit trail

def _audit_trail(conn, task, block_names):
    """Every recorded step of one task, EVERY attempt, oldest first: when,
    which block ran, what it decided, how long it took, and the run
    artifacts (prompt / stdout / verdict / context) when an agent spoke.
    This is the audit surface — full detail lives here, not on the front."""
    esc = html.escape
    rows = conn.execute(
        "SELECT attempt, step, outcome, wall_ms, at, result FROM task_steps"
        " WHERE task_id=? ORDER BY rowid", (task["id"],)).fetchall()
    out = []
    for r in rows:
        res = json.loads(r["result"] or "{}")
        run_id = res.get("_run_id")
        link = (' &middot; <a href="/api/run/%d/prompt">prompt</a>'
                ' <a href="/api/run/%d/stdout">stdout</a>'
                ' <a href="/api/run/%d/verdict">verdict</a>'
                ' <a href="/api/run/%d/context">context</a>'
                % (run_id, run_id, run_id, run_id)) if run_id else ""
        cls = "warn" if r["outcome"] in _BAD_OUTCOMES else "ok"
        out.append(
            "<tr><td class=muted>%s</td><td>a%d</td><td>%s</td>"
            "<td class=muted>%s</td><td class='cell %s'>%s</td><td>%.1fs</td>"
            "<td><details><summary>in/out%s</summary>%s</details>"
            "</td></tr>"
            % (esc(str(r["at"])), r["attempt"], esc(r["step"]),
               esc(block_names.get(r["step"], "")), cls,
               esc(str(r["outcome"])), (r["wall_ms"] or 0) / 1000.0, link,
               _kv_html(res)))
    return out


def _run_audit_page(conn, key, workflows, board, pack_name):
    """/run/<thread value>: the COMPLETE story of one raw request — every
    task it spawned (oldest first), every step of every attempt, every
    decision round with its verdict, every event it emitted."""
    esc = html.escape
    thread_key = (board or {}).get("thread_key")
    if not thread_key:
        return None
    tasks = []
    for r in conn.execute("SELECT * FROM tasks ORDER BY id"):
        try:
            payload = json.loads(r["payload"] or "{}")
        except ValueError:
            continue
        if payload.get(thread_key) == key:
            tasks.append(r)
    if not tasks:
        return None
    from urllib.parse import quote
    parts = ['<p><a href="/">&larr; runs</a></p>',
             '<div class="runhead"><span class="rname">%s</span>'
             '<span class="chip run">audit trail</span>'
             '<span class="when"><a href="/?launch_%s=%s">revise this run'
             ' &rarr;</a></span></div>'
             % (esc(key), quote(thread_key), quote(key))]

    ids = [t["id"] for t in tasks]
    marks = ",".join("?" * len(ids))
    dec = conn.execute(
        "SELECT * FROM decisions WHERE task_id IN (%s) ORDER BY id" % marks,
        ids).fetchall()
    if dec:
        rows = []
        for d in dec:
            answer = json.loads(d["answer"] or "{}")
            said = []
            if answer.get("picked"):
                said.append("picked: %s" % answer["picked"])
            if answer.get("comment"):
                said.append("&ldquo;%s&rdquo;" % esc(str(answer["comment"])))
            rows.append("<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td>"
                        "<td>%s</td><td class=muted>%s</td></tr>"
                        % (esc(d["key"]), d["round"], esc(d["title"]),
                           esc(str(d["verdict"] or "open")),
                           " ".join(said) or "<span class=muted>&mdash;</span>",
                           esc(str(d["resolved_at"] or ""))))
        parts.append("<h2>human decisions</h2><table><tr><th>key</th>"
                     "<th>round</th><th>question</th><th>verdict</th>"
                     "<th>answer</th><th>at</th></tr>%s</table>" % "".join(rows))

    for t in tasks:
        wf = (workflows or {}).get(t["kind"])
        block_names = {s.name: s.block.name for s in wf.steps} if wf else {}
        trail = _audit_trail(conn, t, block_names)
        parts.append(
            "<h2>%s &middot; <a href='/task/%d'>task #%d</a> &middot; "
            "<span class='state-%s'>%s</span></h2>"
            "<table><tr><th>at</th><th>att</th><th>step</th><th>block</th>"
            "<th>outcome</th><th>wall</th><th></th></tr>%s</table>"
            % (esc(t["kind"]), t["id"], t["id"], esc(t["state"]),
               esc(t["state"]),
               "".join(trail) or "<tr><td colspan=7 class=muted>no steps"
                                 " recorded</td></tr>"))

    try:
        evs = conn.execute(
            "SELECT id, name, at FROM events WHERE json_extract(payload, ?)=?"
            " ORDER BY id", ("$." + thread_key, key)).fetchall()
    except Exception:
        evs = []
    if evs:
        parts.append("<h2>events of this run</h2><table><tr><th>id</th>"
                     "<th>at</th><th>event</th></tr>%s</table>"
                     % "".join("<tr><td>%d</td><td class=muted>%s</td>"
                               "<td>%s</td></tr>"
                               % (e["id"], esc(e["at"]), esc(e["name"]))
                               for e in evs))
    return _PAGE % {"title": esc(" · %s · run %s" % (pack_name, key)),
                    "beat": "", "sections": _frame(parts)}


# ------------------------------------------------------------- launch forms

def _launch_form(spec, collapsed, key=None, prefill=None):
    """One pack-declared launch form. `key` (entity views) replaces '{key}'
    in field defaults, so a view-scoped form knows its entity; `prefill`
    ({field: value}, from ?launch_<field>= links) overrides defaults — how a
    finished run's 'revise' link reopens the form with its key filled in."""
    esc = html.escape
    fields = []
    for f in spec["fields"]:
        default = f["default"].replace("{key}", key) if key else f["default"]
        default = (prefill or {}).get(f["name"], default)
        if f["kind"] == "hidden":
            fields.append('<input type="hidden" name="%s" value="%s">'
                          % (esc(f["name"]), esc(default)))
            continue
        ph = ("paste a file path or the text itself"
              if f["kind"] == "path_or_text" else "")
        common = ('name="%s" placeholder="%s"%s'
                  % (esc(f["name"]), esc(ph),
                     " required" if f["required"] else ""))
        if f["kind"] in ("textarea", "path_or_text"):
            inp = ('<textarea %s rows="2">%s</textarea>'
                   % (common, esc(default)))
        else:
            inp = '<input %s value="%s">' % (common, esc(default))
        fields.append('<label class="lf"><span>%s</span>%s</label>'
                      % (esc(f["label"]), inp))
    return ('<section class="card"><details class="hist"%s><summary>%s'
            '</summary><form class="launch" method="post" action="/api/launch">'
            '<input type="hidden" name="event" value="%s">%s'
            '<button class="go">start &rarr;</button></form></details></section>'
            % (" open" if not collapsed else "", esc(spec["title"]),
               esc(spec["event"]), "".join(fields)))


def _launch_forms(board, any_active, prefill=None):
    """Front-page 'start a run' forms (view-scoped ones render on their
    view). Collapsed once runs are in flight, open on an idle board or when
    a 'revise' link arrives with prefill — paste, click, go."""
    return [_launch_form(spec, collapsed=any_active and not prefill,
                         prefill=prefill)
            for spec in (board or {}).get("launch") or []
            if not spec.get("on_view")]
