"""Fail-loud configuration.

Rules (each one is a scar from a predecessor incident):
- Every referenced file (workflow def, prompt, schema, tool) must exist at
  load time or the daemon refuses to start. No fallback to a sibling file,
  ever — a monitor once ran for months with the wrong prompt because of a
  silent fallback.
- Secrets come ONLY from the secrets env file (mode 0600) — never from pack
  files, never from argv (tokens used to leak into process listings).
- Tools are verified, never installed: every non-optional tool in the
  pack's tools section must exist at startup; its version output is
  recorded so runs are attributable to toolchain state.
- The loaded pack records its own git revision; every run pins it.
"""
from __future__ import annotations

import os
import re
import stat
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .util import EVENT_RE, run_cmd, template


class ConfigError(SystemExit):
    """Startup config problems terminate the daemon with a readable message."""


@dataclass(frozen=True)
class Pack:
    name: str
    root: Path
    rev: str                       # git rev of the pack dir at load time
    paths: dict = field(default_factory=dict)     # name -> verified abs path (str)
    params: dict = field(default_factory=dict)    # free-form, path-templated
    workflow_dirs: tuple = ()                     # dirs of workflow YAML defs
    block_files: tuple = ()                       # pack-shipped block modules
    schema_files: tuple = ()                      # pack .sql applied after core
    tools: dict = field(default_factory=dict)     # name -> resolved path (str)
    tool_versions: dict = field(default_factory=dict)  # name -> version line
    agents: dict = field(default_factory=dict)    # llm binding -> backend cfg
    prompts: dict = field(default_factory=dict)   # kind -> abs path (str)
    schemas: dict = field(default_factory=dict)   # name -> parsed schema dict
    models: dict = field(default_factory=dict)    # name -> {path, sha256, params}
    workspace_root: Path = None
    idle_interval_s: int = 15
    unpark_interval_s: int = 600
    # parallel daemon: {workers: N, lanes: {name: cap}}. A step runs in a lane
    # (step.lane, else the block's exec_class); a lane with a cap admits at most
    # that many concurrent steps across workers (e.g. lanes.build=1 serializes a
    # rebuild). Absent -> single worker, no lane caps.
    concurrency: dict = field(default_factory=dict)
    # resource guard: pause claiming new work while free disk on the state root
    # is below this (MB). 0 = disabled. (Per-agent concurrency = lanes above.)
    min_free_disk_mb: int = 0
    # optional health probe for park recovery: the daemon GETs this before
    # unparking agent-backend-dependent tasks (see Engine._agent_online). An
    # "env:VAR" value reads the URL from that env var.
    agent_health_url: str = None
    # effective retry policy: queue.POLICY merged with the pack's retry:
    # overrides (queue.build_policy). Empty = engine defaults.
    policy: dict = field(default_factory=dict)
    # corpora: named selectable tables for the select: context provider —
    # {name: {table, text, key?, ts?, weight?, embed_with?}}. Structure is
    # checked here; table/column existence at engine start (after the pack's
    # schema files have been applied).
    corpora: dict = field(default_factory=dict)
    # board: pack-declared dashboard panels — read-only SELECTs rendered by
    # the engine's generic widgets (table | status_grid | kv). The engine
    # gains a MECHANISM, never domain: what a panel means belongs to the pack.
    # {overview_panels: [...], task_panels: [...]}; task panels may bind SQL
    # named params from the task payload via params: {sql_name: payload_key}.
    board: dict = field(default_factory=dict)
    # notify: {event_name: [argv...]} — when the daemon sees a matching event,
    # it spawns the command (detached) with the event name + JSON payload
    # appended as two extra args. HOW you get pinged (desktop, webhook, chat)
    # is pack configuration; the engine only owns the trigger.
    notify: dict = field(default_factory=dict)
    # item lifecycle — PACK-DECLARED: {state: [allowed next states]}. The
    # engine core ships NO lifecycle (it defines only the initial state
    # 'found' = "recorded"); what states exist and how they may move is
    # domain vocabulary and belongs to the pack. Empty = this pack does not
    # use item transitions at all (staging one is then a loud error).
    item_states: dict = field(default_factory=dict)
    # timed triggers: ({event, every_s, data}, ...) — the daemon emits each
    # event once per every_s window (see Engine._schedule_tick).
    schedule: tuple = ()
    # optional HTTP front door: {host, port, token_ref} — serves the
    # dashboard + JSON API + POST /api/emit inside the daemon (httpd.py).
    http: dict = None


_PACK_KEYS = {"name", "paths", "params", "workflows", "blocks", "schema",
              "tools", "agents", "prompts", "schemas", "models", "workspace_root",
              "idle_interval_s", "unpark_interval_s", "agent_health_url",
              "concurrency", "min_free_disk_mb", "retry", "schedule", "http",
              "corpora", "item_states", "board", "notify"}


def load_pack(pack_dir) -> Pack:
    """Parse <pack_dir>/project.yaml; verify every referenced path/tool."""
    pack_dir = Path(pack_dir).resolve()
    cfg_path = pack_dir / "project.yaml"
    if not cfg_path.is_file():
        raise ConfigError("pack config %s does not exist" % cfg_path)
    with open(cfg_path) as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        raise ConfigError("%s: not a mapping" % cfg_path)
    unknown = set(doc) - _PACK_KEYS
    if unknown:
        raise ConfigError("%s: unknown keys %s" % (cfg_path, sorted(unknown)))
    name = doc.get("name")
    if not name:
        raise ConfigError("%s: pack needs a 'name'" % cfg_path)

    def _fail(msg):
        raise ConfigError("pack '%s' (%s): %s" % (name, cfg_path, msg))

    # paths: every value must exist NOW — no fallback, no deferral
    paths = {}
    for key, value in (doc.get("paths") or {}).items():
        p = Path(str(value)).expanduser()
        if not p.is_absolute():
            p = pack_dir / p
        if not p.exists():
            _fail("paths.%s -> %s does not exist" % (key, p))
        paths[key] = str(p.resolve())

    # params: free-form; {paths.x} resolves NOW (and must), while runtime
    # placeholders ({payload.*} in URL templates etc.) survive for blocks
    try:
        params = template(doc.get("params") or {}, {"paths": paths},
                          partial=True)
    except KeyError as e:
        _fail("params templating: %s" % e)

    # workflow defs: files or dirs, all must exist
    workflow_dirs = []
    for entry in doc.get("workflows") or []:
        p = Path(str(entry))
        if not p.is_absolute():
            p = pack_dir / p
        if not p.is_dir():
            _fail("workflows entry %s is not a directory" % p)
        workflow_dirs.append(p)

    # pack-shipped block modules: files must exist NOW; imported at engine
    # startup so their @block registrations precede workflow compilation
    block_files = []
    for entry in doc.get("blocks") or []:
        p = Path(str(entry))
        if not p.is_absolute():
            p = pack_dir / p
        if not p.is_file():
            _fail("blocks entry %s does not exist" % p)
        block_files.append(str(p))

    # pack-declared schema: .sql files applied AFTER the engine's generic core
    # schema (the engine core names no domain tables; a pack ships its own).
    schema_files = []
    for entry in doc.get("schema") or []:
        p = Path(str(entry))
        if not p.is_absolute():
            p = pack_dir / p
        if not p.is_file():
            _fail("schema entry %s does not exist" % p)
        schema_files.append(str(p))

    # tools (verified, never installed)
    tools, tool_versions = {}, {}
    for tname, spec in (doc.get("tools") or {}).items():
        spec = spec or {}
        tpath = spec.get("path", tname)
        resolved = _resolve_tool(tpath)
        if resolved is None:
            if spec.get("optional"):
                continue
            _fail("tools.%s: '%s' not found — the engine verifies tools, "
                  "it never installs them" % (tname, tpath))
        tools[tname] = resolved
        vcmd = spec.get("version_cmd")
        if vcmd:
            code, out_path, _ = run_cmd([resolved] + list(vcmd), 30,
                                        tempfile.mkdtemp(prefix="toolver-"))
            if code != 0:
                _fail("tools.%s: version_cmd exited %d" % (tname, code))
            first = Path(out_path).read_text().strip().splitlines()
            tool_versions[tname] = first[0] if first else ""

    # prompts and schemas: referenced files must exist
    prompts = {}
    for kind, rel in (doc.get("prompts") or {}).items():
        p = pack_dir / str(rel)
        if not p.is_file():
            _fail("prompts.%s -> %s does not exist" % (kind, p))
        prompts[kind] = str(p)
    schemas = {}
    for sname, rel in (doc.get("schemas") or {}).items():
        p = pack_dir / str(rel)
        if not p.is_file():
            _fail("schemas.%s -> %s does not exist" % (sname, p))
        with open(p) as f:
            schemas[sname] = yaml.safe_load(f)

    # models: local weights (pinned by sha256), an embedding API endpoint
    # (a "BERT-like" local server, any /embeddings-speaking service), OR the
    # built-in zero-setup hashing embedder.
    from .util import sha256_file
    models = {}
    for mname, spec in (doc.get("models") or {}).items():
        spec = spec or {}
        if "hashing" in spec:
            h = spec["hashing"] if isinstance(spec["hashing"], dict) else {}
            if set(spec) - {"hashing"}:
                _fail("models.%s: hashing model takes only 'hashing: {dim}'"
                      % mname)
            if set(h) - {"dim"}:
                _fail("models.%s: hashing accepts only 'dim'" % mname)
            dim = h.get("dim", 256)
            if isinstance(dim, bool) or not isinstance(dim, int) \
                    or not (8 <= dim <= 4096):
                _fail("models.%s: hashing dim must be an integer 8..4096"
                      % mname)
            models[mname] = {"hashing": {"dim": dim}}
        elif "path" in spec:
            if "sha256" not in spec:
                _fail("models.%s: local weights need 'sha256' (pinned)" % mname)
            mp = Path(str(spec["path"])).expanduser()
            if not mp.is_absolute():
                mp = pack_dir / mp
            if not mp.is_file():
                _fail("models.%s -> %s does not exist" % (mname, mp))
            actual = sha256_file(mp)
            if actual != spec["sha256"]:
                _fail("models.%s: sha256 mismatch (file %s, declared %s) — "
                      "weights drifted, refuse to start"
                      % (mname, actual, spec["sha256"]))
            models[mname] = {"path": str(mp), "sha256": spec["sha256"],
                             "params": spec.get("params") or {}}
        elif "base_url" in spec:
            if "model" not in spec:
                _fail("models.%s: api-backed model needs 'model'" % mname)
            # optional startup pin: GET health_url and require the declared
            # fields to match — catches "different weights behind the same
            # port", the api-model analogue of the sha256 check above.
            if spec.get("expect"):
                health_url = spec.get("health_url") or str(spec["base_url"])
                try:
                    import json as _json
                    import urllib.request
                    with urllib.request.urlopen(health_url, timeout=10) as r:
                        health = _json.loads(r.read().decode("utf-8"))
                except Exception as e:
                    _fail("models.%s: health check %s failed: %s"
                          % (mname, health_url, e))
                for key, want in spec["expect"].items():
                    got = health.get(key)
                    if got != want:
                        _fail("models.%s: health %s=%r, expected %r — "
                              "the serving model drifted, refuse to start"
                              % (mname, key, got, want))
            models[mname] = {"base_url": str(spec["base_url"]),
                             "model": str(spec["model"]),
                             "api_key_ref": spec.get("api_key_ref"),
                             "params": spec.get("params") or {}}
        else:
            _fail("models.%s: needs path+sha256 (local weights), "
                  "base_url+model (embedding API), or hashing: {dim} "
                  "(built-in zero-setup embedder)" % mname)

    agents = doc.get("agents") or {}
    for aname, acfg in agents.items():
        _check_agent(aname, acfg, _fail, pack_dir)

    # retry: per-class overrides / pack-defined classes -> effective policy
    from . import queue as queue_mod
    try:
        policy = queue_mod.build_policy(doc.get("retry"))
    except ValueError as e:
        _fail("retry: %s" % e)

    schedule = _parse_schedule(doc.get("schedule"), _fail)
    http = _parse_http(doc.get("http"), _fail)
    corpora = _parse_corpora(doc.get("corpora"), models, agents, _fail)
    item_states = _parse_item_states(doc.get("item_states"), _fail)
    board = _parse_board(doc.get("board"), _fail)
    notify = _parse_notify(doc.get("notify"), _fail)

    workspace_root = doc.get("workspace_root")
    if workspace_root:
        workspace_root = Path(str(workspace_root)).expanduser()
        workspace_root.mkdir(parents=True, exist_ok=True)

    return Pack(
        name=name, root=pack_dir, rev=_git_rev(pack_dir), paths=paths,
        params=params, workflow_dirs=tuple(workflow_dirs),
        block_files=tuple(block_files), schema_files=tuple(schema_files),
        tools=tools,
        tool_versions=tool_versions, agents=agents, prompts=prompts,
        schemas=schemas, models=models, workspace_root=workspace_root,
        idle_interval_s=int(doc.get("idle_interval_s", 15)),
        unpark_interval_s=int(doc.get("unpark_interval_s", 600)),
        agent_health_url=doc.get("agent_health_url"),
        concurrency=doc.get("concurrency") or {},
        min_free_disk_mb=int(doc.get("min_free_disk_mb", 0)),
        policy=policy, schedule=schedule, http=http, corpora=corpora,
        item_states=item_states, board=board, notify=notify,
    )


def _check_panel(pn, where, _fail):
    """One board panel: {title, kind: table|status_grid|kv, sql: SELECT...,
    params?: {sql-name: payload-key}}. SELECT-only is enforced here (the
    board must never write)."""
    if not isinstance(pn, dict):
        _fail("%s must be a mapping" % where)
    bad = set(pn) - {"title", "kind", "sql", "params"}
    if bad:
        _fail("%s: unknown keys %s" % (where, sorted(bad)))
    title, kind, sql = pn.get("title"), pn.get("kind", "table"), pn.get("sql")
    if not title or not isinstance(title, str):
        _fail("%s needs a title" % where)
    if kind not in ("table", "status_grid", "kv"):
        _fail("%s: kind must be table|status_grid|kv" % where)
    if not isinstance(sql, str) or not sql.strip().lower().startswith("select"):
        _fail("%s: sql must be a single SELECT" % where)
    if ";" in sql.rstrip().rstrip(";"):
        _fail("%s: one statement only" % where)
    params = pn.get("params") or {}
    if not isinstance(params, dict) or not all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in params.items()):
        _fail("%s: params must map sql-name -> payload-key" % where)
    return {"title": title, "kind": kind, "sql": sql, "params": params}


_FIELD_KINDS = ("text", "textarea", "path_or_text", "hidden")


def _parse_launch(entries, _fail):
    """board.launch: [{title, event, on_view?, fields: [{name, label?, kind?,
    default?, required?}]}] — a form the board renders; submitting emits
    `event` with one payload key per field. kind path_or_text: if the
    submitted value is a readable file path, the FILE CONTENT becomes the
    value (paste a path or the text itself — same trust as the local CLI).
    on_view: the form renders on that entity view instead of the front page,
    with '{key}' in field defaults replaced by the view key (that is how a
    'change this function' form knows its function). The engine only checks
    shape; whether anyone consumes the event is checked when it fires."""
    if not entries:
        return []
    if not isinstance(entries, list):
        _fail("board.launch must be a list of {title, event, fields}")
    out = []
    for i, e in enumerate(entries):
        where = "board.launch[%d]" % i
        if not isinstance(e, dict):
            _fail("%s must be a mapping" % where)
        bad = set(e) - {"title", "event", "fields", "on_view"}
        if bad:
            _fail("%s: unknown keys %s" % (where, sorted(bad)))
        title, ev = e.get("title"), e.get("event")
        if not title or not isinstance(title, str):
            _fail("%s needs a title" % where)
        if not isinstance(ev, str) or not EVENT_RE.match(ev):
            _fail("%s: malformed event name %r" % (where, ev))
        on_view = e.get("on_view")
        if on_view is not None and (not isinstance(on_view, str) or not on_view):
            _fail("%s: on_view must be a view name" % where)
        fields = e.get("fields") or []
        if not isinstance(fields, list) or not fields:
            _fail("%s needs a non-empty fields list" % where)
        clean = []
        for j, f in enumerate(fields):
            fw = "%s.fields[%d]" % (where, j)
            if not isinstance(f, dict):
                _fail("%s must be a mapping" % fw)
            fbad = set(f) - {"name", "label", "kind", "default", "required"}
            if fbad:
                _fail("%s: unknown keys %s" % (fw, sorted(fbad)))
            fname = f.get("name")
            if not isinstance(fname, str) or not _IDENT_RE.match(fname):
                _fail("%s: 'name' must be a plain identifier (payload key)" % fw)
            fkind = f.get("kind", "text")
            if fkind not in _FIELD_KINDS:
                _fail("%s: kind must be one of %s" % (fw, "|".join(_FIELD_KINDS)))
            clean.append({"name": fname, "label": str(f.get("label") or fname),
                          "kind": fkind, "default": str(f.get("default", "")),
                          "required": bool(f.get("required", False))})
        out.append({"title": title, "event": ev, "fields": clean,
                    "on_view": on_view})
    return out


def _parse_views(doc, _fail):
    """board.views: {name: {title, panels: [...]}} — parameterized entity
    pages at /view/<name>?key=... Panel SQL binds :key (and any other query-
    string arg it names). Cross-linking is a COLUMN ALIAS convention: a
    column named 'link:<view>' renders each cell as a link to that view
    keyed by the cell value — the pack declares relationships in SQL, the
    engine renders them. Corpora pattern: mechanism here, meaning there."""
    if not doc:
        return {}
    if not isinstance(doc, dict):
        _fail("board.views must be a mapping of view-name -> {title, panels}")
    out = {}
    for name, spec in doc.items():
        if not isinstance(name, str) or not _IDENT_RE.match(name):
            _fail("board.views: bad view name %r" % (name,))
        where = "board.views.%s" % name
        if not isinstance(spec, dict):
            _fail("%s must be a mapping" % where)
        bad = set(spec) - {"title", "panels"}
        if bad:
            _fail("%s: unknown keys %s" % (where, sorted(bad)))
        title = spec.get("title") or name
        if not isinstance(title, str):
            _fail("%s: title must be a string" % where)
        panels = spec.get("panels")
        if not isinstance(panels, list) or not panels:
            _fail("%s needs a non-empty panels list" % where)
        out[name] = {"title": title,
                     "panels": [_check_panel(pn, "%s.panels[%d]" % (where, i),
                                             _fail)
                                for i, pn in enumerate(panels)]}
    return out


def _parse_board(doc, _fail):
    """board: {thread_key?, overview_panels?, task_panels?, views?, launch?}.
    thread_key names the payload field that correlates tasks/decisions into
    one RUN (one raw request = one thread through every workflow) — the board
    groups by it; the engine attaches no meaning to the value."""
    if not doc:
        return {}
    if not isinstance(doc, dict):
        _fail("board: must be a mapping")
    unknown = set(doc) - {"overview_panels", "task_panels", "thread_key",
                          "views", "launch"}
    if unknown:
        _fail("board: unknown keys %s" % sorted(unknown))
    out = {}
    tk = doc.get("thread_key")
    if tk is not None:
        if not isinstance(tk, str) or not tk:
            _fail("board: thread_key must be a non-empty payload field name")
        out["thread_key"] = tk
    for section in ("overview_panels", "task_panels"):
        panels = doc.get(section) or []
        if not isinstance(panels, list):
            _fail("board.%s must be a list" % section)
        out[section] = [_check_panel(pn, "board.%s[%d]" % (section, i), _fail)
                        for i, pn in enumerate(panels)]
    out["views"] = _parse_views(doc.get("views"), _fail)
    out["launch"] = _parse_launch(doc.get("launch"), _fail)
    return out


def _parse_notify(doc, _fail):
    """notify: {event_name: [argv...]} — argv list, never a shell string."""
    if not doc:
        return {}
    if not isinstance(doc, dict):
        _fail("notify: must map event names to argv lists")
    out = {}
    for ev, argv in doc.items():
        if not isinstance(ev, str) or not ev:
            _fail("notify: event names must be strings")
        if (not isinstance(argv, list) or not argv
                or not all(isinstance(a, str) for a in argv)):
            _fail("notify.%s: must be a non-empty argv list" % ev)
        out[ev] = list(argv)
    return out


def _parse_item_states(doc, _fail):
    """item_states: {state: [allowed next states]}. The pack owns the item
    lifecycle; the engine only checks the map is well-formed: every successor
    must itself be a declared state, and 'found' (the engine's sole built-in
    initial state) must be declared whenever the map is non-empty."""
    if not doc:
        return {}
    if not isinstance(doc, dict):
        _fail("item_states must be a mapping of state -> [next states]")
    states = {}
    for k, v in doc.items():
        if not isinstance(k, str) or not k:
            _fail("item_states: state names must be non-empty strings")
        nxt = v or []
        if not isinstance(nxt, list) or not all(isinstance(x, str) for x in nxt):
            _fail("item_states.%s must be a list of state names" % k)
        states[k] = set(nxt)
    if "found" not in states:
        _fail("item_states must declare 'found' (items are created in it)")
    for k, nxt in states.items():
        for x in nxt:
            if x not in states:
                _fail("item_states.%s lists undeclared state '%s'" % (k, x))
    return states


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CORPUS_KEYS = {"table", "text", "key", "ts", "weight", "embed_with",
                "summarize_with", "track"}


def _parse_corpora(entries, models, agents, _fail) -> dict:
    """corpora: {name: {table, text, key?, ts?, weight?, embed_with?,
    summarize_with?}}. table/columns are SQL identifiers (validated as
    such — they are later quoted into statements); embed_with is 'hashing'
    or a models: entry; summarize_with is an agents: role (typically a
    local model) that condenses long rows. Existence of the table/columns
    is checked at engine start, after the pack's own schema files have
    been applied."""
    if entries is None:
        return {}
    if not isinstance(entries, dict):
        _fail("corpora: must be a mapping of name -> {table, text, ...}")
    out = {}
    for name, spec in entries.items():
        where = "corpora.%s" % name
        if not isinstance(name, str) or not _IDENT_RE.match(name):
            _fail("corpora: bad corpus name %r" % (name,))
        if not isinstance(spec, dict):
            _fail("%s: must be a mapping" % where)
        unknown = set(spec) - _CORPUS_KEYS
        if unknown:
            _fail("%s: unknown keys %s (accepted: %s)"
                  % (where, sorted(unknown), sorted(_CORPUS_KEYS)))
        for req in ("table", "text"):
            if req not in spec:
                _fail("%s: needs '%s'" % (where, req))
        for field_name in ("table", "text", "key", "ts", "weight"):
            v = spec.get(field_name)
            if v is not None and (not isinstance(v, str)
                                  or not _IDENT_RE.match(v)):
                _fail("%s: '%s' must be a plain SQL identifier, got %r"
                      % (where, field_name, v))
        ew = spec.get("embed_with")
        if ew is not None and ew != "hashing" and ew not in models:
            _fail("%s: embed_with '%s' is neither 'hashing' nor a models: "
                  "entry (defined: %s)" % (where, ew, sorted(models) or "none"))
        tr = spec.get("track")
        if tr is not None and not isinstance(tr, bool):
            _fail("%s: 'track' must be a boolean (false = never record "
                  "selection history for this corpus; utility abstains)"
                  % where)
        sw = spec.get("summarize_with")
        if sw is not None and sw not in agents:
            _fail("%s: summarize_with '%s' is not an agents: role "
                  "(defined: %s)" % (where, sw, sorted(agents) or "none"))
        out[name] = dict(spec)
    return out


_EVERY_RE = re.compile(r"^(\d+)([smhd])$")
_EVERY_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_every(value):
    """'90s' / '5m' / '6h' / '1d' or a plain integer of seconds -> seconds.
    Returns None if malformed (caller fails loud with context)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 1 else None
    if isinstance(value, str):
        m = _EVERY_RE.match(value.strip())
        if m:
            s = int(m.group(1)) * _EVERY_UNITS[m.group(2)]
            return s if s >= 1 else None
    return None


def _parse_schedule(entries, _fail) -> tuple:
    """schedule: [{event, every, data?}, ...] -> ({event, every_s, data}, ...).
    Event names are format-checked here; 'someone must consume it' is checked
    at engine startup once the workflows are loaded."""
    if entries is None:
        return ()
    if not isinstance(entries, list):
        _fail("schedule: must be a list of {event, every, data?} entries")
    out = []
    for i, e in enumerate(entries):
        where = "schedule[%d]" % i
        if not isinstance(e, dict):
            _fail("%s: must be a mapping" % where)
        unknown = set(e) - {"event", "every", "data"}
        if unknown:
            _fail("%s: unknown keys %s" % (where, sorted(unknown)))
        ev = e.get("event")
        if not isinstance(ev, str) or not EVENT_RE.match(ev):
            _fail("%s: malformed event name %r (want e.g. 'nightly.build')"
                  % (where, ev))
        every_s = parse_every(e.get("every"))
        if every_s is None:
            _fail("%s: bad 'every' %r (want '30s'/'5m'/'6h'/'1d' or seconds)"
                  % (where, e.get("every")))
        data = e.get("data") or {}
        if not isinstance(data, dict):
            _fail("%s: 'data' must be a mapping" % where)
        out.append({"event": ev, "every_s": every_s, "data": data})
    return tuple(out)


def _parse_http(spec, _fail):
    """http: {host?, port, token_ref?}. Binding beyond loopback without a
    token is refused outright — an open emit endpoint is an incident."""
    if spec is None:
        return None
    if not isinstance(spec, dict):
        _fail("http: must be a mapping {host, port, token_ref}")
    unknown = set(spec) - {"host", "port", "token_ref"}
    if unknown:
        _fail("http: unknown keys %s" % sorted(unknown))
    port = spec.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not (0 <= port <= 65535):
        _fail("http: 'port' must be an integer 0-65535")
    host = str(spec.get("host", "127.0.0.1"))
    token_ref = spec.get("token_ref")
    if token_ref is not None and (not isinstance(token_ref, str) or not token_ref):
        _fail("http: 'token_ref' must be a non-empty string")
    if host not in ("127.0.0.1", "localhost", "::1") and not token_ref:
        _fail("http: binding to %r beyond loopback requires 'token_ref' "
              "(HTTP_TOKEN_<REF> in the secrets file)" % host)
    return {"host": host, "port": port, "token_ref": token_ref}


# What each agent backend accepts in its binding. STRUCTURE is checked here
# (fail loud with the file+field); ENVIRONMENT (cli on PATH, secret present)
# is checked at engine start via runner.check_binding — after a --replay-from
# wrap, so replaying on a machine without the live backend still works.
_AGENT_KEYS = {
    "claude-cli":    {"backend", "model", "cli", "permission_mode",
                      "env_keys", "max_turns", "extra_args"},
    "openai-compat": {"backend", "model", "base_url", "api_key_ref", "params"},
    "replay":        {"backend", "model", "source"},
}


def _check_agent(aname, acfg, _fail, pack_dir):
    if not isinstance(acfg, dict) or "backend" not in acfg:
        _fail("agents.%s: needs at least 'backend:'" % aname)
    backend = acfg["backend"]
    if backend not in _AGENT_KEYS:
        _fail("agents.%s: unknown backend %r (known: %s)"
              % (aname, backend, ", ".join(sorted(_AGENT_KEYS))))
    unknown = set(acfg) - _AGENT_KEYS[backend]
    if unknown:
        _fail("agents.%s: unknown keys %s for backend %s (accepted: %s)"
              % (aname, sorted(unknown), backend,
                 sorted(_AGENT_KEYS[backend])))
    if backend == "openai-compat":
        base_url = acfg.get("base_url")
        if not isinstance(base_url, str) or not base_url.startswith(("http://", "https://")):
            _fail("agents.%s: openai-compat needs 'base_url' (http(s)://...)" % aname)
        if not acfg.get("model"):
            _fail("agents.%s: openai-compat needs 'model'" % aname)
        params = acfg.get("params")
        if params is not None and not isinstance(params, dict):
            _fail("agents.%s: 'params' must be a mapping of request-body "
                  "fields (temperature, max_tokens, ...)" % aname)
    elif backend == "claude-cli":
        mt = acfg.get("max_turns")
        if mt is not None and (isinstance(mt, bool) or not isinstance(mt, int)
                               or mt < 1):
            _fail("agents.%s: 'max_turns' must be an integer >= 1" % aname)
        for key in ("extra_args", "env_keys"):
            v = acfg.get(key)
            if v is not None and (not isinstance(v, list)
                                  or any(not isinstance(x, str) for x in v)):
                _fail("agents.%s: '%s' must be a list of strings" % (aname, key))
    elif backend == "replay":
        src = acfg.get("source")
        if not src:
            _fail("agents.%s: replay backend needs 'source' (a root "
                  "holding the recording)" % aname)
        sp = Path(str(src)).expanduser()
        if not sp.is_absolute():
            sp = pack_dir / sp
        if not (sp / "state" / "forgeflow.db").is_file():
            _fail("agents.%s: replay source %s has no recording "
                  "(state/forgeflow.db missing)" % (aname, sp))
        acfg["source"] = str(sp)


def _resolve_tool(tpath):
    p = Path(str(tpath)).expanduser()
    if p.is_absolute():
        return str(p) if p.is_file() and os.access(str(p), os.X_OK) else None
    import shutil
    return shutil.which(str(tpath))


def _git_rev(path) -> str:
    try:
        out = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             timeout=10)
        if out.returncode == 0:
            return out.stdout.decode().strip()
    except Exception:
        pass
    return "unversioned"


def load_secrets(path=None) -> dict:
    """KEY=value lines from ~/.config/forgeflow/secrets.env. Refuses to read
    a file that is readable by group/other. Missing file = no secrets (fine
    for local-only packs). Secrets reach subprocesses via env vars only —
    never argv, never pack files, never logs."""
    if path is None:
        path = os.environ.get("FORGEFLOW_SECRETS")   # tests / odd deployments
    path = Path(path) if path else Path.home() / ".config" / "forgeflow" / "secrets.env"
    if not path.exists():
        return {}
    mode = stat.S_IMODE(os.stat(str(path)).st_mode)
    if mode & 0o077:
        raise ConfigError(
            "secrets file %s has mode %o — refuse to read anything looser "
            "than 0600" % (path, mode))
    secrets = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        secrets[key.strip()] = value.strip()
    return secrets
