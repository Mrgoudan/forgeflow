"""run_agent(): the ONLY path to any model (choke point #3).

Order is load-bearing (ENGINE.md "Agent step mechanics"):
  1. assemble prompt from the step's DECLARED context only -> sha256;
  2. INSERT the runs row and COMMIT before exec — a crash mid-run still
     leaves an attributable record;
  3. exec the backend (claude-cli: fixed argv, prompt via stdin,
     cwd=worktree, minimal env, hard timeout);
  4. stdout/stderr archived verbatim under data/runs/<run_id>/;
  5. extract the LAST ```json fenced block, validate against the step's
     schema; on failure re-ask (a correction message) at most twice —
     re-asks continue the same session and the SAME runs row;
  6. UPDATE the runs row (exit_code, verdict, finished_at); return the
     verdict dict or raise RunnerError(error_class).

Error classes come from exit codes and the CLI's structured result
envelope ONLY — never from model prose. The model may reduce yield;
it can never reduce integrity.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

from .util import canonical_json, sha256_text, run_cmd, tx, validate_schema, SchemaError

MAX_REASKS = 2

_FENCE_RE = re.compile(r"```json\s*\n(.*?)\n\s*```", re.DOTALL)


class RunnerError(Exception):
    """Carries a POLICY error class; the agent.run block returns it as the
    step outcome for the workflow to dispatch on."""

    def __init__(self, error_class, detail=""):
        super().__init__("%s: %s" % (error_class, detail))
        self.error_class = error_class


# --------------------------------------------------------------- backends
#
# A backend takes one ask and returns a normalized response dict:
#   { "exit_code": int, "result": model text, "session": opaque continuation
#     state for re-asks, "error_class": None | POLICY class, "detail": str }
# Classification uses exit codes / HTTP status / envelope structure ONLY.

def _agent_env(binding):
    """Minimal env: never leak the daemon's secrets into agent processes.
    Base set = process basics + proxy transport; anything else the backend
    needs must be named explicitly in the binding's env_keys."""
    base_keys = ("PATH", "HOME", "TERM", "LANG", "SHELL",
                 "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY",
                 "http_proxy", "https_proxy", "no_proxy", "all_proxy")
    env = {k: v for k, v in os.environ.items() if k in base_keys}
    for k in binding.get("env_keys", ()):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


def _claude_cli_backend(binding, ask, *, cwd, timeout_s, out_dir,
                        session=None, secrets=None):
    """Agentic CLI backend. Fixed argv; the ask travels via stdin (never
    argv — argv leaks into process listings). Re-asks resume the same CLI
    session."""
    argv = [binding.get("cli", "claude"), "-p",
            "--permission-mode", binding.get("permission_mode", "bypassPermissions"),
            "--output-format", "json"]
    if binding.get("model"):
        argv += ["--model", str(binding["model"])]
    if binding.get("max_turns"):
        argv += ["--max-turns", str(binding["max_turns"])]
    # extra_args: static, from the verified pack file — an escape hatch for
    # CLI flags the engine has no opinion on (--allowedTools etc.)
    argv += [str(a) for a in binding.get("extra_args") or ()]
    if session:
        argv += ["--resume", str(session)]
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    prompt_file = out_dir / "prompt"          # snapshot: what was actually sent
    prompt_file.write_text(ask)
    exit_code, stdout_path, stderr_path = run_cmd(
        argv, timeout_s, out_dir, cwd=cwd, env=_agent_env(binding),
        stdin_path=prompt_file)
    envelope = _parse_envelope(stdout_path)
    error_class = None
    detail = ""
    if exit_code != 0 or envelope["is_error"] or envelope["subtype"] != "success":
        if envelope["subtype"] == "error_max_turns" or \
                envelope.get("api_error_status") in (401, 403, 429):
            error_class = "agent_limit"       # auth/limit: park, human/time fixes
        else:
            error_class = "agent_backend"
        detail = ("exit=%s subtype=%s api_status=%s (archived at %s)"
                  % (exit_code, envelope["subtype"],
                     envelope.get("api_error_status"), out_dir))
    return {"exit_code": exit_code, "result": envelope["result"],
            "session": envelope["session_id"] or session,
            "error_class": error_class, "detail": detail}


def _openai_compat_backend(binding, ask, *, cwd, timeout_s, out_dir,
                           session=None, secrets=None):
    """Text-only HTTP backend speaking the de-facto chat-completions
    protocol (local runtimes, gateways, hosted endpoints). No tools, no
    cwd access: text in, text out. api_key_ref names LLM_API_KEY_<REF> in
    the secrets file — the key itself never appears in pack files, argv,
    or logs. Re-asks carry the message history."""
    messages = list(session or [])
    messages.append({"role": "user", "content": ask})
    body = {"model": binding.get("model", ""), "messages": messages}
    # params: passed through verbatim (temperature, max_tokens, top_p,
    # response_format, ...) — the engine has no opinion on sampling knobs,
    # only that model/messages stay authoritative.
    for k, v in (binding.get("params") or {}).items():
        if k not in ("model", "messages"):
            body[k] = v
    status, data = _http_json(
        binding, "/chat/completions", body, timeout_s, out_dir, secrets)
    try:
        content = data["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError):
        return {"exit_code": status, "result": "", "session": messages,
                "error_class": "agent_backend",
                "detail": "malformed chat-completions response (archived)"}
    messages.append({"role": "assistant", "content": content})
    return {"exit_code": status, "result": content, "session": messages,
            "error_class": None, "detail": ""}


def _replay_backend(binding, ask, *, cwd, timeout_s, out_dir,
                    session=None, secrets=None):
    """Deterministic CI backend: answer from a PRIOR root's recordings
    instead of calling any model. Every successful agent run archives its
    schema-valid verdict (data/runs/<id>/verdict.json, written by _finish);
    replay looks that verdict up by prompt_sha — the sha of the assembled
    prompt — so a changed prompt, context, or schema is a MISS, never a
    silently-wrong answer. A miss surfaces as agent_invalid_output: bounded
    fast retries, then a loud terminal failure (CI-friendly — agent_backend
    would park and stall the run).

    binding: { backend: replay, source: <root that holds the recording> }.
    """
    source = Path(binding.get("source", ""))
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "prompt").write_text(ask)          # parity with live backends
    db_path = source / "state" / "forgeflow.db"
    prompt_sha = sha256_text(ask)

    def miss(why):
        return {"exit_code": 0, "result": "", "session": None,
                "error_class": "agent_invalid_output",
                "detail": "replay miss: %s (source %s, prompt %.12s...)"
                          % (why, source, prompt_sha)}

    if not db_path.is_file():
        return miss("no recording db at %s" % db_path)
    import sqlite3
    src = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    src.row_factory = sqlite3.Row
    try:
        row = src.execute(
            "SELECT id FROM runs WHERE prompt_sha=? AND verdict IS NOT NULL"
            " ORDER BY id DESC LIMIT 1", (prompt_sha,)).fetchone()
    finally:
        src.close()
    if row is None:
        return miss("no recorded run for this prompt")
    verdict_path = source / "data" / "runs" / str(row["id"]) / "verdict.json"
    if not verdict_path.is_file():
        return miss("run %d has no verdict.json (recorded before 0.2.0, "
                    "or archives were gc'd)" % row["id"])
    verdict_text = verdict_path.read_text()
    (out_dir / "replay.json").write_text(canonical_json(
        {"source": str(source), "run_id": row["id"], "prompt_sha": prompt_sha}))
    return {"exit_code": 0,
            "result": "replayed run %d\n```json\n%s\n```" % (row["id"], verdict_text),
            "session": None, "error_class": None, "detail": ""}


BACKENDS = {"claude-cli": _claude_cli_backend,
            "openai-compat": _openai_compat_backend,
            "replay": _replay_backend}


def _http_json(binding, route, body, timeout_s, out_dir, secrets):
    """POST canonical JSON, archive request/response verbatim, classify by
    HTTP status only. Raises RunnerError / TimeoutExpired."""
    import socket
    import urllib.error
    import urllib.request
    base_url = binding.get("base_url")
    if not base_url:
        raise RunnerError("agent_backend", "binding needs base_url")
    url = base_url.rstrip("/") + route
    headers = {"Content-Type": "application/json"}
    ref = binding.get("api_key_ref")
    if ref:
        if secrets is None:
            from .config import load_secrets
            secrets = load_secrets()
        key = secrets.get("LLM_API_KEY_%s" % ref)
        if not key:
            raise RunnerError("agent_limit",
                              "secret LLM_API_KEY_%s not configured" % ref)
        headers["Authorization"] = "Bearer " + key
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = canonical_json(body)
    (out_dir / "request.json").write_text(payload)   # never contains the key
    req = urllib.request.Request(url, data=payload.encode("utf-8"),
                                 headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read()
        (out_dir / "response.json").write_bytes(raw)
        if e.code in (401, 403, 429):
            raise RunnerError("agent_limit", "HTTP %d from %s" % (e.code, url))
        raise RunnerError("agent_backend", "HTTP %d from %s" % (e.code, url))
    except socket.timeout:
        import subprocess
        raise subprocess.TimeoutExpired(url, timeout_s)
    except urllib.error.URLError as e:
        if isinstance(getattr(e, "reason", None), socket.timeout):
            import subprocess
            raise subprocess.TimeoutExpired(url, timeout_s)
        raise RunnerError("agent_backend", "unreachable %s (%s)" % (url, e.reason))
    (out_dir / "response.json").write_bytes(raw)
    try:
        return status, json.loads(raw.decode("utf-8"))
    except ValueError:
        raise RunnerError("agent_backend", "non-JSON response from %s" % url)


def embed_api(model_spec, text, *, timeout_s, out_dir, secrets=None):
    """Embeddings over the same HTTP protocol (/embeddings) — how a
    'BERT-like' local server (or any embedding endpoint) plugs in. Returns
    the vector. Outputs are claims, exactly like local-weight models."""
    body = {"model": model_spec.get("model", ""), "input": [text]}
    status, data = _http_json(model_spec, "/embeddings", body, timeout_s,
                              out_dir, secrets)
    try:
        vec = data["data"][0]["embedding"]
        return [float(x) for x in vec]
    except (KeyError, IndexError, TypeError, ValueError):
        raise RunnerError("agent_backend", "malformed embeddings response")


# ---------------------------------------------------------------- helpers

def assemble_prompt(base_prompt, context_slice, schema):
    """Deterministic prompt assembly: base text, declared context sections
    in sorted order (canonical JSON), then the output contract."""
    parts = [base_prompt.rstrip(), ""]
    for key in sorted(context_slice):
        parts += ["## context: %s" % key, canonical_json(context_slice[key]), ""]
    parts += [
        "## output contract",
        "Your final message MUST end with a ```json fenced block that "
        "validates against this schema:",
        canonical_json(schema), ""]
    return "\n".join(parts)


def extract_verdict(text, schema):
    """The schema-valid verdict from the model text. Prefers the LAST
    ```json fenced block (agentic CLIs narrate around it); falls back to
    the WHOLE stripped response as JSON when there is no fence — that is
    what a JSON-mode endpoint (response_format: json_object) returns, and
    it is now first-class. Raises ValueError/SchemaError — the caller
    turns that into a re-ask."""
    blocks = _FENCE_RE.findall(text or "")
    if blocks:
        verdict = json.loads(blocks[-1])
    else:
        stripped = (text or "").strip()
        # tolerate a bare ``` ... ``` wrapper (some models fence without a
        # language tag) before giving up
        if stripped.startswith("```") and stripped.endswith("```"):
            stripped = stripped[3:-3].strip()
            if stripped.startswith("json"):
                stripped = stripped[4:].strip()
        if not stripped:
            raise ValueError("empty model output")
        try:
            verdict = json.loads(stripped)
        except ValueError:
            raise ValueError("no ```json fenced block and the response is "
                             "not itself JSON")
    validate_schema(verdict, schema)
    return verdict


def _parse_envelope(stdout_path):
    """The CLI's --output-format json envelope. Structure only: is_error /
    subtype are CLI enums, 'result' is the model text (archived, and mined
    only for the fenced verdict block). A non-envelope stdout is treated
    as raw model text (plain -p compatibility)."""
    raw = Path(stdout_path).read_text(errors="replace")
    try:
        obj = json.loads(raw)
    except ValueError:
        return {"result": raw, "session_id": None, "is_error": False,
                "subtype": "success"}
    if not isinstance(obj, dict):
        return {"result": raw, "session_id": None, "is_error": False,
                "subtype": "success", "api_error_status": None}
    return {"result": obj.get("result") or "",
            "session_id": obj.get("session_id"),
            "is_error": bool(obj.get("is_error")),
            "subtype": obj.get("subtype", "success"),
            "api_error_status": obj.get("api_error_status")}


# ------------------------------------------------------------------ core

def run_agent(conn, task, binding, base_prompt, schema, *, data_dir,
              pack_rev, cwd=None, timeout_s=3600, context_slice=None,
              vault_rev=None, probe_rev=None, base_sha=None, build_id=None,
              secrets=None, context_manifest=None):
    """Execute one agent step. Returns the schema-valid verdict dict.
    Raises RunnerError('agent_backend' | 'agent_limit' |
    'agent_invalid_output') for the workflow to dispatch on."""
    backend = BACKENDS.get(binding.get("backend"))
    if backend is None:
        raise RunnerError("agent_backend",
                          "unknown backend %r (known: %s)"
                          % (binding.get("backend"), sorted(BACKENDS)))
    context_slice = context_slice or {}
    prompt = assemble_prompt(base_prompt, context_slice, schema)
    prompt_sha = sha256_text(prompt)

    # PIN before exec: the runs row exists even if we die during the call.
    with tx(conn):
        cur = conn.execute(
            "INSERT INTO runs(task_id, model, prompt_sha, pack_rev, vault_rev,"
            " probe_rev, base_sha, build_id)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (task["id"], str(binding.get("model", "backend-default")),
             prompt_sha, pack_rev, vault_rev, probe_rev, base_sha, build_id))
        run_id = cur.lastrowid
    run_dir = Path(data_dir) / "runs" / str(run_id)
    if context_manifest is not None:
        # what composed the prompt, as data — beside the archived prompt
        from .util import atomic_write
        atomic_write(run_dir / "context.json", canonical_json(context_manifest))

    ask = prompt
    session = None
    exit_code = None
    last_error = None
    started = time.monotonic()
    wall = lambda: int((time.monotonic() - started) * 1000)
    for round_no in range(1 + MAX_REASKS):        # bounded re-asks, same runs row
        out_dir = run_dir / ("ask%d" % round_no)
        try:
            resp = backend(binding, ask, cwd=cwd, timeout_s=timeout_s,
                           out_dir=out_dir, session=session, secrets=secrets)
        except Exception:
            _finish(conn, run_id, exit_code, None, str(run_dir),
                    wall_ms=wall(), reasks=round_no)
            raise                     # TimeoutExpired / RunnerError -> caller
        exit_code = resp.get("exit_code")
        session = resp.get("session") or session
        if resp.get("error_class"):
            _finish(conn, run_id, exit_code, None, str(run_dir),
                    wall_ms=wall(), reasks=round_no)
            raise RunnerError(resp["error_class"], resp.get("detail", ""))
        try:
            verdict = extract_verdict(resp.get("result"), schema)
        except (ValueError, SchemaError) as e:
            last_error = str(e)
            ask = ("Your previous reply did not satisfy the output contract "
                   "(%s). Reply again: end with ONE ```json fenced block "
                   "valid against the schema, and nothing after it." % e)
            continue
        _finish(conn, run_id, exit_code, verdict, str(run_dir),
                wall_ms=wall(), reasks=round_no)
        verdict["_run_id"] = run_id
        return verdict

    _finish(conn, run_id, exit_code, None, str(run_dir),
            wall_ms=wall(), reasks=MAX_REASKS)
    raise RunnerError("agent_invalid_output",
                      "no schema-valid verdict after %d re-asks (%s)"
                      % (MAX_REASKS, last_error))


def _finish(conn, run_id, exit_code, verdict, output_path, wall_ms=None,
            reasks=None):
    with tx(conn):
        conn.execute(
            "UPDATE runs SET exit_code=?, verdict=?, output_path=?, wall_ms=?,"
            " reasks=?, finished_at=datetime('now') WHERE id=?",
            (exit_code,
             verdict.get("verdict") if isinstance(verdict, dict) else None,
             output_path, wall_ms, reasks, run_id))
    if isinstance(verdict, dict):
        # the recording half of record/replay: the schema-valid verdict,
        # canonical and BEFORE the engine's _run_id annotation, so a replay
        # revalidates against the step schema byte-for-byte
        from .util import atomic_write
        atomic_write(Path(output_path) / "verdict.json", canonical_json(verdict))


# ------------------------------------------------- startup + probe checks

def check_binding(name, binding, secrets) -> str:
    """ENVIRONMENT checks for one agent binding, run at engine start (after
    a --replay-from wrap, so replay-bound roles skip live-backend needs).
    No network calls — startup stays fast and offline-safe; `llm check`
    does the live round-trip. Returns an error string or None. May resolve
    binding['cli'] in place (verified path, like the pack tools section)."""
    b = binding.get("backend")
    if b not in BACKENDS:
        return "unknown backend %r (known: %s)" % (b, ", ".join(sorted(BACKENDS)))
    if b == "claude-cli":
        from .config import _resolve_tool
        cli = binding.get("cli", "claude")
        resolved = _resolve_tool(cli)
        if resolved is None:
            return ("cli '%s' not found (or not executable) — install it, "
                    "point 'cli:' at it, or bind this role to another "
                    "backend / --replay-from" % cli)
        binding["cli"] = resolved
    elif b == "openai-compat":
        ref = binding.get("api_key_ref")
        if ref and not secrets.get("LLM_API_KEY_%s" % ref):
            return ("secret LLM_API_KEY_%s is not in the secrets file — "
                    "refuse to start and park later; add it or drop "
                    "api_key_ref" % ref)
    elif b == "replay":
        source = Path(binding.get("source", ""))
        if not (source / "state" / "forgeflow.db").is_file():
            return "replay source %s has no recording (state/forgeflow.db)" % source
    return None


PROBE_SCHEMA = {"type": "object", "required": ["verdict"],
                "properties": {"verdict": {"enum": ["OK"]}}}

PROBE_PROMPT = ("Connectivity probe from a workflow engine. Do nothing "
                "else: reply with the verdict OK per the output contract.")


def probe_binding(name, binding, *, out_dir, secrets=None, timeout_s=60):
    """ONE live round-trip through the binding's real backend with a
    minimal schema. Proves: endpoint reachable, auth valid, model loaded,
    and the model can follow the fenced-JSON output contract (a model that
    can't will fail every agent step as agent_invalid — that's a real
    failure, reported distinctly). Never raises; returns
    {ok, detail, wall_ms?, ...}."""
    backend = BACKENDS.get(binding.get("backend"))
    if backend is None:
        return {"ok": False, "detail": "unknown backend %r" % binding.get("backend")}
    if binding.get("backend") == "replay":
        import sqlite3
        source = Path(binding.get("source", ""))
        db_path = source / "state" / "forgeflow.db"
        if not db_path.is_file():
            return {"ok": False, "detail": "no recording db at %s" % db_path}
        src = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
        try:
            n = src.execute("SELECT count(*) FROM runs"
                            " WHERE verdict IS NOT NULL").fetchone()[0]
        finally:
            src.close()
        return {"ok": n > 0, "recordings": n,
                "detail": ("%d recorded verdict(s) available" % n) if n
                else "recording db exists but holds no verdicts"}
    ask = assemble_prompt(PROBE_PROMPT, {}, PROBE_SCHEMA)
    started = time.monotonic()
    try:
        resp = backend(binding, ask, cwd=None, timeout_s=timeout_s,
                       out_dir=out_dir, session=None, secrets=secrets)
    except Exception as e:                       # RunnerError / TimeoutExpired
        return {"ok": False, "wall_ms": int((time.monotonic() - started) * 1000),
                "detail": "%s: %s" % (type(e).__name__, e)}
    wall_ms = int((time.monotonic() - started) * 1000)
    if resp.get("error_class"):
        return {"ok": False, "wall_ms": wall_ms,
                "detail": "%s: %s" % (resp["error_class"], resp.get("detail", ""))}
    try:
        extract_verdict(resp.get("result"), PROBE_SCHEMA)
    except (ValueError, SchemaError) as e:
        return {"ok": False, "wall_ms": wall_ms,
                "detail": "transport OK but the model did not follow the "
                          "output contract (%s) — agent steps would fail as "
                          "agent_invalid" % e}
    return {"ok": True, "wall_ms": wall_ms, "detail": "answered the contract"}


def probe_model(name, spec, *, out_dir, secrets=None, timeout_s=60):
    """Probe one pack models: entry — /embeddings round-trip for api-backed
    models, load+embed for pinned local weights. Never raises."""
    started = time.monotonic()
    try:
        if "base_url" in spec:
            vec = embed_api(spec, "connectivity probe", timeout_s=timeout_s,
                            out_dir=out_dir, secrets=secrets)
            return {"ok": True, "dim": len(vec),
                    "wall_ms": int((time.monotonic() - started) * 1000),
                    "detail": "embeddings answered (dim %d)" % len(vec)}
        from . import localmodel
        weights, model_sha = localmodel.load_model(
            spec["path"], expected_sha=spec["sha256"])
        vec = localmodel.embed("connectivity probe", weights)
        return {"ok": True, "dim": len(vec),
                "wall_ms": int((time.monotonic() - started) * 1000),
                "detail": "local weights %.12s... loaded, deterministic" % model_sha}
    except Exception as e:
        return {"ok": False,
                "wall_ms": int((time.monotonic() - started) * 1000),
                "detail": "%s: %s" % (type(e).__name__, e)}
