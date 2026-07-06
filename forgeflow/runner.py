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

def _claude_cli_backend(binding, prompt, *, cwd, timeout_s, out_dir,
                        session_ref=None):
    """Agentic CLI backend. Fixed argv; the prompt travels via stdin (never
    argv — argv leaks into process listings). Returns
    (exit_code, stdout_path, stderr_path)."""
    argv = [binding.get("cli", "claude"), "-p",
            "--permission-mode", binding.get("permission_mode", "bypassPermissions"),
            "--output-format", "json"]
    if binding.get("model"):
        argv += ["--model", str(binding["model"])]
    if session_ref:
        argv += ["--resume", session_ref]
    env = {k: v for k, v in os.environ.items()
           if k in ("PATH", "HOME", "TERM", "LANG", "SHELL")}
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    prompt_file = out_dir / "prompt"          # snapshot: what was actually sent
    prompt_file.write_text(prompt)
    return run_cmd(argv, timeout_s, out_dir, cwd=cwd, env=env,
                   stdin_path=prompt_file)


BACKENDS = {"claude-cli": _claude_cli_backend}


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
    """LAST ```json fenced block, parsed and schema-validated. Raises
    ValueError/SchemaError — the caller turns that into a re-ask."""
    blocks = _FENCE_RE.findall(text or "")
    if not blocks:
        raise ValueError("no ```json fenced block in output")
    verdict = json.loads(blocks[-1])
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
                "subtype": "success"}
    return {"result": obj.get("result") or "",
            "session_id": obj.get("session_id"),
            "is_error": bool(obj.get("is_error")),
            "subtype": obj.get("subtype", "success")}


# ------------------------------------------------------------------ core

def run_agent(conn, task, binding, base_prompt, schema, *, data_dir,
              pack_rev, cwd=None, timeout_s=3600, context_slice=None,
              vault_rev=None, probe_rev=None, base_sha=None, build_id=None):
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

    ask = prompt
    session_ref = None
    exit_code = None
    last_error = None
    for round_no in range(1 + MAX_REASKS):        # bounded re-asks, same runs row
        out_dir = run_dir / ("ask%d" % round_no)
        try:
            exit_code, stdout_path, stderr_path = backend(
                binding, ask, cwd=cwd, timeout_s=timeout_s, out_dir=out_dir,
                session_ref=session_ref)
        except Exception:
            _finish(conn, run_id, exit_code, None, str(run_dir))
            raise                                  # TimeoutExpired -> engine
        envelope = _parse_envelope(stdout_path)
        if exit_code != 0:
            _finish(conn, run_id, exit_code, None, str(run_dir))
            raise RunnerError("agent_backend",
                              "CLI exited %d (stderr archived at %s)"
                              % (exit_code, stderr_path))
        if envelope["is_error"] or envelope["subtype"] != "success":
            _finish(conn, run_id, exit_code, None, str(run_dir))
            cls = ("agent_limit" if envelope["subtype"] == "error_max_turns"
                   else "agent_backend")
            raise RunnerError(cls, "CLI envelope subtype=%s" % envelope["subtype"])
        session_ref = envelope["session_id"] or session_ref
        try:
            verdict = extract_verdict(envelope["result"], schema)
        except (ValueError, SchemaError) as e:
            last_error = str(e)
            ask = ("Your previous reply did not satisfy the output contract "
                   "(%s). Reply again: end with ONE ```json fenced block "
                   "valid against the schema, and nothing after it." % e)
            continue
        _finish(conn, run_id, exit_code, verdict, str(run_dir))
        verdict["_run_id"] = run_id
        return verdict

    _finish(conn, run_id, exit_code, None, str(run_dir))
    raise RunnerError("agent_invalid_output",
                      "no schema-valid verdict after %d re-asks (%s)"
                      % (MAX_REASKS, last_error))


def _finish(conn, run_id, exit_code, verdict, output_path):
    with tx(conn):
        conn.execute(
            "UPDATE runs SET exit_code=?, verdict=?, output_path=?,"
            " finished_at=datetime('now') WHERE id=?",
            (exit_code,
             verdict.get("verdict") if isinstance(verdict, dict) else None,
             output_path, run_id))
