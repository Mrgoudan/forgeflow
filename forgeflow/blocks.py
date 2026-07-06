"""Building-block registry: the only Python workflows can reference.

A block is a named, tested, reusable step implementation with a DECLARED
contract: execution class, closed outcome set, the context keys it accepts,
and the params it requires. The YAML loader refuses a workflow whose step
maps outcomes a block cannot emit, omits ones it can, requests context it
doesn't accept, or misses a required param.

Block function signature: fn(ctx, task, prev) -> (outcome: str, result: dict)
- ctx: the step's params (templated) + declared context values + reserved
  engine keys (_timeout_s, _step_dir, _workspaces_dir, _tools);
- task: the claimed task row (payload decoded);
- prev: the result dict of the immediately preceding step.

Rules blocks live by:
- classify from exit codes and whole-file comparisons ONLY — never parse
  output prose for a decision;
- spawn subprocesses ONLY through util.run_cmd and let TimeoutExpired
  escape (the engine maps it to the declared 'timeout' outcome);
- db writes are STAGED (result['_staged']) and applied by the engine inside
  the step-boundary transaction — a block never commits.

The engine ships batteries included: most real workflows should be
expressible with the standard blocks below alone. Custom Python is for
genuinely new capabilities.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from .util import files_equal, run_cmd, template


@dataclass(frozen=True)
class Block:
    name: str                    # e.g. 'shell.run'
    fn: "object"
    exec_class: str              # local | llm | egress | state
    outcomes: frozenset          # closed set the fn may return
    accepts_context: frozenset = field(default_factory=frozenset)
    required_params: frozenset = field(default_factory=frozenset)
    resumable: bool = False      # default for steps using this block


_BLOCKS = {}


def block(name, exec_class, outcomes, accepts_context=(), required_params=(),
          resumable=False):
    """Decorator: register a block. Duplicate names are a startup error."""
    def wrap(fn):
        if name in _BLOCKS:
            raise SystemExit("block '%s' registered twice" % name)
        _BLOCKS[name] = Block(name, fn, exec_class, frozenset(outcomes),
                              frozenset(accepts_context),
                              frozenset(required_params), resumable)
        return fn
    return wrap


def get(name: str) -> Block:
    try:
        return _BLOCKS[name]
    except KeyError:
        raise SystemExit(
            "unknown block '%s' (registered: %s)" % (name, ", ".join(sorted(_BLOCKS))))


def all_blocks() -> dict:
    return dict(_BLOCKS)


# --------------------------------------------------------------- helpers

def _tpl(ctx, task, prev, value):
    """Resolve '{payload.x}' / '{prev.y}' placeholders in a param value."""
    return template(value, {"payload": task.get("payload") or {},
                            "prev": prev or {}, "ctx": ctx})


def _deadline(ctx):
    return time.monotonic() + ctx["_timeout_s"]


def _remaining(ctx, deadline, cmd):
    left = deadline - time.monotonic()
    if left <= 0:
        raise subprocess.TimeoutExpired(cmd, ctx["_timeout_s"])
    return left


def _run(ctx, cmd, sub, deadline, cwd=None, env=None):
    """run_cmd bound to the step's budget, artifact dir and pack tools."""
    out_dir = Path(ctx["_step_dir"]) / sub
    return run_cmd(cmd, _remaining(ctx, deadline, cmd), out_dir,
                   cwd=cwd, env=env, tools=ctx.get("_tools"))


def run_isolated(name, ctx=None, task=None, prev=None, step_dir=None,
                 timeout_s=60, tools=None):
    """BlockTest helper: run a registered block against a plain dict context
    (no engine, no db). Returns (outcome, result)."""
    import tempfile
    b = get(name)
    full = dict(ctx or {})
    full.setdefault("_timeout_s", timeout_s)
    full.setdefault("_step_dir", step_dir or tempfile.mkdtemp(prefix="blocktest-"))
    full.setdefault("_workspaces_dir", tempfile.mkdtemp(prefix="blocktest-ws-"))
    full.setdefault("_tools", dict(tools or {}))
    return b.fn(full, task or {"id": 0, "attempts": 0, "payload": {}}, prev or {})


# ------------------------------------------------------------ git blocks

@block("worktree.create", "local", {"ok", "dirty", "timeout"},
       accepts_context={"payload", "pack"}, required_params={"repo"},
       resumable=True)
def worktree_create(ctx, task, prev):
    """Dedicated git worktree per task attempt, on its own branch. Never
    operates on the source checkout. dirty = git refused (POLICY consumes)."""
    deadline = _deadline(ctx)
    repo = _tpl(ctx, task, prev, ctx["repo"])
    base = _tpl(ctx, task, prev, ctx.get("base") or "HEAD")
    ws = Path(ctx["_workspaces_dir"]) / ("task-%d-a%d" % (task["id"], task["attempts"]))
    branch = "task-%d-a%d" % (task["id"], task["attempts"])
    if (ws / ".git").exists():  # resumable re-entry: worktree survived
        return "ok", {"path": str(ws), "branch": branch, "reused": True}
    ws.parent.mkdir(parents=True, exist_ok=True)
    code, out, err = _run(ctx, ["git", "-C", repo, "worktree", "add", "-B",
                                branch, str(ws), base], "worktree-add", deadline)
    if code != 0:
        return "dirty", {"exit_code": code, "stderr_path": err}
    return "ok", {"path": str(ws), "branch": branch, "reused": False}


@block("worktree.drop", "local", {"ok", "error", "timeout"},
       accepts_context={"payload"})
def worktree_drop(ctx, task, prev):
    deadline = _deadline(ctx)
    path = ctx.get("path") or (prev or {}).get("path")
    if not path:
        raise RuntimeError("worktree.drop: no 'path' in params or prev result")
    path = _tpl(ctx, task, prev, path)
    repo = _tpl(ctx, task, prev, ctx["repo"]) if ctx.get("repo") else None
    if not Path(path).exists():
        return "ok", {"path": path, "already_gone": True}
    args = ["git"] + (["-C", repo] if repo else []) + \
           ["worktree", "remove", "--force", path]
    code, out, err = _run(ctx, args, "worktree-remove", deadline)
    if code != 0:
        return "error", {"exit_code": code, "stderr_path": err}
    return "ok", {"path": path, "already_gone": False}


@block("git.branch", "local", {"ok", "error", "timeout"},
       accepts_context={"payload"}, required_params={"repo", "branch"})
def git_branch(ctx, task, prev):
    """Create/reset a branch ref (no checkout switch)."""
    deadline = _deadline(ctx)
    repo = _tpl(ctx, task, prev, ctx["repo"])
    branch = _tpl(ctx, task, prev, ctx["branch"])
    base = _tpl(ctx, task, prev, ctx.get("base") or "HEAD")
    code, out, err = _run(ctx, ["git", "-C", repo, "branch", "-f", branch, base],
                          "branch", deadline)
    if code != 0:
        return "error", {"exit_code": code, "stderr_path": err}
    return "ok", {"branch": branch, "base": base}


@block("git.fold_commit", "local", {"ok", "nothing", "error", "timeout"},
       accepts_context={"payload"}, required_params={"repo", "base", "message"})
def git_fold_commit(ctx, task, prev):
    """Squash everything since base into exactly one commit (reset --soft +
    commit). nothing = tree identical to base."""
    deadline = _deadline(ctx)
    repo = _tpl(ctx, task, prev, ctx["repo"])
    base = _tpl(ctx, task, prev, ctx["base"])
    message = _tpl(ctx, task, prev, ctx["message"])
    code, out, err = _run(ctx, ["git", "-C", repo, "diff", "--quiet", base],
                          "diff", deadline)
    if code == 0:
        return "nothing", {"base": base}
    if code != 1:
        return "error", {"exit_code": code, "stderr_path": err}
    code, out, err = _run(ctx, ["git", "-C", repo, "reset", "--soft", base],
                          "reset", deadline)
    if code != 0:
        return "error", {"exit_code": code, "stderr_path": err}
    code, out, err = _run(ctx, ["git", "-C", repo, "commit", "-m", message],
                          "commit", deadline)
    if code != 0:
        return "error", {"exit_code": code, "stderr_path": err}
    code, out, err = _run(ctx, ["git", "-C", repo, "rev-parse", "HEAD"],
                          "rev-parse", deadline)
    head = Path(out).read_text().strip() if code == 0 else None
    return "ok", {"base": base, "head": head}


@block("git.branch_advanced", "local", {"advanced", "not_advanced", "error", "timeout"},
       accepts_context={"payload"}, required_params={"repo", "base"})
def git_branch_advanced(ctx, task, prev):
    """Did the branch actually move past base? (exit codes + rev counts —
    the deterministic 'did the agent do anything' check)."""
    deadline = _deadline(ctx)
    repo = _tpl(ctx, task, prev, ctx["repo"])
    base = _tpl(ctx, task, prev, ctx["base"])
    tip = _tpl(ctx, task, prev, ctx.get("branch") or "HEAD")
    code, out, err = _run(ctx, ["git", "-C", repo, "rev-list", "--count",
                                "%s..%s" % (base, tip)], "rev-list", deadline)
    if code != 0:
        return "error", {"exit_code": code, "stderr_path": err}
    count = int(Path(out).read_text().strip() or "0")
    return ("advanced" if count > 0 else "not_advanced"), {"commits": count}


# ---------------------------------------------------------- shell blocks

@block("shell.run", "local", {"ok", "nonzero", "mismatch", "timeout"},
       accepts_context={"payload", "pack"}, required_params={"cmd"})
def shell_run(ctx, task, prev):
    """Generic command block: classify by exit code, optionally compare an
    output file against an expected file (byte-exact)."""
    deadline = _deadline(ctx)
    cmd = [_tpl(ctx, task, prev, c) for c in ctx["cmd"]]
    cwd = _tpl(ctx, task, prev, ctx.get("cwd")) if ctx.get("cwd") else None
    env = None
    if ctx.get("env"):
        env = dict(os.environ)
        env.update({k: str(_tpl(ctx, task, prev, v))
                    for k, v in ctx["env"].items()})
    expected_exit = int(ctx.get("expected_exit", 0))
    code, out, err = _run(ctx, cmd, "cmd", deadline, cwd=cwd, env=env)
    result = {"exit_code": code, "stdout_path": out, "stderr_path": err}
    if code != expected_exit:
        return "nonzero", result
    if ctx.get("expected_file"):
        expected = _tpl(ctx, task, prev, ctx["expected_file"])
        actual = _tpl(ctx, task, prev, ctx["output_file"]) if ctx.get("output_file") else out
        result["compared"] = [actual, expected]
        if not files_equal(actual, expected):
            return "mismatch", result
    return "ok", result


@block("scan.grep_rules", "local", {"ok", "timeout"},
       accepts_context={"payload", "pack"}, required_params={"repo", "rules"})
def scan_grep_rules(ctx, task, prev):
    """No-AI finder: run pattern rules over a tree; hits become candidates.
    grep exit 0 = hits, 1 = clean, anything else = a broken rule (loud)."""
    deadline = _deadline(ctx)
    repo = _tpl(ctx, task, prev, ctx["repo"])
    candidates = []
    for rule in ctx["rules"]:
        args = ["grep", "-rnEI", "--exclude-dir=.git", rule["pattern"], "."]
        for g in rule.get("include", ()):
            args.insert(1, "--include=%s" % g)
        code, out, err = _run(ctx, args, "rule-%s" % rule["id"], deadline, cwd=repo)
        if code not in (0, 1):
            raise RuntimeError(
                "scan.grep_rules: rule '%s' failed (exit %d) — broken pattern?"
                % (rule["id"], code))
        if code == 0:
            for line in Path(out).read_text(errors="replace").splitlines():
                path, _, rest = line.partition(":")
                lineno, _, text = rest.partition(":")
                if not lineno.isdigit():
                    continue  # non-match diagnostics, never candidates
                candidates.append({"rule": rule["id"], "path": path.lstrip("./"),
                                   "line": int(lineno), "text": text[:400]})
    return "ok", {"candidates": candidates, "count": len(candidates)}


@block("oracle.reproduce", "local", {"confirmed", "refuted", "timeout"},
       accepts_context={"payload", "pack"}, required_params={"cmd"})
def oracle_reproduce(ctx, task, prev):
    """Run a repro command; classify by exit code and (optionally) an
    expected-output file comparison. confirmed iff every declared
    expectation holds. No prose ever."""
    deadline = _deadline(ctx)
    cmd = [_tpl(ctx, task, prev, c) for c in ctx["cmd"]]
    cwd = _tpl(ctx, task, prev, ctx.get("cwd")) if ctx.get("cwd") else None
    expect = ctx.get("expect") or {"exit_code": 0}
    code, out, err = _run(ctx, cmd, "repro", deadline, cwd=cwd)
    result = {"exit_code": code, "stdout_path": out, "stderr_path": err}
    ok = True
    if "exit_code" in expect and code != int(expect["exit_code"]):
        ok = False
    if expect.get("exit_nonzero") and code == 0:
        ok = False
    if expect.get("expected_file"):
        expected = _tpl(ctx, task, prev, expect["expected_file"])
        actual = _tpl(ctx, task, prev, expect["output_file"]) if expect.get("output_file") else out
        result["compared"] = [actual, expected]
        if not files_equal(actual, expected):
            ok = False
    return ("confirmed" if ok else "refuted"), result


@block("evidence.suite", "local", {"green", "red_retryable", "red", "timeout"},
       accepts_context={"payload", "pack"}, required_params={"checks"})
def evidence_suite(ctx, task, prev):
    """The evidence gate: run configured verify commands in order, classify
    by exit codes ONLY. First failure decides: red_retryable if its exit
    code is in that check's retryable_exits, else red."""
    deadline = _deadline(ctx)
    ran = []
    for check in ctx["checks"]:
        cmd = [_tpl(ctx, task, prev, c) for c in check["cmd"]]
        cwd = _tpl(ctx, task, prev, check.get("cwd") or ctx.get("cwd")) \
            if (check.get("cwd") or ctx.get("cwd")) else None
        code, out, err = _run(ctx, cmd, "check-%s" % check["name"], deadline, cwd=cwd)
        ran.append({"name": check["name"], "exit_code": code,
                    "stdout_path": out, "stderr_path": err})
        if code != 0:
            outcome = ("red_retryable"
                       if code in check.get("retryable_exits", ()) else "red")
            return outcome, {"checks": ran, "failed": check["name"]}
    return "green", {"checks": ran, "failed": None}


# -------------------------------------------------------------- llm block

@block("agent.run", "llm",
       {"agent_limit", "agent_invalid", "agent_backend", "timeout"},
       accepts_context={"payload", "pack", "lessons", "readings", "chains",
                        "notes", "diff", "finding"})
def agent_run(ctx, task, prev):
    """THE llm block — delegates to runner.run_agent(), the only path to
    any model. The step's schema enums extend this block's outcome set at
    load time (loader), so 'the model said something weird' can only ever
    surface as agent_invalid. A new task attempt is a new runs row; agent
    steps are never resumable."""
    from . import runner
    step = ctx["_step"]
    pack = ctx["_pack"]
    if pack is None:
        raise RuntimeError("agent.run needs a pack (agents/prompts/schemas)")
    binding = pack.agents[step.llm]
    schema = pack.schemas[step.schema]
    base_prompt = Path(pack.prompts[step.llm]).read_text()
    declared = {name for name, _ in step.context}
    context_slice = {k: ctx[k] for k in declared if k in ctx}
    cwd = ctx.get("cwd") or (prev or {}).get("path")
    if cwd:
        cwd = _tpl(ctx, task, prev, cwd)
    try:
        verdict = runner.run_agent(
            ctx["_conn"], task, binding, base_prompt, schema,
            data_dir=ctx["_data_dir"], pack_rev=pack.rev, cwd=cwd,
            timeout_s=ctx["_timeout_s"], context_slice=context_slice,
            base_sha=(prev or {}).get("base_sha"))
    except runner.RunnerError as e:
        outcome = ("agent_invalid" if e.error_class == "agent_invalid_output"
                   else e.error_class)
        return outcome, {"error": str(e)}
    return verdict["verdict"], verdict


# ------------------------------------------------------------ state blocks

@block("db.upsert_finding", "state", {"ok"},
       accepts_context={"payload"}, required_params={"key", "title", "source", "repo"})
def db_upsert_finding(ctx, task, prev):
    """Stage a finding upsert; the engine applies it at the step boundary
    and merges the finding_id into this step's persisted result."""
    op = {"op": "upsert_finding"}
    for f in ("key", "title", "source", "repo", "detail", "severity",
              "pattern", "base_sha"):
        if ctx.get(f) is not None:
            op[f] = _tpl(ctx, task, prev, ctx[f])
    return "ok", {"_staged": [op]}


@block("db.transition", "state", {"ok"},
       accepts_context={"payload"}, required_params={"to_state", "event"})
def db_transition(ctx, task, prev):
    """Stage the ONE finding state change this step is allowed to make.
    Applied via db.record_transition inside the boundary transaction —
    event fan-out to subscribed workflows is atomic with the step."""
    finding_id = ctx.get("finding_id")
    if finding_id is None:
        finding_id = (prev or {}).get("finding_id") \
            or (task.get("payload") or {}).get("finding_id")
    else:
        finding_id = _tpl(ctx, task, prev, finding_id)
    if finding_id is None:
        raise RuntimeError("db.transition: no finding_id in params, prev, or payload")
    evidence = ctx.get("evidence")
    if evidence is not None:
        evidence = _tpl(ctx, task, prev, evidence)
    return "ok", {"_staged": [{
        "op": "transition", "finding_id": int(finding_id),
        "to_state": _tpl(ctx, task, prev, ctx["to_state"]),
        "event": _tpl(ctx, task, prev, ctx["event"]),
        "evidence": evidence,
    }]}
