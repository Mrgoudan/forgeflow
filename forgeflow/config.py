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
import stat
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .util import run_cmd, template


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
    tools: dict = field(default_factory=dict)     # name -> resolved path (str)
    tool_versions: dict = field(default_factory=dict)  # name -> version line
    agents: dict = field(default_factory=dict)    # llm binding -> backend cfg
    prompts: dict = field(default_factory=dict)   # kind -> abs path (str)
    schemas: dict = field(default_factory=dict)   # name -> parsed schema dict
    models: dict = field(default_factory=dict)    # name -> {path, sha256, params}
    workspace_root: Path = None
    idle_interval_s: int = 15
    unpark_interval_s: int = 600


_PACK_KEYS = {"name", "paths", "params", "workflows", "blocks", "tools",
              "agents", "prompts", "schemas", "models", "workspace_root",
              "idle_interval_s", "unpark_interval_s"}


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

    # params: free-form, but any {paths.x} template must resolve
    try:
        params = template(doc.get("params") or {}, {"paths": paths})
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

    # models: local weights (pinned by sha256) OR an embedding API endpoint
    # (a "BERT-like" local server, any /embeddings-speaking service)
    from .util import sha256_file
    models = {}
    for mname, spec in (doc.get("models") or {}).items():
        spec = spec or {}
        if "path" in spec:
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
            models[mname] = {"base_url": str(spec["base_url"]),
                             "model": str(spec["model"]),
                             "api_key_ref": spec.get("api_key_ref"),
                             "params": spec.get("params") or {}}
        else:
            _fail("models.%s: needs either path+sha256 (local weights) or "
                  "base_url+model (embedding API)" % mname)

    agents = doc.get("agents") or {}
    for aname, acfg in agents.items():
        if not isinstance(acfg, dict) or "backend" not in acfg:
            _fail("agents.%s: needs at least 'backend:'" % aname)

    workspace_root = doc.get("workspace_root")
    if workspace_root:
        workspace_root = Path(str(workspace_root)).expanduser()
        workspace_root.mkdir(parents=True, exist_ok=True)

    return Pack(
        name=name, root=pack_dir, rev=_git_rev(pack_dir), paths=paths,
        params=params, workflow_dirs=tuple(workflow_dirs),
        block_files=tuple(block_files), tools=tools,
        tool_versions=tool_versions, agents=agents, prompts=prompts,
        schemas=schemas, models=models, workspace_root=workspace_root,
        idle_interval_s=int(doc.get("idle_interval_s", 15)),
        unpark_interval_s=int(doc.get("unpark_interval_s", 600)),
    )


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
