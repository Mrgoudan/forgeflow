"""Shared primitives. Small on purpose — every function here is a rule:

- run_cmd() is the ONLY place subprocesses are spawned. It enforces the
  timeout, archives stdout/stderr to files, and returns exit codes.
  Classification happens in blocks, from exit codes and file comparisons —
  never from parsing output prose.
- canonical_json()/payload_hash() define the idempotency identity of every
  event and task in the system.
- validate_schema() is a minimal JSON-schema checker (stdlib only) covering
  the subset the engine needs: type, properties, required, items, enum,
  additionalProperties, const.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path


# ---------------------------------------------------------------- identity

def canonical_json(obj) -> str:
    """Sorted keys, no whitespace. The canonical form used for hashing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def payload_hash(payload: dict) -> str:
    """Idempotency key half: sha256 of the canonical payload."""
    return sha256_text(canonical_json(payload))


# ---------------------------------------------------------------- processes

def run_cmd(cmd, timeout_s, out_dir, cwd=None, env=None, tools=None):
    """The single subprocess choke point.

    cmd: list of argv strings. If `tools` (a {name: resolved_path} mapping,
    normally from the pack's verified tools section) contains cmd[0], the
    name is replaced by the verified path — packs declare tools once, blocks
    reference them by name.

    Returns (exit_code, stdout_path, stderr_path). stdout/stderr are always
    archived under out_dir (audit trail; decisions use the exit code and, at
    most, whole-file comparisons). On timeout the process group is killed
    and subprocess.TimeoutExpired propagates — the engine maps it to the
    step's declared 'timeout' outcome.
    """
    if not isinstance(cmd, (list, tuple)) or not cmd:
        raise ValueError("run_cmd: cmd must be a non-empty argv list")
    cmd = [str(c) for c in cmd]
    if tools and cmd[0] in tools:
        cmd = [str(tools[cmd[0]])] + cmd[1:]
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = out_dir / "stdout"
    stderr_path = out_dir / "stderr"
    with open(stdout_path, "wb") as out, open(stderr_path, "wb") as err:
        try:
            proc = subprocess.run(
                cmd, stdout=out, stderr=err, cwd=str(cwd) if cwd else None,
                env=env, timeout=timeout_s,
            )
        except subprocess.TimeoutExpired:
            raise
    return proc.returncode, str(stdout_path), str(stderr_path)


def files_equal(a, b) -> bool:
    """Byte-exact comparison — the deterministic 'did we get what we
    expected' primitive for oracle blocks."""
    pa, pb = Path(a), Path(b)
    if not pa.is_file() or not pb.is_file():
        return False
    return pa.read_bytes() == pb.read_bytes()


# ---------------------------------------------------------------- files

def atomic_write(path, data) -> None:
    """Write-then-rename so readers never see a partial file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "wb" if isinstance(data, bytes) else "w"
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-")
    try:
        with os.fdopen(fd, mode) as f:
            f.write(data)
        os.replace(tmp, str(path))
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


_TEMPLATE_RE = re.compile(r"\{([a-zA-Z0-9_.]+)\}")


def template(value, mapping: dict, partial: bool = False):
    """Recursively substitute '{name}' / '{dotted.name}' placeholders in
    strings inside value using mapping. Unknown names are a loud error —
    a template that silently survives is a config bug waiting downstream.
    partial=True instead leaves placeholders whose ROOT key is absent from
    mapping untouched (load-time pass resolving {paths.*} while runtime
    placeholders like {payload.*} survive for the block to resolve)."""
    if isinstance(value, str):
        class _Keep(Exception):
            pass

        def resolve(key):
            cur = mapping
            if partial and key.split(".")[0] not in mapping:
                raise _Keep()
            for part in key.split("."):
                if isinstance(cur, dict) and part in cur:
                    cur = cur[part]
                else:
                    raise KeyError("unresolved template '{%s}' in %r" % (key, value))
            return cur
        full = _TEMPLATE_RE.fullmatch(value)
        if full:  # whole-string placeholder keeps its native type (ints, lists)
            try:
                return resolve(full.group(1))
            except _Keep:
                return value

        def sub(m):
            try:
                return str(resolve(m.group(1)))
            except _Keep:
                return m.group(0)
        return _TEMPLATE_RE.sub(sub, value)
    if isinstance(value, dict):
        return {k: template(v, mapping, partial) for k, v in value.items()}
    if isinstance(value, list):
        return [template(v, mapping, partial) for v in value]
    return value


# ---------------------------------------------------------------- schema

class SchemaError(ValueError):
    pass


_TYPES = {
    "object": dict, "array": list, "string": str,
    "integer": int, "number": (int, float), "boolean": bool,
    "null": type(None),
}


def validate_schema(instance, schema: dict, path: str = "$") -> None:
    """Minimal JSON-schema validation; raises SchemaError with a path."""
    if "const" in schema and instance != schema["const"]:
        raise SchemaError("%s: expected const %r, got %r" % (path, schema["const"], instance))
    if "enum" in schema and instance not in schema["enum"]:
        raise SchemaError("%s: %r not in enum %r" % (path, instance, schema["enum"]))
    t = schema.get("type")
    if t is not None:
        types = t if isinstance(t, list) else [t]
        py = tuple(_TYPES[x] for x in types if x in _TYPES)
        if isinstance(instance, bool) and "boolean" not in types:
            raise SchemaError("%s: expected %s, got boolean" % (path, types))
        if not isinstance(instance, py):
            raise SchemaError("%s: expected %s, got %s" % (path, types, type(instance).__name__))
    if isinstance(instance, dict):
        for req in schema.get("required", ()):
            if req not in instance:
                raise SchemaError("%s: missing required key '%s'" % (path, req))
        props = schema.get("properties", {})
        for k, v in instance.items():
            if k in props:
                validate_schema(v, props[k], "%s.%s" % (path, k))
            elif schema.get("additionalProperties") is False:
                raise SchemaError("%s: unexpected key '%s'" % (path, k))
    if isinstance(instance, list) and "items" in schema:
        for i, item in enumerate(instance):
            validate_schema(item, schema["items"], "%s[%d]" % (path, i))


# ---------------------------------------------------------------- sqlite tx

@contextmanager
def tx(conn, immediate: bool = True):
    """Explicit transaction; connections run in autocommit (isolation_level
    None), so this is the only way multi-statement writes group."""
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


@contextmanager
def ensure_tx(conn):
    """Join the caller's transaction if one is open, else own one. Lets
    db/queue helpers compose into the engine's step-boundary transaction."""
    if conn.in_transaction:
        yield conn
    else:
        with tx(conn):
            yield conn
