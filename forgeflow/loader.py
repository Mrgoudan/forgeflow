"""YAML workflow loader: compiles defs into contract.Workflow, validating
every WORKFLOWS.md guarantee at startup — none at runtime.

Per step, before the daemon accepts work:
  1. the named block exists in the blocks registry;
  2. mapped outcomes == the block's declared outcomes EXACTLY — an unmapped
     outcome and a phantom outcome are equally fatal;
  3. every declared context key is accepted by the block AND resolves in
     the context-provider registry;
  4. every required param is present (after load-time path templating);
  5. llm: bindings resolve in the pack's agent section and only llm-class
     blocks may carry one (and they must);
  6. consumes/emits are well-formed event names; finding.* events must
     name a real state; a db.transition step's implied emit must be listed;
  7. the compiled workflow passes contract validation (total, bounded,
     terminal).

Interaction wiring: subscriptions() = {event: [workflow kinds]} from every
consumes list. db.record_transition consults it — in the same transaction —
to enqueue consuming tasks. Workflows never call each other.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

from . import blocks as blocks_mod
from .contract import CONTEXT_PROVIDERS, Workflow, WorkflowError
from .db import FINDING_STATES
from .util import template

_EVENT_RE = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)+$")

_STEP_KEYS = {"name", "block", "timeout_s", "params", "context", "outcomes",
              "max_visits", "resumable", "llm", "schema"}
_DOC_KEYS = {"workflow", "consumes", "emits", "steps"}


def _die(where, msg):
    raise WorkflowError("%s: %s" % (where, msg))


def load_workflow_file(path, pack=None) -> Workflow:
    path = Path(path)
    with open(path) as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        _die(path, "not a mapping")
    unknown = set(doc) - _DOC_KEYS
    if unknown:
        _die(path, "unknown top-level keys %s" % sorted(unknown))
    kind = doc.get("workflow")
    if not kind or not isinstance(kind, str):
        _die(path, "missing 'workflow: <kind>'")
    where = "%s (workflow '%s')" % (path.name, kind)

    consumes = doc.get("consumes") or []
    emits = doc.get("emits") or []
    for ev in list(consumes) + list(emits):
        if not isinstance(ev, str) or not _EVENT_RE.match(ev):
            _die(where, "malformed event name %r (want e.g. 'finding.triaged')" % ev)
        if ev.startswith("finding."):
            state = ev.split(".", 1)[1]
            if state not in FINDING_STATES:
                _die(where, "event %r names unknown finding state '%s' (known: %s)"
                     % (ev, state, ", ".join(sorted(FINDING_STATES))))

    steps = doc.get("steps")
    if not steps or not isinstance(steps, list):
        _die(where, "workflow needs a non-empty 'steps' list")

    # load-time templating: resolve pack paths/params now; runtime
    # placeholders ({payload.*}, {prev.*}, {ctx.*}) survive for the block.
    load_map = {}
    if pack is not None:
        load_map = {"paths": dict(pack.paths), "pack": dict(pack.params)}

    wf = Workflow.define(kind)
    wf.consumes = list(consumes)
    wf.emits = list(emits)

    for i, s in enumerate(steps):
        swhere = "%s step[%d]" % (where, i)
        if not isinstance(s, dict):
            _die(swhere, "step must be a mapping")
        unknown = set(s) - _STEP_KEYS
        if unknown:
            _die(swhere, "unknown step keys %s" % sorted(unknown))
        name = s.get("name")
        if not name:
            _die(swhere, "step needs a name")
        swhere = "%s step '%s'" % (where, name)
        if "block" not in s:
            _die(swhere, "step names no block")
        try:
            blk = blocks_mod.get(s["block"])
        except SystemExit as e:
            _die(swhere, str(e))
        if not isinstance(s.get("timeout_s"), int) or s["timeout_s"] <= 0:
            _die(swhere, "timeout_s (positive integer) is required — every step is bounded")

        # llm steps: binding, prompt and schema must resolve; the schema's
        # verdict enum EXTENDS the block's outcome set for this step.
        llm = s.get("llm")
        step_outcomes = set(blk.outcomes)
        if blk.exec_class == "llm":
            if not llm:
                _die(swhere, "block '%s' is llm-class: an 'llm:' binding is required"
                     % blk.name)
            agents = getattr(pack, "agents", None) or {}
            if llm not in agents:
                _die(swhere, "llm binding '%s' not found in pack agent section "
                     "(defined: %s)" % (llm, sorted(agents) or "none"))
            prompts = getattr(pack, "prompts", None) or {}
            if llm not in prompts:
                _die(swhere, "no pack prompt for llm binding '%s'" % llm)
            schema_name = s.get("schema")
            schemas = getattr(pack, "schemas", None) or {}
            if not schema_name or schema_name not in schemas:
                _die(swhere, "llm step needs 'schema:' resolving in pack schemas "
                     "(defined: %s)" % (sorted(schemas) or "none"))
            enum = (schemas[schema_name].get("properties", {})
                    .get("verdict", {}).get("enum"))
            if not enum:
                _die(swhere, "schema '%s' must declare properties.verdict.enum — "
                     "that enum IS the step's success outcome set" % schema_name)
            overlap = set(enum) & step_outcomes
            if overlap:
                _die(swhere, "schema enum values %s collide with framework "
                     "outcomes" % sorted(overlap))
            step_outcomes |= set(enum)
        elif llm:
            _die(swhere, "block '%s' is %s-class: it cannot carry an 'llm:' binding"
                 % (blk.name, blk.exec_class))

        # outcomes: exact set equality with the step's effective set
        outcomes = s.get("outcomes")
        if not isinstance(outcomes, dict) or not outcomes:
            _die(swhere, "step needs an 'outcomes: {outcome: target}' mapping")
        declared = step_outcomes
        mapped = set(outcomes)
        if mapped != declared:
            missing = declared - mapped
            phantom = mapped - declared
            parts = []
            if missing:
                parts.append("unmapped outcomes %s" % sorted(missing))
            if phantom:
                parts.append("phantom outcomes %s (block '%s' declares %s)"
                             % (sorted(phantom), blk.name, sorted(declared)))
            _die(swhere, "; ".join(parts))

        # context: accepted by the block, known to the provider registry
        context = []
        for entry in s.get("context") or []:
            if isinstance(entry, str):
                cname, spec = entry, {}
            elif isinstance(entry, dict) and len(entry) == 1:
                cname, spec = next(iter(entry.items()))
                spec = spec or {}
            else:
                _die(swhere, "malformed context entry %r" % entry)
            if cname not in blk.accepts_context:
                _die(swhere, "context '%s' not accepted by block '%s' (accepts: %s)"
                     % (cname, blk.name, sorted(blk.accepts_context) or "nothing"))
            if cname not in CONTEXT_PROVIDERS:
                _die(swhere, "context '%s' has no registered provider (known: %s)"
                     % (cname, sorted(CONTEXT_PROVIDERS)))
            try:
                spec = template(spec, load_map, partial=True)
            except KeyError as e:
                _die(swhere, "context '%s' spec templating failed: %s" % (cname, e))
            check = getattr(CONTEXT_PROVIDERS[cname], "check_spec", None)
            if check:
                err = check(spec, pack)
                if err:
                    _die(swhere, "context '%s': %s" % (cname, err))
            context.append((cname, spec))

        # params: templated against the pack now, required set enforced
        try:
            params = template(s.get("params") or {}, load_map, partial=True)
        except KeyError as e:
            _die(swhere, "param templating failed: %s" % e)
        missing_params = set(blk.required_params) - set(params)
        if missing_params:
            _die(swhere, "block '%s' requires params %s"
                 % (blk.name, sorted(missing_params)))

        # db.transition steps: the transition they stage is an EMIT — it
        # must be declared, and the target state must exist.
        if blk.name == "db.transition":
            to_state = params.get("to_state")
            if isinstance(to_state, str) and "{" not in to_state:
                if to_state not in FINDING_STATES:
                    _die(swhere, "to_state '%s' is not a finding state" % to_state)
                implied = "finding." + to_state
                if implied not in emits:
                    _die(swhere, "stages transition to '%s' but 'emits:' does not "
                         "declare '%s' — no undeclared emits" % (to_state, implied))

        wf.step(name, blk, timeout_s=s["timeout_s"], params=params,
                context=tuple(context),
                max_visits=s.get("max_visits", 3),
                resumable=s.get("resumable"), llm=llm, schema=s.get("schema"),
                outcomes=step_outcomes)
        for outcome, target in outcomes.items():
            wf.on(name, outcome, target)

    wf.validate()
    return wf


def load_defs(dirs, pack=None) -> dict:
    """Parse every *.yaml under the given dirs; return {kind: Workflow}.
    Duplicate kinds and any validation violation are startup errors."""
    workflows = {}
    for d in dirs:
        d = Path(d)
        if not d.is_dir():
            raise WorkflowError("workflow dir %s does not exist" % d)
        for path in sorted(d.glob("*.yaml")):
            wf = load_workflow_file(path, pack=pack)
            if wf.kind in workflows:
                raise WorkflowError(
                    "workflow kind '%s' defined twice (second: %s)" % (wf.kind, path))
            workflows[wf.kind] = wf
    return workflows


def subscriptions(workflows: dict) -> dict:
    """{event_name: [workflow_kind, ...]} from consumes lists, stable order."""
    subs = {}
    for kind in sorted(workflows):
        for ev in workflows[kind].consumes:
            subs.setdefault(ev, [])
            if kind not in subs[ev]:
                subs[ev].append(kind)
    return subs
