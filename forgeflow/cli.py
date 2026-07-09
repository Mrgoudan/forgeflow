"""The operational front door. Orchestration itself is not here — it is
the claim loop plus the event bus (subscriptions), which already runs
every enabled workflow. This is just how an operator drives and inspects
it:

    forgeflow validate --pack P            prove config + all workflows load;
                                           print the orchestration map
    forgeflow run --pack P --root R        the daemon (flock'd, forever)
    forgeflow once --pack P --root R       drain the queue, then exit (cron)
    forgeflow emit NAME --data JSON        inject an event; --drive runs the
                                           resulting task tree to idle
    forgeflow status --root R              tasks / items / parked / events
    forgeflow unpark [ID]                  release parked task(s)

Per ENGINE.md: one-shot commands (emit --drive, once) run WITHOUT the
daemon lock — they use the same claim/execute code paths, and WAL +
BEGIN IMMEDIATE make that safe next to a live daemon.
"""
from __future__ import annotations

import argparse
import json
import sys

from . import config, db, engine, queue


def _build_engine(args):
    pack = config.load_pack(args.pack) if args.pack else None
    return engine.Engine(args.root, pack=pack)


def cmd_validate(args):
    eng = _build_engine(args)
    print("pack: %s" % (eng.pack.name if eng.pack else "(none)"))
    print("workflows (%d):" % len(eng.workflows))
    for kind in sorted(eng.workflows):
        wf = eng.workflows[kind]
        steps = " -> ".join(s.name for s in wf.steps)
        print("  %-16s steps: %s" % (kind, steps))
        if wf.consumes:
            print("  %16s consumes: %s" % ("", ", ".join(wf.consumes)))
        if wf.emits:
            print("  %16s emits:    %s" % ("", ", ".join(wf.emits)))
    print("orchestration map (event -> consumers):")
    if not eng.subscriptions:
        print("  (no subscriptions)")
    for ev in sorted(eng.subscriptions):
        print("  %-28s -> %s" % (ev, ", ".join(eng.subscriptions[ev])))
    print("OK: every workflow is total, every reference resolves")
    return 0


def cmd_run(args):
    return _build_engine(args).run() or 0


def cmd_once(args):
    n = _build_engine(args).run_until_idle()
    print("executed %d task(s)" % n)
    return 0


def cmd_emit(args):
    eng = _build_engine(args)
    payload = json.loads(args.data)
    if not isinstance(payload, dict):
        print("--data must be a JSON object", file=sys.stderr)
        return 2
    event_id = db.emit_event(eng.conn, args.name, payload, eng.subscriptions)
    kinds = eng.subscriptions.get(args.name, [])
    print("event %d: %s -> %s" % (event_id, args.name,
                                  ", ".join(kinds) if kinds else "(no consumers)"))
    if args.drive:
        n = eng.run_until_idle()
        print("executed %d task(s)" % n)
    return 0


def cmd_status(args):
    eng = _build_engine(args)
    conn = eng.conn
    print("tasks:")
    for r in conn.execute("SELECT state, kind, count(*) c FROM tasks"
                          " GROUP BY state, kind ORDER BY state, kind"):
        print("  %-12s %-16s %d" % (r["state"], r["kind"], r["c"]))
    parked = conn.execute("SELECT id, kind, park_reason, attempts FROM tasks"
                          " WHERE state='parked' ORDER BY id").fetchall()
    if parked:
        print("parked:")
        for r in parked:
            print("  #%d %s reason=%s attempts=%d"
                  % (r["id"], r["kind"], r["park_reason"], r["attempts"]))
    print("items:")
    for r in conn.execute("SELECT state, count(*) c FROM items"
                          " GROUP BY state ORDER BY state"):
        print("  %-12s %d" % (r["state"], r["c"]))
    print("recent events:")
    for r in conn.execute("SELECT id, name, at FROM events"
                          " ORDER BY id DESC LIMIT %d" % int(args.limit)):
        print("  %d  %s  %s" % (r["id"], r["at"], r["name"]))
    return 0


def cmd_unpark(args):
    eng = _build_engine(args)
    n = queue.unpark(eng.conn, args.task_id)
    print("unparked %d task(s)" % n)
    return 0


def cmd_trace(args):
    """The full story of one task, straight from the db: what event created
    it, every step boundary, every model run, what it emitted, and which
    tasks those emissions created. Reads only — safe next to a daemon."""
    import json as _json

    from pathlib import Path

    from .util import payload_hash
    conn = db.connect(Path(args.root) / "state" / "forgeflow.db")
    task = conn.execute("SELECT * FROM tasks WHERE id=?",
                        (args.task_id,)).fetchone()
    if task is None:
        print("no task %d" % args.task_id)
        return 1
    payload = _json.loads(task["payload"])
    print("task %d  kind=%s  state=%s  attempts=%d%s"
          % (task["id"], task["kind"], task["state"], task["attempts"],
             "  error_class=%s" % task["error_class"] if task["error_class"] else ""))
    print("  payload: %s" % _json.dumps(payload, sort_keys=True))

    # origin: a task enqueued via the event bus carries its event name
    ev_name = payload.get("event")
    if ev_name:
        from .util import canonical_json
        ev_payload = {k: v for k, v in payload.items() if k != "event"}
        ev = conn.execute(
            "SELECT * FROM events WHERE name=? AND payload=? ORDER BY id LIMIT 1",
            (ev_name, canonical_json(ev_payload))).fetchone()
        if ev:
            print("  created by event %d: %s  at %s" % (ev["id"], ev["name"], ev["at"]))

    emitted = []
    print("steps:")
    for s in conn.execute(
            "SELECT * FROM task_steps WHERE task_id=? ORDER BY rowid",
            (task["id"],)):
        result = _json.loads(s["result"] or "{}")
        brief = _json.dumps(result, sort_keys=True)
        if len(brief) > 100:
            brief = brief[:100] + "..."
        print("  a%d %-14s -> %-14s %5sms  %s"
              % (s["attempt"], s["step"], s["outcome"], s["wall_ms"], brief))
        if "event_id" in result:
            emitted.append(("event", result["event_id"]))
        if "transition_id" in result:
            emitted.append(("transition", result["transition_id"]))

    for r in conn.execute("SELECT * FROM runs WHERE task_id=?", (task["id"],)):
        print("run %d: model=%s exit=%s verdict=%s prompt_sha=%s..."
              % (r["id"], r["model"], r["exit_code"], r["verdict"],
                 r["prompt_sha"][:12]))

    for kind, ref in emitted:
        if kind == "transition":
            t = conn.execute("SELECT * FROM transitions WHERE id=?", (ref,)).fetchone()
            if t:
                print("transition %d: item %d %s -> %s (%s)"
                      % (t["id"], t["item_id"], t["from_state"],
                         t["to_state"], t["event"]))
            ev = conn.execute(
                "SELECT * FROM events WHERE payload LIKE ? ORDER BY id LIMIT 1",
                ('%%"transition_id":%d%%' % ref,)).fetchone()
        else:
            ev = conn.execute("SELECT * FROM events WHERE id=?", (ref,)).fetchone()
        if not ev:
            continue
        print("emitted event %d: %s  %s" % (ev["id"], ev["name"], ev["payload"]))
        # follow-on tasks: reconstruct the exact enqueue identity
        child_payload = _json.loads(ev["payload"])
        child_payload["event"] = ev["name"]
        h = payload_hash(child_payload)
        for child in conn.execute(
                "SELECT id, kind, state FROM tasks WHERE payload_hash=?", (h,)):
            print("  -> task %d  kind=%s  state=%s   (trace %d to continue)"
                  % (child["id"], child["kind"], child["state"], child["id"]))
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="forgeflow", description=__doc__.split("\n")[0])
    p.add_argument("--root", default=".",
                   help="state root (holds state/, data/, workspaces/)")
    p.add_argument("--pack", default=None, help="pack directory (project.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("validate", help="load everything, print the orchestration map")
    sub.add_parser("run", help="daemon loop (flock'd, one per state root)")
    sub.add_parser("once", help="drain eligible tasks, then exit")

    pe = sub.add_parser("emit", help="inject an event through subscriptions")
    pe.add_argument("name")
    pe.add_argument("--data", default="{}", help="JSON object payload")
    pe.add_argument("--drive", action="store_true",
                    help="then drive the claim loop until idle (one-shot mode)")

    ps = sub.add_parser("status", help="tasks / items / parked / events")
    ps.add_argument("--limit", default=10, help="recent events to show")

    pu = sub.add_parser("unpark", help="parked -> pending (all, or one id)")
    pu.add_argument("task_id", nargs="?", type=int, default=None)

    pt = sub.add_parser("trace", help="one task's full story from the db")
    pt.add_argument("task_id", type=int)

    args = p.parse_args(argv)
    return {"validate": cmd_validate, "run": cmd_run, "once": cmd_once,
            "emit": cmd_emit, "status": cmd_status,
            "unpark": cmd_unpark, "trace": cmd_trace}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
