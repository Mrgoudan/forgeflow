#!/usr/bin/env python3
"""Fake agentic CLI for runner tests. Reads the prompt from stdin, records
argv, and answers per the mode file next to it. Emits the same structured
JSON envelope the real CLI produces with --output-format json."""
import json
import os
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def envelope(result, subtype="success", is_error=False):
    return json.dumps({"type": "result", "subtype": subtype,
                       "is_error": is_error, "session_id": "sess-1",
                       "result": result})


def main():
    mode = open(os.path.join(HERE, "mode")).read().strip()
    calls_path = os.path.join(HERE, "calls")
    n = int(open(calls_path).read()) if os.path.exists(calls_path) else 0
    n += 1
    open(calls_path, "w").write(str(n))
    open(os.path.join(HERE, "stdin.%d" % n), "w").write(sys.stdin.read())
    open(os.path.join(HERE, "argv.%d" % n), "w").write("\n".join(sys.argv[1:]))

    good = "did things.\n```json\n{\"verdict\": \"FIXED\"}\n```"
    if mode == "good":
        print(envelope(good))
    elif mode == "checkdb":
        db_path = open(os.path.join(HERE, "dbpath")).read().strip()
        conn = sqlite3.connect(db_path)
        runs = conn.execute("SELECT count(*) FROM runs").fetchone()[0]
        print(envelope("ok\n```json\n{\"verdict\": \"FIXED\", "
                       "\"runs_at_exec\": %d}\n```" % runs))
    elif mode == "invalid_then_good":
        print(envelope("no fenced block here" if n == 1 else good))
    elif mode == "always_invalid":
        print(envelope("```json\n{\"verdict\": \"SPARKLES\"}\n```"))
    elif mode == "fail":
        sys.stderr.write("transport exploded\n")
        sys.exit(3)
    elif mode == "max_turns":
        print(envelope("", subtype="error_max_turns", is_error=True))
    elif mode == "sleep":
        time.sleep(30)
    else:
        sys.exit(64)


if __name__ == "__main__":
    main()
