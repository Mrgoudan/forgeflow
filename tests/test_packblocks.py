from __future__ import annotations

import unittest

from helpers import tmpdir

from forgeflow import config, db, queue

import itertools

_SEQ = itertools.count()

# Each test writes its own pack copy; the registry is process-global and a
# duplicate name from a DIFFERENT file is (correctly) a startup error — so
# every test stamps a unique block name into its pack.
CUSTOM_BLOCK = """\
from forgeflow.blocks import block


@block("%(name)s", "local", {"ok"})
def stamp(ctx, task, prev):
    return "ok", {"stamp": "made-by-pack-block",
                  "key": (task.get("payload") or {}).get("key")}
"""

PRODUCER = """\
workflow: producer
emits: [custom.handoff]
steps:
  - name: make
    block: %(name)s
    timeout_s: 10
    outcomes: { ok: hand }
  - name: hand
    block: event.emit
    timeout_s: 10
    params: { name: custom.handoff, data: { key: "{payload.key}" } }
    outcomes: { ok: done }
"""

CONSUMER = """\
workflow: consumer
consumes: [custom.handoff]
steps:
  - name: touch
    block: shell.run
    timeout_s: 30
    params: { cmd: ["touch", "{paths.outbox}/got-{payload.key}"] }
    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed }
"""


class PackBlocksTest(unittest.TestCase):
    """Layer 2 ships CODE: a pack-local block module, loaded at startup,
    immediately usable from workflow YAML — no engine changes."""

    def setUp(self):
        self.dir = tmpdir()
        self.outbox = self.dir / "outbox"
        self.outbox.mkdir()
        pack = self.dir / "pack"
        (pack / "workflows").mkdir(parents=True)
        (pack / "blocks").mkdir()
        self.block_name = "demo.stamp_%d" % next(_SEQ)
        subst = {"name": self.block_name}
        (pack / "blocks" / "custom.py").write_text(CUSTOM_BLOCK % subst)
        (pack / "workflows" / "producer.yaml").write_text(PRODUCER % subst)
        (pack / "workflows" / "consumer.yaml").write_text(CONSUMER)
        (pack / "project.yaml").write_text(
            "name: packblocks\n"
            "paths: { outbox: %s }\n"
            "workflows: [workflows]\n"
            "blocks: [blocks/custom.py]\n" % self.outbox)
        self.pack_dir = pack

    def test_pack_block_and_event_chain(self):
        from forgeflow import engine
        eng = engine.Engine(self.dir / "ff", pack=config.load_pack(self.pack_dir))
        # the loader wired consumer to the custom event
        self.assertEqual(eng.subscriptions, {"custom.handoff": ["consumer"]})
        queue.enqueue(eng.conn, "producer", {"key": "k7"})
        self.assertEqual(eng.run_until_idle(), 2)
        tasks = eng.conn.execute("SELECT kind, state FROM tasks ORDER BY id").fetchall()
        self.assertEqual([(t["kind"], t["state"]) for t in tasks],
                         [("producer", "done"), ("consumer", "done")])
        # the event carried the payload; the consumer really ran
        self.assertTrue((self.outbox / "got-k7").exists())
        ev = eng.conn.execute(
            "SELECT * FROM events WHERE name='custom.handoff'").fetchone()
        self.assertIsNotNone(ev)
        # replay: re-running the producer path can't double-trigger consumer
        db.emit_event(eng.conn, "custom.handoff", {"key": "k7"},
                      eng.subscriptions)
        self.assertEqual(eng.run_until_idle(), 0)

    def test_second_engine_same_pack_no_duplicate_registration(self):
        from forgeflow import engine
        engine.Engine(self.dir / "ff", pack=config.load_pack(self.pack_dir))
        engine.Engine(self.dir / "ff2", pack=config.load_pack(self.pack_dir))

    def test_missing_blocks_file_refused(self):
        (self.pack_dir / "project.yaml").write_text(
            "name: p\nblocks: [blocks/ghost.py]\n")
        with self.assertRaisesRegex(SystemExit, "ghost.py does not exist"):
            config.load_pack(self.pack_dir)

    def test_undeclared_emit_refused(self):
        (self.pack_dir / "workflows" / "producer.yaml").write_text(
            (PRODUCER % {"name": self.block_name})
            .replace("emits: [custom.handoff]", "emits: []"))
        from forgeflow import engine
        with self.assertRaisesRegex(SystemExit, "no undeclared emits"):
            engine.Engine(self.dir / "ff", pack=config.load_pack(self.pack_dir))

    def test_malformed_emit_name_refused(self):
        (self.pack_dir / "workflows" / "producer.yaml").write_text(
            (PRODUCER % {"name": self.block_name})
            .replace("custom.handoff", "NotAnEvent")
            .replace("emits: [NotAnEvent]", "emits: [custom.handoff]"))
        from forgeflow import engine
        with self.assertRaisesRegex(SystemExit, "malformed event name"):
            engine.Engine(self.dir / "ff", pack=config.load_pack(self.pack_dir))


if __name__ == "__main__":
    unittest.main()
