"""Timed triggers: 'every' parsing, config validation, the no-consumer
startup guard, and the tick's exactly-once-per-window semantics (including
across a daemon restart — the cursor is persisted)."""
from __future__ import annotations

import unittest

from helpers import make_engine, make_pack, make_target_repo, tmpdir

from forgeflow import config

TICK_WF = (
    "workflow: schedtest\n"
    "consumes: [schedtest.tick]\n"
    "steps:\n"
    "  - name: noop\n"
    "    block: shell.run\n"
    "    timeout_s: 10\n"
    "    params: { cmd: [\"true\"] }\n"
    "    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed }\n")


class ParseEveryTest(unittest.TestCase):
    def test_units(self):
        self.assertEqual(config.parse_every(90), 90)
        self.assertEqual(config.parse_every("30s"), 30)
        self.assertEqual(config.parse_every("5m"), 300)
        self.assertEqual(config.parse_every("6h"), 21600)
        self.assertEqual(config.parse_every("1d"), 86400)

    def test_rejects(self):
        for bad in (0, -5, True, "0s", "5x", "m5", "", None, 1.5):
            self.assertIsNone(config.parse_every(bad), repr(bad))


class ScheduleConfigTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        self.repo = make_target_repo(self.base)

    def _load(self, extra):
        return config.load_pack(make_pack(self.base, self.repo, extra=extra))

    def test_valid_schedule_parses(self):
        pack = self._load("schedule:\n"
                          "  - { event: item.triaged, every: 5m,"
                          " data: { source: timer } }\n")
        self.assertEqual(pack.schedule,
                         ({"event": "item.triaged", "every_s": 300,
                           "data": {"source": "timer"}},))

    def test_rejects_malformed(self):
        for extra in (
                "schedule: { event: a.b, every: 5m }\n",          # not a list
                "schedule:\n  - { event: BAD, every: 5m }\n",     # event name
                "schedule:\n  - { event: a.b, every: nope }\n",   # every
                "schedule:\n  - { event: a.b, every: 5m, x: 1 }\n",  # unknown key
                "schedule:\n  - { event: a.b, every: 5m, data: [1] }\n"):  # data
            with self.assertRaises(SystemExit, msg=extra):
                self._load(extra)


class ScheduleTickTest(unittest.TestCase):
    def setUp(self):
        self.base = tmpdir()
        repo = make_target_repo(self.base)
        wf_dir = self.base / "wf"
        wf_dir.mkdir()
        (wf_dir / "tick.yaml").write_text(TICK_WF)
        self.pack_dir = make_pack(
            self.base, repo, workflows_dir=wf_dir,
            extra="schedule:\n  - { event: schedtest.tick, every: 5m }\n")

    def test_no_consumer_is_a_startup_error(self):
        base2 = tmpdir()
        repo2 = make_target_repo(base2)
        pack_dir = make_pack(          # demo workflows: nobody consumes the tick
            base2, repo2,
            extra="schedule:\n  - { event: schedtest.tick, every: 5m }\n")
        with self.assertRaises(SystemExit):
            make_engine(base2, pack_dir=pack_dir)

    def test_once_per_window_and_restart_safe(self):
        eng = make_engine(self.base, pack_dir=self.pack_dir)
        n_events = lambda c: c.execute(
            "SELECT count(*) FROM events WHERE name='schedtest.tick'").fetchone()[0]

        self.assertEqual(eng._schedule_tick(now=1000), 1)   # first sight fires
        self.assertEqual(eng._schedule_tick(now=1001), 0)   # same window (900)
        self.assertEqual(eng._schedule_tick(now=1199), 0)   # still window 900
        self.assertEqual(eng._schedule_tick(now=1200), 1)   # window 1200
        self.assertEqual(n_events(eng.conn), 2)

        # payload carries the window start; the spawned task consumed it
        ev = eng.conn.execute("SELECT payload FROM events WHERE"
                              " name='schedtest.tick' ORDER BY id").fetchone()
        self.assertIn('"schedule_occurrence":900', ev["payload"])
        self.assertEqual(eng.run_until_idle(), 2)           # both windows ran

        # restart: the cursor survives — the same window never re-fires
        eng2 = make_engine(self.base, pack_dir=self.pack_dir)
        self.assertEqual(eng2._schedule_tick(now=1201), 0)
        self.assertEqual(eng2._schedule_tick(now=1500), 1)  # new window fires
        # a long outage skips missed windows: only the current one fires
        self.assertEqual(eng2._schedule_tick(now=99999), 1)
        self.assertEqual(n_events(eng2.conn), 4)


if __name__ == "__main__":
    unittest.main()
