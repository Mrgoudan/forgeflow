from __future__ import annotations

import unittest
from types import SimpleNamespace

from helpers import REPO_ROOT, tmpdir

from forgeflow import loader
from forgeflow.blocks import block


@block("test.oddctx", "local", {"ok"}, accepts_context={"weird"})
def _oddctx(ctx, task, prev):
    return "ok", {}


GOOD = """\
workflow: demo_wf
consumes: [demo.go]
emits: [item.triaged]
steps:
  - name: only
    block: db.transition
    timeout_s: 10
    params: { item_id: "{payload.item_id}", to_state: triaged, event: "e:x" }
    outcomes: { ok: done }
"""


def fake_pack(**kw):
    return SimpleNamespace(paths=kw.get("paths", {}), params={}, agents=kw.get("agents", {}))


class LoaderTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()

    def _write(self, name, text):
        p = self.dir / name
        p.write_text(text)
        return p

    def test_good_yaml_loads(self):
        p = self._write("w.yaml", GOOD)
        wf = loader.load_workflow_file(p)
        self.assertEqual(wf.kind, "demo_wf")
        self.assertEqual(wf.consumes, ["demo.go"])

    def test_one_unmapped_outcome_refused(self):
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: probe
    block: shell.run
    timeout_s: 10
    params: { cmd: [true] }
    outcomes: { ok: done, nonzero: failed, mismatch: failed }
""")
        with self.assertRaisesRegex(SystemExit,
                                    r"step 'probe'.*unmapped outcomes \['timeout'\]"):
            loader.load_workflow_file(p)

    def test_phantom_outcome_refused(self):
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: probe
    block: shell.run
    timeout_s: 10
    params: { cmd: [true] }
    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed,
                rainbows: done }
""")
        with self.assertRaisesRegex(SystemExit, "phantom outcomes.*rainbows"):
            loader.load_workflow_file(p)

    def test_unknown_block_refused(self):
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: a
    block: no.such.block
    timeout_s: 10
    outcomes: { ok: done }
""")
        with self.assertRaisesRegex(SystemExit, "unknown block 'no.such.block'"):
            loader.load_workflow_file(p)

    def test_context_not_accepted_refused(self):
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: a
    block: db.transition
    timeout_s: 10
    context: [pack]
    params: { to_state: triaged, event: "e:x" }
    outcomes: { ok: done }
""")
        with self.assertRaisesRegex(SystemExit, "context 'pack' not accepted"):
            loader.load_workflow_file(p)

    def test_open_context_block_accepts_any_registered_provider(self):
        # agent.run declares accepts_context={"*"}: any registered provider
        # is allowed (packs add providers without editing the engine), but
        # an unregistered name is still refused.
        pack = fake_pack(agents={"fix": {}})
        pack.prompts = {"fix": "/tmp/x"}
        pack.schemas = {"v": {"properties": {"verdict": {"enum": ["OK"]}}}}
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: a
    block: agent.run
    llm: fix
    schema: v
    timeout_s: 10
    context: [pack]
    outcomes: { OK: done, agent_limit: failed, agent_invalid: failed,
                agent_backend: failed, timeout: failed }
""")
        loader.load_workflow_file(p, pack=pack)  # 'pack' is registered -> OK
        p2 = self._write("w2.yaml", """\
workflow: w
steps:
  - name: a
    block: agent.run
    llm: fix
    schema: v
    timeout_s: 10
    context: [no_such_provider]
    outcomes: { OK: done, agent_limit: failed, agent_invalid: failed,
                agent_backend: failed, timeout: failed }
""")
        with self.assertRaisesRegex(SystemExit, "no registered provider"):
            loader.load_workflow_file(p2, pack=pack)

    def test_context_without_provider_refused(self):
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: a
    block: test.oddctx
    timeout_s: 10
    context: [weird]
    outcomes: { ok: done }
""")
        with self.assertRaisesRegex(SystemExit, "context 'weird' has no registered provider"):
            loader.load_workflow_file(p)

    def test_missing_required_param_refused(self):
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: a
    block: scan.grep_rules
    timeout_s: 10
    params: { repo: /nowhere }
    outcomes: { ok: done, timeout: failed }
""")
        with self.assertRaisesRegex(SystemExit, r"requires params \['rules'\]"):
            loader.load_workflow_file(p)

    def test_undeclared_emit_refused(self):
        p = self._write("w.yaml", """\
workflow: w
emits: []
steps:
  - name: a
    block: db.transition
    timeout_s: 10
    params: { to_state: triaged, event: "e:x" }
    outcomes: { ok: done }
""")
        with self.assertRaisesRegex(SystemExit, "does not declare 'item.triaged'"):
            loader.load_workflow_file(p)

    def test_malformed_event_name_refused(self):
        p = self._write("w.yaml", "workflow: w\nconsumes: [NotAnEvent]\nsteps:\n"
                                  "  - {name: a, block: db.transition, timeout_s: 5,\n"
                                  "     params: {to_state: triaged, event: e}, outcomes: {ok: done}}\n")
        with self.assertRaisesRegex(SystemExit, "malformed event name"):
            loader.load_workflow_file(p)

    def test_unknown_finding_state_event_refused(self):
        p = self._write("w.yaml", "workflow: w\nconsumes: [item.polished]\nsteps:\n"
                                  "  - {name: a, block: db.transition, timeout_s: 5,\n"
                                  "     params: {to_state: triaged, event: e}, outcomes: {ok: done}}\n")
        with self.assertRaisesRegex(SystemExit, "unknown item state 'polished'"):
            loader.load_workflow_file(p)

    def test_llm_binding_on_local_block_refused(self):
        p = self._write("w.yaml", """\
workflow: w
steps:
  - name: a
    block: shell.run
    llm: fix
    timeout_s: 10
    params: { cmd: [true] }
    outcomes: { ok: done, nonzero: failed, mismatch: failed, timeout: failed }
""")
        with self.assertRaisesRegex(SystemExit, "cannot carry an 'llm:' binding"):
            loader.load_workflow_file(p)

    def test_duplicate_kind_refused(self):
        self._write("a.yaml", GOOD)
        self._write("b.yaml", GOOD)
        with self.assertRaisesRegex(SystemExit, "defined twice"):
            loader.load_defs([self.dir])

    def test_demo_pack_defs_load_and_subscribe(self):
        pack = fake_pack(paths={"repo": "/r", "outbox": "/o"})
        wfs = loader.load_defs([REPO_ROOT / "packs" / "demo" / "workflows"], pack=pack)
        self.assertEqual(sorted(wfs), ["filebug", "notify"])
        subs = loader.subscriptions(wfs)
        self.assertEqual(subs, {"demo.scan_requested": ["filebug"],
                                "item.triaged": ["notify"]})
        # pack paths resolved at load; runtime placeholders survived
        scan = wfs["filebug"].steps[0]
        self.assertEqual(scan.params["repo"], "/r")
        record = wfs["filebug"].steps[3]
        self.assertEqual(record.params["item_id"], "{prev.item_id}")


if __name__ == "__main__":
    unittest.main()


class DuplicateContextProviderTest(unittest.TestCase):
    def test_same_provider_twice_is_refused(self):
        """ctx is keyed by provider name — a duplicate would silently
        overwrite the first section (found porting a real pack)."""
        import tempfile
        from pathlib import Path as P
        d = P(tempfile.mkdtemp())
        (d / "w.yaml").write_text(
            "workflow: duptest\n"
            "steps:\n"
            "  - name: s\n"
            "    block: shell.run\n"
            "    timeout_s: 10\n"
            "    params: { cmd: [\"true\"] }\n"
            "    context: [payload, payload]\n"
            "    outcomes: { ok: done, nonzero: failed, mismatch: failed,"
            " timeout: failed }\n")
        from forgeflow import loader
        with self.assertRaisesRegex(SystemExit, "declared twice"):
            loader.load_workflow_file(d / "w.yaml")
