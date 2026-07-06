from __future__ import annotations

import os
import unittest

from helpers import tmpdir

from forgeflow import config


class ConfigTest(unittest.TestCase):
    def setUp(self):
        self.dir = tmpdir()
        self.pack_dir = self.dir / "pack"
        self.pack_dir.mkdir()

    def _write(self, text):
        (self.pack_dir / "project.yaml").write_text(text)

    def test_missing_config_refused(self):
        with self.assertRaisesRegex(SystemExit, "does not exist"):
            config.load_pack(self.pack_dir)

    def test_missing_path_fails_loud(self):
        self._write("name: p\npaths: { repo: /no/such/dir/anywhere }\n")
        with self.assertRaisesRegex(SystemExit, "paths.repo.*does not exist"):
            config.load_pack(self.pack_dir)

    def test_unknown_key_refused(self):
        self._write("name: p\nsurprise: 1\n")
        with self.assertRaisesRegex(SystemExit, "unknown keys.*surprise"):
            config.load_pack(self.pack_dir)

    def test_missing_workflow_dir_refused(self):
        self._write("name: p\nworkflows: [nope]\n")
        with self.assertRaisesRegex(SystemExit, "not a directory"):
            config.load_pack(self.pack_dir)

    def test_missing_tool_fails_optional_tool_skips(self):
        self._write("name: p\ntools:\n  ghost: { path: /no/such/tool }\n")
        with self.assertRaisesRegex(SystemExit, "tools.ghost.*not found"):
            config.load_pack(self.pack_dir)
        self._write("name: p\ntools:\n  ghost: { path: /no/such/tool, optional: true }\n")
        pack = config.load_pack(self.pack_dir)
        self.assertNotIn("ghost", pack.tools)

    def test_tool_verified_and_version_recorded(self):
        self._write("name: p\ntools:\n  git: { path: git, version_cmd: ['--version'] }\n")
        pack = config.load_pack(self.pack_dir)
        self.assertTrue(os.path.isabs(pack.tools["git"]))
        self.assertIn("git version", pack.tool_versions["git"])

    def test_params_template_against_paths(self):
        target = self.dir / "target"
        target.mkdir()
        self._write("name: p\npaths: { repo: %s }\n"
                    "params: { build_dir: '{paths.repo}/build' }\n" % target)
        pack = config.load_pack(self.pack_dir)
        self.assertEqual(pack.params["build_dir"], "%s/build" % target.resolve())

    def test_unresolved_param_template_refused(self):
        self._write("name: p\nparams: { x: '{paths.nope}/y' }\n")
        with self.assertRaisesRegex(SystemExit, "params templating"):
            config.load_pack(self.pack_dir)

    def test_secrets_mode_enforced(self):
        sec = self.dir / "secrets.env"
        sec.write_text("TOKEN_A=abc\n# comment\nTOKEN_B = spaced\n")
        os.chmod(sec, 0o644)
        with self.assertRaisesRegex(SystemExit, "refuse to read"):
            config.load_secrets(sec)
        os.chmod(sec, 0o600)
        secrets = config.load_secrets(sec)
        self.assertEqual(secrets, {"TOKEN_A": "abc", "TOKEN_B": "spaced"})

    def test_missing_secrets_file_is_empty(self):
        self.assertEqual(config.load_secrets(self.dir / "absent.env"), {})


if __name__ == "__main__":
    unittest.main()
