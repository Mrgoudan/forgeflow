"""forgeflow.__version__ is the ONE version definition (it once drifted
four releases behind pyproject). pyproject must read it dynamically — a
literal version there means two sources again — and the CLI must serve it."""
from __future__ import annotations

import io
import re
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import forgeflow
from forgeflow import cli


class VersionSingleSourceTest(unittest.TestCase):
    def test_pyproject_declares_dynamic_version(self):
        text = (Path(__file__).resolve().parent.parent
                / "pyproject.toml").read_text()
        self.assertIn('dynamic = ["version"]', text)
        self.assertIn('version = { attr = "forgeflow.__version__" }', text)
        project = text.split("[project]", 1)[1].split("[project.", 1)[0]
        self.assertNotRegex(project,
                            re.compile(r'^version\s*=\s*"', re.MULTILINE))

    def test_version_matches_changelog_top(self):
        """The CHANGELOG's newest entry must name the current version — the
        drift the single-source fixed was __init__ vs pyproject; this
        catches __init__ vs CHANGELOG (a release that forgot the bump)."""
        import re
        cl = (Path(__file__).resolve().parent.parent / "CHANGELOG.md").read_text()
        m = re.search(r"^## (\d+\.\d+\.\d+)", cl, re.MULTILINE)
        self.assertIsNotNone(m, "no version heading in CHANGELOG")
        self.assertEqual(m.group(1), forgeflow.__version__,
                         "CHANGELOG top %s != __version__ %s"
                         % (m.group(1), forgeflow.__version__))

    def test_version_is_semver_and_served_by_cli(self):
        self.assertRegex(forgeflow.__version__, r"^\d+\.\d+\.\d+$")
        out = io.StringIO()
        with self.assertRaises(SystemExit) as cm, redirect_stdout(out):
            cli.main(["--version"])
        self.assertEqual(cm.exception.code, 0)
        self.assertEqual(out.getvalue().strip(),
                         "forgeflow %s" % forgeflow.__version__)


if __name__ == "__main__":
    unittest.main()
