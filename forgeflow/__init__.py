"""forgeflow: a YAML-configurable, pluggable, deterministic workflow engine.

SQLite-backed, totality-checked at load time, crash-resumable. Domain
processes (whatever they are) live in packs: YAML workflows + plugin blocks.
"""
from __future__ import annotations

# The ONE version definition: pyproject.toml reads it via
# [tool.setuptools.dynamic], the CLI serves it via --version, and a test
# guards that no second copy reappears. Bump here, nowhere else.
__version__ = "0.5.0"
