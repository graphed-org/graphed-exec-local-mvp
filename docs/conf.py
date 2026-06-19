"""Sphinx configuration for graphed-exec-local."""

from __future__ import annotations

project = "graphed-exec-local"
author = "graphed-org"
release = "0.0.1"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]
templates_path = ["_templates"]
exclude_patterns = ["_build"]
html_theme = "furo"
html_title = "graphed-exec-local"
autodoc_typehints = "description"
# autosummary recursively generates the API reference (docs/api.rst) from the package itself, so it
# can never drift from the code. Imported re-exports are documented in their defining module only.
autosummary_generate = True
autosummary_imported_members = False
