"""
An opinionated sphinx setup for academic or scientific reports in Sphinx.

Sphinx Documentation: https://www.sphinx-doc.org/en/master/usage/configuration.html
"""
import re
from datetime import datetime

from dynaconf import settings

# -- Project Information ------

# Project or Documentation Title
project = settings.get("project_title", "My Project")

project_slug = re.sub("([^a-zA-Z0-9])+", "-", project).lower()

# TODO: Handle Multiple Authors
author = settings.get("author", "")

# Academic Work should not be copy righted
copyright = settings.get("copyright", "") or f"{datetime.today().year}, {author}"

# -- Document Settings ------

# Index Document (excluding suffix)
master_doc = "index"

# Files to ignore
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Parsers for source file extensions
source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}

# Template Paths

templates_path = ["templates"]

# -- Extensions -----

extensions = [
    # Autodoc
    "sphinx.ext.autodoc",
    
    # TODOs for WIP Docs
    "sphinx.ext.todo",

    # Render LaTeX for HTML outputs
    "sphinx.ext.mathjax",

    # Configuration-driven Content
    "sphinx.ext.ifconfig",

    # Add Code Documentation if there is associated code
    "sphinx.ext.viewcode",

    # Embed Jupyter Notebooks
    "nbsphinx",

    # PlantUML driven Diagrams
    "sphinx.ext.graphviz",
    "sphinxcontrib.plantuml",

    # Citations and Reference Management
    "sphinxcontrib.bibtex",

    # Markdown Support
    "recommonmark",
]

# TODO: Add in other extensions in the toml file
# Pseudo-code: 
extensions.extend(filter(lambda ext: ext not in extensions, settings.get("extensions")))


# -- Figure and Caption Settings -----

# See: https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-numfig
numfig = True


# -- LaTeX Settings -----
# See: https://www.sphinx-doc.org/en/master/latex.html

# See: https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-latex_documents

_targetname = f"{project_slug}.tex"

latex_documents = [
    (master_doc, _targetname, project, author, "howto")
]

latex_logo = settings.get("logo", None)

if settings.get("printed", False):
    latex_show_pagerefs = True
    latex_show_urls = "footnote"

_latex_preamble = r"""
\usepackage{booktabs}

% See: https://sphinxcontrib-bibtex.readthedocs.io/en/latest/usage.html
% make phantomsection empty inside figures
\usepackage{etoolbox}
\AtBeginEnvironment{figure}{\renewcommand{\phantomsection}{}}
"""

# See: https://www.sphinx-doc.org/en/master/latex.html#the-latex-elements-configuration-setting
latex_elements = {
    "figure_align": "H",
    "preamble": _latex_preamble,
}

# -- HTML Settings -----

html_theme = "alabaster"

html_static_path = ["static"]


# -- TODOS Settings -----

todo_include_todos = True