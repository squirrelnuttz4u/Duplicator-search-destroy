"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure ``src/`` is on sys.path so tests can import the package without installation.
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture()
def tmp_db(tmp_path):
    from duplicator_search_destroy.models.database import Database

    db = Database(tmp_path / "test.db")
    try:
        yield db
    finally:
        db.close()
