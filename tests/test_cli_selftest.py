"""Tests for CLI flags — --selftest + arg parsing."""

from __future__ import annotations

import pytest

from duplicator_search_destroy.app import _parse_args, _run_selftest


def test_parse_args_defaults():
    ns = _parse_args(["prog"])
    assert ns.db is None
    assert ns.verbose is False
    assert ns.selftest is False


def test_parse_args_selftest_flag():
    ns = _parse_args(["prog", "--selftest"])
    assert ns.selftest is True


def test_parse_args_verbose_flag():
    ns = _parse_args(["prog", "-v"])
    assert ns.verbose is True


def test_parse_args_custom_db(tmp_path):
    ns = _parse_args(["prog", "--db", str(tmp_path / "x.db")])
    assert str(ns.db).endswith("x.db")


def test_selftest_imports_and_roundtrips_db(tmp_path):
    """The selftest should exercise every bundling-sensitive module.

    PySide6 is skipped on platforms where it isn't installed (Linux CI
    runners for unit tests don't install GUI deps).
    """
    db = tmp_path / "selftest.db"
    try:
        rc = _run_selftest(db)
    except ImportError as exc:
        # PySide6 isn't in the dev-test requirements. The CI Windows job
        # installs it via requirements.txt.
        if "PySide6" in str(exc):
            pytest.skip("PySide6 not installed — skip GUI-import leg of selftest")
        raise
    assert rc == 0
    assert db.exists()
