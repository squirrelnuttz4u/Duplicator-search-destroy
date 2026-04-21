"""Round-3 smoke tests: every public module imports cleanly and key
invariants (schema indexes, CLI help) are in place.

These tests are cheap and catch breakage from moves/renames that unit tests
might miss.
"""

from __future__ import annotations

import importlib

import pytest


PUBLIC_MODULES = [
    "duplicator_search_destroy",
    "duplicator_search_destroy.app",
    "duplicator_search_destroy.__main__",
    "duplicator_search_destroy.models",
    "duplicator_search_destroy.models.database",
    "duplicator_search_destroy.models.schema",
    "duplicator_search_destroy.scanner",
    "duplicator_search_destroy.scanner.network",
    "duplicator_search_destroy.scanner.shares",
    "duplicator_search_destroy.scanner.files",
    "duplicator_search_destroy.scanner.hasher",
    "duplicator_search_destroy.scanner.duplicates",
    "duplicator_search_destroy.scanner.orchestrator",
    "duplicator_search_destroy.utils.ip_utils",
    "duplicator_search_destroy.utils.crypto",
    "duplicator_search_destroy.utils.formatting",
    "duplicator_search_destroy.utils.logging_setup",
]


@pytest.mark.parametrize("mod", PUBLIC_MODULES)
def test_module_imports(mod):
    importlib.import_module(mod)


def test_version_exported():
    import duplicator_search_destroy as pkg

    assert isinstance(pkg.__version__, str)
    assert pkg.__version__.count(".") >= 2


def test_cli_help(capsys):
    from duplicator_search_destroy.app import _parse_args

    with pytest.raises(SystemExit):
        _parse_args(["prog", "--help"])
    captured = capsys.readouterr()
    assert "duplicator-search-destroy" in captured.out
    assert "--db" in captured.out


def test_cli_custom_db(tmp_path):
    from duplicator_search_destroy.app import _parse_args

    ns = _parse_args(["prog", "--db", str(tmp_path / "x.db"), "-v"])
    assert str(ns.db).endswith("x.db")
    assert ns.verbose is True


def test_schema_indexes_present(tmp_db):
    rows = tmp_db.query(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
    )
    names = {r["name"] for r in rows}
    # Core indexes that reports & dedup rely on:
    for expected in (
        "idx_files_size",
        "idx_files_name",
        "idx_files_full_hash",
        "idx_files_dedup_key",
        "idx_folders_size",
    ):
        assert expected in names, f"missing index {expected}"


def test_schema_foreign_keys_enabled(tmp_db):
    row = tmp_db.query("PRAGMA foreign_keys")
    assert row[0][0] == 1


def test_wal_journal_mode(tmp_db):
    row = tmp_db.query("PRAGMA journal_mode")
    assert row[0][0].lower() == "wal"


def test_scanner_reexports():
    from duplicator_search_destroy import scanner

    assert hasattr(scanner, "discover_hosts")
    assert hasattr(scanner, "enumerate_shares")
    assert hasattr(scanner, "walk_share")
    assert hasattr(scanner, "hash_full")
    assert hasattr(scanner, "find_duplicates")
