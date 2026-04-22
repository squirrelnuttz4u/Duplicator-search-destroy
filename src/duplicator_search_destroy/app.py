"""Application bootstrap."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List

from duplicator_search_destroy.models.database import open_database
from duplicator_search_destroy.utils.logging_setup import configure_logging

log = logging.getLogger(__name__)

__all__ = ["run_gui", "default_db_path"]


def default_db_path() -> Path:
    base = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
    directory = Path(base) / "DuplicatorSearchDestroy"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "inventory.db"


def _parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="duplicator-search-destroy")
    p.add_argument("--db", type=Path, default=None, help="Path to the SQLite database")
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    p.add_argument(
        "--selftest",
        action="store_true",
        help=(
            "Validate that every bundled module imports cleanly and the "
            "database layer works, then exit. Used by CI to verify the "
            "PyInstaller build before shipping it."
        ),
    )
    return p.parse_args(argv[1:])


def _run_selftest(db_path: Path) -> int:
    """Smoke-test the frozen .exe without launching the GUI.

    We import everything the runtime will ever need (crypto, SMB, hashing,
    GUI), open the DB, close it, and exit 0. Any ``ImportError`` or other
    failure propagates with exit code 1.

    This is what CI runs against the built .exe because the windowed build
    has no stdout, so ``--help`` can't be used to verify imports.
    """
    # Core modules that have caused bundling pain before (cryptography,
    # smbprotocol, pywin32) — import them eagerly so the selftest fails
    # fast on a broken bundle.
    from duplicator_search_destroy import scanner as _scanner  # noqa: F401
    from duplicator_search_destroy.scanner import network, shares, files, hasher, duplicates  # noqa: F401
    from duplicator_search_destroy.utils.crypto import CredentialCipher  # noqa: F401
    from duplicator_search_destroy.scanner import progress  # noqa: F401

    # DB round-trip
    db = open_database(db_path)
    try:
        db.counts()  # exercises a real SQL query
    finally:
        db.close()

    # Qt imports: validate that PySide6 is fully bundled — but don't actually
    # create a QApplication, so no display is required.
    from PySide6 import QtCore, QtWidgets  # noqa: F401
    return 0


def run_gui(argv: List[str]) -> int:
    args = _parse_args(argv)

    if args.selftest:
        # Intentionally skip configure_logging(): in a PyInstaller --windowed
        # build sys.stderr is None, and we want the selftest to be fully
        # silent so the CI runner just checks the exit code.
        return _run_selftest(args.db or default_db_path())

    configure_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    db_path = args.db or default_db_path()
    log.info("Opening database at %s", db_path)
    db = open_database(db_path)

    # Import Qt lazily so unit tests and `--help` don't need it.
    from PySide6.QtWidgets import QApplication
    from duplicator_search_destroy.gui.main_window import MainWindow

    app = QApplication.instance() or QApplication(argv)
    window = MainWindow(db)
    window.show()
    return app.exec()
