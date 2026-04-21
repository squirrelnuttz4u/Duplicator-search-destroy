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
    return p.parse_args(argv[1:])


def run_gui(argv: List[str]) -> int:
    args = _parse_args(argv)
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
