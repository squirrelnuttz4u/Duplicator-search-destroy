"""Centralised logging configuration.

The same settings are used by the GUI, CLI and unit tests. Logs go to both
stderr and a rotating file under the user's config dir so a nightly run can
leave a record for debugging.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

__all__ = ["configure_logging", "log_path"]

_CONFIGURED = False


def log_path() -> Path:
    base = os.environ.get("APPDATA") or os.path.expanduser("~/.config")
    directory = Path(base) / "DuplicatorSearchDestroy" / "logs"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "app.log"


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    root.addHandler(stream)

    try:
        file_handler = logging.handlers.RotatingFileHandler(
            log_path(), maxBytes=5_000_000, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    except OSError:
        # Read-only filesystem or permission issue — fall back to stderr only.
        pass

    _CONFIGURED = True
