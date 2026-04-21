"""CLI entry point.

Usage::

    python -m duplicator_search_destroy            # launches the GUI
    python -m duplicator_search_destroy --cli ...  # reserved for headless ops
"""

from __future__ import annotations

import sys


def main() -> int:
    # Import lazily so unit tests that don't need Qt don't pay the import cost.
    from duplicator_search_destroy.app import run_gui

    return run_gui(sys.argv)


if __name__ == "__main__":
    raise SystemExit(main())
