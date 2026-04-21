"""Human-friendly formatters used by the UI and reports."""

from __future__ import annotations

from datetime import datetime, timezone

__all__ = ["human_size", "human_time", "shorten_path"]

_UNITS = ("B", "KB", "MB", "GB", "TB", "PB", "EB")


def human_size(n_bytes: int | float | None) -> str:
    if n_bytes is None:
        return "—"
    if n_bytes < 0:
        return f"-{human_size(-n_bytes)}"
    value = float(n_bytes)
    for unit in _UNITS:
        if value < 1024.0 or unit == _UNITS[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} {_UNITS[-1]}"  # pragma: no cover


def human_time(ts: float | int | None) -> str:
    if ts is None:
        return "—"
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone()
    except (OverflowError, OSError, ValueError):
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def shorten_path(path: str, width: int = 80) -> str:
    if len(path) <= width:
        return path
    head = path[: width // 2 - 2]
    tail = path[-(width // 2 - 1):]
    return f"{head}...{tail}"
