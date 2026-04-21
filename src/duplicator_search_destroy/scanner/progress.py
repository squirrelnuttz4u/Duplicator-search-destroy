"""Shared progress model for the scanning phases.

:class:`ScanStats` is a thread-safe counter bag — scanner threads call
``add_files()`` / ``add_folders()`` / ``mark_share_done()`` as they work;
the UI thread calls ``snapshot()`` on a timer to read a consistent
copy-out.

:class:`ThrottledEmitter` wraps a callback with a minimum inter-call
delay so scanning 10 M files doesn't fire 10 M Qt signals — we coalesce
to a fixed rate (default 4 Hz).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

__all__ = ["ScanStats", "ThrottledEmitter", "StatsSnapshot"]


@dataclass(slots=True)
class StatsSnapshot:
    """Immutable copy of the counters at a point in time.

    Everything the GUI needs to render progress bars, rates and ETA.
    """

    phase: str
    started_at: float
    now: float
    hosts_total: int
    hosts_done: int
    shares_total: int
    shares_done: int
    files_seen: int
    folders_seen: int
    bytes_seen: int
    active_workers: List[str]
    errors: int
    last_message: str

    @property
    def elapsed(self) -> float:
        return max(0.0, self.now - self.started_at)

    @property
    def files_per_sec(self) -> float:
        e = self.elapsed
        return self.files_seen / e if e > 0 else 0.0

    @property
    def bytes_per_sec(self) -> float:
        e = self.elapsed
        return self.bytes_seen / e if e > 0 else 0.0

    @property
    def eta_seconds(self) -> Optional[float]:
        """Estimated seconds remaining, based on shares-done rate.

        Returns None if we don't have enough data to extrapolate (no share
        has finished yet, or shares_total is zero).
        """
        if self.shares_total <= 0 or self.shares_done <= 0:
            return None
        per_share = self.elapsed / self.shares_done
        remaining = max(0, self.shares_total - self.shares_done)
        return per_share * remaining


class ScanStats:
    """Thread-safe counter aggregate."""

    def __init__(self, phase: str, *, hosts_total: int = 0, shares_total: int = 0) -> None:
        self._lock = threading.Lock()
        self._phase = phase
        self._started_at = time.monotonic()
        self._hosts_total = hosts_total
        self._hosts_done = 0
        self._shares_total = shares_total
        self._shares_done = 0
        self._files_seen = 0
        self._folders_seen = 0
        self._bytes_seen = 0
        self._active: Dict[int, str] = {}  # thread ident -> UNC path
        self._errors = 0
        self._last_message = ""

    def begin_share(self, unc: str) -> None:
        with self._lock:
            self._active[threading.get_ident()] = unc
            self._last_message = unc

    def end_share(self, unc: str, *, error: bool = False) -> None:
        with self._lock:
            self._active.pop(threading.get_ident(), None)
            self._shares_done += 1
            if error:
                self._errors += 1

    def end_host(self) -> None:
        with self._lock:
            self._hosts_done += 1

    def add_files(self, n: int, total_bytes: int = 0) -> None:
        with self._lock:
            self._files_seen += n
            self._bytes_seen += total_bytes

    def add_folders(self, n: int) -> None:
        with self._lock:
            self._folders_seen += n

    def note_error(self, message: str) -> None:
        with self._lock:
            self._errors += 1
            self._last_message = message

    def update_totals(self, *, hosts_total: Optional[int] = None, shares_total: Optional[int] = None) -> None:
        with self._lock:
            if hosts_total is not None:
                self._hosts_total = hosts_total
            if shares_total is not None:
                self._shares_total = shares_total

    def snapshot(self) -> StatsSnapshot:
        with self._lock:
            return StatsSnapshot(
                phase=self._phase,
                started_at=self._started_at,
                now=time.monotonic(),
                hosts_total=self._hosts_total,
                hosts_done=self._hosts_done,
                shares_total=self._shares_total,
                shares_done=self._shares_done,
                files_seen=self._files_seen,
                folders_seen=self._folders_seen,
                bytes_seen=self._bytes_seen,
                active_workers=list(self._active.values()),
                errors=self._errors,
                last_message=self._last_message,
            )


class ThrottledEmitter:
    """Wrap a callback so it fires at most once per ``min_interval`` seconds.

    The final ``flush()`` call always emits, so the UI gets one last update
    when a phase ends (with ``shares_done == shares_total``).
    """

    def __init__(
        self,
        callback: Optional[Callable[[StatsSnapshot], None]],
        *,
        min_interval: float = 0.25,
    ) -> None:
        self._cb = callback
        self._min_interval = max(0.0, float(min_interval))
        self._last = 0.0
        self._lock = threading.Lock()

    def emit(self, snap: StatsSnapshot) -> None:
        if self._cb is None:
            return
        now = time.monotonic()
        with self._lock:
            if now - self._last < self._min_interval:
                return
            self._last = now
        try:
            self._cb(snap)
        except Exception:
            # Never let a UI callback kill a scan.
            pass

    def flush(self, snap: StatsSnapshot) -> None:
        if self._cb is None:
            return
        try:
            self._cb(snap)
        except Exception:
            pass
