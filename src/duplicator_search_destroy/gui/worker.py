"""Generic background worker that runs a scanner function on a QThread.

The worker exposes THREE Qt signals:

* ``progress(message, done, total)`` — coarse step-level progress
* ``stats(snapshot)``               — rich live counters (files/sec, ETA, …)
* ``finished(result)`` / ``failed(tb)``

The scanner modules have zero Qt imports — the translation from plain
callbacks to Qt signals happens here.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any, Callable

from PySide6.QtCore import QObject, QThread, Signal

log = logging.getLogger(__name__)

__all__ = ["ScanWorker", "run_in_thread"]


class ScanWorker(QObject):
    progress = Signal(str, int, int)
    stats = Signal(object)         # StatsSnapshot
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self._cancel = False

    def cancel(self) -> None:
        self._cancel = True

    def is_cancelled(self) -> bool:
        return self._cancel

    def run(self) -> None:
        kwargs = dict(self._kwargs)
        kwargs.setdefault("on_progress", self._emit_progress)
        kwargs.setdefault("on_stats", self._emit_stats)
        kwargs.setdefault("cancel", self.is_cancelled)
        try:
            result = self._fn(*self._args, **kwargs)
            self.finished.emit(result)
        except Exception:
            tb = traceback.format_exc()
            log.error("Worker failed:\n%s", tb)
            self.failed.emit(tb)

    def _emit_progress(self, message: str, done: int, total: int) -> None:
        try:
            self.progress.emit(str(message), int(done), int(total))
        except Exception:
            pass

    def _emit_stats(self, snapshot) -> None:  # noqa: ANN001 - Qt-side
        try:
            self.stats.emit(snapshot)
        except Exception:
            pass


def run_in_thread(worker: ScanWorker) -> QThread:
    thread = QThread()
    worker.moveToThread(thread)
    thread.started.connect(worker.run)
    worker.finished.connect(thread.quit)
    worker.failed.connect(thread.quit)
    thread.start()
    return thread
