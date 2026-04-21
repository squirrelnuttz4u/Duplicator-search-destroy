"""Generic background worker that runs a callable on a QThread.

Keeps Qt concerns out of the scanner modules. A scanner function takes
``on_progress`` and ``cancel`` callables — this wrapper turns those into
Qt signals the GUI can bind to.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any, Callable

from PySide6.QtCore import QObject, QThread, Signal

log = logging.getLogger(__name__)

__all__ = ["ScanWorker"]


class ScanWorker(QObject):
    progress = Signal(str, int, int)  # message, done, total
    finished = Signal(object)           # return value from fn
    failed = Signal(str)                # traceback text

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


def run_in_thread(worker: ScanWorker) -> QThread:
    """Spin up a QThread running *worker*, return it so the caller can join."""
    thread = QThread()
    worker.moveToThread(thread)
    thread.started.connect(worker.run)
    worker.finished.connect(thread.quit)
    worker.failed.connect(thread.quit)
    thread.start()
    return thread
