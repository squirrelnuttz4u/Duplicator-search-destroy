"""Tab 1 — Dashboard.

UI for entering IP targets, kicking off host discovery, and listing which
hosts came back alive.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from PySide6.QtCore import Qt, QThread
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from duplicator_search_destroy.gui.worker import ScanWorker, run_in_thread
from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner.orchestrator import Orchestrator
from duplicator_search_destroy.utils.formatting import human_time
from duplicator_search_destroy.utils.ip_utils import InvalidTargetError, count_targets

log = logging.getLogger(__name__)

__all__ = ["DashboardTab"]


class DashboardTab(QWidget):
    def __init__(self, db: Database, orchestrator: Orchestrator) -> None:
        super().__init__()
        self.db = db
        self.orchestrator = orchestrator
        self._worker: Optional[ScanWorker] = None
        self._thread: Optional[QThread] = None
        self._build_ui()
        self.refresh_hosts()

    # -- UI construction --------------------------------------------------

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        intro = QLabel(
            "<b>Step 1 — Network discovery.</b> Enter one subnet per line "
            "(e.g. <code>10.0.0.0/24</code>, <code>10.0.0.1-10.0.0.50</code> "
            "or <code>10.0.0.*</code>), then press <b>Scan network</b>."
        )
        intro.setWordWrap(True)
        root.addWidget(intro)

        self.targets = QPlainTextEdit()
        self.targets.setPlaceholderText(
            "10.0.0.0/24\n192.168.10.0/23\n172.16.5.1-172.16.5.50"
        )
        self.targets.setMinimumHeight(100)
        root.addWidget(self.targets)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Timeout (s):"))
        self.timeout = QSpinBox()
        self.timeout.setRange(1, 30)
        self.timeout.setValue(2)
        controls.addWidget(self.timeout)
        controls.addWidget(QLabel("Workers:"))
        self.workers = QSpinBox()
        self.workers.setRange(1, 512)
        self.workers.setValue(128)
        controls.addWidget(self.workers)

        self.btn_scan = QPushButton("Scan network")
        self.btn_scan.clicked.connect(self._start_scan)
        controls.addWidget(self.btn_scan)

        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._cancel_scan)
        controls.addWidget(self.btn_cancel)

        controls.addStretch(1)
        root.addLayout(controls)

        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        root.addWidget(self.progress)

        self.status = QLabel("Idle.")
        root.addWidget(self.status)

        self.model = QStandardItemModel(0, 4)
        self.model.setHorizontalHeaderLabels(["IP", "Hostname", "Status", "Last seen"])
        self.table = QTableView()
        self.table.setModel(self.model)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setEditTriggers(QTableView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        root.addWidget(self.table, 1)

    # -- actions ----------------------------------------------------------

    def _start_scan(self) -> None:
        raw = self.targets.toPlainText().strip()
        if not raw:
            QMessageBox.warning(self, "No targets", "Enter at least one subnet or IP.")
            return
        try:
            total = count_targets(raw)
        except InvalidTargetError as exc:
            QMessageBox.critical(self, "Invalid target", str(exc))
            return
        if total == 0:
            QMessageBox.warning(self, "No targets", "Parsed zero addresses.")
            return
        if total > 16_384:
            ok = QMessageBox.question(
                self,
                "Large sweep",
                f"About to probe {total:,} addresses — continue?",
            )
            if ok != QMessageBox.Yes:
                return

        self.progress.setRange(0, total)
        self.progress.setValue(0)
        self.status.setText(f"Probing {total:,} address(es)…")
        self.btn_scan.setEnabled(False)
        self.btn_cancel.setEnabled(True)

        worker = ScanWorker(
            self.orchestrator.discover,
            raw,
            timeout=float(self.timeout.value()),
            max_workers=int(self.workers.value()),
        )
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_finished)
        worker.failed.connect(self._on_failed)
        self._worker = worker
        self._thread = run_in_thread(worker)

    def _cancel_scan(self) -> None:
        if self._worker:
            self._worker.cancel()
            self.status.setText("Cancelling…")

    def _on_progress(self, message: str, done: int, total: int) -> None:
        self.progress.setMaximum(max(total, 1))
        self.progress.setValue(done)
        self.status.setText(f"{done}/{total}  •  last: {message}")

    def _on_finished(self, result) -> None:  # noqa: ANN001 - Qt signal
        count = len(result) if hasattr(result, "__len__") else 0
        self.status.setText(f"Done — {count} live SMB host(s) discovered.")
        self.btn_scan.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self.refresh_hosts()

    def _on_failed(self, tb: str) -> None:
        QMessageBox.critical(self, "Scan failed", tb)
        self.btn_scan.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self.status.setText("Failed.")

    # -- refresh ----------------------------------------------------------

    def refresh_hosts(self) -> None:
        self.model.removeRows(0, self.model.rowCount())
        for h in self.db.list_hosts():
            items: List[QStandardItem] = [
                QStandardItem(h.ip),
                QStandardItem(h.hostname or ""),
                QStandardItem(h.status),
                QStandardItem(human_time(h.last_seen)),
            ]
            for it in items:
                it.setEditable(False)
            self.model.appendRow(items)
