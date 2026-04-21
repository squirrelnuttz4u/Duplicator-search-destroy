"""Tab 3 — File scan + duplicate detection."""

from __future__ import annotations

import logging
from typing import Optional

from PySide6.QtCore import QThread
from PySide6.QtWidgets import (
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from duplicator_search_destroy.gui.worker import ScanWorker, run_in_thread
from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner.orchestrator import Orchestrator
from duplicator_search_destroy.utils.formatting import human_size

log = logging.getLogger(__name__)

__all__ = ["ScanTab"]


class ScanTab(QWidget):
    def __init__(self, db: Database, orchestrator: Orchestrator) -> None:
        super().__init__()
        self.db = db
        self.orchestrator = orchestrator
        self._worker: Optional[ScanWorker] = None
        self._thread: Optional[QThread] = None
        self._build_ui()
        self.refresh_summary()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        root.addWidget(QLabel(
            "<b>Step 3 — Index files &amp; find duplicates.</b> "
            "This walks every share indexed on the Credentials tab, records every "
            "file into the local database, then hashes candidate duplicates."
        ))

        summary = QGroupBox("Current index")
        sblayout = QVBoxLayout(summary)
        self.lbl_summary = QLabel("—")
        sblayout.addWidget(self.lbl_summary)
        root.addWidget(summary)

        opt_row = QHBoxLayout()
        opt_row.addWidget(QLabel("Hash workers:"))
        self.hash_workers = QSpinBox()
        self.hash_workers.setRange(1, 32)
        self.hash_workers.setValue(8)
        opt_row.addWidget(self.hash_workers)

        opt_row.addWidget(QLabel("Min file size (bytes):"))
        self.min_size = QSpinBox()
        self.min_size.setRange(0, 10_000_000)
        self.min_size.setValue(1)
        opt_row.addWidget(self.min_size)

        opt_row.addStretch(1)
        root.addLayout(opt_row)

        btn_row = QHBoxLayout()
        self.btn_scan_files = QPushButton("Scan all shares for files & folders")
        self.btn_scan_files.clicked.connect(self._scan_files)
        btn_row.addWidget(self.btn_scan_files)

        self.btn_hash = QPushButton("Hash candidates & find duplicates")
        self.btn_hash.clicked.connect(self._hash)
        btn_row.addWidget(self.btn_hash)

        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._cancel)
        btn_row.addWidget(self.btn_cancel)

        btn_row.addStretch(1)
        root.addLayout(btn_row)

        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        root.addWidget(self.progress)

        self.status = QLabel("Idle.")
        root.addWidget(self.status)

        root.addStretch(1)

    def refresh_summary(self) -> None:
        c = self.db.counts()
        total_size_row = self.db.query("SELECT COALESCE(SUM(size),0) AS s FROM files")
        total_bytes = int(total_size_row[0]["s"]) if total_size_row else 0
        self.lbl_summary.setText(
            f"Hosts: <b>{c['hosts']}</b> &nbsp; Shares: <b>{c['shares']}</b> &nbsp; "
            f"Folders: <b>{c['folders']:,}</b> &nbsp; Files: <b>{c['files']:,}</b> &nbsp; "
            f"Total size: <b>{human_size(total_bytes)}</b>"
        )

    # -- actions ----------------------------------------------------------

    def _scan_files(self) -> None:
        shares = self.db.list_shares()
        if not shares:
            QMessageBox.warning(
                self,
                "No shares",
                "There are no shares to scan. Run share enumeration on the Credentials tab first.",
            )
            return
        self._run(self.orchestrator.scan_files, label="Indexing files")

    def _hash(self) -> None:
        self._run(
            self.orchestrator.hash_and_find_duplicates,
            label="Hashing duplicate candidates",
            max_workers=int(self.hash_workers.value()),
            min_size=int(self.min_size.value()),
        )

    def _run(self, fn, *, label: str, **kwargs) -> None:
        self.btn_scan_files.setEnabled(False)
        self.btn_hash.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.status.setText(f"{label}…")
        worker = ScanWorker(fn, **kwargs)
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_done)
        worker.failed.connect(self._on_failed)
        self._worker = worker
        self._thread = run_in_thread(worker)

    def _cancel(self) -> None:
        if self._worker:
            self._worker.cancel()
            self.status.setText("Cancelling…")

    def _on_progress(self, message: str, done: int, total: int) -> None:
        self.progress.setMaximum(max(total, 1))
        self.progress.setValue(done)
        self.status.setText(f"{done}/{total} — {message}")

    def _on_done(self, result) -> None:  # noqa: ANN001
        self.status.setText(f"Finished. Result: {result}")
        self.btn_scan_files.setEnabled(True)
        self.btn_hash.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self.refresh_summary()

    def _on_failed(self, tb: str) -> None:
        QMessageBox.critical(self, "Scan failed", tb)
        self.btn_scan_files.setEnabled(True)
        self.btn_hash.setEnabled(True)
        self.btn_cancel.setEnabled(False)
