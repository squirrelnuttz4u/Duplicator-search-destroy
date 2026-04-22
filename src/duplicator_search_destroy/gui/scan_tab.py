"""Tab 3 — File scan + duplicate detection.

Rich progress display:
  * **Overall** progress bar: shares done vs total across all hosts.
  * **Activity** panel: files / folders / bytes, running rate, elapsed
    and ETA (extrapolated from per-share completion time).
  * **Active workers** list: shows which UNC path each worker thread is
    currently walking so you can tell the scan isn't stuck.

Parallelism is configurable — "Parallel hosts" spinner controls how many
servers we scan concurrently. Four is a good default; raise it if your
scanning box has spare NIC + CPU capacity.
"""

from __future__ import annotations

import logging
from typing import Optional

from PySide6.QtCore import QThread, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QCheckBox,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QListView,
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


def _fmt_seconds(seconds: Optional[float]) -> str:
    if seconds is None or seconds <= 0:
        return "—"
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


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
            "This walks every share indexed on the Credentials tab in parallel, "
            "records every file into the local database, then hashes candidate "
            "duplicates."
        ))

        summary = QGroupBox("Current index")
        sblayout = QVBoxLayout(summary)
        self.lbl_summary = QLabel("—")
        sblayout.addWidget(self.lbl_summary)
        root.addWidget(summary)

        # Parallelism + options
        opt_row = QHBoxLayout()
        opt_row.addWidget(QLabel("Parallel hosts:"))
        self.parallel_hosts = QSpinBox()
        self.parallel_hosts.setRange(1, 32)
        self.parallel_hosts.setValue(4)
        self.parallel_hosts.setToolTip(
            "Number of servers to scan simultaneously. 4 is a good default; "
            "raise to 8-16 if your scanning box has spare bandwidth."
        )
        opt_row.addWidget(self.parallel_hosts)

        opt_row.addWidget(QLabel("Hash workers:"))
        self.hash_workers = QSpinBox()
        self.hash_workers.setRange(1, 64)
        self.hash_workers.setValue(8)
        self.hash_workers.setToolTip(
            "Number of files to hash in parallel. I/O-bound, so higher is "
            "usually fine up to NIC saturation."
        )
        opt_row.addWidget(self.hash_workers)

        opt_row.addWidget(QLabel("Min size:"))
        self.min_size = QSpinBox()
        self.min_size.setRange(0, 10_000_000)
        self.min_size.setValue(1)
        self.min_size.setSuffix(" B")
        opt_row.addWidget(self.min_size)

        opt_row.addStretch(1)
        root.addLayout(opt_row)

        # Resume / full-rescan toggle
        resume_row = QHBoxLayout()
        self.chk_resume = QCheckBox("Resume: skip shares already indexed in a previous run")
        self.chk_resume.setChecked(True)
        self.chk_resume.setToolTip(
            "When checked, scan_files will skip any share whose previous run "
            "completed successfully (last_scan is set). Un-check to force a "
            "full re-scan from scratch.\n\n"
            "Shares cancelled mid-walk are always re-scanned — partial rows "
            "from the interrupted run get wiped first."
        )
        resume_row.addWidget(self.chk_resume)
        resume_row.addStretch(1)

        self.lbl_scan_state = QLabel("")
        resume_row.addWidget(self.lbl_scan_state)
        root.addLayout(resume_row)

        # Action buttons
        btn_row = QHBoxLayout()
        self.btn_scan_files = QPushButton("Scan all shares")
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

        # Progress bars
        self.progress_overall = QProgressBar()
        self.progress_overall.setFormat("Overall: %v / %m (%p%)")
        self.progress_overall.setTextVisible(True)
        root.addWidget(self.progress_overall)

        # Rich stats
        stats_box = QGroupBox("Live stats")
        stats_layout = QVBoxLayout(stats_box)
        self.lbl_stats = QLabel(
            "Phase: — &nbsp;•&nbsp; Files: 0 &nbsp;•&nbsp; Folders: 0 &nbsp;•&nbsp; "
            "Bytes: 0 B &nbsp;•&nbsp; Rate: 0 files/s &nbsp;•&nbsp; Elapsed: 0s &nbsp;•&nbsp; ETA: —"
        )
        self.lbl_stats.setTextFormat(Qt.RichText)
        self.lbl_stats.setWordWrap(True)
        stats_layout.addWidget(self.lbl_stats)

        self.lbl_errors = QLabel("")
        self.lbl_errors.setStyleSheet("color: #b3261e;")
        self.lbl_errors.setWordWrap(True)
        stats_layout.addWidget(self.lbl_errors)

        self.active_model = QStandardItemModel(0, 1)
        self.active_model.setHorizontalHeaderLabels(["Currently scanning"])
        self.active_view = QListView()
        self.active_view.setModel(self.active_model)
        self.active_view.setMaximumHeight(120)
        stats_layout.addWidget(QLabel("Active workers:"))
        stats_layout.addWidget(self.active_view)

        root.addWidget(stats_box)

        self.status = QLabel("Idle.")
        root.addWidget(self.status)

        root.addStretch(1)

    # -- summary --------------------------------------------------------

    def refresh_summary(self) -> None:
        c = self.db.counts()
        total_size_row = self.db.query("SELECT COALESCE(SUM(size),0) AS s FROM files")
        total_bytes = int(total_size_row[0]["s"]) if total_size_row else 0
        self.lbl_summary.setText(
            f"Hosts: <b>{c['hosts']}</b> &nbsp; Shares: <b>{c['shares']}</b> &nbsp; "
            f"Folders: <b>{c['folders']:,}</b> &nbsp; Files: <b>{c['files']:,}</b> &nbsp; "
            f"Total size: <b>{human_size(total_bytes)}</b>"
        )
        # Scan-state indicator next to the Resume checkbox.
        shares = self.db.list_shares()
        total_shares = len(shares)
        done = sum(1 for s in shares if s.last_scan is not None)
        pending = total_shares - done
        if total_shares == 0:
            self.lbl_scan_state.setText("")
        elif pending == 0:
            self.lbl_scan_state.setText(
                f"<span style='color:#1b5e20;'>All {total_shares} share(s) indexed.</span>"
            )
        else:
            self.lbl_scan_state.setText(
                f"<b>{done}</b>/{total_shares} share(s) indexed, "
                f"<b>{pending}</b> pending."
            )

    # -- actions --------------------------------------------------------

    def _scan_files(self) -> None:
        shares = self.db.list_shares()
        if not shares:
            QMessageBox.warning(
                self,
                "No shares",
                "There are no shares to scan. Run share enumeration on the Credentials tab first.",
            )
            return
        resume = self.chk_resume.isChecked()
        pending = [s for s in shares if s.last_scan is None]
        if resume and not pending:
            ok = QMessageBox.question(
                self,
                "Nothing to resume",
                "Every share already has a completed scan. Run a full re-scan instead?\n\n"
                "Yes → un-check Resume and scan everything.\n"
                "No  → do nothing.",
            )
            if ok != QMessageBox.Yes:
                return
            resume = False
            self.chk_resume.setChecked(False)
        self._run(
            self.orchestrator.scan_files,
            label="Indexing files",
            max_workers=int(self.parallel_hosts.value()),
            resume=resume,
        )

    def _hash(self) -> None:
        self._run(
            self.orchestrator.hash_and_find_duplicates,
            label="Hashing duplicate candidates",
            max_workers=int(self.hash_workers.value()),
            min_size=int(self.min_size.value()),
        )

    def _run(self, fn, *, label: str, **kwargs) -> None:
        self._reset_stats()
        self.btn_scan_files.setEnabled(False)
        self.btn_hash.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.status.setText(f"{label}…")
        worker = ScanWorker(fn, **kwargs)
        worker.progress.connect(self._on_progress)
        worker.stats.connect(self._on_stats)
        worker.finished.connect(self._on_done)
        worker.failed.connect(self._on_failed)
        self._worker = worker
        self._thread = run_in_thread(worker)

    def _cancel(self) -> None:
        if self._worker:
            self._worker.cancel()
            self.status.setText(
                "Cancelling… whatever has been indexed so far is already in "
                "the database — switch to the Duplicates or Reports tab to "
                "query it, or click 'Scan all shares' again (with Resume on) "
                "to pick up where this run left off."
            )

    # -- slots ----------------------------------------------------------

    def _on_progress(self, message: str, done: int, total: int) -> None:
        self.progress_overall.setMaximum(max(total, 1))
        self.progress_overall.setValue(done)
        self.status.setText(f"{done}/{total} — {message}")

    def _on_stats(self, snap) -> None:  # snap: StatsSnapshot
        if snap is None:
            return
        self.progress_overall.setMaximum(max(snap.shares_total or snap.hosts_total, 1))
        self.progress_overall.setValue(snap.shares_done or snap.hosts_done)

        eta = _fmt_seconds(snap.eta_seconds)
        elapsed = _fmt_seconds(snap.elapsed)
        self.lbl_stats.setText(
            f"Phase: <b>{snap.phase}</b> &nbsp;•&nbsp; "
            f"Hosts: <b>{snap.hosts_done}/{snap.hosts_total}</b> &nbsp;•&nbsp; "
            f"Shares: <b>{snap.shares_done}/{snap.shares_total}</b> &nbsp;•&nbsp; "
            f"Files: <b>{snap.files_seen:,}</b> &nbsp;•&nbsp; "
            f"Folders: <b>{snap.folders_seen:,}</b> &nbsp;•&nbsp; "
            f"Bytes: <b>{human_size(snap.bytes_seen)}</b> &nbsp;•&nbsp; "
            f"Rate: <b>{snap.files_per_sec:,.0f}</b> files/s "
            f"(<b>{human_size(snap.bytes_per_sec)}/s</b>) &nbsp;•&nbsp; "
            f"Elapsed: <b>{elapsed}</b> &nbsp;•&nbsp; "
            f"ETA: <b>{eta}</b>"
        )

        if snap.errors:
            self.lbl_errors.setText(
                f"⚠ {snap.errors} error(s) so far. Latest: {snap.last_message}"
            )
        else:
            self.lbl_errors.setText("")

        # Active workers — rebuild list each snapshot (cheap, bounded by N)
        self.active_model.removeRows(0, self.active_model.rowCount())
        for unc in snap.active_workers:
            item = QStandardItem(unc)
            item.setEditable(False)
            self.active_model.appendRow(item)

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

    # -- helpers --------------------------------------------------------

    def _reset_stats(self) -> None:
        self.progress_overall.setValue(0)
        self.progress_overall.setMaximum(1)
        self.lbl_stats.setText(
            "Phase: — &nbsp;•&nbsp; Files: 0 &nbsp;•&nbsp; Folders: 0 &nbsp;•&nbsp; "
            "Bytes: 0 B &nbsp;•&nbsp; Rate: 0 files/s &nbsp;•&nbsp; Elapsed: 0s &nbsp;•&nbsp; ETA: —"
        )
        self.lbl_errors.setText("")
        self.active_model.removeRows(0, self.active_model.rowCount())
