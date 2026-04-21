"""Tab 2 — Credentials.

Per-host DOMAIN\\user + password editor, with a shortcut to apply a single
set of credentials to every known host, and a button to enumerate shares
for the currently-configured targets.
"""

from __future__ import annotations

import logging
from typing import Optional

from PySide6.QtCore import Qt, QThread
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from duplicator_search_destroy.gui.worker import ScanWorker, run_in_thread
from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner.orchestrator import Orchestrator

log = logging.getLogger(__name__)

__all__ = ["CredentialsTab"]


class CredentialsTab(QWidget):
    def __init__(self, db: Database, orchestrator: Orchestrator) -> None:
        super().__init__()
        self.db = db
        self.orchestrator = orchestrator
        self._worker: Optional[ScanWorker] = None
        self._thread: Optional[QThread] = None
        self._build_ui()
        self.refresh()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        # "Apply to all" block
        box = QGroupBox("Apply credentials to all hosts")
        form = QFormLayout(box)
        self.all_domain = QLineEdit()
        self.all_domain.setPlaceholderText("CORP  (optional)")
        self.all_user = QLineEdit()
        self.all_user.setPlaceholderText("administrator")
        self.all_pass = QLineEdit()
        self.all_pass.setEchoMode(QLineEdit.Password)
        form.addRow("Domain", self.all_domain)
        form.addRow("Username", self.all_user)
        form.addRow("Password", self.all_pass)
        btn_row = QHBoxLayout()
        btn_apply = QPushButton("Apply to every host")
        btn_apply.clicked.connect(self._apply_all)
        btn_row.addWidget(btn_apply)
        btn_row.addStretch(1)
        form.addRow(btn_row)
        root.addWidget(box)

        # Per-host editor
        intro = QLabel(
            "Double-click a row to override credentials for a single host. "
            "The <b>Save</b> button at the bottom stores per-host edits."
        )
        intro.setWordWrap(True)
        root.addWidget(intro)

        self.model = QStandardItemModel(0, 5)
        self.model.setHorizontalHeaderLabels(["IP", "Hostname", "Domain", "Username", "Password"])
        self.table = QTableView()
        self.table.setModel(self.model)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        root.addWidget(self.table, 1)

        btns = QHBoxLayout()
        self.btn_save = QPushButton("Save edits")
        self.btn_save.clicked.connect(self._save_per_host)
        btns.addWidget(self.btn_save)

        self.btn_enum = QPushButton("Enumerate shares on all hosts")
        self.btn_enum.clicked.connect(self._enumerate)
        btns.addWidget(self.btn_enum)

        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._cancel)
        btns.addWidget(self.btn_cancel)
        btns.addStretch(1)
        root.addLayout(btns)

        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        root.addWidget(self.progress)
        self.status = QLabel("")
        root.addWidget(self.status)

    # -- actions ----------------------------------------------------------

    def _apply_all(self) -> None:
        u = self.all_user.text().strip()
        p = self.all_pass.text()
        d = self.all_domain.text().strip() or None
        if not u or not p:
            QMessageBox.warning(self, "Missing", "Username and password are required.")
            return
        n = self.db.apply_credentials_to_all(u, p, domain=d)
        QMessageBox.information(self, "Applied", f"Credentials set on {n} host(s).")
        self.refresh()

    def _save_per_host(self) -> None:
        saved = 0
        for row in range(self.model.rowCount()):
            ip = self.model.item(row, 0).text()
            domain = self.model.item(row, 2).text().strip() or None
            user = self.model.item(row, 3).text().strip()
            pwd = self.model.item(row, 4).text()
            if not user:
                continue
            host_rows = self.db.query("SELECT id FROM hosts WHERE ip = ?", (ip,))
            if not host_rows:
                continue
            host_id = host_rows[0]["id"]
            # Only store if password is non-empty OR we want to change domain/user
            # with an existing credential. Empty password+empty user => skip.
            if pwd or user:
                self.db.set_credentials(host_id, user, pwd, domain=domain)
                saved += 1
        QMessageBox.information(self, "Saved", f"Updated credentials for {saved} host(s).")

    def _enumerate(self) -> None:
        self.btn_enum.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.status.setText("Enumerating shares…")
        worker = ScanWorker(self.orchestrator.enumerate_shares)
        worker.progress.connect(self._on_progress)
        worker.finished.connect(self._on_done)
        worker.failed.connect(self._on_failed)
        self._worker = worker
        self._thread = run_in_thread(worker)

    def _cancel(self) -> None:
        if self._worker:
            self._worker.cancel()

    def _on_progress(self, message: str, done: int, total: int) -> None:
        self.progress.setMaximum(max(total, 1))
        self.progress.setValue(done)
        self.status.setText(f"{done}/{total} — {message}")

    def _on_done(self, result) -> None:  # noqa: ANN001
        self.status.setText(f"Enumeration finished — {result} shares discovered.")
        self.btn_enum.setEnabled(True)
        self.btn_cancel.setEnabled(False)

    def _on_failed(self, tb: str) -> None:
        QMessageBox.critical(self, "Enumeration failed", tb)
        self.btn_enum.setEnabled(True)
        self.btn_cancel.setEnabled(False)

    # -- refresh ----------------------------------------------------------

    def refresh(self) -> None:
        self.model.removeRows(0, self.model.rowCount())
        for h in self.db.list_hosts():
            cred = self.db.get_credentials(h.id)
            items = [
                QStandardItem(h.ip),
                QStandardItem(h.hostname or ""),
                QStandardItem((cred.domain if cred and cred.domain else "") or ""),
                QStandardItem(cred.username if cred else ""),
                QStandardItem("••••••••" if cred else ""),
            ]
            items[0].setEditable(False)
            items[1].setEditable(False)
            # Domain / user / password are editable inline.
            self.model.appendRow(items)
