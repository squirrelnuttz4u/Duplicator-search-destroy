"""Tab 5 — Search & reports."""

from __future__ import annotations

from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QTableView,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.utils.formatting import human_size, human_time

__all__ = ["ReportsTab"]


class ReportsTab(QWidget):
    def __init__(self, db: Database) -> None:
        super().__init__()
        self.db = db
        self._build_ui()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        inner = QTabWidget()
        inner.addTab(self._build_search_tab(), "Search")
        inner.addTab(self._build_largest_files_tab(), "Largest files")
        inner.addTab(self._build_largest_folders_tab(), "Largest folders")
        root.addWidget(inner)

    # -- Sub-tab: search --------------------------------------------------

    def _build_search_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        controls = QHBoxLayout()
        controls.addWidget(QLabel("Query (path or filename):"))
        self.search_edit = QLineEdit()
        self.search_edit.returnPressed.connect(self._do_search)
        controls.addWidget(self.search_edit)
        btn = QPushButton("Search")
        btn.clicked.connect(self._do_search)
        controls.addWidget(btn)
        layout.addLayout(controls)

        self.search_model = QStandardItemModel(0, 5)
        self.search_model.setHorizontalHeaderLabels(
            ["Filename", "Path", "Size", "Modified", "Share"]
        )
        self.search_table = QTableView()
        self.search_table.setModel(self.search_model)
        self.search_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.search_table.setEditTriggers(QTableView.NoEditTriggers)
        layout.addWidget(self.search_table, 1)
        self.search_summary = QLabel("")
        layout.addWidget(self.search_summary)
        return w

    def _do_search(self) -> None:
        q = self.search_edit.text().strip()
        self.search_model.removeRows(0, self.search_model.rowCount())
        if not q:
            self.search_summary.setText("")
            return
        rows = self.db.search(q, limit=1000)
        # Load share/host map in one query for display
        share_map = {
            s.id: s for s in self.db.list_shares()
        }
        host_map = {h.id: h for h in self.db.list_hosts()}
        total_size = 0
        for f in rows:
            share = share_map.get(f.share_id)
            host = host_map.get(share.host_id) if share else None
            where = f"\\\\{(host.hostname or host.ip) if host else '?'}\\{share.name if share else '?'}"
            total_size += f.size
            items = [
                QStandardItem(f.name),
                QStandardItem(f.relative_path),
                QStandardItem(human_size(f.size)),
                QStandardItem(human_time(f.modified_at)),
                QStandardItem(where),
            ]
            for it in items:
                it.setEditable(False)
            self.search_model.appendRow(items)
        self.search_table.resizeColumnsToContents()
        self.search_summary.setText(f"{len(rows):,} match(es) — {human_size(total_size)} total.")

    # -- Sub-tab: largest files ------------------------------------------

    def _build_largest_files_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        ctl = QHBoxLayout()
        ctl.addWidget(QLabel("Top N:"))
        self.largest_files_limit = QSpinBox()
        self.largest_files_limit.setRange(10, 5000)
        self.largest_files_limit.setValue(100)
        ctl.addWidget(self.largest_files_limit)
        btn = QPushButton("Refresh")
        btn.clicked.connect(self._refresh_largest_files)
        ctl.addWidget(btn)
        ctl.addStretch(1)
        layout.addLayout(ctl)

        self.largest_files_model = QStandardItemModel(0, 4)
        self.largest_files_model.setHorizontalHeaderLabels(["Size", "Name", "Path", "Modified"])
        self.largest_files_table = QTableView()
        self.largest_files_table.setModel(self.largest_files_model)
        self.largest_files_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        layout.addWidget(self.largest_files_table, 1)
        return w

    def _refresh_largest_files(self) -> None:
        self.largest_files_model.removeRows(0, self.largest_files_model.rowCount())
        for f in self.db.largest_files(int(self.largest_files_limit.value())):
            items = [
                QStandardItem(human_size(f.size)),
                QStandardItem(f.name),
                QStandardItem(f.relative_path),
                QStandardItem(human_time(f.modified_at)),
            ]
            for it in items:
                it.setEditable(False)
            self.largest_files_model.appendRow(items)
        self.largest_files_table.resizeColumnsToContents()

    # -- Sub-tab: largest folders ----------------------------------------

    def _build_largest_folders_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        ctl = QHBoxLayout()
        ctl.addWidget(QLabel("Top N:"))
        self.largest_folders_limit = QSpinBox()
        self.largest_folders_limit.setRange(10, 5000)
        self.largest_folders_limit.setValue(100)
        ctl.addWidget(self.largest_folders_limit)
        btn = QPushButton("Refresh")
        btn.clicked.connect(self._refresh_largest_folders)
        ctl.addWidget(btn)
        ctl.addStretch(1)
        layout.addLayout(ctl)

        self.largest_folders_model = QStandardItemModel(0, 4)
        self.largest_folders_model.setHorizontalHeaderLabels(
            ["Total size", "File count", "Depth", "Path"]
        )
        self.largest_folders_table = QTableView()
        self.largest_folders_table.setModel(self.largest_folders_model)
        self.largest_folders_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        layout.addWidget(self.largest_folders_table, 1)
        return w

    def _refresh_largest_folders(self) -> None:
        self.largest_folders_model.removeRows(0, self.largest_folders_model.rowCount())
        for f in self.db.largest_folders(int(self.largest_folders_limit.value())):
            items = [
                QStandardItem(human_size(f.total_size)),
                QStandardItem(str(f.file_count)),
                QStandardItem(str(f.depth)),
                QStandardItem(f.relative_path or "/"),
            ]
            for it in items:
                it.setEditable(False)
            self.largest_folders_model.appendRow(items)
        self.largest_folders_table.resizeColumnsToContents()
