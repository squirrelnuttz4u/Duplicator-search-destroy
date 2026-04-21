"""Tab 4 — Duplicate file report."""

from __future__ import annotations

import csv
from pathlib import Path

from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.utils.formatting import human_size

__all__ = ["DuplicatesTab"]


class DuplicatesTab(QWidget):
    def __init__(self, db: Database) -> None:
        super().__init__()
        self.db = db
        self._build_ui()
        self.refresh()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.addWidget(QLabel(
            "<b>Duplicate sets</b> found across all indexed shares, sorted by "
            "wasted bytes (size × (count − 1))."
        ))

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Min size (bytes):"))
        self.min_size = QSpinBox()
        self.min_size.setRange(0, 10_000_000)
        self.min_size.setValue(1)
        controls.addWidget(self.min_size)

        controls.addWidget(QLabel("Limit:"))
        self.limit = QSpinBox()
        self.limit.setRange(10, 10_000)
        self.limit.setValue(500)
        controls.addWidget(self.limit)

        self.btn_refresh = QPushButton("Refresh")
        self.btn_refresh.clicked.connect(self.refresh)
        controls.addWidget(self.btn_refresh)

        self.btn_export = QPushButton("Export CSV…")
        self.btn_export.clicked.connect(self._export_csv)
        controls.addWidget(self.btn_export)

        controls.addStretch(1)
        root.addLayout(controls)

        self.model = QStandardItemModel(0, 5)
        self.model.setHorizontalHeaderLabels(
            ["Hash (blake3)", "Size per file", "Copies", "Wasted", "Locations"]
        )
        self.table = QTableView()
        self.table.setModel(self.model)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setSortingEnabled(False)
        root.addWidget(self.table, 1)

        self.lbl_total = QLabel("")
        root.addWidget(self.lbl_total)

    def refresh(self) -> None:
        self.model.removeRows(0, self.model.rowCount())
        sets = self.db.duplicate_sets(min_size=int(self.min_size.value()), limit=int(self.limit.value()))
        wasted_total = 0
        for d in sets:
            wasted_total += d.wasted_bytes
            locations = "\n".join(f.relative_path for f in d.files[:20])
            items = [
                QStandardItem((d.full_hash or "—")[:16] + "…"),
                QStandardItem(human_size(d.size)),
                QStandardItem(str(d.count)),
                QStandardItem(human_size(d.wasted_bytes)),
                QStandardItem(locations),
            ]
            for it in items:
                it.setEditable(False)
            self.model.appendRow(items)
        self.table.resizeColumnsToContents()
        self.lbl_total.setText(
            f"<b>{len(sets):,}</b> duplicate sets &nbsp;•&nbsp; "
            f"<b>{human_size(wasted_total)}</b> reclaimable."
        )

    def _export_csv(self) -> None:
        sets = self.db.duplicate_sets(min_size=int(self.min_size.value()), limit=int(self.limit.value()))
        if not sets:
            QMessageBox.information(self, "No data", "No duplicates to export.")
            return
        target, _ = QFileDialog.getSaveFileName(self, "Export duplicates", "duplicates.csv", "CSV (*.csv)")
        if not target:
            return
        out = Path(target)
        with out.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["hash", "size_bytes", "copies", "wasted_bytes", "path"])
            for d in sets:
                for f in d.files:
                    w.writerow([d.full_hash, d.size, d.count, d.wasted_bytes, f.relative_path])
        QMessageBox.information(self, "Exported", f"Wrote {out}")
