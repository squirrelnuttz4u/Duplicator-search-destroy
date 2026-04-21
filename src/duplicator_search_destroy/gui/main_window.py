"""Top-level main window — wires the five tabs together."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtGui import QAction
from PySide6.QtWidgets import QFileDialog, QLabel, QMainWindow, QMessageBox, QTabWidget

from duplicator_search_destroy import __version__
from duplicator_search_destroy.gui.credentials_tab import CredentialsTab
from duplicator_search_destroy.gui.dashboard_tab import DashboardTab
from duplicator_search_destroy.gui.duplicates_tab import DuplicatesTab
from duplicator_search_destroy.gui.reports_tab import ReportsTab
from duplicator_search_destroy.gui.scan_tab import ScanTab
from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner.orchestrator import Orchestrator

__all__ = ["MainWindow"]


class MainWindow(QMainWindow):
    def __init__(self, db: Database) -> None:
        super().__init__()
        self.db = db
        self.orchestrator = Orchestrator(db)

        self.setWindowTitle(f"Duplicator Search & Destroy v{__version__}")
        self.resize(1180, 760)

        self.tabs = QTabWidget()
        self.dashboard = DashboardTab(db, self.orchestrator)
        self.credentials = CredentialsTab(db, self.orchestrator)
        self.scan = ScanTab(db, self.orchestrator)
        self.duplicates = DuplicatesTab(db)
        self.reports = ReportsTab(db)

        self.tabs.addTab(self.dashboard, "1 · Dashboard")
        self.tabs.addTab(self.credentials, "2 · Credentials & Shares")
        self.tabs.addTab(self.scan, "3 · Scan & Dedup")
        self.tabs.addTab(self.duplicates, "4 · Duplicates")
        self.tabs.addTab(self.reports, "5 · Reports")
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self.setCentralWidget(self.tabs)

        self.statusBar().showMessage(f"DB: {db.path}")
        self._build_menu()

    def _build_menu(self) -> None:
        file_menu = self.menuBar().addMenu("&File")

        act_switch = QAction("Switch database…", self)
        act_switch.triggered.connect(self._switch_database)
        file_menu.addAction(act_switch)

        act_clear = QAction("Clear all data…", self)
        act_clear.triggered.connect(self._clear_database)
        file_menu.addAction(act_clear)

        file_menu.addSeparator()
        act_quit = QAction("Quit", self)
        act_quit.setShortcut("Ctrl+Q")
        act_quit.triggered.connect(self.close)
        file_menu.addAction(act_quit)

        help_menu = self.menuBar().addMenu("&Help")
        act_about = QAction("About", self)
        act_about.triggered.connect(self._about)
        help_menu.addAction(act_about)

    def _on_tab_changed(self, index: int) -> None:
        w = self.tabs.widget(index)
        if hasattr(w, "refresh_hosts"):
            w.refresh_hosts()
        if hasattr(w, "refresh_summary"):
            w.refresh_summary()
        if hasattr(w, "refresh") and not hasattr(w, "refresh_hosts") and not hasattr(w, "refresh_summary"):
            w.refresh()

    def _switch_database(self) -> None:
        target, _ = QFileDialog.getSaveFileName(
            self, "Select or create database", str(self.db.path), "SQLite (*.db)"
        )
        if not target:
            return
        QMessageBox.information(
            self,
            "Restart required",
            "The application needs to restart to open a different database.\n\n"
            f"Run it again with:  --db {target}",
        )

    def _clear_database(self) -> None:
        ok = QMessageBox.question(
            self,
            "Clear all data",
            "This deletes every host, share, file and folder record in the database. Continue?",
        )
        if ok != QMessageBox.Yes:
            return
        self.db.execute("DELETE FROM files")
        self.db.execute("DELETE FROM folders")
        self.db.execute("DELETE FROM shares")
        self.db.execute("DELETE FROM credentials")
        self.db.execute("DELETE FROM hosts")
        self.db.execute("DELETE FROM scan_runs")
        self.db.execute("VACUUM")
        for tab in (self.dashboard, self.credentials, self.scan, self.duplicates, self.reports):
            if hasattr(tab, "refresh_hosts"):
                tab.refresh_hosts()
            if hasattr(tab, "refresh_summary"):
                tab.refresh_summary()
            if hasattr(tab, "refresh"):
                try:
                    tab.refresh()
                except Exception:
                    pass
        QMessageBox.information(self, "Cleared", "Database reset.")

    def _about(self) -> None:
        QMessageBox.about(
            self,
            "About",
            f"<h3>Duplicator Search &amp; Destroy</h3>"
            f"<p>Version {__version__}</p>"
            "<p>Network file inventory and duplicate detector for Windows SMB shares.</p>"
            "<p>https://github.com/squirrelnuttz4u/duplicator-search-destroy</p>",
        )
