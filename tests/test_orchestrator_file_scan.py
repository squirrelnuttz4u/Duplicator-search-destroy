"""Integration test for the file-scan orchestration.

Stubs out the SMB walker so we can exercise the DB plumbing without a
real server. Verifies that folder parent_id gets resolved, file rows are
written, summary counts are right, and re-running a scan overwrites cleanly.
"""

from __future__ import annotations

from typing import Iterator, List

import pytest

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner import files as files_mod
from duplicator_search_destroy.scanner import orchestrator as orch_mod
from duplicator_search_destroy.scanner.files import WalkedFile, WalkedFolder
from duplicator_search_destroy.scanner.orchestrator import Orchestrator


def _fake_walk(host, share, *, max_depth=64, cancel=None, on_progress=None, subpath=""):
    """Return a fake share tree::

        /              (root)
        /subdir        (one subfolder)
          file1.txt    (100 bytes)
          file2.log    (200 bytes)
    """
    root = WalkedFolder(
        relative_path="",
        name=share,
        depth=0,
        parent_rel_path=None,
        file_count=0,
        total_size=0,
        created_at=0.0,
        modified_at=0.0,
    )
    sub = WalkedFolder(
        relative_path="subdir",
        name="subdir",
        depth=1,
        parent_rel_path="",
        file_count=2,
        total_size=300,
        created_at=0.0,
        modified_at=0.0,
    )

    def root_files() -> Iterator[WalkedFile]:
        return iter(())

    def sub_files() -> Iterator[WalkedFile]:
        yield WalkedFile(
            relative_path="subdir\\file1.txt",
            name="file1.txt",
            extension="txt",
            size=100,
            created_at=0.0,
            modified_at=0.0,
            accessed_at=0.0,
            folder_rel_path="subdir",
        )
        yield WalkedFile(
            relative_path="subdir\\file2.log",
            name="file2.log",
            extension="log",
            size=200,
            created_at=0.0,
            modified_at=0.0,
            accessed_at=0.0,
            folder_rel_path="subdir",
        )

    yield root, root_files()
    yield sub, sub_files()


@pytest.fixture()
def patched_walk(monkeypatch):
    monkeypatch.setattr(orch_mod, "walk_share", _fake_walk)
    monkeypatch.setattr(orch_mod, "register_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "unregister_session", lambda *a, **k: None)


def test_scan_files_indexes_tree(tmp_db: Database, patched_walk):
    hid = tmp_db.upsert_host("10.0.0.1", hostname="srv1", status="online")
    tmp_db.upsert_share(hid, "DATA", accessible=True)
    o = Orchestrator(tmp_db)
    count = o.scan_files()
    assert count == 2
    # folders + files landed
    folders = tmp_db.query("SELECT * FROM folders ORDER BY depth")
    assert len(folders) == 2
    files = tmp_db.query("SELECT * FROM files ORDER BY name")
    assert [f["name"] for f in files] == ["file1.txt", "file2.log"]
    # parent_id resolved for depth-1 folder
    sub = [r for r in folders if r["relative_path"] == "subdir"][0]
    assert sub["parent_id"] is not None


def test_rescan_overwrites(tmp_db: Database, patched_walk):
    hid = tmp_db.upsert_host("10.0.0.1", hostname="srv1", status="online")
    tmp_db.upsert_share(hid, "DATA", accessible=True)
    o = Orchestrator(tmp_db)
    o.scan_files()
    o.scan_files()  # second run
    files = tmp_db.query("SELECT COUNT(*) AS n FROM files")[0]["n"]
    assert files == 2
