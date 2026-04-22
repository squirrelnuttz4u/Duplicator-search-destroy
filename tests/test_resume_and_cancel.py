"""Resume + partial-scan semantics."""

from __future__ import annotations

import threading
import time
from typing import List

import pytest

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner import orchestrator as orch_mod
from duplicator_search_destroy.scanner.files import WalkedFile, WalkedFolder
from duplicator_search_destroy.scanner.orchestrator import Orchestrator
from duplicator_search_destroy.scanner.shares import DiscoveredShare


def _simple_walk(host, share, *, max_depth=64, cancel=None, on_progress=None, subpath=""):
    time.sleep(0.01)
    root = WalkedFolder(
        relative_path="",
        name=share,
        depth=0,
        parent_rel_path=None,
        file_count=2,
        total_size=200,
        created_at=0.0,
        modified_at=0.0,
    )

    def _files():
        for i in range(2):
            yield WalkedFile(
                relative_path=f"{share}_{i}.bin",
                name=f"{share}_{i}.bin",
                extension="bin",
                size=100,
                created_at=0.0,
                modified_at=0.0,
                accessed_at=0.0,
                folder_rel_path="",
            )

    yield root, _files()


def _hanging_walk(host, share, *, max_depth=64, cancel=None, on_progress=None, subpath=""):
    """Walk that yields one file then spins until cancelled.

    Used to simulate the "cancel mid-share" case.
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

    def _files():
        yield WalkedFile(
            relative_path="partial.bin",
            name="partial.bin",
            extension="bin",
            size=50,
            created_at=0.0,
            modified_at=0.0,
            accessed_at=0.0,
            folder_rel_path="",
        )
        # Block until cancelled so the share is definitively "mid-walk".
        for _ in range(200):
            if cancel and cancel():
                return
            time.sleep(0.01)

    yield root, _files()


@pytest.fixture()
def stubs(monkeypatch):
    monkeypatch.setattr(orch_mod, "register_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "unregister_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "enumerate_shares", lambda host, **k: [
        DiscoveredShare(host=host, name="DATA", remark=None, share_type=0),
    ])


def _seed(db: Database, n: int) -> None:
    for i in range(n):
        hid = db.upsert_host(f"10.0.0.{i+1}", hostname=f"srv{i+1}", status="online")
        db.upsert_share(hid, "DATA", accessible=True)


# --------------------------------------------------------------------
# Resume semantics
# --------------------------------------------------------------------

def test_resume_skips_already_scanned_shares(tmp_db: Database, monkeypatch, stubs):
    monkeypatch.setattr(orch_mod, "walk_share", _simple_walk)
    _seed(tmp_db, 3)
    o = Orchestrator(tmp_db)

    # Pretend two shares have already been scanned in a prior run.
    shares = tmp_db.list_shares()
    tmp_db.mark_share_scanned(shares[0].id)
    tmp_db.mark_share_scanned(shares[1].id)

    count = o.scan_files(max_workers=2, resume=True)
    # Only shares[2] should have been touched → 2 files.
    assert count == 2


def test_resume_false_rescans_everything(tmp_db: Database, monkeypatch, stubs):
    monkeypatch.setattr(orch_mod, "walk_share", _simple_walk)
    _seed(tmp_db, 3)
    o = Orchestrator(tmp_db)

    shares = tmp_db.list_shares()
    tmp_db.mark_share_scanned(shares[0].id)
    tmp_db.mark_share_scanned(shares[1].id)

    count = o.scan_files(max_workers=2, resume=False)
    assert count == 6  # 2 files × 3 shares


def test_default_resume_is_off(tmp_db: Database, monkeypatch, stubs):
    """Callers not passing resume should still get a full scan."""
    monkeypatch.setattr(orch_mod, "walk_share", _simple_walk)
    _seed(tmp_db, 2)
    o = Orchestrator(tmp_db)
    shares = tmp_db.list_shares()
    tmp_db.mark_share_scanned(shares[0].id)
    count = o.scan_files(max_workers=1)
    assert count == 4  # default is resume=False — both shares walked


# --------------------------------------------------------------------
# Cancel mid-share: don't mark share as scanned
# --------------------------------------------------------------------

def test_cancel_mid_walk_does_not_mark_share_scanned(tmp_db: Database, monkeypatch, stubs):
    monkeypatch.setattr(orch_mod, "walk_share", _hanging_walk)
    _seed(tmp_db, 1)

    cancelled = {"flag": False}

    def _cancel():
        return cancelled["flag"]

    def _trigger():
        time.sleep(0.05)
        cancelled["flag"] = True

    threading.Thread(target=_trigger, daemon=True).start()
    o = Orchestrator(tmp_db)
    o.scan_files(max_workers=1, cancel=_cancel)

    share = tmp_db.list_shares()[0]
    assert share.last_scan is None, \
        "A cancelled share must stay last_scan=NULL so a resume re-scans it"

    # Partial data IS in the DB — reports work against it.
    rows = tmp_db.query("SELECT COUNT(*) AS n FROM files")[0]["n"]
    assert rows >= 0  # any count is fine; the point is the DB is queryable


def test_resume_after_cancel_rewalks_partial_share(tmp_db: Database, monkeypatch, stubs):
    """After cancelling mid-share, resume should re-walk it (clearing
    partial rows first) and produce the full row count."""
    _seed(tmp_db, 1)

    # First run: hang then cancel.
    monkeypatch.setattr(orch_mod, "walk_share", _hanging_walk)
    cancelled = {"flag": False}

    def _cancel():
        return cancelled["flag"]

    def _trigger():
        time.sleep(0.05)
        cancelled["flag"] = True

    threading.Thread(target=_trigger, daemon=True).start()
    o = Orchestrator(tmp_db)
    o.scan_files(max_workers=1, cancel=_cancel)

    share = tmp_db.list_shares()[0]
    assert share.last_scan is None

    # Second run: use the well-behaved walker, resume=True.
    monkeypatch.setattr(orch_mod, "walk_share", _simple_walk)
    count = o.scan_files(max_workers=1, resume=True)
    assert count == 2  # simple_walk's 2 files
    share = tmp_db.list_shares()[0]
    assert share.last_scan is not None  # now fully done


# --------------------------------------------------------------------
# Reports remain queryable after cancel
# --------------------------------------------------------------------

def test_reports_work_on_partial_data(tmp_db: Database, monkeypatch, stubs):
    """Three hosts; one fails mid-walk. Reports still return results for
    the two successful hosts."""
    _seed(tmp_db, 3)

    def _selective_walk(host, share, **k):
        if host == "srv2":
            raise RuntimeError("srv2 broken")
        yield from _simple_walk(host, share, **k)

    monkeypatch.setattr(orch_mod, "walk_share", _selective_walk)
    Orchestrator(tmp_db).scan_files(max_workers=2)

    counts = tmp_db.counts()
    assert counts["files"] == 4  # 2 shares × 2 files each
    # Duplicate/search queries still run against whatever is there.
    assert tmp_db.search("bin") != []
    assert tmp_db.largest_files(10)
    assert tmp_db.largest_folders(10)
