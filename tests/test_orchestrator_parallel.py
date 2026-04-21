"""Parallel scan + rich progress tests.

Verifies that:
* scan_files launches concurrent per-host workers
* stats snapshots are emitted during the run
* the final file/folder counts are correct under parallelism
* errors on one host don't block the rest
* enumerate_shares also runs hosts in parallel
"""

from __future__ import annotations

import threading
import time
from typing import List

import pytest

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner import orchestrator as orch_mod
from duplicator_search_destroy.scanner.files import WalkedFile, WalkedFolder
from duplicator_search_destroy.scanner.orchestrator import Orchestrator
from duplicator_search_destroy.scanner.progress import StatsSnapshot
from duplicator_search_destroy.scanner.shares import DiscoveredShare


# ------------------------------------------------------------------
# Stubs
# ------------------------------------------------------------------

def _slow_walk(host, share, *, max_depth=64, cancel=None, on_progress=None, subpath=""):
    """Yield one folder + 3 files after a short sleep to make concurrency observable."""
    time.sleep(0.05)
    root = WalkedFolder(
        relative_path="",
        name=share,
        depth=0,
        parent_rel_path=None,
        file_count=3,
        total_size=300,
        created_at=0.0,
        modified_at=0.0,
    )

    def _files():
        for i in range(3):
            yield WalkedFile(
                relative_path=f"f{i}.bin",
                name=f"f{i}.bin",
                extension="bin",
                size=100,
                created_at=0.0,
                modified_at=0.0,
                accessed_at=0.0,
                folder_rel_path="",
            )

    yield root, _files()


def _boom_walk(host, share, *, max_depth=64, cancel=None, on_progress=None, subpath=""):
    raise RuntimeError(f"simulated failure on {host}/{share}")


@pytest.fixture()
def stubs(monkeypatch):
    monkeypatch.setattr(orch_mod, "register_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "unregister_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "enumerate_shares", lambda host, **k: [
        DiscoveredShare(host=host, name="DATA", remark=None, share_type=0),
    ])


def _seed_hosts(db: Database, n: int) -> None:
    for i in range(n):
        hid = db.upsert_host(f"10.0.0.{i+1}", hostname=f"srv{i+1}", status="online")
        db.upsert_share(hid, "DATA", accessible=True)


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

def test_scan_files_runs_hosts_in_parallel(tmp_db: Database, monkeypatch, stubs):
    monkeypatch.setattr(orch_mod, "walk_share", _slow_walk)
    n_hosts = 8
    _seed_hosts(tmp_db, n_hosts)

    # Observe that multiple workers overlap by recording thread idents per-share.
    active_idents: set[int] = set()
    lock = threading.Lock()
    orig_walk = _slow_walk

    def _tracking(host, share, **k):
        with lock:
            active_idents.add(threading.get_ident())
        yield from orig_walk(host, share, **k)

    monkeypatch.setattr(orch_mod, "walk_share", _tracking)

    o = Orchestrator(tmp_db)
    t0 = time.monotonic()
    count = o.scan_files(max_workers=4)
    elapsed = time.monotonic() - t0

    assert count == n_hosts * 3  # 3 files per host, 1 share per host
    # With 4 workers × 0.05s sleeps and 8 hosts we should finish in ~2 batches
    # (so well under serial time of 8 × 0.05 = 0.4s).
    assert elapsed < 0.35, f"scan didn't parallelise: elapsed={elapsed:.3f}s"
    # More than one worker thread must have been active.
    assert len(active_idents) > 1


def test_scan_files_emits_stats_snapshots(tmp_db: Database, monkeypatch, stubs):
    monkeypatch.setattr(orch_mod, "walk_share", _slow_walk)
    _seed_hosts(tmp_db, 3)
    snaps: List[StatsSnapshot] = []
    o = Orchestrator(tmp_db)
    o.scan_files(max_workers=2, on_stats=snaps.append)

    assert snaps, "no stats snapshots emitted"
    last = snaps[-1]
    assert last.phase == "files"
    assert last.shares_done == 3
    assert last.files_seen == 9


def test_scan_files_one_host_error_does_not_block_others(tmp_db: Database, monkeypatch, stubs):
    """One host raises, the other two still complete."""
    def _selective_walk(host, share, **k):
        if host == "srv2":
            raise RuntimeError("srv2 broken")
        yield from _slow_walk(host, share, **k)

    monkeypatch.setattr(orch_mod, "walk_share", _selective_walk)
    _seed_hosts(tmp_db, 3)
    snaps: List[StatsSnapshot] = []
    o = Orchestrator(tmp_db)
    count = o.scan_files(max_workers=3, on_stats=snaps.append)
    assert count == 6  # 2 × 3 files
    # Stats record the error without aborting.
    last = snaps[-1]
    assert last.errors >= 1
    assert last.shares_done == 3


def test_scan_files_respects_cancel(tmp_db: Database, monkeypatch, stubs):
    monkeypatch.setattr(orch_mod, "walk_share", _slow_walk)
    _seed_hosts(tmp_db, 20)

    cancelled = {"flag": False}

    def _cancel():
        return cancelled["flag"]

    # Fire the cancel from a side thread after a brief delay.
    def _trigger():
        time.sleep(0.02)
        cancelled["flag"] = True

    threading.Thread(target=_trigger, daemon=True).start()
    o = Orchestrator(tmp_db)
    o.scan_files(max_workers=4, cancel=_cancel)

    # Should have stopped well before completing all 20.
    files = tmp_db.query("SELECT COUNT(*) AS n FROM files")[0]["n"]
    assert files < 20 * 3


def test_enumerate_shares_parallel(tmp_db: Database, monkeypatch, stubs):
    _seed_hosts(tmp_db, 6)
    # Slow down enum_shares to make concurrency observable.
    def _slow_enum(host, **kwargs):
        time.sleep(0.04)
        return [DiscoveredShare(host=host, name="DATA", remark=None, share_type=0)]

    monkeypatch.setattr(orch_mod, "enumerate_shares", _slow_enum)
    o = Orchestrator(tmp_db)
    t0 = time.monotonic()
    n = o.enumerate_shares(max_workers=3)
    elapsed = time.monotonic() - t0
    assert n == 6
    # Serial would be 6 × 0.04 = 0.24s. With 3 workers we should see <0.2s.
    assert elapsed < 0.18, f"enumerate_shares didn't parallelise: {elapsed:.3f}s"
