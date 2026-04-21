"""Round-4: full pipeline — discover → shares → file scan → hash → dedup.

Every network-touching call is stubbed so the test is self-contained, but
it exercises every real DB write and the orchestrator's phase wiring.
"""

from __future__ import annotations

from typing import Dict, List

import pytest

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner import (
    duplicates as dedup_mod,
    orchestrator as orch_mod,
)
from duplicator_search_destroy.scanner.files import WalkedFile, WalkedFolder
from duplicator_search_destroy.scanner.hasher import HashResult
from duplicator_search_destroy.scanner.network import DiscoveredHost
from duplicator_search_destroy.scanner.orchestrator import Orchestrator
from duplicator_search_destroy.scanner.shares import DiscoveredShare


# ---- stub data ----------------------------------------------------------

HOSTS = [
    ("10.0.0.1", "srv1"),
    ("10.0.0.2", "srv2"),
    ("10.0.0.3", "srv3"),
]

SHARES_BY_HOST: Dict[str, List[DiscoveredShare]] = {
    "srv1": [
        DiscoveredShare(host="srv1", name="DATA", remark=None, share_type=0),
        DiscoveredShare(host="srv1", name="APPS", remark=None, share_type=0),
    ],
    "srv2": [
        DiscoveredShare(host="srv2", name="DATA", remark=None, share_type=0),
    ],
    "srv3": [
        DiscoveredShare(host="srv3", name="BACKUP", remark=None, share_type=0),
    ],
}

# Every share has:
#   /                  (root)
#   /report.pdf        (2048 bytes — this same file exists on srv1:DATA, srv2:DATA, srv3:BACKUP → TRIPLE dup)
#   /unique_<share>    (different content on each share)
#
#  srv1:APPS additionally has a 100-byte file identical to one on srv2:DATA


def _walk_for(host: str, share: str):
    """Return (root, [files]) in the walker's expected tuple form."""
    root = WalkedFolder(
        relative_path="",
        name=share,
        depth=0,
        parent_rel_path=None,
        file_count=2,
        total_size=2148,
        created_at=0.0,
        modified_at=0.0,
    )

    files: List[WalkedFile] = [
        WalkedFile(
            relative_path="report.pdf",
            name="report.pdf",
            extension="pdf",
            size=2048,
            created_at=0.0,
            modified_at=0.0,
            accessed_at=0.0,
            folder_rel_path="",
        ),
        WalkedFile(
            relative_path=f"unique_{share}.bin",
            name=f"unique_{share}.bin",
            extension="bin",
            size=512,
            created_at=0.0,
            modified_at=0.0,
            accessed_at=0.0,
            folder_rel_path="",
        ),
    ]
    # Extra shared file between srv1:APPS and srv2:DATA
    if (host, share) in (("srv1", "APPS"), ("srv2", "DATA")):
        files.append(
            WalkedFile(
                relative_path="shared_small.cfg",
                name="shared_small.cfg",
                extension="cfg",
                size=100,
                created_at=0.0,
                modified_at=0.0,
                accessed_at=0.0,
                folder_rel_path="",
            )
        )
    return root, files


def _stub_walk(host, share, *, max_depth=64, cancel=None, on_progress=None, subpath=""):
    root, files = _walk_for(host, share)
    yield root, iter(files)


def _stub_discover(ips, *, timeout, max_workers, on_result, cancel):
    results = []
    for ip, name in HOSTS:
        r = DiscoveredHost(ip=ip, hostname=name, port_open=True)
        results.append(r)
        if on_result:
            on_result(r)
    return results


def _stub_enum_shares(host, *, username="", password="", domain=""):
    return SHARES_BY_HOST.get(host, [])


def _deterministic_hash(unc_path: str, size: int) -> HashResult:
    """Compute hashes that make duplicates duplicates.

    Key insight: map every (share, filename) pair to content. Files with the
    same (size, name) across shares represent intentional duplicates.
    """
    parts = unc_path.strip("\\").split("\\")
    # \\srv1\DATA\report.pdf  -> parts == ['srv1', 'DATA', 'report.pdf']
    filename = parts[-1]
    # Distinguish the truly-unique files.
    if filename.startswith("unique_"):
        # Every server's unique_<share>.bin has different content
        content_key = f"content:{unc_path}:{size}"
    else:
        # report.pdf & shared_small.cfg are identical across hosts.
        content_key = f"content:{filename}:{size}"
    import hashlib

    digest = hashlib.sha256(content_key.encode()).hexdigest()
    return HashResult(size=size, prefix_hash=digest[:32], suffix_hash=digest[32:64], full_hash=digest)


# ---- fixtures -----------------------------------------------------------

@pytest.fixture()
def wired(monkeypatch):
    monkeypatch.setattr(orch_mod, "discover_hosts", _stub_discover)
    monkeypatch.setattr(orch_mod, "walk_share", _stub_walk)
    monkeypatch.setattr(orch_mod, "register_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "unregister_session", lambda *a, **k: None)
    monkeypatch.setattr(orch_mod, "enumerate_shares", _stub_enum_shares)
    monkeypatch.setattr(dedup_mod, "cascade_hash", _deterministic_hash)


# ---- the test itself ----------------------------------------------------

def test_full_workflow(tmp_db: Database, wired):
    o = Orchestrator(tmp_db)

    # Phase 1: discovery
    live = o.discover("10.0.0.0/29")
    assert len(live) == 3
    # All hosts get stored (the stub reports them all as online)
    assert all(h.status == "online" for h in tmp_db.list_hosts())

    # Phase 2: share enumeration
    total_shares = o.enumerate_shares()
    # 2 + 1 + 1
    assert total_shares == 4
    assert len(tmp_db.list_shares()) == 4

    # Phase 3: file scan
    n_files = o.scan_files()
    # 2 files on every share + 1 extra on srv1/APPS + 1 extra on srv2/DATA
    # = 2*4 + 2 = 10
    assert n_files == 10

    counts = tmp_db.counts()
    assert counts["hosts"] == 3
    assert counts["shares"] == 4
    assert counts["files"] == 10

    # Phase 4: hash + dedup
    hashed = o.hash_and_find_duplicates(min_size=1, max_workers=2)
    # Candidates are files in size-buckets with >1 member:
    #   size=2048: report.pdf appears 4 times (on every share) → 4
    #   size=100:  shared_small.cfg appears 2 times              → 2
    #   size=512:  unique_*.bin — one per share, but ALL have size=512 → 4
    # Total = 10 hashes
    assert hashed == 10

    # Report
    dups = tmp_db.duplicate_sets()
    # Distinct full_hash groups with count>1:
    #   report.pdf (size 2048)    — 4 copies → wasted = 3*2048 = 6144
    #   shared_small.cfg (100)    — 2 copies → wasted = 100
    #   unique_*.bin (size 512)   — 4 distinct hashes → each count=1 → NOT dup
    assert len(dups) == 2
    by_hash = {d.size: d for d in dups}
    assert by_hash[2048].count == 4
    assert by_hash[2048].wasted_bytes == 3 * 2048
    assert by_hash[100].count == 2
    assert by_hash[100].wasted_bytes == 100

    # Total reclaimable bytes
    total_waste = sum(d.wasted_bytes for d in dups)
    assert total_waste == 3 * 2048 + 100

    # Resumability: run hash phase again, nothing new to do
    hashed_again = o.hash_and_find_duplicates(min_size=1, max_workers=2)
    assert hashed_again == 0

    # Search still works after dedup.
    found = tmp_db.search("report")
    assert len(found) == 4
    assert all(f.name == "report.pdf" for f in found)

    # scan_runs has entries for every phase.
    runs = tmp_db.query("SELECT kind, status FROM scan_runs")
    kinds = {r["kind"] for r in runs}
    assert kinds == {"discovery", "shares", "files", "hash"}
    assert all(r["status"] == "done" for r in runs)


def test_clear_all_resets_state(tmp_db: Database, wired):
    o = Orchestrator(tmp_db)
    o.discover("10.0.0.0/29")
    o.enumerate_shares()
    o.scan_files()

    # Simulate menu → clear all
    tmp_db.execute("DELETE FROM files")
    tmp_db.execute("DELETE FROM folders")
    tmp_db.execute("DELETE FROM shares")
    tmp_db.execute("DELETE FROM credentials")
    tmp_db.execute("DELETE FROM hosts")
    tmp_db.execute("DELETE FROM scan_runs")

    assert tmp_db.counts() == {"hosts": 0, "shares": 0, "folders": 0, "files": 0}
