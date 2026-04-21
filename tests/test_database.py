"""Tests for the SQLite DAO layer."""

from __future__ import annotations

import time

from duplicator_search_destroy.models.database import Database


def test_schema_created(tmp_db: Database):
    rows = tmp_db.query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    names = [r["name"] for r in rows]
    for expected in ("hosts", "shares", "folders", "files", "credentials", "scan_runs"):
        assert expected in names


def test_upsert_host_and_list(tmp_db: Database):
    h1 = tmp_db.upsert_host("10.0.0.1", hostname="srv1", status="online")
    h2 = tmp_db.upsert_host("10.0.0.2", hostname="srv2", status="offline")
    assert h1 != h2
    hosts = tmp_db.list_hosts()
    assert [h.ip for h in hosts] == ["10.0.0.1", "10.0.0.2"]
    assert hosts[0].hostname == "srv1"
    # Upsert updates status
    tmp_db.upsert_host("10.0.0.1", status="offline")
    hosts = tmp_db.list_hosts()
    assert [h.status for h in hosts if h.ip == "10.0.0.1"][0] == "offline"


def test_credentials_round_trip(tmp_db: Database):
    host_id = tmp_db.upsert_host("10.0.0.5", status="online")
    tmp_db.set_credentials(host_id, "admin", "hunter2!", domain="CORP")
    cred = tmp_db.get_credentials(host_id)
    assert cred is not None
    assert cred.username == "admin"
    assert cred.password == "hunter2!"
    assert cred.domain == "CORP"


def test_apply_credentials_to_all(tmp_db: Database):
    for ip in ("10.0.0.1", "10.0.0.2", "10.0.0.3"):
        tmp_db.upsert_host(ip, status="online")
    n = tmp_db.apply_credentials_to_all("administrator", "s3cret", domain="CORP")
    assert n == 3
    for h in tmp_db.list_hosts():
        cred = tmp_db.get_credentials(h.id)
        assert cred and cred.password == "s3cret"


def test_shares_and_folders(tmp_db: Database):
    hid = tmp_db.upsert_host("10.0.0.1", status="online")
    sid = tmp_db.upsert_share(hid, "DATA", remark="Shared data", accessible=True)
    # Idempotent upsert:
    sid2 = tmp_db.upsert_share(hid, "DATA", remark="Shared data (updated)", accessible=True)
    assert sid == sid2
    shares = tmp_db.list_shares(hid)
    assert len(shares) == 1 and shares[0].name == "DATA"


def test_file_insertion_and_search(tmp_db: Database):
    hid = tmp_db.upsert_host("10.0.0.1", hostname="srv1", status="online")
    sid = tmp_db.upsert_share(hid, "DATA", accessible=True)
    tmp_db.insert_folders(
        [(sid, None, "", "", 0, 2, 300, time.time(), time.time())]
    )
    folder_id = tmp_db.query("SELECT id FROM folders WHERE share_id=?", (sid,))[0]["id"]
    tmp_db.insert_files(
        [
            (sid, folder_id, "a.txt", "txt", "a.txt", 100, 0, 0, 0),
            (sid, folder_id, "b.log", "log", "b.log", 200, 0, 0, 0),
        ]
    )
    counts = tmp_db.counts()
    assert counts["files"] == 2
    # search is case-insensitive via LIKE on most platforms for ASCII
    found = tmp_db.search("a.txt")
    assert any(f.name == "a.txt" for f in found)
    largest = tmp_db.largest_files(10)
    assert largest[0].name == "b.log"


def test_duplicate_detection_end_to_end(tmp_db: Database):
    hid = tmp_db.upsert_host("10.0.0.1", status="online")
    sid = tmp_db.upsert_share(hid, "DATA", accessible=True)
    tmp_db.insert_folders([(sid, None, "", "", 0, 3, 3000, 0, 0)])
    folder_id = tmp_db.query("SELECT id FROM folders WHERE share_id=?", (sid,))[0]["id"]
    tmp_db.insert_files(
        [
            (sid, folder_id, "a.bin", "bin", "a.bin", 1000, 0, 0, 0),
            (sid, folder_id, "b.bin", "bin", "b.bin", 1000, 0, 0, 0),
            (sid, folder_id, "c.bin", "bin", "c.bin", 1000, 0, 0, 0),
            (sid, folder_id, "d.bin", "bin", "d.bin", 500, 0, 0, 0),
        ]
    )
    groups = tmp_db.size_bucket_candidates()
    sizes = {size for size, _ in groups}
    assert sizes == {1000}

    # Simulate hashing: mark a, b same hash; c different
    rows = tmp_db.query("SELECT id, name FROM files ORDER BY name")
    updates = []
    now = time.time()
    for r in rows:
        if r["name"] == "a.bin":
            updates.append(("p1", "s1", "HASH_AB", now, r["id"]))
        elif r["name"] == "b.bin":
            updates.append(("p1", "s1", "HASH_AB", now, r["id"]))
        elif r["name"] == "c.bin":
            updates.append(("p2", "s2", "HASH_C", now, r["id"]))
    tmp_db.update_file_hashes(updates)

    dups = tmp_db.duplicate_sets()
    assert len(dups) == 1
    assert dups[0].full_hash == "HASH_AB"
    assert dups[0].count == 2
    assert dups[0].wasted_bytes == 1000


def test_scan_run_lifecycle(tmp_db: Database):
    rid = tmp_db.start_run("test", "starting")
    tmp_db.finish_run(rid, "done", "ok")
    row = tmp_db.query("SELECT * FROM scan_runs WHERE id=?", (rid,))[0]
    assert row["status"] == "done"
    assert row["finished_at"] is not None
