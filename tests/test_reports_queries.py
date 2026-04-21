"""Round-2 report/query tests."""

from __future__ import annotations

from duplicator_search_destroy.models.database import Database


def _seed(db: Database) -> int:
    hid = db.upsert_host("10.0.0.1", status="online")
    sid = db.upsert_share(hid, "DATA", accessible=True)
    db.insert_folders(
        [
            (sid, None, "", "", 0, 0, 0, 0, 0),
            (sid, None, "sub", "sub", 1, 3, 600, 0, 0),
        ]
    )
    folder_id = db.query("SELECT id FROM folders WHERE relative_path='sub'")[0]["id"]
    db.insert_files(
        [
            (sid, folder_id, "A.TXT", "txt", "sub\\A.TXT", 100, 0, 0, 0),
            (sid, folder_id, "b.txt", "txt", "sub\\b.txt", 200, 0, 0, 0),
            (sid, folder_id, "big.bin", "bin", "sub\\big.bin", 300, 0, 0, 0),
        ]
    )
    return sid


def test_search_matches_by_name_and_path(tmp_db: Database):
    _seed(tmp_db)
    # Match by partial name
    r = tmp_db.search("big")
    assert [f.name for f in r] == ["big.bin"]
    # Match by path fragment
    r2 = tmp_db.search("sub\\")
    assert len(r2) == 3


def test_search_limit_enforced(tmp_db: Database):
    _seed(tmp_db)
    r = tmp_db.search("", limit=1)
    # Empty string LIKE %% matches all; limit cuts it to 1.
    assert len(r) == 1


def test_largest_files_sorted(tmp_db: Database):
    _seed(tmp_db)
    r = tmp_db.largest_files(10)
    sizes = [f.size for f in r]
    assert sizes == sorted(sizes, reverse=True)


def test_largest_folders_sorted(tmp_db: Database):
    _seed(tmp_db)
    r = tmp_db.largest_folders(10)
    assert r[0].total_size >= r[-1].total_size


def test_counts_on_empty_db(tmp_db: Database):
    c = tmp_db.counts()
    assert c == {"hosts": 0, "shares": 0, "folders": 0, "files": 0}


def test_duplicate_sets_empty_on_no_hashes(tmp_db: Database):
    _seed(tmp_db)
    # No hashes populated yet
    assert tmp_db.duplicate_sets() == []


def test_delete_host_cascades(tmp_db: Database):
    sid = _seed(tmp_db)
    hid = tmp_db.list_hosts()[0].id
    tmp_db.delete_host(hid)
    assert tmp_db.counts() == {"hosts": 0, "shares": 0, "folders": 0, "files": 0}
