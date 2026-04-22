"""Unit tests for scanner.remote_hash + hash_candidates_via_winrm.

pypsrp and a real WinRM listener aren't available in unit tests, so we
monkey-patch :func:`remote_hash_files` (and the underlying
:class:`WinRmClient`) with a fake that returns pre-canned hashes.
"""

from __future__ import annotations

import base64
import json

import pytest

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner import duplicates as dup_mod
from duplicator_search_destroy.scanner import remote_hash as rh
from duplicator_search_destroy.scanner.remote_hash import (
    RemoteHashResult,
    SUPPORTED_ALGORITHMS,
    build_remote_hash_script,
    prefix_hash,
)
from duplicator_search_destroy.scanner.winrm_client import (
    WinRmAuthError,
    WinRmConnectError,
    WinRmError,
)


# ------------------------------------------------------------------
# build_remote_hash_script
# ------------------------------------------------------------------

def test_build_remote_hash_script_encodes_payload_safely():
    # Paths that contain characters which would break naive string
    # interpolation — quotes, backticks, newlines.
    nasty = [
        "normal.bin",
        "weird name'with\"quotes.log",
        "has`backtick.sql",
    ]
    script = build_remote_hash_script(nasty, "DATA", algorithm="sha256", throttle=4)
    # The script shouldn't splice the raw strings — it should carry a
    # single base64 blob.
    for n in nasty:
        assert n not in script, f"unsafe interpolation of {n!r} into script"

    # The base64 blob should decode to the exact JSON we sent.
    import re
    m = re.search(r"\$encoded = '([A-Za-z0-9+/=]+)'", script)
    assert m, "expected base64-encoded config line"
    decoded = base64.b64decode(m.group(1)).decode("utf-8")
    payload = json.loads(decoded)
    assert payload["paths"] == nasty
    assert payload["share"] == "DATA"
    assert payload["algo"] == "SHA256"
    assert payload["throttle"] == 4


def test_build_remote_hash_script_rejects_unsupported_algorithm():
    with pytest.raises(ValueError):
        build_remote_hash_script(["a"], "DATA", algorithm="blake3")


def test_supported_algorithms_are_get_filehash_builtin():
    # Sanity: these should all be algorithms Get-FileHash accepts natively.
    assert "sha256" in SUPPORTED_ALGORITHMS
    assert "sha512" in SUPPORTED_ALGORITHMS


def test_prefix_hash_formats_consistently():
    assert prefix_hash("sha256", "ABCDEF") == "sha256:abcdef"
    assert prefix_hash("SHA256", "abcdef") == "sha256:abcdef"


# ------------------------------------------------------------------
# hash_candidates_via_winrm (orchestration-level tests)
# ------------------------------------------------------------------

def _seed_size_bucket(db: Database) -> list[int]:
    """Create one host, one share, two files of equal size that should
    dedup to each other. Return the file ids."""
    hid = db.upsert_host("10.0.0.1", hostname="srv1", status="online")
    sid = db.upsert_share(hid, "DATA", accessible=True)
    db.insert_folders([(sid, None, "", "", 0, 2, 200, 0, 0)])
    folder_id = db.query("SELECT id FROM folders WHERE share_id=?", (sid,))[0]["id"]
    db.insert_files([
        (sid, folder_id, "a.bin", "bin", "a.bin", 1000, 0, 0, 0),
        (sid, folder_id, "b.bin", "bin", "b.bin", 1000, 0, 0, 0),
    ])
    return [r["id"] for r in db.query("SELECT id FROM files ORDER BY name")]


def test_hash_candidates_via_winrm_happy_path(tmp_db: Database, monkeypatch):
    ids = _seed_size_bucket(tmp_db)

    calls: list = []

    def fake_remote(host, share, paths, **kwargs):
        calls.append((host, share, tuple(paths), kwargs.get("algorithm")))
        # Pretend both files have the same SHA256 content.
        return {
            p: RemoteHashResult(relative_path=p, hash="deadbeef" * 8, error=None)
            for p in paths
        }

    # Patch on the remote_hash module: the lazy `from … import remote_hash_files`
    # inside hash_candidates_via_winrm resolves via getattr on the module,
    # so patching the attribute here is enough.
    monkeypatch.setattr(rh, "remote_hash_files", fake_remote)

    n = dup_mod.hash_candidates_via_winrm(tmp_db)

    assert n == 2
    assert len(calls) == 1
    host, share, paths, algo = calls[0]
    assert host == "srv1"  # hostname preferred over ip
    assert share == "DATA"
    assert set(paths) == {"a.bin", "b.bin"}
    assert algo == "sha256"

    # Both files should now carry an sha256-prefixed full hash.
    rows = tmp_db.query("SELECT full_hash FROM files ORDER BY name")
    for row in rows:
        assert row["full_hash"].startswith("sha256:")

    # A dedup query should find the pair.
    dups = tmp_db.duplicate_sets()
    assert len(dups) == 1
    assert dups[0].count == 2


def test_hash_candidates_via_winrm_skips_already_hashed(tmp_db: Database, monkeypatch):
    ids = _seed_size_bucket(tmp_db)
    # Pre-hash one of the two files.
    tmp_db.update_file_hashes([(None, None, "sha256:preset", 1.0, ids[0])])

    captured_paths: list = []

    def fake_remote(host, share, paths, **kwargs):
        captured_paths.extend(paths)
        return {
            p: RemoteHashResult(relative_path=p, hash="beefcafe" * 8, error=None)
            for p in paths
        }

    monkeypatch.setattr(rh, "remote_hash_files", fake_remote)
    dup_mod.hash_candidates_via_winrm(tmp_db)

    # Only b.bin was pending.
    assert captured_paths == ["b.bin"]


def test_hash_candidates_via_winrm_handles_remote_errors(tmp_db: Database, monkeypatch):
    _seed_size_bucket(tmp_db)

    def fake_remote(host, share, paths, **kwargs):
        return {
            "a.bin": RemoteHashResult(relative_path="a.bin", hash="deadbeef" * 8, error=None),
            "b.bin": RemoteHashResult(relative_path="b.bin", hash=None, error="Access denied"),
        }

    monkeypatch.setattr(rh, "remote_hash_files", fake_remote)
    n = dup_mod.hash_candidates_via_winrm(tmp_db)
    assert n == 2  # both attempted
    hashes = [r["full_hash"] for r in tmp_db.query("SELECT full_hash FROM files ORDER BY name")]
    assert hashes[0] is not None and hashes[0].startswith("sha256:")
    assert hashes[1] is None  # error case — full_hash stays NULL


def test_hash_candidates_via_winrm_falls_back_to_smb(tmp_db: Database, monkeypatch):
    """When WinRM throws, the SMB cascade_hash should pick up for that host."""
    _seed_size_bucket(tmp_db)

    def fake_remote(*a, **k):
        raise WinRmConnectError("could not reach host")

    smb_calls: list = []

    def fake_cascade(unc, size):
        smb_calls.append(unc)
        from duplicator_search_destroy.scanner.hasher import HashResult
        return HashResult(size=size, prefix_hash="p", suffix_hash="s",
                          full_hash=f"blake3:fake{size}")

    monkeypatch.setattr(rh, "remote_hash_files", fake_remote)
    monkeypatch.setattr(dup_mod, "cascade_hash", fake_cascade)

    n = dup_mod.hash_candidates_via_winrm(tmp_db, fallback_to_smb=True)
    assert n == 2
    assert len(smb_calls) == 2
    for call in smb_calls:
        assert call.startswith("\\\\srv1\\DATA\\")

    # SMB fallback used BLAKE3, so the hashes carry a blake3: prefix.
    rows = tmp_db.query("SELECT full_hash FROM files ORDER BY name")
    for row in rows:
        assert row["full_hash"].startswith("blake3:")


def test_hash_candidates_via_winrm_no_fallback_leaves_hashes_null(tmp_db: Database, monkeypatch):
    _seed_size_bucket(tmp_db)

    def fake_remote(*a, **k):
        raise WinRmAuthError("bad creds")

    monkeypatch.setattr(rh, "remote_hash_files", fake_remote)
    n = dup_mod.hash_candidates_via_winrm(tmp_db, fallback_to_smb=False)
    assert n == 0
    rows = tmp_db.query("SELECT full_hash FROM files")
    for row in rows:
        assert row["full_hash"] is None


# ------------------------------------------------------------------
# Algorithm-prefix integrity — BLAKE3 and SHA256 must not cross-match
# ------------------------------------------------------------------

def test_different_algorithms_do_not_cross_dedup(tmp_db: Database):
    """Two files with the same underlying bytes hashed with different algos
    should NOT show up in the same duplicate group, because the stored
    string includes the algorithm prefix."""
    hid = tmp_db.upsert_host("10.0.0.1", status="online")
    sid = tmp_db.upsert_share(hid, "DATA", accessible=True)
    tmp_db.insert_folders([(sid, None, "", "", 0, 2, 200, 0, 0)])
    fid = tmp_db.query("SELECT id FROM folders WHERE share_id=?", (sid,))[0]["id"]
    tmp_db.insert_files([
        (sid, fid, "a.bin", "bin", "a.bin", 1000, 0, 0, 0),
        (sid, fid, "b.bin", "bin", "b.bin", 1000, 0, 0, 0),
    ])
    ids = [r["id"] for r in tmp_db.query("SELECT id FROM files ORDER BY name")]
    # Same underlying hex, different algo tag.
    tmp_db.update_file_hashes([
        (None, None, "blake3:ff00ff00", 1.0, ids[0]),
        (None, None, "sha256:ff00ff00", 1.0, ids[1]),
    ])
    dups = tmp_db.duplicate_sets()
    assert dups == []
