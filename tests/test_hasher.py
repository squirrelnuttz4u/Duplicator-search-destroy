"""Hash-engine tests on local files (no SMB needed)."""

from __future__ import annotations

import os
import random
import string

from duplicator_search_destroy.scanner.hasher import (
    PREFIX_BYTES,
    cascade_hash,
    hash_full,
    hash_local_bytes,
    hash_local_file,
    hash_prefix,
    hash_suffix,
)


def _write(path, content: bytes):
    with open(path, "wb") as fh:
        fh.write(content)


def test_prefix_hash_stable(tmp_path):
    p = tmp_path / "a.bin"
    _write(p, b"hello world" * 10)
    assert hash_prefix(str(p)) == hash_prefix(str(p))


def test_prefix_different_for_different_content(tmp_path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    _write(a, b"hello world")
    _write(b, b"hello WORLD")
    assert hash_prefix(str(a)) != hash_prefix(str(b))


def test_full_hash_matches_cascade(tmp_path):
    p = tmp_path / "big.bin"
    data = bytes(random.choices(range(256), k=2 * PREFIX_BYTES + 1234))
    _write(p, data)
    r = cascade_hash(str(p), len(data))
    assert r.full_hash is not None
    assert r.full_hash == hash_full(str(p), size=len(data))


def test_cascade_identical_files(tmp_path):
    data = ("".join(random.choices(string.ascii_letters, k=50_000))).encode()
    a = tmp_path / "a"
    b = tmp_path / "b"
    _write(a, data)
    _write(b, data)
    ra = cascade_hash(str(a), len(data))
    rb = cascade_hash(str(b), len(data))
    assert ra.prefix_hash == rb.prefix_hash
    assert ra.suffix_hash == rb.suffix_hash
    assert ra.full_hash == rb.full_hash


def test_cascade_near_identical_differ_on_full(tmp_path):
    # Same prefix and suffix, but middle differs (forces full-hash discrimination)
    prefix = b"A" * PREFIX_BYTES
    suffix = b"B" * 4096
    a = tmp_path / "a"
    b = tmp_path / "b"
    _write(a, prefix + b"middle-1" * 100 + suffix)
    _write(b, prefix + b"middle-2" * 100 + suffix)
    ra = cascade_hash(str(a), os.path.getsize(a))
    rb = cascade_hash(str(b), os.path.getsize(b))
    # Prefix + suffix identical
    assert ra.prefix_hash == rb.prefix_hash
    assert ra.suffix_hash == rb.suffix_hash
    # Full hash differs — this is the discrimination point
    assert ra.full_hash != rb.full_hash


def test_hash_local_bytes_stable():
    x, b = hash_local_bytes(b"the quick brown fox")
    x2, b2 = hash_local_bytes(b"the quick brown fox")
    assert x == x2
    assert b == b2


def test_hash_local_file_tiny(tmp_path):
    p = tmp_path / "tiny.txt"
    _write(p, b"x")
    r = hash_local_file(str(p))
    assert r.size == 1
    assert r.prefix_hash is not None
    assert r.full_hash is not None


def test_hash_suffix_small_file(tmp_path):
    p = tmp_path / "s.txt"
    _write(p, b"abcd")
    # hash_suffix with a size smaller than SUFFIX_BYTES should still succeed.
    h = hash_suffix(str(p), size=4)
    assert h  # non-empty hex
