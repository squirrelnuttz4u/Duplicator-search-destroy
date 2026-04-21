"""Tests for the duplicates module."""

from __future__ import annotations

from duplicator_search_destroy.models.database import FileRow
from duplicator_search_destroy.scanner.duplicates import group_by_size


def _f(id_, size):
    return FileRow(
        id=id_, share_id=1, folder_id=None, name=f"f{id_}", extension=None,
        relative_path=f"f{id_}", size=size, created_at=None, modified_at=None, full_hash=None,
    )


def test_group_by_size_buckets():
    files = [_f(1, 100), _f(2, 100), _f(3, 200), _f(4, 300), _f(5, 300)]
    buckets = group_by_size(files)
    assert 100 in buckets and len(buckets[100]) == 2
    assert 300 in buckets and len(buckets[300]) == 2
    assert 200 not in buckets  # singleton dropped


def test_group_by_size_skips_zero():
    files = [_f(1, 0), _f(2, 0), _f(3, 1), _f(4, 1)]
    buckets = group_by_size(files)
    assert 0 not in buckets
    assert 1 in buckets
