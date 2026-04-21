from __future__ import annotations

from duplicator_search_destroy.utils.formatting import human_size, human_time, shorten_path


def test_human_size_bytes():
    assert human_size(0) == "0 B"
    assert human_size(512) == "512 B"


def test_human_size_kb():
    assert human_size(2048).endswith("KB")


def test_human_size_none():
    assert human_size(None) == "—"


def test_human_size_negative():
    assert human_size(-1024).startswith("-")


def test_human_time_none():
    assert human_time(None) == "—"


def test_human_time_bad():
    assert human_time("not-a-number") == "—"


def test_shorten_path_noop():
    assert shorten_path("abc", width=80) == "abc"


def test_shorten_path_long():
    s = shorten_path("x" * 200, width=40)
    assert "..." in s
    assert len(s) <= 41
