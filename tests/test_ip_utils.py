"""Unit tests for ip_utils."""

from __future__ import annotations

import pytest

from duplicator_search_destroy.utils.ip_utils import (
    InvalidTargetError,
    count_targets,
    expand_targets,
    parse_targets,
)


def test_parse_cidr():
    r = parse_targets("10.0.0.0/30")
    assert r == [("10.0.0.0", "10.0.0.3")]


def test_parse_single_ip():
    r = parse_targets("192.168.1.5")
    assert r == [("192.168.1.5", "192.168.1.5")]


def test_parse_dashed_full():
    r = parse_targets("10.0.0.1-10.0.0.10")
    assert r == [("10.0.0.1", "10.0.0.10")]


def test_parse_dashed_short():
    r = parse_targets("10.0.0.1-10")
    assert r == [("10.0.0.1", "10.0.0.10")]


def test_parse_wildcard():
    r = parse_targets("10.1.2.*")
    assert r == [("10.1.2.0", "10.1.2.255")]


def test_parse_multiline_and_comma():
    r = parse_targets("10.0.0.1, 10.0.0.2\n10.0.0.3\n# comment\n")
    assert r == [
        ("10.0.0.1", "10.0.0.1"),
        ("10.0.0.2", "10.0.0.2"),
        ("10.0.0.3", "10.0.0.3"),
    ]


def test_parse_invalid_ip():
    with pytest.raises(InvalidTargetError):
        parse_targets("10.300.0.1")


def test_parse_invalid_cidr():
    with pytest.raises(InvalidTargetError):
        parse_targets("10.0.0.0/99")


def test_parse_end_before_start():
    with pytest.raises(InvalidTargetError):
        parse_targets("10.0.0.20-10.0.0.10")


def test_expand_cidr_count():
    ips = list(expand_targets("10.0.0.0/29"))
    assert len(ips) == 8
    assert ips[0] == "10.0.0.0"
    assert ips[-1] == "10.0.0.7"


def test_expand_dedupes():
    ips = list(expand_targets("10.0.0.1\n10.0.0.1\n10.0.0.1-3"))
    assert ips == ["10.0.0.1", "10.0.0.2", "10.0.0.3"]


def test_expand_safety_limit():
    with pytest.raises(InvalidTargetError):
        list(expand_targets("10.0.0.0/16", limit=100))


def test_count_targets_matches_expand():
    raw = "10.0.0.0/28\n10.0.1.0-10"
    assert count_targets(raw) == 16 + 11


def test_count_targets_dedupes():
    raw = "10.0.0.1\n10.0.0.1\n10.0.0.1-3"
    assert count_targets(raw) == 3
