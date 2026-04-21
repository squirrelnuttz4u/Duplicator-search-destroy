"""Additional edge cases for ip_utils — round 2."""

from __future__ import annotations

import pytest

from duplicator_search_destroy.utils.ip_utils import (
    InvalidTargetError,
    expand_targets,
    format_targets,
    parse_targets,
)


def test_parse_cidr_host_bits_are_tolerated():
    """RFC strictness: /24 with host bits set should normalise via strict=False."""
    r = parse_targets("10.0.0.5/24")
    assert r == [("10.0.0.0", "10.0.0.255")]


def test_parse_tabs_and_extra_whitespace():
    r = parse_targets("\t10.0.0.1  ,  10.0.0.2\n\n   10.0.0.3  ")
    assert len(r) == 3


def test_parse_comment_only_lines_ignored():
    r = parse_targets("# subnet\n10.0.0.1\n# another\n")
    assert r == [("10.0.0.1", "10.0.0.1")]


def test_parse_empty_input_returns_empty():
    assert parse_targets("") == []
    assert parse_targets("   \n\n# just comment") == []


def test_parse_rejects_ipv6():
    with pytest.raises(InvalidTargetError):
        parse_targets("::1")


def test_parse_rejects_mid_octet_wildcard():
    with pytest.raises(InvalidTargetError):
        parse_targets("10.0.*.5")


def test_parse_short_range_out_of_bounds():
    with pytest.raises(InvalidTargetError):
        parse_targets("10.0.0.1-300")


def test_format_targets_roundtrip():
    parsed = parse_targets("10.0.0.0/30")
    back = format_targets(parsed)
    # /30 expands to .0 .. .3 — round-tripped as a dashed range.
    assert back == "10.0.0.0-10.0.0.3"


def test_expand_targets_is_deterministic():
    ips1 = list(expand_targets("10.0.0.0/28"))
    ips2 = list(expand_targets("10.0.0.0/28"))
    assert ips1 == ips2


def test_expand_targets_single_host():
    assert list(expand_targets("10.0.0.99")) == ["10.0.0.99"]
