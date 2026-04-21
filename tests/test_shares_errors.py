"""Round-3 error-path tests for shares.enumerate_shares."""

from __future__ import annotations

import pytest

from duplicator_search_destroy.scanner import shares as shares_mod
from duplicator_search_destroy.scanner.shares import (
    DiscoveredShare,
    ShareEnumerationError,
    enumerate_shares,
)


def test_filters_hidden_when_requested(monkeypatch):
    fake = [
        DiscoveredShare(host="h", name="PUBLIC", remark=None, share_type=0),
        DiscoveredShare(host="h", name="ADMIN$", remark=None, share_type=0x80000000),
        DiscoveredShare(host="h", name="IPC$", remark=None, share_type=3),
    ]
    monkeypatch.setattr(shares_mod, "_enum_with_impacket", lambda *a, **k: fake)
    # Default: hidden allowed, IPC excluded
    default = enumerate_shares("h")
    names_default = {s.name for s in default}
    assert "PUBLIC" in names_default
    assert "ADMIN$" in names_default
    assert "IPC$" not in names_default

    # Filter hidden
    hidden_off = enumerate_shares("h", include_hidden=False)
    names_off = {s.name for s in hidden_off}
    assert "PUBLIC" in names_off
    assert "ADMIN$" not in names_off


def test_non_disk_shares_dropped(monkeypatch):
    fake = [
        DiscoveredShare(host="h", name="printer", remark=None, share_type=1),  # PRINT
        DiscoveredShare(host="h", name="data", remark=None, share_type=0),     # DISK
    ]
    monkeypatch.setattr(shares_mod, "_enum_with_impacket", lambda *a, **k: fake)
    out = enumerate_shares("h")
    assert [s.name for s in out] == ["data"]


def test_propagates_failure_when_all_backends_fail(monkeypatch):
    def _boom(*a, **k):
        raise ShareEnumerationError("no impacket")

    monkeypatch.setattr(shares_mod, "_enum_with_impacket", _boom)
    monkeypatch.setattr(shares_mod, "_enum_with_pywin32", _boom)
    with pytest.raises(ShareEnumerationError):
        enumerate_shares("h")


def test_properties_on_discovered_share():
    s1 = DiscoveredShare(host="h", name="DATA", remark=None, share_type=0)
    assert s1.is_disk
    assert not s1.is_hidden
    s2 = DiscoveredShare(host="h", name="ADMIN$", remark=None, share_type=0x80000000)
    assert s2.is_hidden
    s3 = DiscoveredShare(host="h", name="c$", remark=None, share_type=0)
    assert s3.is_hidden  # name-based
