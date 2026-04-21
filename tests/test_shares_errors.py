"""Error-path tests for shares.enumerate_shares (pywin32-only backend)."""

from __future__ import annotations

import os

import pytest

from duplicator_search_destroy.scanner import shares as shares_mod
from duplicator_search_destroy.scanner.shares import (
    DiscoveredShare,
    ShareEnumerationError,
    enumerate_shares,
)


@pytest.fixture(autouse=True)
def _pretend_windows(monkeypatch):
    """The real enumerate_shares refuses to run on non-Windows. For unit tests
    we patch os.name so we can exercise the filter/error logic with a stub
    backend."""
    monkeypatch.setattr(shares_mod, "os", type("_os", (), {"name": "nt"}))


def test_filters_hidden_when_requested(monkeypatch):
    fake = [
        DiscoveredShare(host="h", name="PUBLIC", remark=None, share_type=0),
        DiscoveredShare(host="h", name="ADMIN$", remark=None, share_type=0x80000000),
        DiscoveredShare(host="h", name="IPC$", remark=None, share_type=3),
    ]
    monkeypatch.setattr(shares_mod, "_enum_with_pywin32", lambda *a, **k: fake)
    default = enumerate_shares("h")
    names_default = {s.name for s in default}
    assert "PUBLIC" in names_default
    assert "ADMIN$" in names_default
    assert "IPC$" not in names_default

    hidden_off = enumerate_shares("h", include_hidden=False)
    names_off = {s.name for s in hidden_off}
    assert "PUBLIC" in names_off
    assert "ADMIN$" not in names_off


def test_non_disk_shares_dropped(monkeypatch):
    fake = [
        DiscoveredShare(host="h", name="printer", remark=None, share_type=1),
        DiscoveredShare(host="h", name="data", remark=None, share_type=0),
    ]
    monkeypatch.setattr(shares_mod, "_enum_with_pywin32", lambda *a, **k: fake)
    out = enumerate_shares("h")
    assert [s.name for s in out] == ["data"]


def test_propagates_failure_from_backend(monkeypatch):
    def _boom(*a, **k):
        raise ShareEnumerationError("simulated failure")

    monkeypatch.setattr(shares_mod, "_enum_with_pywin32", _boom)
    with pytest.raises(ShareEnumerationError):
        enumerate_shares("h")


def test_refuses_on_non_windows(monkeypatch):
    # Undo the autouse fixture's os-name patch for this one test.
    monkeypatch.setattr(shares_mod, "os", type("_os", (), {"name": "posix"}))
    with pytest.raises(ShareEnumerationError, match="requires Windows"):
        enumerate_shares("h")


def test_properties_on_discovered_share():
    s1 = DiscoveredShare(host="h", name="DATA", remark=None, share_type=0)
    assert s1.is_disk
    assert not s1.is_hidden
    s2 = DiscoveredShare(host="h", name="ADMIN$", remark=None, share_type=0x80000000)
    assert s2.is_hidden
    s3 = DiscoveredShare(host="h", name="c$", remark=None, share_type=0)
    assert s3.is_hidden
