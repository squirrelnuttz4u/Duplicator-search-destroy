"""Tests for the WinRmClient wrapper.

pypsrp's real transport can't be used in unit tests, so we inject a fake
pypsrp.client module into sys.modules and drive the wrapper against it.
"""

from __future__ import annotations

import sys
import types

import pytest


@pytest.fixture()
def fake_pypsrp(monkeypatch):
    """Install a stub pypsrp.client module for the duration of one test."""
    mod = types.ModuleType("pypsrp.client")
    pkg = types.ModuleType("pypsrp")
    pkg.client = mod
    monkeypatch.setitem(sys.modules, "pypsrp", pkg)
    monkeypatch.setitem(sys.modules, "pypsrp.client", mod)
    return mod


def _install_client_stub(fake_pypsrp, *, execute_return=None, execute_raises=None):
    class _StubStreams:
        def __init__(self):
            self.error = []

    class _StubClient:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def execute_ps(self, script):
            if execute_raises is not None:
                raise execute_raises
            if callable(execute_return):
                return execute_return(script)
            return execute_return or ("", _StubStreams(), False)

        def close(self):
            pass

    fake_pypsrp.Client = _StubClient
    return _StubClient


def test_client_username_includes_domain_when_not_qualified(fake_pypsrp):
    _install_client_stub(fake_pypsrp)
    from duplicator_search_destroy.scanner.winrm_client import WinRmClient

    c = WinRmClient("host1", username="admin", domain="CORP", password="p")
    assert c.username == "CORP\\admin"


def test_client_keeps_qualified_usernames(fake_pypsrp):
    _install_client_stub(fake_pypsrp)
    from duplicator_search_destroy.scanner.winrm_client import WinRmClient

    c = WinRmClient("host1", username="CORP\\admin", password="p")
    assert c.username == "CORP\\admin"
    c2 = WinRmClient("host1", username="admin@corp.local", password="p")
    assert c2.username == "admin@corp.local"


def test_run_powershell_happy_path(fake_pypsrp):
    class _Streams:
        error = []

    _install_client_stub(
        fake_pypsrp,
        execute_return=lambda script: ("hello\n", _Streams(), False),
    )
    from duplicator_search_destroy.scanner.winrm_client import WinRmClient

    r = WinRmClient("host1").run_powershell("Write-Host 'hello'")
    assert r.stdout.strip() == "hello"
    assert r.had_errors is False


def test_run_powershell_auth_error_translated(fake_pypsrp):
    _install_client_stub(fake_pypsrp, execute_raises=Exception("Access is denied"))
    from duplicator_search_destroy.scanner.winrm_client import (
        WinRmAuthError,
        WinRmClient,
    )
    with pytest.raises(WinRmAuthError):
        WinRmClient("host1", username="u", password="p").run_powershell("x")


def test_run_powershell_connect_error_translated(fake_pypsrp):
    _install_client_stub(fake_pypsrp, execute_raises=Exception("connection refused"))
    from duplicator_search_destroy.scanner.winrm_client import (
        WinRmConnectError,
        WinRmClient,
    )
    with pytest.raises(WinRmConnectError):
        WinRmClient("host1").run_powershell("x")


def test_unavailable_when_pypsrp_missing(monkeypatch):
    # Make sure a clean import of the real module fails. We achieve that by
    # stuffing None into sys.modules, which raises ImportError on import.
    monkeypatch.setitem(sys.modules, "pypsrp", None)
    monkeypatch.setitem(sys.modules, "pypsrp.client", None)

    from duplicator_search_destroy.scanner.winrm_client import (
        WinRmClient,
        WinRmUnavailableError,
    )
    with pytest.raises(WinRmUnavailableError):
        WinRmClient("host1").run_powershell("x")
