"""Tests for network.probe_host / discover_hosts using a loopback fake SMB server."""

from __future__ import annotations

import socket
import threading
import time

import pytest

from duplicator_search_destroy.scanner.network import (
    DiscoveredHost,
    discover_hosts,
    probe_host,
)


class _FakeListener:
    """Tiny TCP listener used to simulate 'port 445 is open'."""

    def __init__(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(8)
        self.port = self.server.getsockname()[1]
        self._stop = False
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def _accept(self):
        self.server.settimeout(0.2)
        while not self._stop:
            try:
                conn, _ = self.server.accept()
                conn.close()
            except (socket.timeout, OSError):
                continue

    def close(self):
        self._stop = True
        try:
            self.server.close()
        except Exception:
            pass
        self.thread.join(timeout=1.0)


@pytest.fixture()
def listener():
    lst = _FakeListener()
    try:
        yield lst
    finally:
        lst.close()


def test_probe_host_port_open(listener, monkeypatch):
    # Pretend SMB_PORT == our ephemeral listener.
    from duplicator_search_destroy.scanner import network

    monkeypatch.setattr(network, "SMB_PORT", listener.port, raising=True)
    result = probe_host("127.0.0.1", timeout=1.0, resolve=False)
    assert result.port_open is True
    assert result.ip == "127.0.0.1"


def test_probe_host_port_closed():
    # Bind-and-close to obtain a guaranteed-unused high port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    time.sleep(0.05)
    from duplicator_search_destroy.scanner import network

    orig = network.SMB_PORT
    network.SMB_PORT = port
    try:
        r = probe_host("127.0.0.1", timeout=0.5, resolve=False)
        assert r.port_open is False
    finally:
        network.SMB_PORT = orig


def test_discover_hosts_parallel(listener, monkeypatch):
    from duplicator_search_destroy.scanner import network

    monkeypatch.setattr(network, "SMB_PORT", listener.port, raising=True)
    seen: list[DiscoveredHost] = []
    results = discover_hosts(
        ["127.0.0.1"] * 5,
        timeout=1.0,
        max_workers=4,
        on_result=seen.append,
    )
    assert len(results) == 5
    assert all(r.port_open for r in results)
    assert len(seen) == 5


def test_discover_hosts_respects_cancel(listener, monkeypatch):
    from duplicator_search_destroy.scanner import network

    monkeypatch.setattr(network, "SMB_PORT", listener.port, raising=True)
    # Feed 200 IPs to give the cancel a chance to fire mid-sweep.
    ips = [f"127.0.0.{i%255+1}" for i in range(200)]
    stop_after = {"n": 0}

    def cancel():
        stop_after["n"] += 1
        return stop_after["n"] > 5

    results = discover_hosts(ips, timeout=0.5, max_workers=4, cancel=cancel)
    # Cancel should prevent all 200 from being probed.
    assert len(results) < 200
