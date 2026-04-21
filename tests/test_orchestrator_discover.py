"""Round-2 test for Orchestrator.discover() with a stubbed network layer."""

from __future__ import annotations

import pytest

from duplicator_search_destroy.models.database import Database
from duplicator_search_destroy.scanner import orchestrator as orch_mod
from duplicator_search_destroy.scanner.network import DiscoveredHost
from duplicator_search_destroy.scanner.orchestrator import Orchestrator


def _fake_discover(ips, *, timeout, max_workers, on_result, cancel):
    """Simulate half the hosts being up."""
    out = []
    for i, ip in enumerate(ips):
        res = DiscoveredHost(
            ip=ip,
            hostname=f"srv-{i}" if i % 2 == 0 else None,
            port_open=(i % 2 == 0),
        )
        out.append(res)
        if on_result:
            on_result(res)
    return out


def test_discover_writes_hosts_with_correct_status(tmp_db: Database, monkeypatch):
    monkeypatch.setattr(orch_mod, "discover_hosts", _fake_discover)
    o = Orchestrator(tmp_db)
    live = o.discover("10.0.0.0/30")
    assert len(live) == 2
    hosts = tmp_db.list_hosts()
    assert len(hosts) == 4
    online = [h for h in hosts if h.status == "online"]
    offline = [h for h in hosts if h.status == "offline"]
    assert len(online) == 2
    assert len(offline) == 2
    # Scan run metadata persisted
    runs = tmp_db.query("SELECT * FROM scan_runs WHERE kind='discovery'")
    assert len(runs) == 1
    assert runs[0]["status"] == "done"


def test_discover_progress_emitted(tmp_db: Database, monkeypatch):
    monkeypatch.setattr(orch_mod, "discover_hosts", _fake_discover)
    o = Orchestrator(tmp_db)
    events = []
    o.discover("10.0.0.0/30", on_progress=lambda msg, d, t: events.append((msg, d, t)))
    assert len(events) == 4
    assert events[-1][1] == events[-1][2]  # last done==total
