"""Tests for scanner.progress — ScanStats thread-safety + ThrottledEmitter."""

from __future__ import annotations

import threading
import time

from duplicator_search_destroy.scanner.progress import (
    ScanStats,
    StatsSnapshot,
    ThrottledEmitter,
)


def test_snapshot_after_counters():
    s = ScanStats("files", hosts_total=2, shares_total=4)
    s.add_files(100, total_bytes=10_000)
    s.add_folders(5)
    s.end_share("\\\\a\\b")
    s.end_host()
    snap = s.snapshot()
    assert snap.files_seen == 100
    assert snap.folders_seen == 5
    assert snap.bytes_seen == 10_000
    assert snap.shares_done == 1
    assert snap.hosts_done == 1
    assert snap.hosts_total == 2


def test_active_workers_tracked_by_thread():
    s = ScanStats("files")
    s.begin_share("\\\\host1\\share1")
    assert "\\\\host1\\share1" in s.snapshot().active_workers
    s.end_share("\\\\host1\\share1")
    assert s.snapshot().active_workers == []


def test_files_per_sec_and_eta():
    s = ScanStats("files", shares_total=10)
    # Force elapsed to a non-trivial value so rate is computable.
    s._started_at -= 2.0  # pretend 2 seconds have passed
    s.add_files(2000, total_bytes=0)
    for _ in range(5):
        s.end_share("x")
    snap = s.snapshot()
    # ~1000 files/sec, ~5 shares in ~2s → ETA ~2s for the remaining 5.
    assert 500 < snap.files_per_sec < 5000
    assert snap.eta_seconds is not None and snap.eta_seconds > 0


def test_eta_none_until_something_finishes():
    s = ScanStats("files", shares_total=10)
    assert s.snapshot().eta_seconds is None


def test_thread_safe_increments():
    s = ScanStats("files")

    def _bump():
        for _ in range(1000):
            s.add_files(1)
            s.add_folders(1)

    threads = [threading.Thread(target=_bump) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = s.snapshot()
    assert snap.files_seen == 8 * 1000
    assert snap.folders_seen == 8 * 1000


def test_throttled_emitter_drops_rapid_calls():
    calls: list[StatsSnapshot] = []
    emitter = ThrottledEmitter(calls.append, min_interval=0.1)
    s = ScanStats("files")
    # Fire 20 emits in quick succession — only the first should go through.
    for _ in range(20):
        emitter.emit(s.snapshot())
    assert len(calls) == 1


def test_throttled_emitter_lets_time_spaced_calls_through():
    calls = []
    emitter = ThrottledEmitter(calls.append, min_interval=0.05)
    s = ScanStats("files")
    emitter.emit(s.snapshot())
    time.sleep(0.08)
    emitter.emit(s.snapshot())
    assert len(calls) == 2


def test_emitter_flush_always_fires():
    calls = []
    emitter = ThrottledEmitter(calls.append, min_interval=10.0)
    s = ScanStats("files")
    emitter.emit(s.snapshot())   # one goes through as first call
    emitter.emit(s.snapshot())   # throttled
    assert len(calls) == 1
    emitter.flush(s.snapshot())  # flush bypasses throttle
    assert len(calls) == 2


def test_emitter_swallows_callback_exceptions():
    def boom(_s):
        raise RuntimeError("ui crashed")

    emitter = ThrottledEmitter(boom, min_interval=0.0)
    s = ScanStats("files")
    # Must not raise.
    emitter.emit(s.snapshot())
    emitter.flush(s.snapshot())


def test_emitter_none_callback_is_noop():
    emitter = ThrottledEmitter(None)
    s = ScanStats("files")
    emitter.emit(s.snapshot())
    emitter.flush(s.snapshot())  # shouldn't raise
