"""Workflow orchestration — glue between the GUI and the scanner modules.

Each public method is a *long-running* operation that:

* emits coarse progress through ``on_progress(msg, done, total)``
* emits rich stats through ``on_stats(StatsSnapshot)``
* accepts a ``cancel`` predicate
* writes its results into the DB

The scanning phases (share enumeration, file indexing) are parallelised
across hosts: each worker thread owns one host at a time, so concurrent
SMB sessions saturate the scanning client's bandwidth without oversubscribing
any single target.
"""

from __future__ import annotations

import concurrent.futures as futures
import logging
import threading
import time
from typing import Callable, List, Optional

from duplicator_search_destroy.models.database import Database, Host, Share
from duplicator_search_destroy.scanner.duplicates import (
    hash_candidate_files,
    hash_candidates_via_winrm,
)
from duplicator_search_destroy.scanner.files import (
    register_session,
    unregister_session,
    walk_share,
)
from duplicator_search_destroy.scanner.network import DiscoveredHost, discover_hosts
from duplicator_search_destroy.scanner.progress import (
    ScanStats,
    StatsSnapshot,
    ThrottledEmitter,
)
from duplicator_search_destroy.scanner.shares import (
    ShareEnumerationError,
    enumerate_shares,
)
from duplicator_search_destroy.utils.ip_utils import expand_targets

log = logging.getLogger(__name__)

ProgressCb = Callable[[str, int, int], None]
StatsCb = Callable[[StatsSnapshot], None]
CancelCb = Callable[[], bool]

__all__ = ["Orchestrator"]


def _start_emitter_thread(
    stats: ScanStats,
    emitter: ThrottledEmitter,
    interval: float = 0.25,
) -> tuple[threading.Thread, threading.Event]:
    """Spawn a daemon that pushes a snapshot every ``interval`` seconds."""
    stop = threading.Event()

    def _loop() -> None:
        while not stop.is_set():
            emitter.emit(stats.snapshot())
            stop.wait(interval)

    t = threading.Thread(target=_loop, name="scan-stats", daemon=True)
    t.start()
    return t, stop


class Orchestrator:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Phase 1 — discover live SMB hosts
    # ------------------------------------------------------------------

    def discover(
        self,
        targets_raw: str,
        *,
        timeout: float = 2.0,
        max_workers: int = 128,
        on_progress: Optional[ProgressCb] = None,
        on_stats: Optional[StatsCb] = None,
        cancel: Optional[CancelCb] = None,
    ) -> List[DiscoveredHost]:
        run = self.db.start_run("discovery", f"targets={targets_raw[:256]}")
        ips = list(expand_targets(targets_raw))
        total = len(ips)
        stats = ScanStats("discovery", hosts_total=total)
        emitter = ThrottledEmitter(on_stats)
        live: List[DiscoveredHost] = []

        def _each(result: DiscoveredHost) -> None:
            if result.port_open:
                self.db.upsert_host(result.ip, hostname=result.hostname, status="online")
                live.append(result)
            else:
                self.db.upsert_host(result.ip, hostname=result.hostname, status="offline")
            stats.end_host()
            snap = stats.snapshot()
            emitter.emit(snap)
            if on_progress:
                on_progress(result.ip, snap.hosts_done, snap.hosts_total)

        try:
            discover_hosts(
                ips,
                timeout=timeout,
                max_workers=max_workers,
                on_result=_each,
                cancel=cancel,
            )
            self.db.finish_run(run, status="done", message=f"{len(live)}/{total} live")
        except Exception as exc:
            self.db.finish_run(run, status="failed", message=str(exc))
            raise
        finally:
            emitter.flush(stats.snapshot())
        return live

    # ------------------------------------------------------------------
    # Phase 2 — enumerate shares on each online host (parallel per-host)
    # ------------------------------------------------------------------

    def enumerate_shares(
        self,
        hosts: Optional[List[Host]] = None,
        *,
        max_workers: int = 8,
        on_progress: Optional[ProgressCb] = None,
        on_stats: Optional[StatsCb] = None,
        cancel: Optional[CancelCb] = None,
    ) -> int:
        run = self.db.start_run("shares")
        if hosts is None:
            hosts = [h for h in self.db.list_hosts() if h.status == "online"]
        total = len(hosts)
        stats = ScanStats("shares", hosts_total=total)
        emitter = ThrottledEmitter(on_stats)
        emit_thread, emit_stop = _start_emitter_thread(stats, emitter)

        found_lock = threading.Lock()
        total_shares = 0

        def _do_host(host: Host) -> None:
            nonlocal total_shares
            if cancel and cancel():
                return
            target = host.hostname or host.ip
            cred = self.db.get_credentials(host.id)
            try:
                shares = enumerate_shares(
                    target,
                    username=cred.username if cred else "",
                    password=cred.password if cred else "",
                    domain=(cred.domain or "") if cred else "",
                )
                for s in shares:
                    self.db.upsert_share(
                        host.id,
                        s.name,
                        remark=s.remark,
                        share_type=s.share_type,
                        accessible=True,
                    )
                with found_lock:
                    total_shares += len(shares)
            except ShareEnumerationError as exc:
                log.warning("Share enum failed for %s: %s", target, exc)
                stats.note_error(f"{target}: {exc}")
            except Exception as exc:  # pragma: no cover
                log.exception("Unexpected share-enum error for %s: %s", target, exc)
                stats.note_error(f"{target}: {exc}")
            finally:
                stats.end_host()
                snap = stats.snapshot()
                if on_progress:
                    on_progress(target, snap.hosts_done, snap.hosts_total)

        try:
            with futures.ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="enum") as pool:
                futs = [pool.submit(_do_host, h) for h in hosts]
                for _ in futures.as_completed(futs):
                    if cancel and cancel():
                        for f in futs:
                            f.cancel()
                        break
            self.db.finish_run(run, "done", f"{total_shares} shares across {total} hosts")
            return total_shares
        finally:
            emit_stop.set()
            emit_thread.join(timeout=1.0)
            emitter.flush(stats.snapshot())

    # ------------------------------------------------------------------
    # Phase 3 — walk every accessible share (parallel per-host)
    # ------------------------------------------------------------------

    def scan_files(
        self,
        *,
        max_workers: int = 4,
        max_depth: int = 64,
        resume: bool = False,
        on_progress: Optional[ProgressCb] = None,
        on_stats: Optional[StatsCb] = None,
        cancel: Optional[CancelCb] = None,
    ) -> int:
        """Walk every accessible share in parallel across hosts.

        ``resume=True`` skips shares that have a recorded successful scan
        (``last_scan IS NOT NULL``). Use it to continue a cancelled run
        without re-indexing what's already done. Partial shares — where a
        prior scan was cancelled mid-walk — are *not* skipped, because
        ``mark_share_scanned`` is only called on clean completion, so
        ``last_scan`` stays NULL on them and they get re-walked.
        """
        run = self.db.start_run("files")
        all_shares = self.db.list_shares()
        if resume:
            all_shares = [s for s in all_shares if s.last_scan is None]

        # Bucket shares by host — each worker processes one host at a time
        # so we only open one SMB session per target and the server sees
        # predictable load.
        by_host: dict[int, list[Share]] = {}
        for s in all_shares:
            by_host.setdefault(s.host_id, []).append(s)

        hosts_total = len(by_host)
        shares_total = len(all_shares)
        stats = ScanStats("files", hosts_total=hosts_total, shares_total=shares_total)
        emitter = ThrottledEmitter(on_stats)
        emit_thread, emit_stop = _start_emitter_thread(stats, emitter)

        def _do_host(host_id: int, shares: list[Share]) -> int:
            if cancel and cancel():
                return 0
            host = self.db.get_host(host_id)
            if host is None:
                stats.end_host()
                return 0
            target = host.hostname or host.ip
            cred = self.db.get_credentials(host.id)
            try:
                register_session(
                    target,
                    username=cred.username if cred else "",
                    password=cred.password if cred else "",
                    domain=(cred.domain or "") if cred else "",
                )
            except Exception as exc:
                log.warning("Session register failed for %s: %s", target, exc)
                stats.note_error(f"{target}: session failed: {exc}")
                for _s in shares:
                    stats.end_share(f"\\\\{target}\\{_s.name}", error=True)
                stats.end_host()
                return 0

            files_count = 0
            try:
                for share in shares:
                    if cancel and cancel():
                        break
                    unc = f"\\\\{target}\\{share.name}"
                    stats.begin_share(unc)
                    err = False
                    try:
                        files_count += self._scan_one_share(
                            share, target, max_depth=max_depth, cancel=cancel, stats=stats
                        )
                        # Only mark the share as scanned if we got through it
                        # without being cancelled. A cancel mid-walk leaves
                        # last_scan NULL so a later resume will re-process it
                        # (and clear_share_index will wipe the partial rows
                        # before the re-walk).
                        if not (cancel and cancel()):
                            self.db.mark_share_scanned(share.id)
                    except Exception as exc:
                        err = True
                        log.exception("Scan failed for %s: %s", unc, exc)
                        stats.note_error(f"{unc}: {exc}")
                    finally:
                        stats.end_share(unc, error=err)
                        snap = stats.snapshot()
                        if on_progress:
                            on_progress(unc, snap.shares_done, snap.shares_total)
            finally:
                unregister_session(target)
                stats.end_host()
            return files_count

        total_files = 0
        try:
            with futures.ThreadPoolExecutor(max_workers=max(1, max_workers), thread_name_prefix="scan") as pool:
                fut_map = {
                    pool.submit(_do_host, hid, shares): hid
                    for hid, shares in by_host.items()
                }
                for fut in futures.as_completed(fut_map):
                    if cancel and cancel():
                        for f in fut_map:
                            f.cancel()
                    try:
                        total_files += fut.result()
                    except Exception as exc:  # pragma: no cover
                        log.exception("Host worker raised: %s", exc)
            self.db.finish_run(run, "done", f"{total_files} files indexed")
            return total_files
        finally:
            emit_stop.set()
            emit_thread.join(timeout=1.0)
            emitter.flush(stats.snapshot())

    # --- per-share walker (owned by a single worker thread) ------------

    def _scan_one_share(
        self,
        share: Share,
        host_target: str,
        *,
        max_depth: int,
        cancel: Optional[CancelCb],
        stats: ScanStats,
    ) -> int:
        """Walk one share and persist folders + files.

        Parent folder IDs are resolved at insert time because the walker
        yields in DFS pre-order (parent before child), so a small local
        dict always has what we need by the time a child appears.

        Counters are pushed to *stats* on every flush, which is what
        makes the live files/sec + bytes/sec numbers feel responsive.
        """
        self.db.clear_share_index(share.id)
        folder_ids: dict[str, int] = {}
        file_rows_batch: list[tuple] = []
        files_count = 0
        bytes_count = 0
        folder_rows_since_emit = 0

        def _flush_files() -> None:
            nonlocal file_rows_batch, bytes_count, files_count
            if not file_rows_batch:
                return
            self.db.insert_files(file_rows_batch)
            stats.add_files(len(file_rows_batch), bytes_count)
            file_rows_batch = []
            bytes_count = 0

        for folder, files_iter in walk_share(
            host_target, share.name, max_depth=max_depth, cancel=cancel
        ):
            if cancel and cancel():
                break
            parent_id = (
                folder_ids.get(folder.parent_rel_path)
                if folder.parent_rel_path is not None
                else None
            )
            self.db.insert_folders(
                [
                    (
                        share.id,
                        parent_id,
                        folder.name,
                        folder.relative_path,
                        folder.depth,
                        folder.file_count,
                        folder.total_size,
                        folder.created_at,
                        folder.modified_at,
                    )
                ]
            )
            row = self.db.query(
                "SELECT id FROM folders WHERE share_id = ? AND relative_path = ?",
                (share.id, folder.relative_path),
            )
            folder_id = row[0]["id"] if row else None
            folder_ids[folder.relative_path] = folder_id
            folder_rows_since_emit += 1
            if folder_rows_since_emit >= 32:
                stats.add_folders(folder_rows_since_emit)
                folder_rows_since_emit = 0

            for wf in files_iter:
                file_rows_batch.append(
                    (
                        share.id,
                        folder_id,
                        wf.name,
                        wf.extension,
                        wf.relative_path,
                        wf.size,
                        wf.created_at,
                        wf.modified_at,
                        wf.accessed_at,
                    )
                )
                bytes_count += int(wf.size)
                files_count += 1
                if len(file_rows_batch) >= 1000:
                    _flush_files()

        _flush_files()
        if folder_rows_since_emit:
            stats.add_folders(folder_rows_since_emit)
        return files_count

    # ------------------------------------------------------------------
    # Phase 4 — hash + dedup (already parallel internally)
    # ------------------------------------------------------------------

    def hash_and_find_duplicates(
        self,
        *,
        min_size: int = 1,
        max_workers: int = 8,
        use_winrm: bool = False,
        winrm_algorithm: str = "sha256",
        winrm_throttle: int = 8,
        winrm_fallback_to_smb: bool = True,
        on_progress: Optional[ProgressCb] = None,
        on_stats: Optional[StatsCb] = None,
        cancel: Optional[CancelCb] = None,
    ) -> int:
        """Hash candidate files, then let :func:`find_duplicates` collate.

        ``use_winrm=True`` switches from the per-file SMB-read back-end
        (default, BLAKE3) to a per-host WinRM PowerShell call (SHA256). See
        :mod:`scanner.remote_hash` for the wire format and server-side
        requirements. Hash strings carry an ``algo:`` prefix so the two
        back-ends never cross-match false positives.
        """
        run = self.db.start_run("hash")
        stats = ScanStats("hash")
        emitter = ThrottledEmitter(on_stats)

        def _cb(done: int, total: int) -> None:
            stats.update_totals(shares_total=total)
            snap_count = done
            while snap_count > stats.snapshot().shares_done:
                stats.end_share("hashing", error=False)
            emitter.emit(stats.snapshot())
            if on_progress:
                on_progress("hashing", done, total)

        try:
            if use_winrm:
                count = hash_candidates_via_winrm(
                    self.db,
                    min_size=min_size,
                    algorithm=winrm_algorithm,
                    throttle=winrm_throttle,
                    fallback_to_smb=winrm_fallback_to_smb,
                    smb_max_workers=max_workers,
                    on_progress=_cb,
                    cancel=cancel,
                )
            else:
                count = hash_candidate_files(
                    self.db,
                    min_size=min_size,
                    max_workers=max_workers,
                    on_progress=_cb,
                    cancel=cancel,
                )
            self.db.finish_run(run, "done", f"{count} files hashed")
            emitter.flush(stats.snapshot())
            return count
        except Exception as exc:
            self.db.finish_run(run, "failed", str(exc))
            emitter.flush(stats.snapshot())
            raise
