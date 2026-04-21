"""Workflow orchestration — glue between the GUI and the scanner modules.

Each public method is a *long-running* operation that:
* emits progress through callbacks
* accepts a cancel predicate
* writes its results into the DB

The GUI runs these on a worker thread so the Qt event loop stays responsive.
"""

from __future__ import annotations

import logging
from typing import Callable, List, Optional

from duplicator_search_destroy.models.database import Database, Host
from duplicator_search_destroy.scanner.duplicates import hash_candidate_files
from duplicator_search_destroy.scanner.files import (
    register_session,
    unregister_session,
    walk_share,
)
from duplicator_search_destroy.scanner.network import DiscoveredHost, discover_hosts
from duplicator_search_destroy.scanner.shares import (
    ShareEnumerationError,
    enumerate_shares,
)
from duplicator_search_destroy.utils.ip_utils import expand_targets

log = logging.getLogger(__name__)

ProgressCb = Callable[[str, int, int], None]
CancelCb = Callable[[], bool]

__all__ = ["Orchestrator"]


class Orchestrator:
    def __init__(self, db: Database) -> None:
        self.db = db

    # -- Phase 1: discover live SMB hosts ----------------------------------

    def discover(
        self,
        targets_raw: str,
        *,
        timeout: float = 2.0,
        max_workers: int = 128,
        on_progress: Optional[ProgressCb] = None,
        cancel: Optional[CancelCb] = None,
    ) -> List[DiscoveredHost]:
        run = self.db.start_run("discovery", f"targets={targets_raw[:256]}")
        ips = list(expand_targets(targets_raw))
        total = len(ips)
        done = 0
        live: List[DiscoveredHost] = []

        def _each(result: DiscoveredHost) -> None:
            nonlocal done
            done += 1
            if result.port_open:
                self.db.upsert_host(result.ip, hostname=result.hostname, status="online")
                live.append(result)
            else:
                self.db.upsert_host(result.ip, hostname=result.hostname, status="offline")
            if on_progress:
                on_progress(result.ip, done, total)

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
        return live

    # -- Phase 2: enumerate shares on each online host ---------------------

    def enumerate_shares(
        self,
        hosts: Optional[List[Host]] = None,
        *,
        on_progress: Optional[ProgressCb] = None,
        cancel: Optional[CancelCb] = None,
    ) -> int:
        run = self.db.start_run("shares")
        if hosts is None:
            hosts = [h for h in self.db.list_hosts() if h.status == "online"]
        total = len(hosts)
        total_shares = 0
        for idx, host in enumerate(hosts, 1):
            if cancel and cancel():
                break
            cred = self.db.get_credentials(host.id)
            target = host.hostname or host.ip
            try:
                shares = enumerate_shares(
                    target,
                    username=cred.username if cred else "",
                    password=cred.password if cred else "",
                    domain=cred.domain or "" if cred else "",
                )
                for s in shares:
                    self.db.upsert_share(
                        host.id,
                        s.name,
                        remark=s.remark,
                        share_type=s.share_type,
                        accessible=True,
                    )
                total_shares += len(shares)
            except ShareEnumerationError as exc:
                log.warning("Share enum failed for %s: %s", target, exc)
            except Exception as exc:  # pragma: no cover
                log.exception("Unexpected share enum error for %s: %s", target, exc)
            if on_progress:
                on_progress(target, idx, total)
        self.db.finish_run(run, "done", f"{total_shares} shares across {total} hosts")
        return total_shares

    # -- Phase 3: walk every accessible share ------------------------------

    def scan_files(
        self,
        *,
        on_progress: Optional[ProgressCb] = None,
        cancel: Optional[CancelCb] = None,
        max_depth: int = 64,
    ) -> int:
        run = self.db.start_run("files")
        total_files = 0
        all_shares = self.db.list_shares()
        total = len(all_shares)
        for idx, share in enumerate(all_shares, 1):
            if cancel and cancel():
                break
            host = self.db.get_host(share.host_id)
            if host is None:
                continue
            cred = self.db.get_credentials(host.id)
            target = host.hostname or host.ip
            try:
                register_session(
                    target,
                    username=cred.username if cred else "",
                    password=cred.password if cred else "",
                    domain=cred.domain or "" if cred else "",
                )
            except Exception as exc:
                log.warning("Session register failed for %s: %s", target, exc)
                continue
            try:
                total_files += self._scan_one_share(share, target, max_depth=max_depth, cancel=cancel)
                self.db.mark_share_scanned(share.id)
            except Exception as exc:
                log.exception("Scan failed for \\\\%s\\%s: %s", target, share.name, exc)
            finally:
                unregister_session(target)
            if on_progress:
                on_progress(f"\\\\{target}\\{share.name}", idx, total)
        self.db.finish_run(run, "done", f"{total_files} files indexed")
        return total_files

    def _scan_one_share(
        self,
        share,
        host_target: str,
        *,
        max_depth: int,
        cancel: Optional[CancelCb],
    ) -> int:
        """Walk the share and persist folders & files.

        Because the walker emits folders in DFS pre-order, every folder's
        parent is already known by the time we reach the child — so we can
        resolve ``parent_id`` at insert time and skip the fragile post-pass
        SQL we used to rely on.
        """
        self.db.clear_share_index(share.id)
        folder_ids: dict[str, int] = {}
        file_rows_batch: list[tuple] = []
        files_count = 0

        def _flush_files():
            nonlocal file_rows_batch
            if file_rows_batch:
                self.db.insert_files(file_rows_batch)
                file_rows_batch = []

        for folder, files_iter in walk_share(
            host_target, share.name, max_depth=max_depth, cancel=cancel
        ):
            parent_id = (
                folder_ids.get(folder.parent_rel_path)
                if folder.parent_rel_path is not None
                else None
            )
            # Single-row insert so we can capture the id immediately. Walker
            # emits one folder per subtree, so this is still O(#folders) —
            # the expensive channel is the files batch below.
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
                files_count += 1
                if len(file_rows_batch) >= 1000:
                    _flush_files()

        _flush_files()
        return files_count

    # -- Phase 4: hash + dedup --------------------------------------------

    def hash_and_find_duplicates(
        self,
        *,
        min_size: int = 1,
        max_workers: int = 8,
        on_progress: Optional[ProgressCb] = None,
        cancel: Optional[CancelCb] = None,
    ) -> int:
        run = self.db.start_run("hash")

        def _cb(done: int, total: int) -> None:
            if on_progress:
                on_progress("hashing", done, total)

        try:
            count = hash_candidate_files(
                self.db,
                min_size=min_size,
                max_workers=max_workers,
                on_progress=_cb,
                cancel=cancel,
            )
            self.db.finish_run(run, "done", f"{count} files hashed")
            return count
        except Exception as exc:
            self.db.finish_run(run, "failed", str(exc))
            raise
