"""Duplicate-detection driver.

Two entry points:

* :func:`find_duplicates` — high-level, reads the DB, produces a list of
  :class:`~duplicator_search_destroy.models.database.DuplicateSet`.
* :func:`hash_candidate_files` — the expensive step: fingerprints every file
  in every size-bucket that has more than one member, writing results back
  into the DB. The UI calls this during the "Find duplicates" phase.

Two hash back-ends:

* **SMB (default)**: read the file's bytes over SMB and hash locally on
  the scanner with BLAKE3. Works anywhere that SMB reads work.
* **WinRM (opt-in)**: push a ``Get-FileHash`` PowerShell call to the
  server, which hashes locally with SHA256 and returns 64 bytes of hex.
  Typically 10-50× faster on LAN deployments — see
  :mod:`scanner.remote_hash` for details.

Both honour a ``cancel`` callback and surface progress via ``on_progress``.
"""

from __future__ import annotations

import concurrent.futures as futures
import logging
import time
from typing import Callable, Dict, List, Optional, Sequence

from duplicator_search_destroy.models.database import Database, DuplicateSet, FileRow
from duplicator_search_destroy.scanner.hasher import cascade_hash

log = logging.getLogger(__name__)

__all__ = [
    "hash_candidate_files",
    "hash_candidates_via_winrm",
    "find_duplicates",
    "group_by_size",
]


def group_by_size(files: Sequence[FileRow]) -> dict[int, list[FileRow]]:
    """Pure helper — bucket *files* by size (unit-testable without DB)."""
    buckets: dict[int, list[FileRow]] = {}
    for f in files:
        if f.size <= 0:
            continue
        buckets.setdefault(f.size, []).append(f)
    return {sz: fs for sz, fs in buckets.items() if len(fs) > 1}


def _unc_for(db: Database, f: FileRow) -> str:
    """Build the UNC path for *f* by looking up its share + host."""
    rows = db.query(
        """
        SELECT hosts.ip, hosts.hostname, shares.name AS share
          FROM files
          JOIN shares ON shares.id = files.share_id
          JOIN hosts  ON hosts.id  = shares.host_id
         WHERE files.id = ?
        """,
        (f.id,),
    )
    if not rows:
        raise KeyError(f"file {f.id} not found")
    r = rows[0]
    host = r["hostname"] or r["ip"]
    rel = f.relative_path.lstrip("\\/")
    return f"\\\\{host}\\{r['share']}\\{rel}"


def hash_candidate_files(
    db: Database,
    *,
    min_size: int = 1,
    max_workers: int = 8,
    on_progress: Optional[Callable[[int, int], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> int:
    """Hash every file that shares a size with at least one other file.

    Returns the number of files actually hashed (not the number of calls —
    files that already have ``full_hash`` set are skipped).
    """
    groups = db.size_bucket_candidates(min_size=min_size)
    candidate_ids: list[int] = [fid for _size, ids in groups for fid in ids]
    if not candidate_ids:
        return 0

    # Skip files that already have a hash (resumability).
    placeholders = ",".join("?" * len(candidate_ids))
    rows = db.query(
        f"SELECT id FROM files WHERE id IN ({placeholders}) AND full_hash IS NULL",
        tuple(candidate_ids),
    )
    pending_ids = [r["id"] for r in rows]
    total = len(pending_ids)
    if total == 0:
        return 0

    log.info("Hashing %d candidate files across %d size buckets", total, len(groups))

    # Build (id, unc_path, size) for each pending file in one query.
    placeholders = ",".join("?" * len(pending_ids))
    detail_rows = db.query(
        f"""
        SELECT files.id AS id,
               files.size AS size,
               files.relative_path AS rel,
               shares.name AS share,
               COALESCE(hosts.hostname, hosts.ip) AS host
          FROM files
          JOIN shares ON shares.id = files.share_id
          JOIN hosts  ON hosts.id  = shares.host_id
         WHERE files.id IN ({placeholders})
        """,
        tuple(pending_ids),
    )

    done = 0
    batch: list[tuple] = []

    def _flush():
        nonlocal batch
        if not batch:
            return
        db.update_file_hashes(batch)
        batch = []

    with futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="hash") as pool:
        futs: dict[futures.Future, tuple[int, int]] = {}
        for r in detail_rows:
            if cancel and cancel():
                break
            unc = f"\\\\{r['host']}\\{r['share']}\\{r['rel'].lstrip(chr(92) + '/')}"
            fid = int(r["id"])
            size = int(r["size"])
            futs[pool.submit(cascade_hash, unc, size)] = (fid, size)

        import time as _time

        for fut in futures.as_completed(futs):
            if cancel and cancel():
                for pending in futs:
                    pending.cancel()
                break
            fid, _size = futs[fut]
            try:
                result = fut.result()
            except Exception as exc:  # pragma: no cover
                log.warning("Hash future raised for file id %s: %s", fid, exc)
                continue
            batch.append(
                (
                    result.prefix_hash,
                    result.suffix_hash,
                    result.full_hash,
                    _time.time() if result.full_hash else None,
                    fid,
                )
            )
            done += 1
            if len(batch) >= 500:
                _flush()
            if on_progress:
                try:
                    on_progress(done, total)
                except Exception:
                    log.exception("on_progress callback raised")

    _flush()
    return done


def find_duplicates(db: Database, *, min_size: int = 1, limit: int = 500) -> List[DuplicateSet]:
    """Return duplicate sets already present in the DB (requires prior hashing)."""
    return db.duplicate_sets(min_size=min_size, limit=limit)


# ---------------------------------------------------------------------------
# WinRM-pushed hash back-end
# ---------------------------------------------------------------------------


def hash_candidates_via_winrm(
    db: Database,
    *,
    min_size: int = 1,
    algorithm: str = "sha256",
    throttle: int = 8,
    fallback_to_smb: bool = True,
    smb_max_workers: int = 8,
    on_progress: Optional[Callable[[int, int], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> int:
    """Hash duplicate-candidate files by invoking ``Get-FileHash`` on each host.

    Works like :func:`hash_candidate_files` but replaces the expensive
    per-file SMB read with a per-host WinRM batch. One WinRM session per
    host, many files per batch.

    If ``fallback_to_smb`` is true and WinRM fails for a host (auth,
    transport, remote error), we drop back to the SMB code path for just
    that host — one server without WinRM doesn't kill the whole run.

    Returns the count of files for which a ``full_hash`` was written.
    """
    # Lazy-import so the module tree doesn't require pypsrp at top level.
    from duplicator_search_destroy.scanner.remote_hash import (
        remote_hash_files,
        prefix_hash,
    )
    from duplicator_search_destroy.scanner.winrm_client import WinRmError

    groups = db.size_bucket_candidates(min_size=min_size)
    candidate_ids: list[int] = [fid for _size, ids in groups for fid in ids]
    if not candidate_ids:
        return 0

    placeholders = ",".join("?" * len(candidate_ids))
    rows = db.query(
        f"""
        SELECT files.id         AS id,
               files.size       AS size,
               files.relative_path AS rel,
               shares.name      AS share,
               hosts.id         AS host_id,
               hosts.ip         AS ip,
               hosts.hostname   AS hostname
          FROM files
          JOIN shares ON shares.id = files.share_id
          JOIN hosts  ON hosts.id  = shares.host_id
         WHERE files.id IN ({placeholders})
           AND files.full_hash IS NULL
        """,
        tuple(candidate_ids),
    )
    if not rows:
        return 0

    # Group pending files by (host_id, share_name) so each WinRM call stays
    # on one share. That keeps the PowerShell payload trivial (no cross-share
    # path resolution).
    by_host_share: Dict[tuple, list[dict]] = {}
    for r in rows:
        key = (r["host_id"], r["share"], r["hostname"] or r["ip"])
        by_host_share.setdefault(key, []).append(
            {"id": int(r["id"]), "rel": r["rel"], "size": int(r["size"])}
        )

    total = len(rows)
    done = 0
    update_batch: list[tuple] = []

    def _flush() -> None:
        nonlocal update_batch
        if update_batch:
            db.update_file_hashes(update_batch)
            update_batch = []

    for (host_id, share_name, host_target), file_records in by_host_share.items():
        if cancel and cancel():
            break

        cred = db.get_credentials(host_id)
        username = cred.username if cred else ""
        password = cred.password if cred else ""
        domain = (cred.domain or "") if cred else ""

        paths = [f["rel"] for f in file_records]
        try:
            results = remote_hash_files(
                host_target,
                share_name,
                paths,
                username=username,
                password=password,
                domain=domain,
                algorithm=algorithm,
                throttle=throttle,
            )
        except WinRmError as exc:
            log.warning(
                "WinRM hash failed for \\\\%s\\%s (%d files): %s",
                host_target, share_name, len(paths), exc,
            )
            if not fallback_to_smb:
                # Mark these as hashed-attempted-failed so we don't loop
                # forever (full_hash stays NULL, cancel the WinRM mode).
                continue
            # Fall back to SMB cascade for this host's files.
            _smb_hash_fallback(
                db, host_target, file_records, update_batch,
                max_workers=smb_max_workers, cancel=cancel,
            )
            done += len(file_records)
            if on_progress:
                on_progress(done, total)
            if len(update_batch) >= 500:
                _flush()
            continue

        now = time.time()
        for fr in file_records:
            rec = results.get(fr["rel"])
            if rec is None or rec.hash is None:
                # Remote couldn't hash it — leave full_hash NULL.
                done += 1
                continue
            full = prefix_hash(algorithm, rec.hash)
            update_batch.append((None, None, full, now, fr["id"]))
            done += 1
            if len(update_batch) >= 500:
                _flush()
        if on_progress:
            on_progress(done, total)

    _flush()
    return done


def _smb_hash_fallback(
    db: Database,
    host_target: str,
    file_records: Sequence[dict],
    update_batch: list,
    *,
    max_workers: int,
    cancel: Optional[Callable[[], bool]],
) -> None:
    """Hash a host's files via SMB when WinRM was unavailable. Appends to
    *update_batch* rather than flushing on its own — caller owns the
    transaction rhythm."""
    rows = db.query(
        "SELECT shares.name AS share, files.relative_path AS rel "
        "FROM files JOIN shares ON shares.id = files.share_id "
        f"WHERE files.id IN ({','.join('?' * len(file_records))})",
        tuple(f["id"] for f in file_records),
    )
    share_lookup = {(r["rel"],): r["share"] for r in rows}

    now = time.time()
    with futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="smb-hash-fb") as pool:
        futs = {}
        for f in file_records:
            if cancel and cancel():
                break
            share = share_lookup.get((f["rel"],), "")
            unc = f"\\\\{host_target}\\{share}\\{f['rel'].lstrip(chr(92) + '/')}"
            futs[pool.submit(cascade_hash, unc, f["size"])] = f["id"]
        for fut in futures.as_completed(futs):
            if cancel and cancel():
                for pending in futs:
                    pending.cancel()
                break
            fid = futs[fut]
            try:
                res = fut.result()
            except Exception as exc:  # pragma: no cover
                log.warning("SMB fallback hash raised for id %s: %s", fid, exc)
                continue
            update_batch.append(
                (res.prefix_hash, res.suffix_hash, res.full_hash, now if res.full_hash else None, fid)
            )
