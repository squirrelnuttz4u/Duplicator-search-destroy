"""Duplicate-detection driver.

Two entry points:

* :func:`find_duplicates` — high-level, reads the DB, produces a list of
  :class:`~duplicator_search_destroy.models.database.DuplicateSet`.
* :func:`hash_candidate_files` — the expensive step: fingerprints every file
  in every size-bucket that has more than one member, writing results back
  into the DB. The UI calls this during the "Find duplicates" phase.

Both honour a ``cancel`` callback and surface progress via ``on_progress``.
"""

from __future__ import annotations

import concurrent.futures as futures
import logging
from typing import Callable, List, Optional, Sequence

from duplicator_search_destroy.models.database import Database, DuplicateSet, FileRow
from duplicator_search_destroy.scanner.hasher import cascade_hash

log = logging.getLogger(__name__)

__all__ = ["hash_candidate_files", "find_duplicates", "group_by_size"]


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
