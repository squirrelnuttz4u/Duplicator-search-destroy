"""SQLite wrapper.

Exposes a thin ``Database`` class with a context-manager-friendly API and a
set of typed helper methods the rest of the application calls. Every write
method is parameter-bound; no string interpolation into SQL.

Thread-safety: SQLite is used with ``check_same_thread=False`` and a single
connection guarded by a re-entrant lock. Scanner threads submit writes
through :meth:`executemany_txn` so a single BEGIN/COMMIT groups thousands of
rows — essential for 10M-file indexes.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Sequence

from duplicator_search_destroy.models.schema import SCHEMA_SQL, SCHEMA_VERSION
from duplicator_search_destroy.utils.crypto import CredentialCipher, default_cipher

log = logging.getLogger(__name__)

__all__ = [
    "Database",
    "open_database",
    "Host",
    "Share",
    "Credential",
    "FolderRow",
    "FileRow",
    "DuplicateSet",
]


# ---------------------------------------------------------------------------
# Row dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Host:
    id: int
    ip: str
    hostname: Optional[str]
    status: str
    last_seen: Optional[float]


@dataclass(slots=True)
class Credential:
    host_id: int
    domain: Optional[str]
    username: str
    password: str  # plaintext — decrypted when read from the DB


@dataclass(slots=True)
class Share:
    id: int
    host_id: int
    name: str
    remark: Optional[str]
    share_type: int
    accessible: bool
    last_scan: Optional[float]


@dataclass(slots=True)
class FolderRow:
    id: int
    share_id: int
    parent_id: Optional[int]
    name: str
    relative_path: str
    depth: int
    file_count: int
    total_size: int


@dataclass(slots=True)
class FileRow:
    id: int
    share_id: int
    folder_id: Optional[int]
    name: str
    extension: Optional[str]
    relative_path: str
    size: int
    created_at: Optional[float]
    modified_at: Optional[float]
    full_hash: Optional[str]


@dataclass(slots=True)
class DuplicateSet:
    full_hash: str
    size: int
    count: int
    wasted_bytes: int
    files: Sequence[FileRow]


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def open_database(path: str | Path) -> "Database":
    return Database(Path(path))


class Database:
    def __init__(self, path: Path, *, cipher: Optional[CredentialCipher] = None) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(path),
            check_same_thread=False,
            isolation_level=None,  # explicit transactions
            timeout=30.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._cipher = cipher or default_cipher()
        self._init_schema()

    # -- lifecycle ---------------------------------------------------------

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- low-level helpers -------------------------------------------------

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    def executemany_txn(self, sql: str, rows: Iterable[Sequence[Any]]) -> int:
        """Bulk insert/update inside a single transaction. Returns row count."""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                cur.executemany(sql, rows)
                count = cur.rowcount
                cur.execute("COMMIT")
                return count
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params))

    # -- hosts -------------------------------------------------------------

    def upsert_host(self, ip: str, *, hostname: Optional[str] = None, status: str = "pending") -> int:
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO hosts(ip, hostname, status, last_seen)
                     VALUES(?, ?, ?, ?)
                ON CONFLICT(ip) DO UPDATE SET
                    hostname  = COALESCE(excluded.hostname, hosts.hostname),
                    status    = excluded.status,
                    last_seen = excluded.last_seen
                RETURNING id
                """,
                (ip, hostname, status, now),
            )
            return cur.fetchone()[0]

    def set_host_status(self, host_id: int, status: str, *, hostname: Optional[str] = None) -> None:
        with self._lock:
            if hostname is not None:
                self._conn.execute(
                    "UPDATE hosts SET status = ?, hostname = ?, last_seen = ? WHERE id = ?",
                    (status, hostname, time.time(), host_id),
                )
            else:
                self._conn.execute(
                    "UPDATE hosts SET status = ?, last_seen = ? WHERE id = ?",
                    (status, time.time(), host_id),
                )

    def list_hosts(self) -> list[Host]:
        rows = self.query("SELECT id, ip, hostname, status, last_seen FROM hosts ORDER BY ip")
        return [Host(**dict(r)) for r in rows]

    def get_host(self, host_id: int) -> Optional[Host]:
        rows = self.query(
            "SELECT id, ip, hostname, status, last_seen FROM hosts WHERE id = ?",
            (host_id,),
        )
        return Host(**dict(rows[0])) if rows else None

    def delete_host(self, host_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM hosts WHERE id = ?", (host_id,))

    # -- credentials -------------------------------------------------------

    def set_credentials(self, host_id: int, username: str, password: str, *, domain: Optional[str] = None) -> None:
        encrypted = self._cipher.encrypt(password)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO credentials(host_id, domain, username, password, updated_at)
                     VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(host_id) DO UPDATE SET
                    domain     = excluded.domain,
                    username   = excluded.username,
                    password   = excluded.password,
                    updated_at = excluded.updated_at
                """,
                (host_id, domain, username, encrypted, time.time()),
            )

    def apply_credentials_to_all(self, username: str, password: str, *, domain: Optional[str] = None) -> int:
        encrypted = self._cipher.encrypt(password)
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                cur.execute("SELECT id FROM hosts")
                ids = [r[0] for r in cur.fetchall()]
                cur.executemany(
                    """
                    INSERT INTO credentials(host_id, domain, username, password, updated_at)
                         VALUES(?, ?, ?, ?, ?)
                    ON CONFLICT(host_id) DO UPDATE SET
                        domain=excluded.domain,
                        username=excluded.username,
                        password=excluded.password,
                        updated_at=excluded.updated_at
                    """,
                    [(hid, domain, username, encrypted, now) for hid in ids],
                )
                cur.execute("COMMIT")
                return len(ids)
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def get_credentials(self, host_id: int) -> Optional[Credential]:
        rows = self.query(
            "SELECT host_id, domain, username, password FROM credentials WHERE host_id = ?",
            (host_id,),
        )
        if not rows:
            return None
        r = rows[0]
        try:
            pwd = self._cipher.decrypt(bytes(r["password"]))
        except Exception as exc:  # pragma: no cover
            log.warning("Could not decrypt credentials for host %s: %s", host_id, exc)
            return None
        return Credential(host_id=r["host_id"], domain=r["domain"], username=r["username"], password=pwd)

    def delete_credentials(self, host_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM credentials WHERE host_id = ?", (host_id,))

    # -- shares ------------------------------------------------------------

    def upsert_share(
        self,
        host_id: int,
        name: str,
        *,
        remark: Optional[str] = None,
        share_type: int = 0,
        accessible: bool = False,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO shares(host_id, name, remark, share_type, accessible)
                     VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(host_id, name) DO UPDATE SET
                    remark     = excluded.remark,
                    share_type = excluded.share_type,
                    accessible = excluded.accessible
                RETURNING id
                """,
                (host_id, name, remark, share_type, 1 if accessible else 0),
            )
            return cur.fetchone()[0]

    def list_shares(self, host_id: Optional[int] = None) -> list[Share]:
        if host_id is None:
            rows = self.query(
                "SELECT id, host_id, name, remark, share_type, accessible, last_scan FROM shares ORDER BY host_id, name"
            )
        else:
            rows = self.query(
                "SELECT id, host_id, name, remark, share_type, accessible, last_scan FROM shares WHERE host_id = ? ORDER BY name",
                (host_id,),
            )
        return [
            Share(
                id=r["id"],
                host_id=r["host_id"],
                name=r["name"],
                remark=r["remark"],
                share_type=r["share_type"],
                accessible=bool(r["accessible"]),
                last_scan=r["last_scan"],
            )
            for r in rows
        ]

    def mark_share_scanned(self, share_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE shares SET last_scan = ? WHERE id = ?", (time.time(), share_id)
            )

    # -- folders / files ---------------------------------------------------

    def clear_share_index(self, share_id: int) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                cur.execute("DELETE FROM files WHERE share_id = ?", (share_id,))
                cur.execute("DELETE FROM folders WHERE share_id = ?", (share_id,))
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def insert_folders(self, rows: Iterable[tuple]) -> int:
        return self.executemany_txn(
            """
            INSERT OR REPLACE INTO folders(
                share_id, parent_id, name, relative_path, depth,
                file_count, total_size, created_at, modified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def insert_files(self, rows: Iterable[tuple]) -> int:
        return self.executemany_txn(
            """
            INSERT OR REPLACE INTO files(
                share_id, folder_id, name, extension, relative_path, size,
                created_at, modified_at, accessed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def update_file_hashes(self, updates: Iterable[tuple]) -> int:
        """``updates`` = iterable of ``(prefix_hash, suffix_hash, full_hash, hashed_at, file_id)``."""
        return self.executemany_txn(
            "UPDATE files SET prefix_hash = ?, suffix_hash = ?, full_hash = ?, hashed_at = ? WHERE id = ?",
            updates,
        )

    # -- reporting ---------------------------------------------------------

    def size_bucket_candidates(self, *, min_size: int = 1) -> list[tuple[int, list[int]]]:
        """Return groups of file ids that share the same size (size > 1 byte)."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT size, GROUP_CONCAT(id) AS ids, COUNT(*) AS n
                  FROM files
                 WHERE size >= ?
                 GROUP BY size
                HAVING n > 1
                """,
                (min_size,),
            ).fetchall()
        result: list[tuple[int, list[int]]] = []
        for r in rows:
            ids = [int(x) for x in (r["ids"] or "").split(",") if x]
            result.append((r["size"], ids))
        return result

    def fetch_files_by_ids(self, ids: Sequence[int]) -> list[FileRow]:
        if not ids:
            return []
        placeholders = ",".join("?" * len(ids))
        rows = self.query(
            f"""
            SELECT id, share_id, folder_id, name, extension, relative_path,
                   size, created_at, modified_at, full_hash
              FROM files
             WHERE id IN ({placeholders})
            """,
            tuple(ids),
        )
        return [FileRow(**dict(r)) for r in rows]

    def duplicate_sets(self, *, min_size: int = 1, limit: int = 500) -> list[DuplicateSet]:
        rows = self.query(
            """
            SELECT full_hash,
                   MAX(size)    AS size,
                   COUNT(*)     AS n,
                   (COUNT(*) - 1) * MAX(size) AS wasted
              FROM files
             WHERE full_hash IS NOT NULL
               AND size >= ?
             GROUP BY full_hash
            HAVING n > 1
             ORDER BY wasted DESC
             LIMIT ?
            """,
            (min_size, limit),
        )
        sets: list[DuplicateSet] = []
        for r in rows:
            files = [
                FileRow(**dict(f))
                for f in self.query(
                    """
                    SELECT id, share_id, folder_id, name, extension, relative_path,
                           size, created_at, modified_at, full_hash
                      FROM files
                     WHERE full_hash = ?
                     ORDER BY relative_path
                    """,
                    (r["full_hash"],),
                )
            ]
            sets.append(
                DuplicateSet(
                    full_hash=r["full_hash"],
                    size=r["size"],
                    count=r["n"],
                    wasted_bytes=r["wasted"],
                    files=files,
                )
            )
        return sets

    def largest_files(self, limit: int = 100) -> list[FileRow]:
        rows = self.query(
            """
            SELECT id, share_id, folder_id, name, extension, relative_path,
                   size, created_at, modified_at, full_hash
              FROM files
             ORDER BY size DESC
             LIMIT ?
            """,
            (limit,),
        )
        return [FileRow(**dict(r)) for r in rows]

    def largest_folders(self, limit: int = 100) -> list[FolderRow]:
        rows = self.query(
            """
            SELECT id, share_id, parent_id, name, relative_path, depth,
                   file_count, total_size
              FROM folders
             ORDER BY total_size DESC
             LIMIT ?
            """,
            (limit,),
        )
        return [FolderRow(**dict(r)) for r in rows]

    def search(self, pattern: str, *, limit: int = 500) -> list[FileRow]:
        like = f"%{pattern}%"
        rows = self.query(
            """
            SELECT id, share_id, folder_id, name, extension, relative_path,
                   size, created_at, modified_at, full_hash
              FROM files
             WHERE name LIKE ? OR relative_path LIKE ?
             ORDER BY size DESC
             LIMIT ?
            """,
            (like, like, limit),
        )
        return [FileRow(**dict(r)) for r in rows]

    def counts(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for table in ("hosts", "shares", "folders", "files"):
            row = self.query(f"SELECT COUNT(*) AS n FROM {table}")[0]
            result[table] = int(row["n"])
        return result

    # -- scan runs ---------------------------------------------------------

    def start_run(self, kind: str, message: str = "") -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO scan_runs(kind, started_at, status, message) VALUES(?, ?, 'running', ?) RETURNING id",
                (kind, time.time(), message),
            )
            return cur.fetchone()[0]

    def finish_run(self, run_id: int, status: str = "done", message: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE scan_runs SET finished_at = ?, status = ?, message = ? WHERE id = ?",
                (time.time(), status, message, run_id),
            )
