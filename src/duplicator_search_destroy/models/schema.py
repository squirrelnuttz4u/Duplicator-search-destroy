"""SQLite schema definitions.

Kept in a single string for easy migration — bump ``SCHEMA_VERSION`` and add
a migration block whenever any column changes.
"""

from __future__ import annotations

SCHEMA_VERSION = 1

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hosts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ip            TEXT    NOT NULL UNIQUE,
    hostname      TEXT,
    status        TEXT    NOT NULL DEFAULT 'pending',
    last_seen     REAL,
    discovered_at REAL    NOT NULL DEFAULT (strftime('%s','now')),
    notes         TEXT
);
CREATE INDEX IF NOT EXISTS idx_hosts_status ON hosts(status);

CREATE TABLE IF NOT EXISTS credentials (
    host_id   INTEGER PRIMARY KEY REFERENCES hosts(id) ON DELETE CASCADE,
    domain    TEXT,
    username  TEXT NOT NULL,
    password  BLOB NOT NULL,
    updated_at REAL NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS shares (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id    INTEGER NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    name       TEXT    NOT NULL,
    remark     TEXT,
    share_type INTEGER NOT NULL DEFAULT 0,
    accessible INTEGER NOT NULL DEFAULT 0,
    last_scan  REAL,
    UNIQUE(host_id, name)
);
CREATE INDEX IF NOT EXISTS idx_shares_host ON shares(host_id);

CREATE TABLE IF NOT EXISTS folders (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    share_id     INTEGER NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    parent_id    INTEGER REFERENCES folders(id) ON DELETE CASCADE,
    name         TEXT    NOT NULL,
    relative_path TEXT   NOT NULL,
    depth        INTEGER NOT NULL,
    file_count   INTEGER NOT NULL DEFAULT 0,
    total_size   INTEGER NOT NULL DEFAULT 0,
    created_at   REAL,
    modified_at  REAL,
    UNIQUE(share_id, relative_path)
);
CREATE INDEX IF NOT EXISTS idx_folders_parent ON folders(parent_id);
CREATE INDEX IF NOT EXISTS idx_folders_size   ON folders(total_size DESC);
CREATE INDEX IF NOT EXISTS idx_folders_name   ON folders(name);

CREATE TABLE IF NOT EXISTS files (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    share_id      INTEGER NOT NULL REFERENCES shares(id) ON DELETE CASCADE,
    folder_id     INTEGER REFERENCES folders(id) ON DELETE CASCADE,
    name          TEXT    NOT NULL,
    extension     TEXT,
    relative_path TEXT    NOT NULL,
    size          INTEGER NOT NULL,
    created_at    REAL,
    modified_at   REAL,
    accessed_at   REAL,
    prefix_hash   TEXT,
    suffix_hash   TEXT,
    full_hash     TEXT,
    hashed_at     REAL,
    UNIQUE(share_id, relative_path)
);
CREATE INDEX IF NOT EXISTS idx_files_size      ON files(size DESC);
CREATE INDEX IF NOT EXISTS idx_files_name      ON files(name);
CREATE INDEX IF NOT EXISTS idx_files_full_hash ON files(full_hash);
CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension);
CREATE INDEX IF NOT EXISTS idx_files_folder    ON files(folder_id);
CREATE INDEX IF NOT EXISTS idx_files_dedup_key ON files(size, prefix_hash);

CREATE TABLE IF NOT EXISTS scan_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT    NOT NULL,   -- 'discovery' | 'shares' | 'files' | 'hash'
    started_at  REAL    NOT NULL,
    finished_at REAL,
    status      TEXT    NOT NULL DEFAULT 'running',
    message     TEXT
);
CREATE INDEX IF NOT EXISTS idx_scan_runs_kind ON scan_runs(kind);
"""
