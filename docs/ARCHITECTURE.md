# Architecture

```
+---------------------- GUI (PySide6) --------------------------------+
| DashboardTab | CredentialsTab | ScanTab | DuplicatesTab | ReportsTab |
+--------------------------+------------------------------------------+
                           | QThread + Signal/Slot
                           v
                    +----------------+
                    |  Orchestrator  |   (scanner/orchestrator.py)
                    +----------------+
           /           |            |            \
          v            v            v             v
  discover_hosts  enumerate_shares  walk_share  hash_candidate_files
     (network)       (shares)       (files)        (duplicates +
                                                    hasher)
          \            |            |             /
           \           v            v            /
            +----------- Database (SQLite/WAL) -----------+
            |   hosts, credentials, shares, folders,      |
            |   files, scan_runs                          |
            +---------------------------------------------+
```

## Modules

| Module                          | Responsibility                                                      |
|---------------------------------|---------------------------------------------------------------------|
| `utils.ip_utils`                | Parse/expand/count IPv4 targets (CIDR, ranges, wildcards)           |
| `utils.crypto`                  | DPAPI (Windows) / Fernet (fallback) credential encryption           |
| `utils.formatting`              | Human-readable size/time/path helpers                               |
| `utils.logging_setup`           | Single `configure_logging()` entry point                            |
| `models.schema`                 | SQL DDL + pragmas                                                   |
| `models.database`               | `Database` DAO with typed row dataclasses                           |
| `scanner.network`               | TCP 445 probe + NetBIOS node-status resolver                        |
| `scanner.shares`                | `NetrShareEnum` via impacket / pywin32                              |
| `scanner.files`                 | `walk_share` — DFS walk over an SMB share                           |
| `scanner.hasher`                | Cascaded xxh3 prefix/suffix + BLAKE3 full hash                      |
| `scanner.duplicates`            | Size-bucket → hash candidate files → duplicate sets                 |
| `scanner.orchestrator`          | Phase coordination; DB writes; progress + cancel plumbing           |
| `gui.worker`                    | QObject that runs scanner fns on a QThread                          |
| `gui.*_tab`                     | One tab per workflow phase                                          |
| `gui.main_window`               | QMainWindow, menu bar, tab wiring                                   |

## Key design choices

**One SQLite connection, many threads.** All DB writes go through a single
`Database` instance guarded by an RLock. Bulk inserts use
`executemany_txn()` — a single BEGIN/COMMIT around ≤1000 rows. This keeps
the hot path ≤50 µs/row even at 10M files.

**Parent id resolved at insert time, not after.** The walker emits folders
in DFS pre-order, so we can look up the parent's primary key from a small
in-memory dict rather than running a post-pass UPDATE with brittle string
arithmetic.

**Hash cascade.** Most size-groups are singletons; the ones that aren't
rarely survive a prefix+suffix xxh3 comparison. Only files that agree on
both get a full BLAKE3 hash — typically <1% of the total bytes on disk.

**Resumability.** Files already carrying a `full_hash` are skipped by
`hash_candidate_files()`. A crashed/cancelled hash phase resumes where it
left off on the next click.

**GUI is thin.** Every tab is a passive view — tabs hand work to the
`Orchestrator` via a `ScanWorker` that runs on a `QThread`. The scanner
modules have zero Qt imports, which is why they're fully unit-testable.
