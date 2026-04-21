# Duplicator Search & Destroy — operator guide

Typical workflow for sweeping ~200 servers:

1. **Dashboard tab** — paste the subnets, one per line:
   ```
   10.0.0.0/24
   10.0.1.0/24
   10.1.0.0/23
   ```
   Press **Scan network**. Live SMB hosts (port 445 open) appear in the
   table; their hostname is resolved via reverse DNS, falling back to
   NetBIOS node-status.

2. **Credentials & Shares tab** — enter one domain account that can read
   every host, and click **Apply to every host**. Then click **Enumerate
   shares on all hosts** to populate the share list via ``NetrShareEnum``.
   Adjust per-host credentials in the table if needed.

3. **Scan & Dedup tab** — click **Scan all shares for files & folders**.
   This walks every share and writes one row per folder and one row per
   file to the local SQLite DB. Progress is reported live.

   Once file indexing finishes, click **Hash candidates & find duplicates**.
   Only files whose exact byte-size is matched on another file are hashed —
   for a typical deployment this is under 5% of files. The hash cascade
   is: xxh3 head (64 KiB) → xxh3 tail (4 KiB) → BLAKE3 full. Files that
   match the full hash are considered byte-for-byte identical.

4. **Duplicates tab** — sorted by reclaimable bytes. Export to CSV for
   offline review or to feed a deletion workflow. Values shown:
   * ``Size per file`` — each copy's size
   * ``Copies`` — how many instances exist
   * ``Wasted`` — ``(copies − 1) × size``

5. **Reports tab** — three sub-reports:
   * **Search** — wildcard-free LIKE over filename and full path
   * **Largest files** — top-N by size
   * **Largest folders** — top-N by aggregate bytes

## Re-scanning

All phases are idempotent. A re-scan of a share clears its rows and
re-inserts. Hashes that were already computed are never recomputed —
the hash phase is fully resumable, so you can stop and restart at any
point without repeating work.

## Installation options

Two ways to deploy the app — both produce the same runtime behaviour:

**Option A — portable folder (`build_windows.bat`)**

Copy `dist\DuplicatorSearchDestroy\` anywhere (USB stick, network share,
user profile). Double-click the `.exe` to run. No Start-menu entry, no
registry changes, no admin required. Best for ad-hoc use from an admin
workstation.

**Option B — installed (`build_installer.bat` + Inno Setup)**

Produces `DuplicatorSearchDestroy-Setup-<version>.exe`. The installer:

* Lets the user pick per-user or per-machine install on the first page.
* Creates Start-menu and (optionally) desktop icons.
* Adds a Control Panel uninstall entry.
* Supports silent install for mass deployment:
  ```
  setup.exe /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /DIR="C:\Program Files\DuplicatorSearchDestroy"
  ```
* Preserves the `%APPDATA%\DuplicatorSearchDestroy\` inventory DB during
  uninstall so reinstalling doesn't destroy months of indexing work.

## Database location

* **Windows**: ``%APPDATA%\DuplicatorSearchDestroy\inventory.db``
* **Elsewhere**: ``~/.config/DuplicatorSearchDestroy/inventory.db``

Override with ``--db <path>``.

## Cancelling a long scan

Every phase honours a **Cancel** button. In-flight SMB round-trips complete
normally; outstanding work items are dropped. Nothing in the DB is rolled
back — partial results are kept so you can review what was indexed.

## Troubleshooting

| Symptom                              | Fix                                              |
|--------------------------------------|--------------------------------------------------|
| Host shows offline but is up         | Port 445 firewalled — confirm with `Test-NetConnection -Port 445` |
| Share enum returns nothing           | Credentials lack `Server` role; use an admin account |
| "Access denied" during file scan     | Share permits list but not traverse — grant read |
| Hash phase very slow                 | Increase `Hash workers`; for WAN-separated hosts drop it to 1-2 |
| False positives in duplicates        | None possible — BLAKE3 collision is infeasible |

## Scale notes

* Each folder row ~120 B, each file row ~200 B. 10 M files → ~2 GB SQLite.
* Run on the server host or a well-provisioned workstation — WAL mode
  keeps writes fast, but the DB is still on disk.
* The hash phase is network-I/O bound, not CPU. Default 8 workers saturates
  a gigabit link without overwhelming any single server.
