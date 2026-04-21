# Duplicator Search & Destroy

A native Windows application that discovers SMB shares across one or more IP
subnets, inventories every file and folder on those shares into a local
SQLite database, detects duplicate files across the entire network, and
provides a search & reporting UI to find where data lives and reclaim space.

Built for environments with large numbers of Windows servers (tested design
target: 200+ servers, 10M+ files).

## Features

- **Network dashboard** — enter multiple IPv4 subnets (CIDR or ranges),
  probe port 445, discover live SMB hosts, resolve computer names, list
  every share.
- **Credentials manager** — per-host `DOMAIN\user` + password, or "apply
  credentials to entire list" for a single domain account. Stored encrypted
  on disk via Windows DPAPI (Fernet fallback on non-Windows).
- **File inventory scanner** — walks every share, records every file and
  folder with size, created/modified/accessed timestamps, file count per
  folder, full UNC path.
- **Duplicate detection** — cascaded size → xxh3 prefix → xxh3 suffix →
  BLAKE3 full-hash pipeline. Only reads bytes when it has to.
- **Reports** — top-N largest folders, top-N largest files, duplicate
  sets sorted by wasted space, free-text search over path/filename.
- **Offline-safe** — every pass is resumable; the SQLite index survives
  between runs and incremental re-scans only touch changed files.

## Tech stack

| Component        | Library                               |
|------------------|---------------------------------------|
| GUI              | PySide6 (Qt 6)                        |
| SMB              | smbprotocol + smbclient               |
| Share enum       | impacket (`srvsvc.hNetrShareEnum`)    |
| Non-crypto hash  | xxhash (xxh3_128)                     |
| Crypto hash      | blake3                                |
| Storage          | SQLite (stdlib, WAL mode)             |
| Packaging        | PyInstaller 6 (`--onedir`, Windows)   |

## Building the Windows .exe

On a Windows host with Python 3.11+ installed:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install pyinstaller
python build\build_windows.py
```

The built application appears under `dist\DuplicatorSearchDestroy\`.

## Running from source

```bash
python -m duplicator_search_destroy
```

## Running tests

```bash
python -m pytest -q
```

## Project layout

```
src/duplicator_search_destroy/
  __main__.py            # python -m entry point
  app.py                 # QApplication wiring
  scanner/               # network + SMB + hashing engine
  gui/                   # PySide6 windows, tabs, dialogs
  models/                # SQLite schema + DAO
  utils/                 # ip, crypto, logging helpers
tests/                   # pytest suite
build/                   # PyInstaller spec + build script
```

## License

MIT — see `LICENSE`.
