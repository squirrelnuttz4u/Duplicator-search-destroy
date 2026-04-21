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

## Getting the Windows build

Every push to `main` and every pushed feature branch triggers the
[build-windows](.github/workflows/build-windows.yml) GitHub Actions
workflow, which:

1. Runs the 101-test pytest suite on a Windows runner.
2. Produces a self-contained PyInstaller `--onedir` build.
3. Smoke-tests the resulting `.exe` by running `--help` (catches missing
   module errors that wouldn't show up at build time).
4. Zips the onedir output and compiles the Inno Setup installer.
5. Uploads both as Actions artifacts (30-day retention).
6. On a tag push of the form `vX.Y.Z`, attaches them to a GitHub Release.

To get a build:

- **Latest from this branch**: open the repo's
  [Actions tab](https://github.com/squirrelnuttz4u/duplicator-search-destroy/actions/workflows/build-windows.yml)
  → click the most recent run → download `DuplicatorSearchDestroy-windows-x64`
  or `DuplicatorSearchDestroy-installer`.
- **Tagged release**: `git tag v1.0.0 && git push origin v1.0.0`, then
  grab the assets from the Releases page a few minutes later.

If you'd rather build locally, the instructions below still apply.

## Building the Windows .exe

On a Windows host with Python 3.11+ installed:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pip install pyinstaller
python build\build_windows.py
```

The built application appears under `dist\DuplicatorSearchDestroy\`. The
folder is fully self-contained — copy it to any Windows 10 1809+ / 11
machine, double-click `DuplicatorSearchDestroy.exe`, and it runs. No
Python installation required on target machines.

### Optional: build a Windows installer (.exe)

If you want Start-menu shortcuts, an uninstall entry in Control Panel, and
a single-file installer suitable for SCCM/Intune deployment:

1. Install [Inno Setup 6](https://jrsoftware.org/isdl.php).
2. From the repo root run:

   ```bat
   build\build_installer.bat
   ```

3. The signed-capable installer appears at
   `dist\installer\DuplicatorSearchDestroy-Setup-1.0.0.exe`.

The installer offers per-user (no admin) or per-machine (admin) install on
the first wizard page, preserves the SQLite inventory under `%APPDATA%`
across upgrades, and force-closes any running instance before overwriting
files.

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
