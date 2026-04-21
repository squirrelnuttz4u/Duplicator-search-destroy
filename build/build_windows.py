"""PyInstaller build driver for the Windows .exe.

Invoke from a Windows shell with a venv active:

    python build\\build_windows.py

Produces ``dist/DuplicatorSearchDestroy/DuplicatorSearchDestroy.exe``.
Use ``--onefile`` for a single-file build at the cost of slower startup and
more aggressive AV false positives.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENTRY = ROOT / "src" / "duplicator_search_destroy" / "__main__.py"
NAME = "DuplicatorSearchDestroy"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--onefile", action="store_true", help="Produce a single .exe")
    ap.add_argument("--clean", action="store_true", help="Purge build artefacts first")
    ap.add_argument("--debug", action="store_true", help="Console window + unstripped binary")
    args = ap.parse_args()

    if args.clean:
        for d in ("build/output", "dist"):
            p = ROOT / d
            if p.exists():
                shutil.rmtree(p)

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", NAME,
        "--noconfirm",
        "--workpath", str(ROOT / "build" / "output"),
        "--distpath", str(ROOT / "dist"),
        "--specpath", str(ROOT / "build"),
        "--paths", str(ROOT / "src"),
        # ---- networking + SMB stack -----------------------------------
        "--collect-all", "smbprotocol",
        "--collect-all", "smbclient",
        "--collect-all", "spnego",
        "--collect-all", "impacket",
        # ---- crypto ----------------------------------------------------
        # cryptography uses cffi and dynamically imports backend modules
        # that PyInstaller's default scan misses — `--collect-all` is the
        # reliable fix. `--copy-metadata` is required because some of the
        # downstream libraries check the installed version via
        # importlib.metadata at import time.
        "--collect-all", "cryptography",
        "--copy-metadata", "cryptography",
        "--collect-submodules", "cffi",
        "--hidden-import", "_cffi_backend",
        # impacket pulls these in via runtime imports:
        "--collect-submodules", "pyasn1",
        "--collect-submodules", "pyasn1_modules",
        "--hidden-import", "Cryptodome",
        "--hidden-import", "Cryptodome.Cipher",
        "--hidden-import", "Cryptodome.Hash",
        "--hidden-import", "Cryptodome.PublicKey",
        "--hidden-import", "Crypto",
        # ---- GUI -------------------------------------------------------
        "--collect-all", "PySide6",
        # ---- hashing ---------------------------------------------------
        "--collect-all", "blake3",
        "--collect-all", "xxhash",
    ]
    if not args.debug:
        cmd += ["--windowed"]
    if args.onefile:
        cmd += ["--onefile"]
    else:
        cmd += ["--onedir"]
    cmd.append(str(ENTRY))

    print(">>>", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc == 0:
        print(f"\n[OK] Build complete: {ROOT / 'dist' / NAME}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
