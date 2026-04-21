"""Hash files — cascaded size → xxh3 prefix → xxh3 suffix → BLAKE3 full.

Why cascade: most ``(size, N)`` size-groups have only a handful of members
and collisions are rare, so the cheapest hash often settles it. We only
pay for a full-file hash when prefix+suffix agree — which is almost always
a real duplicate.

All hashes return hex strings. Partial hashes use xxh3_128 (fast,
non-crypto); the final hash uses BLAKE3 (fast, cryptographically strong)
so equality implies equality without a byte-by-byte check.

Input files may live on SMB. We use plain ``open()`` on the UNC/SMB path via
``smbclient.open_file``; the caller is expected to have registered a
session for the host already.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, Optional, Tuple

__all__ = [
    "HashResult",
    "hash_prefix",
    "hash_suffix",
    "hash_full",
    "cascade_hash",
    "PREFIX_BYTES",
    "SUFFIX_BYTES",
]

log = logging.getLogger(__name__)

PREFIX_BYTES = 64 * 1024  # 64 KiB head — captures most file-format magic
SUFFIX_BYTES = 4 * 1024   # 4 KiB tail — catches head-identical logs/binaries
READ_CHUNK = 1 * 1024 * 1024  # 1 MiB


@dataclass(slots=True)
class HashResult:
    size: int
    prefix_hash: Optional[str]
    suffix_hash: Optional[str]
    full_hash: Optional[str]
    error: Optional[str] = None


def _open_file(path: str):
    """Open *path* for binary reading.

    When the path is a UNC path we use smbclient so authenticated sessions
    are honoured; otherwise fall back to built-in open().
    """
    if path.startswith("\\\\") or path.startswith("//"):
        try:
            import smbclient  # type: ignore[import-not-found]
        except ImportError:  # pragma: no cover
            raise RuntimeError("smbprotocol is required to read UNC paths")
        return smbclient.open_file(path, mode="rb")
    return open(path, "rb")


def _xxh3_128():
    import xxhash  # type: ignore[import-not-found]
    return xxhash.xxh3_128()


def _blake3():
    try:
        import blake3  # type: ignore[import-not-found]
        return blake3.blake3()
    except ImportError:
        # Fallback — SHA-256 is slower but always available.
        import hashlib
        return hashlib.sha256()


def hash_prefix(path: str, *, size: int | None = None, nbytes: int = PREFIX_BYTES) -> str:
    h = _xxh3_128()
    with _open_file(path) as fh:
        data = fh.read(nbytes)
    h.update(data)
    return h.hexdigest()


def hash_suffix(path: str, *, size: int, nbytes: int = SUFFIX_BYTES) -> str:
    h = _xxh3_128()
    read_from = max(0, size - nbytes)
    with _open_file(path) as fh:
        try:
            fh.seek(read_from)
        except (OSError, ValueError):
            # Tiny file or non-seekable — just hash whatever read_prefix returned.
            pass
        data = fh.read(nbytes)
    h.update(data)
    return h.hexdigest()


def hash_full(
    path: str,
    *,
    size: Optional[int] = None,
    on_progress: Optional[Callable[[int, Optional[int]], None]] = None,
) -> str:
    h = _blake3()
    read_so_far = 0
    with _open_file(path) as fh:
        while True:
            chunk = fh.read(READ_CHUNK)
            if not chunk:
                break
            h.update(chunk)
            read_so_far += len(chunk)
            if on_progress:
                try:
                    on_progress(read_so_far, size)
                except Exception:
                    pass
    return h.hexdigest()


def cascade_hash(path: str, size: int) -> HashResult:
    """Produce all three tiers for *path* in one call.

    Used when every file in a size-bucket candidate needs fingerprinting.
    """
    try:
        with _open_file(path) as fh:
            head = fh.read(PREFIX_BYTES)
            prefix = _xxh3_128()
            prefix.update(head)
            prefix_hex = prefix.hexdigest()

            if size > PREFIX_BYTES + SUFFIX_BYTES:
                try:
                    fh.seek(size - SUFFIX_BYTES)
                    tail = fh.read(SUFFIX_BYTES)
                except (OSError, ValueError):
                    tail = b""
            else:
                tail = b""
            suffix = _xxh3_128()
            suffix.update(tail)
            suffix_hex = suffix.hexdigest()

            full = _blake3()
            full.update(head)
            remaining = size - len(head)
            if remaining > 0:
                try:
                    fh.seek(len(head))
                except (OSError, ValueError):
                    pass
                while True:
                    chunk = fh.read(READ_CHUNK)
                    if not chunk:
                        break
                    full.update(chunk)
            full_hex = full.hexdigest()
        return HashResult(
            size=size,
            prefix_hash=prefix_hex,
            suffix_hash=suffix_hex,
            full_hash=full_hex,
        )
    except Exception as exc:
        log.warning("Failed to hash %s: %s", path, exc)
        return HashResult(size=size, prefix_hash=None, suffix_hash=None, full_hash=None, error=str(exc))


def hash_local_bytes(data: bytes) -> Tuple[str, str]:
    """Utility used by unit tests: return (xxh3_128_hex, blake3_hex) for *data*."""
    x = _xxh3_128()
    x.update(data)
    b = _blake3()
    b.update(data)
    return x.hexdigest(), b.hexdigest()


def hash_local_file(path: str) -> HashResult:
    size = os.path.getsize(path)
    return cascade_hash(path, size)
