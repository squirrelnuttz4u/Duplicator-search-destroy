"""Walk files & folders inside an SMB share.

Strategy:
* Uses ``smbclient`` (from smbprotocol) which exposes ``scandir``/``stat``
  semantics over SMB2/3. ``scandir`` returns ``DirEntry``-like objects with
  stat pre-populated — one round trip per directory instead of one per entry.
* Produces two iterables: :class:`WalkedFolder` (one per directory) and
  :class:`WalkedFile` (one per file). The caller drains them into the DB in
  bulk.

The walker is fault-tolerant: a permission error on a subtree is logged and
skipped, not fatal.
"""

from __future__ import annotations

import logging
import os
import posixpath
import time
from dataclasses import dataclass
from typing import Callable, Iterator, Optional, Tuple

__all__ = ["WalkedFile", "WalkedFolder", "walk_share", "register_session", "unregister_session"]

log = logging.getLogger(__name__)


@dataclass(slots=True)
class WalkedFile:
    relative_path: str
    name: str
    extension: Optional[str]
    size: int
    created_at: Optional[float]
    modified_at: Optional[float]
    accessed_at: Optional[float]
    folder_rel_path: str


@dataclass(slots=True)
class WalkedFolder:
    relative_path: str
    name: str
    depth: int
    parent_rel_path: Optional[str]
    file_count: int
    total_size: int
    created_at: Optional[float]
    modified_at: Optional[float]


def register_session(host: str, *, username: str = "", password: str = "", domain: str = "") -> None:
    """Authenticate a reusable SMB session for *host*.

    ``smbclient`` maintains an internal connection pool keyed by host — once
    registered, subsequent calls just reuse the session.
    """
    try:
        import smbclient  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover
        raise RuntimeError("smbprotocol/smbclient is not installed")
    # smbclient accepts domain either in username (DOMAIN\\user) or as a separate
    # argument via register_session connection_cache. The simplest portable path
    # is to encode it into the username if provided.
    full_user = f"{domain}\\{username}" if domain and username and "\\" not in username else username
    smbclient.register_session(host, username=full_user or None, password=password or None)


def unregister_session(host: str) -> None:
    try:
        import smbclient  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover
        return
    try:
        smbclient.delete_session(host)
    except Exception:  # pragma: no cover - best effort
        pass


def _norm_join(*parts: str) -> str:
    """UNC-friendly path join that normalises slashes to backslash."""
    joined = posixpath.join(*(p.replace("\\", "/") for p in parts if p))
    return joined.replace("/", "\\")


def _ext_of(name: str) -> Optional[str]:
    _, ext = os.path.splitext(name)
    return ext.lower().lstrip(".") if ext else None


def walk_share(
    host: str,
    share: str,
    *,
    subpath: str = "",
    max_depth: int = 64,
    cancel: Optional[Callable[[], bool]] = None,
    on_progress: Optional[Callable[[str, int, int], None]] = None,
) -> Iterator[Tuple[WalkedFolder, Iterator[WalkedFile]]]:
    """Yield ``(folder, files_iter)`` pairs in depth-first order.

    ``files_iter`` must be consumed before advancing to the next folder,
    because we share a single underlying SMB TreeConnect.

    The root of the share is yielded first with ``relative_path == ''``.
    """
    try:
        import smbclient  # type: ignore[import-not-found]
    except ImportError:  # pragma: no cover
        raise RuntimeError("smbprotocol/smbclient is not installed")

    unc_root = f"\\\\{host}\\{share}"
    start_rel = subpath.strip("\\/")
    start_unc = _norm_join(unc_root, start_rel) if start_rel else unc_root

    # Stack: (relative_path, absolute_unc, depth, parent_rel)
    stack: list[tuple[str, str, int, Optional[str]]] = [
        (start_rel, start_unc, 0, None)
    ]

    total_files_seen = 0
    while stack:
        if cancel and cancel():
            log.info("walk_share cancelled at %s", unc_root)
            return
        rel, absolute, depth, parent_rel = stack.pop()
        try:
            entries = list(smbclient.scandir(absolute))
        except PermissionError as exc:
            log.warning("Permission denied: %s (%s)", absolute, exc)
            continue
        except FileNotFoundError:
            log.warning("Path disappeared during scan: %s", absolute)
            continue
        except Exception as exc:
            log.warning("Failed to scan %s: %s", absolute, exc)
            continue

        # Classify entries into subfolders vs files.
        subfolders: list[tuple[str, str, int, Optional[str]]] = []
        file_entries: list = []
        folder_created: Optional[float] = None
        folder_modified: Optional[float] = None
        try:
            stat_dir = smbclient.stat(absolute)
            folder_created = getattr(stat_dir, "st_birthtime", None) or stat_dir.st_ctime
            folder_modified = stat_dir.st_mtime
        except Exception:
            pass

        file_count = 0
        total_size = 0
        for entry in entries:
            name = entry.name
            if name in (".", ".."):
                continue
            try:
                if entry.is_dir():
                    if depth + 1 <= max_depth:
                        child_rel = _norm_join(rel, name) if rel else name
                        child_abs = _norm_join(absolute, name)
                        subfolders.append((child_rel, child_abs, depth + 1, rel or ""))
                else:
                    file_entries.append(entry)
                    st = entry.stat()
                    file_count += 1
                    total_size += int(st.st_size)
            except Exception as exc:
                log.debug("Skipping %s in %s: %s", name, absolute, exc)

        folder = WalkedFolder(
            relative_path=rel,
            name=posixpath.basename(rel.replace("\\", "/")) if rel else share,
            depth=depth,
            parent_rel_path=parent_rel,
            file_count=file_count,
            total_size=total_size,
            created_at=folder_created,
            modified_at=folder_modified,
        )

        def _file_iter(parent_abs=absolute, parent_rel=rel, entries=file_entries):
            for entry in entries:
                try:
                    st = entry.stat()
                except Exception as exc:
                    log.debug("stat failed on %s: %s", entry.name, exc)
                    continue
                name = entry.name
                rel_path = _norm_join(parent_rel, name) if parent_rel else name
                yield WalkedFile(
                    relative_path=rel_path,
                    name=name,
                    extension=_ext_of(name),
                    size=int(st.st_size),
                    created_at=getattr(st, "st_birthtime", None) or st.st_ctime,
                    modified_at=st.st_mtime,
                    accessed_at=st.st_atime,
                    folder_rel_path=parent_rel,
                )

        yield folder, _file_iter()

        total_files_seen += file_count
        if on_progress:
            try:
                on_progress(absolute, file_count, total_files_seen)
            except Exception:
                log.exception("on_progress callback raised")

        # Pushing in reverse keeps depth-first lexical order consistent.
        for child in reversed(subfolders):
            stack.append(child)

        # Tiny pause to avoid saturating a single-CPU SMB server.
        time.sleep(0)
