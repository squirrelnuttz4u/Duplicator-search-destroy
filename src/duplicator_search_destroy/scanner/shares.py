"""Enumerate SMB shares exposed by a remote host.

Uses pywin32's ``win32net.NetShareEnum`` — the native Windows API that
Explorer, ``net view``, and every AD admin tool already call. For hosts
that require alternate credentials we first establish an authenticated
session to ``\\\\host\\IPC$`` via ``WNetAddConnection2``, then run the
enumeration under that session, then drop it. No third-party protocol
code, no red-team-flavoured dependencies (impacket) that get flagged
by Defender.

On non-Windows platforms (e.g. a Linux dev box) enumeration raises
:class:`ShareEnumerationError` with a clear message; the rest of the
app continues to work for testing with mocks.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import List, Optional

__all__ = [
    "DiscoveredShare",
    "enumerate_shares",
    "ShareEnumerationError",
]

log = logging.getLogger(__name__)

SHARE_TYPE_DISK = 0x00000000
SHARE_TYPE_PRINT = 0x00000001
SHARE_TYPE_DEVICE = 0x00000002
SHARE_TYPE_IPC = 0x00000003
SHARE_TYPE_SPECIAL = 0x80000000  # high bit = hidden/admin share

DEFAULT_SKIP = {"IPC$", "PRINT$"}


@dataclass(slots=True)
class DiscoveredShare:
    host: str
    name: str
    remark: Optional[str]
    share_type: int

    @property
    def is_disk(self) -> bool:
        return (self.share_type & 0xFF) == SHARE_TYPE_DISK

    @property
    def is_hidden(self) -> bool:
        return bool(self.share_type & SHARE_TYPE_SPECIAL) or self.name.endswith("$")


class ShareEnumerationError(Exception):
    """Raised when share enumeration failed for a host."""


def _enum_with_pywin32(
    host: str,
    *,
    username: str = "",
    password: str = "",
    domain: str = "",
) -> List[DiscoveredShare]:
    """Enumerate shares via Win32 ``NetShareEnum``.

    If *username* is supplied, first establish an authenticated session to
    ``\\\\host\\IPC$`` so the subsequent ``NetShareEnum`` uses those
    credentials. The session is dropped before the function returns.
    """
    try:
        import win32net  # type: ignore[import-not-found]
        import win32wnet  # type: ignore[import-not-found]
        import pywintypes  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ShareEnumerationError(
            "pywin32 is required for share enumeration. "
            "This feature only runs on Windows."
        ) from exc

    unc_target = f"\\\\{host}"
    ipc_target = f"\\\\{host}\\IPC$"

    cred_user: Optional[str] = None
    if username:
        # win32wnet accepts either DOMAIN\user or just user — build the first
        # form if the caller provided a domain and the username isn't already
        # domain-qualified.
        if domain and "\\" not in username:
            cred_user = f"{domain}\\{username}"
        else:
            cred_user = username

    connection_target: Optional[str] = None
    try:
        if cred_user:
            nr = win32wnet.NETRESOURCE()
            nr.lpRemoteName = ipc_target
            nr.dwType = win32wnet.RESOURCETYPE_ANY
            try:
                win32wnet.WNetAddConnection2(nr, password, cred_user, 0)
                connection_target = ipc_target
            except pywintypes.error as exc:
                raise ShareEnumerationError(
                    f"Could not authenticate to {ipc_target} as {cred_user}: {exc}"
                ) from exc

        try:
            entries, _total, _resume = win32net.NetShareEnum(unc_target, 1)
        except pywintypes.error as exc:
            raise ShareEnumerationError(
                f"NetShareEnum failed for {host}: {exc}"
            ) from exc

        result: List[DiscoveredShare] = []
        for entry in entries:
            name = str(entry.get("netname", "")).strip()
            if not name:
                continue
            result.append(
                DiscoveredShare(
                    host=host,
                    name=name,
                    remark=(str(entry.get("remark", "")) or None),
                    share_type=int(entry.get("type", 0)),
                )
            )
        return result
    finally:
        if connection_target:
            try:
                win32wnet.WNetCancelConnection2(connection_target, 0, True)
            except Exception:  # pragma: no cover - best effort
                log.debug("Failed to drop session to %s", connection_target)


def enumerate_shares(
    host: str,
    *,
    username: str = "",
    password: str = "",
    domain: str = "",
    include_hidden: bool = True,
    include_ipc: bool = False,
) -> List[DiscoveredShare]:
    """Return the share list for *host*.

    Raises :class:`ShareEnumerationError` on failure. Filters out printer
    queues (only disk shares are returned) and — unless asked — the IPC$
    pseudo-share.
    """
    if os.name != "nt":
        # Non-Windows host: we literally can't make this syscall. Tests
        # monkey-patch this out entirely.
        raise ShareEnumerationError(
            "Share enumeration requires Windows (pywin32 NetShareEnum)."
        )

    shares = _enum_with_pywin32(host, username=username, password=password, domain=domain)

    filtered: List[DiscoveredShare] = []
    for s in shares:
        if s.name in DEFAULT_SKIP and not include_ipc and s.name == "IPC$":
            continue
        if not include_hidden and s.is_hidden:
            continue
        if not s.is_disk:
            continue  # never try to walk a printer queue
        filtered.append(s)
    return filtered
