"""Enumerate SMB shares exposed by a remote host.

Two strategies, tried in order:

1. **impacket srvsvc** — NetrShareEnum over DCE-RPC. Works cross-platform,
   supports alternate credentials. This is the canonical equivalent of
   Windows' ``NetShareEnum`` and is what CrackMapExec and NetExec use.
2. **pywin32** fallback — ``win32net.NetShareEnum``. Only available on
   Windows with the current-user token already authenticated to the target.

Both paths return the same :class:`DiscoveredShare` dataclass so the caller
never has to branch on which backend succeeded.
"""

from __future__ import annotations

import logging
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

# Default-hidden shares we usually DO want to inspect (admin shares are fine
# when the credentials allow). Callers can pass ``include_hidden=False`` to
# filter them out.
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
    """Raised when every enumeration strategy failed for a host."""


def _enum_with_impacket(
    host: str,
    *,
    username: str = "",
    password: str = "",
    domain: str = "",
    timeout: int = 10,
) -> List[DiscoveredShare]:
    """Call ``NetrShareEnum`` via impacket."""
    try:
        from impacket.smbconnection import SMBConnection  # type: ignore[import-not-found]
        from impacket.dcerpc.v5 import transport, srvs  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - import guard
        raise ShareEnumerationError("impacket is not installed") from exc

    smb = SMBConnection(host, host, sess_port=445, timeout=timeout)
    try:
        smb.login(username, password, domain)
    except Exception as exc:
        try:
            smb.close()
        except Exception:
            pass
        raise ShareEnumerationError(f"SMB login failed for {host}: {exc}") from exc

    try:
        rpctransport = transport.SMBTransport(
            smb.getRemoteHost(), smb.getRemoteHost(), filename=r"\srvsvc", smb_connection=smb
        )
        dce = rpctransport.get_dce_rpc()
        dce.connect()
        dce.bind(srvs.MSRPC_UUID_SRVS)
        resp = srvs.hNetrShareEnum(dce, 1)
        shares: List[DiscoveredShare] = []
        for entry in resp["InfoStruct"]["ShareInfo"]["Level1"]["Buffer"]:
            name = str(entry["shi1_netname"][:-1])  # strip trailing NUL
            remark = str(entry["shi1_remark"][:-1]) if entry["shi1_remark"] else None
            stype = int(entry["shi1_type"])
            shares.append(DiscoveredShare(host=host, name=name, remark=remark, share_type=stype))
        dce.disconnect()
        return shares
    finally:
        try:
            smb.close()
        except Exception:
            pass


def _enum_with_pywin32(host: str) -> List[DiscoveredShare]:  # pragma: no cover - Windows-only
    try:
        import win32net  # type: ignore[import-not-found]
        import win32netcon  # type: ignore[import-not-found]  # noqa: F401
    except ImportError as exc:
        raise ShareEnumerationError("pywin32 not available") from exc
    try:
        entries, _total, _resume = win32net.NetShareEnum(f"\\\\{host}", 1)
    except Exception as exc:
        raise ShareEnumerationError(f"NetShareEnum failed for {host}: {exc}") from exc
    return [
        DiscoveredShare(
            host=host,
            name=str(e["netname"]),
            remark=str(e.get("remark", "")) or None,
            share_type=int(e["type"]),
        )
        for e in entries
    ]


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

    Tries impacket first (cross-platform + custom creds); if that fails and
    pywin32 is available, falls back to it. Raises
    :class:`ShareEnumerationError` if every backend failed.
    """
    errors: List[str] = []

    try:
        shares = _enum_with_impacket(host, username=username, password=password, domain=domain)
    except ShareEnumerationError as exc:
        errors.append(f"impacket: {exc}")
        shares = []

    if not shares:
        try:
            shares = _enum_with_pywin32(host)
        except ShareEnumerationError as exc:
            errors.append(f"pywin32: {exc}")

    if not shares and errors:
        raise ShareEnumerationError("; ".join(errors))

    filtered = []
    for s in shares:
        if s.name in DEFAULT_SKIP and not include_ipc and s.name == "IPC$":
            continue
        if not include_hidden and s.is_hidden:
            continue
        if not s.is_disk:
            continue  # never try to walk a printer queue
        filtered.append(s)
    return filtered
