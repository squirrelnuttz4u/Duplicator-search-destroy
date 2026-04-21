"""Network discovery: probe IPs for SMB (port 445), resolve hostnames.

The probe is a plain TCP connect to port 445 — fast, reliable, doesn't
require privileges. Hostname resolution uses reverse DNS first, falling
back to NetBIOS name-service (``UDP/137``) so we still get a name on
segregated networks.
"""

from __future__ import annotations

import concurrent.futures as futures
import logging
import socket
import struct
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

__all__ = [
    "DiscoveredHost",
    "probe_host",
    "discover_hosts",
    "netbios_name_query",
]

log = logging.getLogger(__name__)

SMB_PORT = 445
NBT_PORT = 137


@dataclass(slots=True)
class DiscoveredHost:
    ip: str
    hostname: Optional[str]
    port_open: bool
    error: Optional[str] = None


def _tcp_port_open(ip: str, port: int, timeout: float) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def netbios_name_query(ip: str, timeout: float = 1.0) -> Optional[str]:
    """Issue an NBT Node Status Request and return the first unique name.

    Returns ``None`` on timeout or malformed reply. Errors are swallowed —
    discovery should never fail outright because one host is misbehaving.
    """
    # NetBIOS Node Status Request — see RFC 1002, section 4.2.18.
    trn_id = 0x4E53  # 'NS'
    flags = 0x0000
    qdcount = 1
    header = struct.pack(">HHHHHH", trn_id, flags, qdcount, 0, 0, 0)
    name = b"CKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\x00"  # encoded '*' wildcard
    question = name + struct.pack(">HH", 0x0021, 0x0001)  # NBSTAT + IN

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(header + question, (ip, NBT_PORT))
        data, _ = sock.recvfrom(4096)
    except (OSError, socket.timeout):
        return None
    finally:
        sock.close()

    try:
        # Skip header (12 bytes) + question (len(name) + 4).
        offset = 12 + len(name) + 4
        # Answer: Name(same) + Type(2) + Class(2) + TTL(4) + RDLEN(2) + RDATA
        offset += len(name) + 2 + 2 + 4 + 2
        if offset >= len(data):
            return None
        num_names = data[offset]
        offset += 1
        for _ in range(num_names):
            if offset + 18 > len(data):
                break
            raw_name = data[offset : offset + 15].decode("ascii", errors="ignore").rstrip()
            suffix = data[offset + 15]
            flags_b = struct.unpack(">H", data[offset + 16 : offset + 18])[0]
            offset += 18
            # Suffix 0x20 = file-server service; 0x00 = workstation.
            # Group bit (0x8000) = group name — skip those.
            if flags_b & 0x8000:
                continue
            if suffix in (0x20, 0x00) and raw_name:
                return raw_name
    except Exception:  # pragma: no cover - defensive
        return None
    return None


def _resolve_hostname(ip: str, timeout: float) -> Optional[str]:
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror, OSError):
        pass
    return netbios_name_query(ip, timeout=min(timeout, 1.0))


def probe_host(ip: str, *, timeout: float = 2.0, resolve: bool = True) -> DiscoveredHost:
    open_ = _tcp_port_open(ip, SMB_PORT, timeout)
    if not open_:
        return DiscoveredHost(ip=ip, hostname=None, port_open=False)
    hostname = _resolve_hostname(ip, timeout) if resolve else None
    return DiscoveredHost(ip=ip, hostname=hostname, port_open=True)


def discover_hosts(
    ips: Iterable[str],
    *,
    timeout: float = 2.0,
    max_workers: int = 64,
    on_result: Optional[Callable[[DiscoveredHost], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> List[DiscoveredHost]:
    """Parallel SMB sweep across an iterable of IPs.

    Respects a ``cancel`` callable — if it returns True, outstanding probes
    are abandoned and the partial list is returned. This lets the GUI stop
    a sweep without leaking threads.
    """
    ips_list = list(ips)
    results: List[DiscoveredHost] = []
    if not ips_list:
        return results

    with futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="probe") as pool:
        future_map = {pool.submit(probe_host, ip, timeout=timeout): ip for ip in ips_list}
        for fut in futures.as_completed(future_map):
            if cancel and cancel():
                for pending in future_map:
                    pending.cancel()
                break
            try:
                res = fut.result()
            except Exception as exc:  # pragma: no cover - defensive
                res = DiscoveredHost(ip=future_map[fut], hostname=None, port_open=False, error=str(exc))
            results.append(res)
            if on_result:
                try:
                    on_result(res)
                except Exception:
                    log.exception("on_result callback raised")
    return results
