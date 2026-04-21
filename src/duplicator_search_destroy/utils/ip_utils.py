"""IP-range parsing and enumeration.

Accepted input formats (one per line or comma-separated):

* Single IP            ``10.0.0.5``
* CIDR block           ``10.0.0.0/24``
* Dashed range         ``10.0.0.1-10.0.0.50``  or  ``10.0.0.1-50``
* Wildcard             ``10.0.0.*``           (expands to /24)
"""

from __future__ import annotations

import ipaddress
import re
from typing import Iterable, Iterator, List, Tuple

__all__ = [
    "parse_targets",
    "expand_targets",
    "InvalidTargetError",
]


class InvalidTargetError(ValueError):
    """Raised when a target expression cannot be parsed."""


_OCTET_RE = re.compile(r"^\d{1,3}$")
_DASH_SHORT_RE = re.compile(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})-(\d{1,3})$")
_WILDCARD_RE = re.compile(r"^(\d{1,3}\.\d{1,3}\.\d{1,3})\.\*$")


def _split_tokens(raw: str) -> List[str]:
    parts: List[str] = []
    for line in raw.splitlines():
        for chunk in line.split(","):
            token = chunk.strip()
            if token and not token.startswith("#"):
                parts.append(token)
    return parts


def _parse_token(token: str) -> Tuple[ipaddress.IPv4Address, ipaddress.IPv4Address]:
    """Resolve one token to an inclusive (start, end) tuple."""
    # CIDR
    if "/" in token:
        try:
            net = ipaddress.ip_network(token, strict=False)
        except ValueError as exc:
            raise InvalidTargetError(f"Invalid CIDR: {token}") from exc
        if not isinstance(net, ipaddress.IPv4Network):
            raise InvalidTargetError(f"Only IPv4 supported: {token}")
        return net.network_address, net.broadcast_address

    # 10.0.0.1-10.0.0.50
    if "-" in token:
        left, right = (s.strip() for s in token.split("-", 1))
        try:
            start = ipaddress.IPv4Address(left)
        except ValueError as exc:
            raise InvalidTargetError(f"Invalid start IP: {left}") from exc

        # Full end IP
        if "." in right:
            try:
                end = ipaddress.IPv4Address(right)
            except ValueError as exc:
                raise InvalidTargetError(f"Invalid end IP: {right}") from exc
        # Short form: 10.0.0.1-50 -> same /24, end octet only
        elif _OCTET_RE.match(right):
            prefix = ".".join(left.split(".")[:3])
            end_octet = int(right)
            if end_octet > 255:
                raise InvalidTargetError(f"Octet out of range: {right}")
            try:
                end = ipaddress.IPv4Address(f"{prefix}.{end_octet}")
            except ValueError as exc:
                raise InvalidTargetError(f"Invalid range end: {token}") from exc
        else:
            raise InvalidTargetError(f"Unparseable range: {token}")

        if int(end) < int(start):
            raise InvalidTargetError(f"End before start: {token}")
        return start, end

    # Wildcard
    if "*" in token:
        m = _WILDCARD_RE.match(token)
        if not m:
            raise InvalidTargetError(f"Only trailing-octet wildcards supported: {token}")
        prefix = m.group(1)
        return (
            ipaddress.IPv4Address(f"{prefix}.0"),
            ipaddress.IPv4Address(f"{prefix}.255"),
        )

    # Single address
    try:
        addr = ipaddress.IPv4Address(token)
    except ValueError as exc:
        raise InvalidTargetError(f"Invalid IP: {token}") from exc
    return addr, addr


def parse_targets(raw: str) -> List[Tuple[str, str]]:
    """Return a list of ``(start, end)`` IPv4 string tuples (inclusive).

    Raises :class:`InvalidTargetError` on any parse failure — callers should
    surface the message to the user rather than silently dropping input.
    """
    tokens = _split_tokens(raw)
    out: List[Tuple[str, str]] = []
    for token in tokens:
        start, end = _parse_token(token)
        out.append((str(start), str(end)))
    return out


def expand_targets(raw: str, *, limit: int = 65_536) -> Iterator[str]:
    """Yield every IPv4 address contained in *raw*, de-duplicated.

    ``limit`` is a safety net so a typo like ``10.0.0.0/8`` doesn't generate
    16 million addresses before the user realises.
    """
    seen: set[int] = set()
    count = 0
    for start_s, end_s in parse_targets(raw):
        start = int(ipaddress.IPv4Address(start_s))
        end = int(ipaddress.IPv4Address(end_s))
        for n in range(start, end + 1):
            if n in seen:
                continue
            seen.add(n)
            count += 1
            if count > limit:
                raise InvalidTargetError(
                    f"Target list exceeds safety limit of {limit} addresses. "
                    f"Narrow the ranges or raise the limit explicitly."
                )
            yield str(ipaddress.IPv4Address(n))


def count_targets(raw: str) -> int:
    """Count how many unique addresses *raw* expands to."""
    total = 0
    seen: set[int] = set()
    for start_s, end_s in parse_targets(raw):
        s = int(ipaddress.IPv4Address(start_s))
        e = int(ipaddress.IPv4Address(end_s))
        for n in range(s, e + 1):
            if n not in seen:
                seen.add(n)
                total += 1
    return total


def format_targets(targets: Iterable[Tuple[str, str]]) -> str:
    """Round-trip a parsed list back to a user-friendly string."""
    lines: List[str] = []
    for start, end in targets:
        if start == end:
            lines.append(start)
        else:
            lines.append(f"{start}-{end}")
    return "\n".join(lines)
