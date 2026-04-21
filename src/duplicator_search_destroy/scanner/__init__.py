"""Network, share, file and hash scanners."""

from duplicator_search_destroy.scanner.network import (
    DiscoveredHost,
    discover_hosts,
    probe_host,
)
from duplicator_search_destroy.scanner.shares import (
    DiscoveredShare,
    enumerate_shares,
)
from duplicator_search_destroy.scanner.hasher import (
    HashResult,
    hash_prefix,
    hash_full,
)
from duplicator_search_destroy.scanner.duplicates import find_duplicates
from duplicator_search_destroy.scanner.files import walk_share, WalkedFile, WalkedFolder

__all__ = [
    "DiscoveredHost",
    "DiscoveredShare",
    "HashResult",
    "WalkedFile",
    "WalkedFolder",
    "discover_hosts",
    "probe_host",
    "enumerate_shares",
    "hash_prefix",
    "hash_full",
    "find_duplicates",
    "walk_share",
]
