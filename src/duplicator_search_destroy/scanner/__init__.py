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
from duplicator_search_destroy.scanner.duplicates import find_duplicates, hash_candidates_via_winrm
from duplicator_search_destroy.scanner.files import walk_share, WalkedFile, WalkedFolder
from duplicator_search_destroy.scanner.progress import ScanStats, StatsSnapshot, ThrottledEmitter

__all__ = [
    "DiscoveredHost",
    "DiscoveredShare",
    "HashResult",
    "WalkedFile",
    "WalkedFolder",
    "ScanStats",
    "StatsSnapshot",
    "ThrottledEmitter",
    "discover_hosts",
    "probe_host",
    "enumerate_shares",
    "hash_prefix",
    "hash_full",
    "find_duplicates",
    "hash_candidates_via_winrm",
    "walk_share",
]
