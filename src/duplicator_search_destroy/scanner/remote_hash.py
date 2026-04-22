"""Hash files remotely via PowerShell Remoting (WinRM).

Why this exists
---------------
The classic SMB-based hash pipeline (:mod:`scanner.hasher`) has to read every
candidate file's **bytes** over the network. For a 200-server dedup pass with
thousands of GB-sized duplicate candidates, that transfer dominates the run
time.

If we instead push a PowerShell snippet to each host via WinRM that computes
``Get-FileHash`` **locally** and streams back ``{path, hash}`` JSON, the
network only carries metadata — roughly 64 bytes per file. This is typically
a 10-50× speedup for the dedup phase on LAN deployments.

What we return
--------------
All hashes are returned as strings of the form ``"sha256:<hex>"``. The
prefix is important: the local BLAKE3 pipeline tags its hashes ``"blake3:"``,
and the dedup SQL groups by the full string, so a mix of local-BLAKE3 and
remote-SHA256 hashes won't produce false positives — files hashed with
different algorithms simply can't match each other.

Server-side requirements
------------------------
* WinRM enabled (``winrm quickconfig`` on the target)
* TCP 5985 (HTTP) or 5986 (HTTPS) reachable from the scanner
* Account with WinRM rights — domain admin / ``BUILTIN\\Remote Management Users``
* PowerShell 5.1+ (for ``ForEach-Object -Parallel`` we need 7+, but we fall
  back to a serial pipeline if that cmdlet isn't available)
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from duplicator_search_destroy.scanner.winrm_client import (
    WinRmClient,
    WinRmError,
    WinRmRemoteError,
    WinRmResult,
)

log = logging.getLogger(__name__)

__all__ = [
    "RemoteHashResult",
    "remote_hash_files",
    "build_remote_hash_script",
    "SUPPORTED_ALGORITHMS",
]

# Get-FileHash natively supports these — no module deployment required.
SUPPORTED_ALGORITHMS = ("sha256", "sha1", "sha512", "md5", "sha384")

# Max paths per WinRM call. Keeps each request well under the default 500 KB
# MaxEnvelopeSizeKb and caps PowerShell memory on the remote side.
DEFAULT_BATCH_SIZE = 500


@dataclass(slots=True)
class RemoteHashResult:
    """One entry per input path.

    * ``hash`` is ``None`` when the remote side couldn't read / hash the file
    * ``error`` contains the PowerShell error message in that case
    """

    relative_path: str
    hash: Optional[str]
    error: Optional[str]


def build_remote_hash_script(
    paths: List[str],
    share_name: str,
    *,
    algorithm: str = "sha256",
    throttle: int = 8,
) -> str:
    """Construct the PowerShell script body for one WinRM call.

    We base64-encode the JSON payload so arbitrary filenames (including
    quotes and backticks) can't escape the script. The server:

    1. Decodes the JSON blob of share-relative paths.
    2. Resolves the share's local filesystem root via ``Get-SmbShare``
       (falls back to a loopback UNC if the cmdlet isn't available or the
       share is special).
    3. Joins each relative path to the local root.
    4. Hashes each file in parallel with ``Get-FileHash``. On PS 7+ this
       uses ``ForEach-Object -Parallel``; on PS 5.1 it falls back to a
       normal pipeline.
    5. Emits one JSON blob to stdout: an array of
       ``{path, hash, error}`` records.
    """
    if algorithm.lower() not in SUPPORTED_ALGORITHMS:
        raise ValueError(
            f"Unsupported hash algorithm {algorithm!r}; expected one of "
            f"{SUPPORTED_ALGORITHMS}"
        )

    payload = {
        "paths": paths,
        "share": share_name,
        "algo": algorithm.upper(),
        "throttle": max(1, int(throttle)),
    }
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")

    script = f"""
$ErrorActionPreference = 'Stop'
$encoded = '{encoded}'
$cfg = [System.Text.Encoding]::UTF8.GetString(
    [System.Convert]::FromBase64String($encoded)
) | ConvertFrom-Json

$shareName = $cfg.share
$algo      = $cfg.algo
$throttle  = [int]$cfg.throttle

# Resolve the share's local path. Get-SmbShare is on every supported
# Windows Server; fall back to SMB loopback if it's not available.
$localRoot = $null
try {{
    $sh = Get-SmbShare -Name $shareName -ErrorAction Stop
    $localRoot = $sh.Path
}} catch {{
    $localRoot = "\\\\$($env:COMPUTERNAME)\\$shareName"
}}

function Hash-One($rel, $algo, $root) {{
    $full = Join-Path $root $rel
    try {{
        $h = (Get-FileHash -LiteralPath $full -Algorithm $algo -ErrorAction Stop).Hash
        [PSCustomObject]@{{ path = $rel; hash = $h.ToLower(); error = $null }}
    }} catch {{
        [PSCustomObject]@{{ path = $rel; hash = $null; error = $_.Exception.Message }}
    }}
}}

$useParallel = $PSVersionTable.PSVersion.Major -ge 7
if ($useParallel) {{
    $results = $cfg.paths | ForEach-Object -ThrottleLimit $throttle -Parallel {{
        $full = Join-Path $using:localRoot $_
        try {{
            $h = (Get-FileHash -LiteralPath $full -Algorithm $using:algo -ErrorAction Stop).Hash
            [PSCustomObject]@{{ path = $_; hash = $h.ToLower(); error = $null }}
        }} catch {{
            [PSCustomObject]@{{ path = $_; hash = $null; error = $_.Exception.Message }}
        }}
    }}
}} else {{
    $results = foreach ($p in $cfg.paths) {{ Hash-One $p $algo $localRoot }}
}}

$results | ConvertTo-Json -Depth 3 -Compress
""".strip()
    return script


def _parse_results(result: WinRmResult) -> List[RemoteHashResult]:
    """Parse the JSON blob emitted by the remote script."""
    text = (result.stdout or "").strip()
    if not text:
        if result.had_errors:
            raise WinRmRemoteError(
                "remote hash script produced no output",
                stderr=result.stderr,
            )
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise WinRmRemoteError(
            f"remote hash script produced invalid JSON: {exc}. "
            f"First 200 chars: {text[:200]!r}",
            stderr=result.stderr,
        ) from exc

    # ConvertTo-Json emits a single object rather than an array when the
    # input has length 1. Normalise to a list.
    if isinstance(parsed, dict):
        parsed = [parsed]

    out: List[RemoteHashResult] = []
    for rec in parsed:
        rel = str(rec.get("path", ""))
        h = rec.get("hash")
        err = rec.get("error")
        out.append(
            RemoteHashResult(
                relative_path=rel,
                hash=str(h).lower() if h else None,
                error=str(err) if err else None,
            )
        )
    return out


def remote_hash_files(
    host: str,
    share_name: str,
    relative_paths: List[str],
    *,
    username: str = "",
    password: str = "",
    domain: str = "",
    algorithm: str = "sha256",
    throttle: int = 8,
    ssl: bool = False,
    port: Optional[int] = None,
    auth: str = "negotiate",
    cert_validation: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    operation_timeout: int = 300,
) -> Dict[str, RemoteHashResult]:
    """Hash *relative_paths* (relative to ``\\\\host\\share_name``) on the remote host.

    Returns a ``{relative_path: RemoteHashResult}`` dict. Missing entries mean
    the remote side didn't return a record for that path — treat as
    ``hash=None, error="no response"`` at the caller.

    Raises :class:`WinRmError` subclasses on transport / auth / parse
    failures. The caller is expected to fall back to SMB-based hashing in
    that case.
    """
    if not relative_paths:
        return {}

    client = WinRmClient(
        host,
        username=username,
        password=password,
        domain=domain,
        ssl=ssl,
        port=port,
        auth=auth,
        cert_validation=cert_validation,
        operation_timeout=operation_timeout,
    )

    results: Dict[str, RemoteHashResult] = {}
    for start in range(0, len(relative_paths), batch_size):
        batch = relative_paths[start:start + batch_size]
        script = build_remote_hash_script(
            batch, share_name, algorithm=algorithm, throttle=throttle
        )
        log.debug(
            "WinRM hash batch: host=%s share=%s n=%d (algo=%s throttle=%d)",
            host, share_name, len(batch), algorithm, throttle,
        )
        wsman_result = client.run_powershell(script)
        parsed = _parse_results(wsman_result)
        for entry in parsed:
            results[entry.relative_path] = entry
    return results


def prefix_hash(algorithm: str, hex_hash: str) -> str:
    """Return the algorithm-prefixed form we store in ``files.full_hash``."""
    return f"{algorithm.lower()}:{hex_hash.lower()}"
