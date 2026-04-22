"""Thin wrapper around pypsrp so the rest of the app doesn't care which
Python WinRM library we chose.

Usage::

    client = WinRmClient("srv1", username="CORP\\admin", password="…")
    stdout = client.run_powershell(script_body)

Exceptions:
    * :class:`WinRmUnavailableError`  — the library isn't installed / can't be imported
    * :class:`WinRmAuthError`         — authentication failed
    * :class:`WinRmConnectError`      — transport / TLS / firewall problem
    * :class:`WinRmRemoteError`       — the remote script ran but reported errors
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, List, Optional

log = logging.getLogger(__name__)

__all__ = [
    "WinRmClient",
    "WinRmError",
    "WinRmUnavailableError",
    "WinRmAuthError",
    "WinRmConnectError",
    "WinRmRemoteError",
]


class WinRmError(Exception):
    """Base class for WinRM client errors."""


class WinRmUnavailableError(WinRmError):
    """pypsrp isn't installed or can't be imported."""


class WinRmAuthError(WinRmError):
    """Auth to the remote host failed."""


class WinRmConnectError(WinRmError):
    """TCP / TLS / firewall / service-not-running level failure."""


class WinRmRemoteError(WinRmError):
    """The PowerShell ran but produced error records.

    ``stderr`` contains the PowerShell error stream as a list of strings.
    """

    def __init__(self, message: str, *, stderr: List[str] | None = None) -> None:
        super().__init__(message)
        self.stderr = stderr or []


@dataclass(slots=True)
class WinRmResult:
    stdout: str
    stderr: List[str]
    had_errors: bool


class WinRmClient:
    """Synchronous WinRM / PowerShell-remoting client.

    Lazy-imports pypsrp so this module parses on machines where it isn't
    installed (e.g. Linux dev boxes running unit tests).
    """

    def __init__(
        self,
        host: str,
        *,
        username: str = "",
        password: str = "",
        domain: str = "",
        ssl: bool = False,
        port: Optional[int] = None,
        auth: str = "negotiate",
        cert_validation: bool = True,
        operation_timeout: int = 60,
        connect_timeout: int = 10,
    ) -> None:
        self.host = host
        self.ssl = ssl
        self.port = port or (5986 if ssl else 5985)
        self.auth = auth
        self.cert_validation = cert_validation
        self.operation_timeout = operation_timeout
        self.connect_timeout = connect_timeout

        if username and domain and "\\" not in username and "@" not in username:
            self.username = f"{domain}\\{username}"
        else:
            self.username = username
        self.password = password

    # -- transport --------------------------------------------------------

    def _make_client(self) -> Any:
        try:
            from pypsrp.client import Client  # type: ignore[import-not-found]
        except ImportError as exc:
            raise WinRmUnavailableError(
                "pypsrp is not installed — install it with `pip install pypsrp`."
            ) from exc
        try:
            return Client(
                self.host,
                username=self.username or None,
                password=self.password or None,
                ssl=self.ssl,
                port=self.port,
                auth=self.auth,
                cert_validation=self.cert_validation,
                operation_timeout=self.operation_timeout,
                connection_timeout=self.connect_timeout,
            )
        except Exception as exc:
            raise WinRmConnectError(
                f"Could not construct WinRM client for {self.host}:{self.port}: {exc}"
            ) from exc

    # -- execution --------------------------------------------------------

    def run_powershell(self, script: str) -> WinRmResult:
        """Execute *script* remotely, return the result.

        Any stream output (informational / warnings) is dropped — only stdout
        and the error stream are returned. Wrap your script in
        ``ConvertTo-Json`` if you want structured output.
        """
        client = self._make_client()
        try:
            try:
                output, streams, had_errors = client.execute_ps(script)
            except Exception as exc:
                msg = str(exc).lower()
                if "access is denied" in msg or "unauthorized" in msg or "credential" in msg:
                    raise WinRmAuthError(
                        f"Auth failed against {self.host}: {exc}"
                    ) from exc
                if "connection" in msg or "timeout" in msg or "refused" in msg:
                    raise WinRmConnectError(
                        f"Could not reach {self.host}:{self.port}: {exc}"
                    ) from exc
                raise WinRmRemoteError(f"WinRM call failed: {exc}") from exc

            stdout_text = "" if output is None else str(output)
            err_list: List[str] = []
            try:
                for rec in streams.error or []:
                    err_list.append(str(rec))
            except AttributeError:
                # Older/newer pypsrp versions expose streams differently.
                pass
            if had_errors and not err_list:
                err_list.append("remote reported errors but no error records captured")
            return WinRmResult(stdout=stdout_text, stderr=err_list, had_errors=bool(had_errors))
        finally:
            try:
                client.close()
            except Exception:  # pragma: no cover
                pass
