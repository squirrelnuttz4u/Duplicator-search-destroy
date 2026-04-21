"""At-rest encryption for stored credentials.

Strategy:

* **Windows**: use DPAPI (``CryptProtectData``) via ``win32crypt`` — the
  secret is bound to the current Windows user account, which is the standard
  approach for Windows-native apps.
* **Non-Windows / no pywin32**: fall back to a Fernet key derived from a
  machine-unique identifier. This is weaker than DPAPI but good enough to
  keep passwords out of plain-text DB dumps.

The on-disk form is always ``b"vN:<payload>"`` where ``N`` selects the
algorithm, so we can migrate schemes later without breaking existing data.
"""

from __future__ import annotations

import base64
import hashlib
import os
import platform
import uuid
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

__all__ = ["CredentialCipher", "DecryptionError"]


class DecryptionError(Exception):
    """Raised when an encrypted blob cannot be decrypted."""


def _machine_key() -> bytes:
    """Return a 32-byte key derived from a stable per-machine identifier."""
    components = [platform.node(), str(uuid.getnode())]
    # /etc/machine-id on Linux, registry MachineGuid on Windows if we have it
    for p in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            with open(p, "r", encoding="utf-8") as fh:
                components.append(fh.read().strip())
                break
        except OSError:
            continue
    if os.name == "nt":
        components.append(os.environ.get("COMPUTERNAME", ""))
    raw = "|".join(components).encode("utf-8")
    return hashlib.sha256(raw).digest()


class CredentialCipher:
    """Opaque encrypt/decrypt facade used by the credentials DAO."""

    def __init__(self) -> None:
        self._dpapi = self._try_load_dpapi()
        key32 = _machine_key()
        self._fernet = Fernet(base64.urlsafe_b64encode(key32))

    @staticmethod
    def _try_load_dpapi():
        if os.name != "nt":
            return None
        try:
            import win32crypt  # type: ignore[import-not-found]
        except Exception:
            return None
        return win32crypt

    def encrypt(self, plaintext: str) -> bytes:
        if plaintext is None:
            raise ValueError("plaintext must not be None")
        data = plaintext.encode("utf-8")
        if self._dpapi is not None:
            blob = self._dpapi.CryptProtectData(data, "duplicator-cred", None, None, None, 0)
            return b"v1:" + blob
        token = self._fernet.encrypt(data)
        return b"v2:" + token

    def decrypt(self, payload: bytes) -> str:
        if not payload:
            raise DecryptionError("empty payload")
        if payload.startswith(b"v1:"):
            if self._dpapi is None:
                raise DecryptionError(
                    "Encrypted with DPAPI but pywin32 is unavailable on this host."
                )
            try:
                _desc, data = self._dpapi.CryptUnprotectData(payload[3:], None, None, None, 0)
            except Exception as exc:  # pragma: no cover - platform specific
                raise DecryptionError(str(exc)) from exc
            return data.decode("utf-8")
        if payload.startswith(b"v2:"):
            try:
                return self._fernet.decrypt(payload[3:]).decode("utf-8")
            except InvalidToken as exc:
                raise DecryptionError("Fernet token invalid (wrong machine key?)") from exc
        raise DecryptionError(f"Unknown credential envelope: {payload[:4]!r}")


_singleton: Optional[CredentialCipher] = None


def default_cipher() -> CredentialCipher:
    global _singleton
    if _singleton is None:
        _singleton = CredentialCipher()
    return _singleton
