"""Tests for CredentialCipher."""

from __future__ import annotations

import pytest

from duplicator_search_destroy.utils.crypto import CredentialCipher, DecryptionError


def test_round_trip_ascii():
    c = CredentialCipher()
    cipher = c.encrypt("hunter2!")
    assert cipher != b"hunter2!"
    assert c.decrypt(cipher) == "hunter2!"


def test_round_trip_unicode():
    c = CredentialCipher()
    cipher = c.encrypt("pässwörd‒🔑")
    assert c.decrypt(cipher) == "pässwörd‒🔑"


def test_round_trip_empty():
    c = CredentialCipher()
    cipher = c.encrypt("")
    assert c.decrypt(cipher) == ""


def test_decrypt_unknown_envelope():
    c = CredentialCipher()
    with pytest.raises(DecryptionError):
        c.decrypt(b"v9:garbage")


def test_decrypt_empty_raises():
    c = CredentialCipher()
    with pytest.raises(DecryptionError):
        c.decrypt(b"")
