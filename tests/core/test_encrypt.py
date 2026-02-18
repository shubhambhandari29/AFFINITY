from __future__ import annotations

import pytest

from core import encrypt


def test_hash_password_requires_value():
    with pytest.raises(ValueError):
        encrypt.hash_password(None)


def test_hash_and_verify_password_round_trip():
    hashed = encrypt.hash_password("secret")
    assert hashed.startswith("$2")
    assert encrypt.verify_password("secret", hashed) is True
    assert encrypt.verify_password("wrong", hashed) is False


def test_verify_password_rejects_non_bcrypt_hash():
    with pytest.raises(ValueError):
        encrypt.verify_password("secret", "plain")

    with pytest.raises(ValueError):
        encrypt.verify_password("secret", "$2")


def test_verify_password_handles_none_inputs():
    assert encrypt.verify_password(None, "hash") is False
    assert encrypt.verify_password("secret", None) is False
