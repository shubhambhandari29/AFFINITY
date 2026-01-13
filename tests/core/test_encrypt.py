import pytest

from core import encrypt


def test_hash_and_verify_round_trip():
    hashed = encrypt.hash_password("super-secret")
    assert hashed.startswith("$2")
    assert encrypt.verify_password("super-secret", hashed) is True


def test_verify_password_rejects_non_bcrypt():
    with pytest.raises(ValueError):
        encrypt.verify_password("pw", "notahash")

    assert encrypt.verify_password(None, "anything") is False
