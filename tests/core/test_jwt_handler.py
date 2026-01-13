import pytest
from fastapi import HTTPException

from core import jwt_handler


def test_create_and_decode_token():
    token = jwt_handler.create_access_token({"id": 123})
    payload = jwt_handler.decode_access_token(token)

    assert payload["user"]["id"] == 123
    assert "exp" in payload


def test_decode_invalid_token_raises():
    with pytest.raises(HTTPException) as exc:
        jwt_handler.decode_access_token("invalid-token")

    assert exc.value.status_code == 403


def test_verify_token_invalid():
    with pytest.raises(HTTPException):
        jwt_handler.verify_token("invalid-token")
