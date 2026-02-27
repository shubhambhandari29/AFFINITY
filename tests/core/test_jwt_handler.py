from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi import HTTPException

from core import jwt_handler


def test_create_and_decode_access_token(monkeypatch):
    monkeypatch.setattr(jwt_handler, "SECRET_KEY", "secret")
    monkeypatch.setattr(jwt_handler, "ACCESS_TOKEN_VALIDITY", 5)

    token = jwt_handler.create_access_token("123", role="admin")
    payload = jwt_handler.decode_access_token(token)

    assert payload["sub"] == "123"
    assert payload["role"] == "admin"
    assert payload["type"] == "access"


def test_decode_access_token_expired(monkeypatch):
    monkeypatch.setattr(jwt_handler, "SECRET_KEY", "secret")
    expired = datetime.now(UTC) - timedelta(minutes=1)
    token = jwt.encode({"sub": "1", "exp": expired}, "secret", algorithm="HS256")

    with pytest.raises(HTTPException) as excinfo:
        jwt_handler.decode_access_token(token)

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == "Token expired"


def test_decode_access_token_invalid(monkeypatch):
    monkeypatch.setattr(jwt_handler, "SECRET_KEY", "secret")

    with pytest.raises(HTTPException) as excinfo:
        jwt_handler.decode_access_token("bad-token")

    assert excinfo.value.status_code == 403
    assert excinfo.value.detail == "Invalid token"


def test_create_and_decode_refresh_token(monkeypatch):
    monkeypatch.setattr(jwt_handler, "SECRET_KEY", "secret")
    monkeypatch.setattr(jwt_handler, "REFRESH_TOKEN_VALIDITY", 60)

    token = jwt_handler.create_refresh_token("123")
    payload = jwt_handler.decode_refresh_token(token)

    assert payload["sub"] == "123"
    assert payload["type"] == "refresh"


def test_decode_refresh_token_expired(monkeypatch):
    monkeypatch.setattr(jwt_handler, "SECRET_KEY", "secret")
    expired = datetime.now(UTC) - timedelta(minutes=1)
    token = jwt.encode({"sub": "1", "exp": expired, "type": "refresh"}, "secret", algorithm="HS256")

    with pytest.raises(HTTPException) as excinfo:
        jwt_handler.decode_refresh_token(token)

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == "Refresh token expired"


def test_decode_refresh_token_invalid_type(monkeypatch):
    monkeypatch.setattr(jwt_handler, "SECRET_KEY", "secret")
    token = jwt.encode(
        {"sub": "1", "exp": datetime.now(UTC) + timedelta(minutes=5), "type": "access"},
        "secret",
        algorithm="HS256",
    )

    with pytest.raises(HTTPException) as excinfo:
        jwt_handler.decode_refresh_token(token)

    assert excinfo.value.status_code == 403
    assert excinfo.value.detail == "Invalid refresh token"
