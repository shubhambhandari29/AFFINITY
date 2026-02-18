from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException, Request, Response

from services import auth_service


def test_set_and_clear_session_cookie():
    response = Response()
    auth_service._set_session_cookie(response, "token")
    assert auth_service.SESSION_COOKIE_NAME in response.headers.get("set-cookie", "")

    auth_service._clear_session_cookie(response)
    assert auth_service.SESSION_COOKIE_NAME in response.headers.get("set-cookie", "")


def test_login_user_missing_data():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.login_user({"email": "a"}, Response()))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Missing email or password"}


def test_login_user_user_not_found(monkeypatch):
    monkeypatch.setattr(auth_service, "get_user_by_email", lambda email: None)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            auth_service.login_user({"email": "a@example.com", "password": "x"}, Response())
        )

    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == {"error": "User not found"}


def test_login_user_wrong_password(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "get_user_by_email",
        lambda email: {
            "ID": 1,
            "FirstName": "A",
            "LastName": "B",
            "Email": email,
            "Role": "User",
            "BranchName": "HQ",
            "Password": "hashed",
        },
    )
    monkeypatch.setattr(auth_service, "verify_password", lambda p, s: False)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            auth_service.login_user({"email": "a@example.com", "password": "x"}, Response())
        )

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "Wrong password"}


def test_login_user_legacy_password_rehash(monkeypatch):
    captured = {"rehash": None, "cookie": None}

    monkeypatch.setattr(
        auth_service,
        "get_user_by_email",
        lambda email: {
            "ID": 2,
            "FirstName": "A",
            "LastName": "B",
            "Email": email,
            "Role": "User",
            "BranchName": "HQ",
            "Password": "legacy",
        },
    )

    def fake_verify_password(password, stored):
        raise ValueError("not bcrypt")

    def fake_hash_password(password):
        return "newhash"

    def fake_persist(user_id, new_hash):
        captured["rehash"] = (user_id, new_hash)

    def fake_create_access_token(user_id, role):
        return "token"

    def fake_set_cookie(response, token):
        captured["cookie"] = token

    monkeypatch.setattr(auth_service, "verify_password", fake_verify_password)
    monkeypatch.setattr(auth_service, "hash_password", fake_hash_password)
    monkeypatch.setattr(auth_service, "_persist_hashed_password", fake_persist)
    monkeypatch.setattr(auth_service, "create_access_token", fake_create_access_token)
    monkeypatch.setattr(auth_service, "_set_session_cookie", fake_set_cookie)

    response = Response()
    result = asyncio.run(
        auth_service.login_user({"email": "a@example.com", "password": "legacy"}, response)
    )

    assert result["message"] == "Sign in successful"
    assert result["token"] == "token"
    assert captured["rehash"] == (2, "newhash")
    assert captured["cookie"] == "token"


def test_login_user_valid_password(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "get_user_by_email",
        lambda email: {
            "ID": 1,
            "FirstName": "A",
            "LastName": "B",
            "Email": email,
            "Role": "User",
            "BranchName": "HQ",
            "Password": "hashed",
        },
    )
    monkeypatch.setattr(auth_service, "verify_password", lambda p, s: True)
    monkeypatch.setattr(auth_service, "create_access_token", lambda uid, role: "token")
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)

    result = asyncio.run(
        auth_service.login_user({"email": "a@example.com", "password": "x"}, Response())
    )

    assert result["message"] == "Sign in successful"
    assert result["user"]["email"] == "a@example.com"
    assert result["token"] == "token"


def test_get_current_user_from_token_success(monkeypatch):
    monkeypatch.setattr(auth_service, "decode_access_token", lambda token: {"sub": 1})
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: {
            "ID": 1,
            "FirstName": "A",
            "LastName": "B",
            "Email": "a@example.com",
            "Role": "User",
            "BranchName": "HQ",
        },
    )

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    result = asyncio.run(auth_service.get_current_user_from_token(request))
    assert result["user"]["email"] == "a@example.com"


def test_get_current_user_from_token_invalid(monkeypatch):
    monkeypatch.setattr(auth_service, "decode_access_token", lambda token: {})

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.get_current_user_from_token(request))

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "Invalid token"}


def test_refresh_user_token_no_token():
    request = Request({"type": "http", "headers": [], "query_string": b""})
    response = Response()

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.refresh_user_token(request, response, token=None))

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "No token found"}


def test_refresh_user_token_success(monkeypatch):
    monkeypatch.setattr(auth_service, "decode_access_token", lambda token: {"sub": 1})
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: {"ID": 1, "Email": "a@example.com"},
    )
    monkeypatch.setattr(auth_service, "create_access_token", lambda uid, role: "newtoken")
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}
    response = Response()

    result = asyncio.run(auth_service.refresh_user_token(request, response, token=None))
    assert result == {"message": "Token refreshed", "token": "newtoken"}
