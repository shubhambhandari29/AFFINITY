from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException, Request, Response

from services import auth_service


def _request_with_headers(headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    return Request({"type": "http", "headers": headers or [], "query_string": b""})


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
            "Password": "expected-password",
        },
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            auth_service.login_user({"email": "a@example.com", "password": "x"}, Response())
        )

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "Wrong password"}


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
            "Password": "x",
        },
    )
    monkeypatch.setattr(auth_service, "create_access_token", lambda uid, role: "token")
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)

    result = asyncio.run(
        auth_service.login_user({"email": "a@example.com", "password": "x"}, Response())
    )

    assert result["message"] == "Sign in successful"
    assert result["user"]["email"] == "a@example.com"
    assert result["token"] == "token"


def test_login_user_non_local_uses_f5_headers(monkeypatch):
    captured = {"cookie": None, "token_args": None}

    monkeypatch.setattr(auth_service.settings, "ENVIRONMENT", "prod")
    monkeypatch.setattr(auth_service.settings, "F5_USER_HEADER", "X-Auth-User")
    monkeypatch.setattr(auth_service.settings, "F5_GROUPS_HEADER", "X-Auth-Groups")
    monkeypatch.setattr(auth_service.settings, "F5_UNDERWRITER_GROUP", "aad-underwriter")
    monkeypatch.setattr(auth_service.settings, "F5_DIRECTOR_GROUP", "aad-director")
    monkeypatch.setattr(auth_service.settings, "F5_ADMIN_GROUP", "aad-admin")

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
        },
    )

    def fake_create_access_token(user_id, role):
        captured["token_args"] = (user_id, role)
        return "token"

    def fake_set_cookie(response, token):
        captured["cookie"] = token

    monkeypatch.setattr(auth_service, "create_access_token", fake_create_access_token)
    monkeypatch.setattr(auth_service, "_set_session_cookie", fake_set_cookie)

    request = _request_with_headers(
        [
            (b"x-auth-user", b"a@example.com"),
            (b"x-auth-groups", b"aad-underwriter,aad-director"),
        ]
    )
    result = asyncio.run(auth_service.login_user(None, Response(), request))

    assert result["message"] == "Sign in successful"
    assert result["user"]["email"] == "a@example.com"
    assert result["user"]["role"] == "director"
    assert result["token"] == "token"
    assert captured["token_args"] == (1, "director")
    assert captured["cookie"] == "token"


def test_login_user_non_local_missing_f5_user_header(monkeypatch):
    monkeypatch.setattr(auth_service.settings, "ENVIRONMENT", "prod")
    monkeypatch.setattr(auth_service.settings, "F5_USER_HEADER", "X-Auth-User")

    request = _request_with_headers()
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.login_user(None, Response(), request))

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "Not authenticated"}


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


def test_get_current_user_from_token_prefers_token_role(monkeypatch):
    monkeypatch.setattr(auth_service, "decode_access_token", lambda token: {"sub": 1, "role": "admin"})
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
    assert result["user"]["role"] == "admin"


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
