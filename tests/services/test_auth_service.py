from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException, Request, Response

from services import auth_service


def test_set_and_clear_session_cookie():
    response = Response()
    auth_service._set_session_cookie(response, "token")
    auth_service._set_refresh_cookie(response, "refresh-token")
    headers = "\n".join(
        value.decode() for key, value in response.raw_headers if key == b"set-cookie"
    )
    assert auth_service.SESSION_COOKIE_NAME in headers
    assert auth_service.REFRESH_COOKIE_NAME in headers

    auth_service._clear_auth_cookies(response)
    headers = "\n".join(
        value.decode() for key, value in response.raw_headers if key == b"set-cookie"
    )
    assert auth_service.SESSION_COOKIE_NAME in headers
    assert auth_service.REFRESH_COOKIE_NAME in headers


def test_login_user_missing_email():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.login_user({}, Response()))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Missing email"}


def test_login_user_not_authorized_when_no_sac_groups(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "_get_user_groups_from_graph",
        lambda email: [{"id": "x", "displayName": "SomeOtherGroup"}],
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            auth_service.login_user({"email": "a@example.com"}, Response())
        )

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "User is not authorized"}


def test_login_user_graph_error(monkeypatch):
    def fake_get_groups(email):
        raise HTTPException(status_code=404, detail={"error": "User not found in Entra ID"})

    monkeypatch.setattr(auth_service, "_get_user_groups_from_graph", fake_get_groups)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            auth_service.login_user({"email": "a@example.com"}, Response())
        )

    assert excinfo.value.status_code == 404


def test_login_user_role_priority_and_cookie_flow(monkeypatch):
    captured = {"session_cookie": None, "refresh_cookie": None}

    monkeypatch.setattr(
        auth_service,
        "_get_user_groups_from_graph",
        lambda email: [
            {"id": "1", "displayName": "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_DIRECTORS"},
            {"id": "2", "displayName": "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_ADMIN"},
            {"id": "3", "displayName": "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_UNDERWRITERS"},
        ],
    )
    monkeypatch.setattr(auth_service, "create_access_token", lambda user_id, role: "token")
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: "refresh-token",
    )
    monkeypatch.setattr(
        auth_service,
        "_set_session_cookie",
        lambda response, token: captured.__setitem__("session_cookie", token),
    )
    monkeypatch.setattr(
        auth_service,
        "_set_refresh_cookie",
        lambda response, token: captured.__setitem__("refresh_cookie", token),
    )

    response = Response()
    result = asyncio.run(auth_service.login_user({"email": "a@example.com"}, response))

    assert result["message"] == "Sign in successful"
    assert result["token"] == "token"
    assert result["user"]["email"] == "a@example.com"
    assert result["user"]["role"] == "Admin,Director,Underwriter"
    assert captured["session_cookie"] == "token"
    assert captured["refresh_cookie"] == "refresh-token"


def test_login_user_sets_director_branch_from_mapping(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "_get_user_groups_from_graph",
        lambda email: [
            {"id": "2", "displayName": "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_DIRECTORS"},
        ],
    )
    monkeypatch.setattr(auth_service, "create_access_token", lambda user_id, role: "token")
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: "refresh-token",
    )
    monkeypatch.setattr(
        auth_service,
        "get_branch_name_by_email",
        lambda email: "Northeast",
    )
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)
    monkeypatch.setattr(auth_service, "_set_refresh_cookie", lambda response, token: None)

    response = Response()
    result = asyncio.run(auth_service.login_user({"email": "mdeluca@hanover.com"}, response))

    assert result["user"]["role"] == "Director"
    assert result["user"]["branch"] == "Northeast"


def test_login_user_sets_all_branch_from_mapping(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "_get_user_groups_from_graph",
        lambda email: [
            {"id": "3", "displayName": "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_DIRECTORS"},
        ],
    )
    monkeypatch.setattr(auth_service, "create_access_token", lambda user_id, role: "token")
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: "refresh-token",
    )
    monkeypatch.setattr(
        auth_service,
        "get_branch_name_by_email",
        lambda email: "All",
    )
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)
    monkeypatch.setattr(auth_service, "_set_refresh_cookie", lambda response, token: None)

    response = Response()
    result = asyncio.run(auth_service.login_user({"email": "mbond@hanover.com"}, response))

    assert result["user"]["role"] == "Director"
    assert result["user"]["branch"] == "All"


def test_get_current_user_from_token_success_db_path(monkeypatch):
    monkeypatch.setattr(auth_service, "decode_access_token", lambda token: {"sub": "1"})
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
    assert result["user"]["role"] == "User"


def test_get_current_user_from_token_success_graph_path(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "decode_access_token",
        lambda token: {"sub": "a@example.com", "role": "Director"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for email-based id"),
    )

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    result = asyncio.run(auth_service.get_current_user_from_token(request))
    assert result["user"]["email"] == "a@example.com"
    assert result["user"]["role"] == "Director"
    assert result["user"]["branch"] is None


def test_get_current_user_from_token_success_graph_path_branch_mapping(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "decode_access_token",
        lambda token: {"sub": "jhoule@hanover.com", "role": "Director"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for email-based id"),
    )
    monkeypatch.setattr(
        auth_service,
        "get_branch_name_by_email",
        lambda email: "All",
    )

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    result = asyncio.run(auth_service.get_current_user_from_token(request))
    assert result["user"]["email"] == "jhoule@hanover.com"
    assert result["user"]["branch"] == "All"


def test_get_branch_name_by_email_returns_none_when_lookup_fails(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "run_raw_query",
        lambda query, params: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )

    assert auth_service.get_branch_name_by_email("mbond@hanover.com") is None


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
    assert excinfo.value.detail == {"error": "No refresh token found"}


def test_refresh_user_token_success_db_path(monkeypatch):
    monkeypatch.setattr(auth_service, "decode_refresh_token", lambda token: {"sub": "1"})
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: {"ID": 1, "Email": "a@example.com", "Role": "User"},
    )
    monkeypatch.setattr(auth_service, "create_access_token", lambda uid, role: "newtoken")
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda uid: pytest.fail("refresh token must not be rotated during refresh"),
    )
    monkeypatch.setattr(
        auth_service,
        "_set_refresh_cookie",
        lambda response, token: pytest.fail("refresh cookie must not be reset during refresh"),
    )

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.REFRESH_COOKIE_NAME: "refresh-token"}
    response = Response()

    result = asyncio.run(auth_service.refresh_user_token(request, response, token=None))
    assert result == {"message": "Token refreshed", "token": "newtoken"}


def test_refresh_user_token_success_graph_path(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "decode_refresh_token",
        lambda token: {"sub": "a@example.com", "role": "admin"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for email-based id"),
    )
    monkeypatch.setattr(auth_service, "create_access_token", lambda uid, role: "newtoken")
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.REFRESH_COOKIE_NAME: "refresh-token"}
    response = Response()

    result = asyncio.run(auth_service.refresh_user_token(request, response, token=None))
    assert result == {"message": "Token refreshed", "token": "newtoken"}


def test_refresh_user_token_invalid_clears_cookies(monkeypatch):
    captured = {"cleared": False}

    def fake_decode_refresh_token(token):
        raise HTTPException(status_code=401, detail="Refresh token expired")

    def fake_clear_auth_cookies(response):
        captured["cleared"] = True

    monkeypatch.setattr(auth_service, "decode_refresh_token", fake_decode_refresh_token)
    monkeypatch.setattr(auth_service, "_clear_auth_cookies", fake_clear_auth_cookies)

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.REFRESH_COOKIE_NAME: "refresh-token"}
    response = Response()

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.refresh_user_token(request, response, token=None))

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "Invalid refresh token"}
    assert captured["cleared"] is True
