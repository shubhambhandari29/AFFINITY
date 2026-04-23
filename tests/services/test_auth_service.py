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


def test_clear_auth_cookies_clears_refresh_cookie_on_refresh_path():
    response = Response()

    auth_service._clear_auth_cookies(response)

    refresh_headers = [
        value.decode()
        for key, value in response.raw_headers
        if key == b"set-cookie"
        and value.decode().startswith(f"{auth_service.REFRESH_COOKIE_NAME}=")
    ]
    refresh_paths = {
        segment.split("=", 1)[1]
        for header in refresh_headers
        for segment in header.split("; ")
        if segment.startswith("Path=")
    }

    assert refresh_paths == {auth_service.REFRESH_COOKIE_OPTIONS["path"]}


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
            "Password": "stored-password",
        },
    )

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            auth_service.login_user({"email": "a@example.com", "password": "x"}, Response())
        )

    assert excinfo.value.status_code == 401
    assert excinfo.value.detail == {"error": "Wrong password"}


def test_login_user_valid_password(monkeypatch):
    captured = {"session_cookie": None, "refresh_cookie": None}

    monkeypatch.setattr(
        auth_service,
        "get_user_by_email",
        lambda email: {
            "ID": 2,
            "FirstName": "A",
            "LastName": "B",
            "Email": email,
            "Role": "User",
            "Password": "legacy",
        },
    )
    monkeypatch.setattr(auth_service, "get_branch_name_by_email", lambda email: "HQ")
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
    result = asyncio.run(
        auth_service.login_user({"email": "a@example.com", "password": "legacy"}, response)
    )

    assert result["message"] == "Sign in successful"
    assert result["token"] == "token"
    assert result["user"]["email"] == "a@example.com"
    assert result["user"]["role"] == "User"
    assert result["user"]["branch"] == "HQ"
    assert captured["session_cookie"] == "token"
    assert captured["refresh_cookie"] == "refresh-token"


def test_f5_login_user_missing_user():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.f5_login_user({}, Response()))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Missing user"}


def test_f5_login_user_invalid_groups_payload():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(auth_service.f5_login_user({"user": "MRM468", "groups": "bad"}, Response()))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Invalid groups"}


def test_f5_login_user_not_authorized_when_no_sac_groups():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            auth_service.f5_login_user(
                {"user": "MRM468", "groups": ["SomeOtherGroup"]},
                Response(),
            )
        )

    assert excinfo.value.status_code == 403
    assert excinfo.value.detail == {
        "error": (
            "You are not an authorized user. For getting access, "
            "share an email to mbond@hanover.com for next steps."
        )
    }


def test_f5_login_user_role_priority_and_cookie_flow(monkeypatch):
    captured = {"access": None, "refresh": None}

    monkeypatch.setattr(
        auth_service,
        "create_access_token",
        lambda user_id, role: captured.__setitem__("access", (user_id, role)) or "token",
    )
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: captured.__setitem__("refresh", (user_id, role))
        or "refresh-token",
    )
    monkeypatch.setattr(
        auth_service,
        "_set_session_cookie",
        lambda response, token: None,
    )
    monkeypatch.setattr(
        auth_service,
        "_set_refresh_cookie",
        lambda response, token: None,
    )

    response = Response()
    result = asyncio.run(
        auth_service.f5_login_user(
            {
                "user": "MRM468",
                "groups": [
                    "DIRECTORS",
                    "ADMIN",
                    "UNDERWRITERS",
                ],
            },
            response,
        )
    )

    assert result["message"] == "Sign in successful"
    assert result["token"] == "token"
    assert result["user"]["id"] == "MRM468"
    assert result["user"]["email"] == ""
    assert result["user"]["role"] == "Admin,Director,Underwriter"
    assert captured["access"] == ("MRM468", "Admin,Director,Underwriter")
    assert captured["refresh"] == ("MRM468", "Admin,Director,Underwriter")


def test_f5_login_user_keeps_underwriter_for_mbond_with_all_three_groups(monkeypatch):
    monkeypatch.setattr(auth_service, "create_access_token", lambda user_id, role: "token")
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: "refresh-token",
    )
    monkeypatch.setattr(
        auth_service,
        "_set_session_cookie",
        lambda response, token: None,
    )
    monkeypatch.setattr(
        auth_service,
        "_set_refresh_cookie",
        lambda response, token: None,
    )

    response = Response()
    result = asyncio.run(
        auth_service.f5_login_user(
            {
                "user": "MBOND",
                "groups": [
                    "DIRECTORS",
                    "ADMIN",
                    "UNDERWRITERS",
                ],
            },
            response,
        )
    )

    assert result["user"]["id"] == "MBOND"
    assert result["user"]["email"] == ""
    assert result["user"]["role"] == "Admin,Director,Underwriter"


def test_f5_login_user_returns_cct_role(monkeypatch):
    monkeypatch.setattr(auth_service, "create_access_token", lambda user_id, role: "token")
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: "refresh-token",
    )
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)
    monkeypatch.setattr(auth_service, "_set_refresh_cookie", lambda response, token: None)

    response = Response()
    result = asyncio.run(
        auth_service.f5_login_user(
            {
                "user": "MRM468",
                "groups": ["CCT"],
            },
            response,
        )
    )

    assert result["user"]["role"] == "CCT_User"


def test_normalize_role_keeps_order_and_dedupes():
    result = auth_service._normalize_role("Admin,Director,Admin,Underwriter,CCT_User")

    assert result == "Admin,Director,Underwriter,CCT_User"


def test_resolve_role_from_groups_uses_plain_group_names():
    result = auth_service._resolve_role_from_groups(
        [
            "CCT",
            "ADMIN",
        ]
    )

    assert result == "Admin,CCT_User"


def test_f5_login_user_sets_director_branch_from_mapping(monkeypatch):
    captured = {}

    monkeypatch.setattr(auth_service, "create_access_token", lambda user_id, role: "token")
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: "refresh-token",
    )
    monkeypatch.setattr(
        auth_service,
        "get_branch_name_by_user_identifier",
        lambda user_id: captured.__setitem__("user_id", user_id) or "Northeast",
    )
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)
    monkeypatch.setattr(auth_service, "_set_refresh_cookie", lambda response, token: None)

    response = Response()
    result = asyncio.run(
        auth_service.f5_login_user(
            {
                "user": "MDELUCA",
                "groups": ["DIRECTORS"],
            },
            response,
        )
    )

    assert result["user"]["role"] == "Director"
    assert result["user"]["branch"] == "Northeast"
    assert captured["user_id"] == "mdeluca"


def test_f5_login_user_sets_all_branch_from_mapping(monkeypatch):
    monkeypatch.setattr(auth_service, "create_access_token", lambda user_id, role: "token")
    monkeypatch.setattr(
        auth_service,
        "create_refresh_token",
        lambda user_id, role=None: "refresh-token",
    )
    monkeypatch.setattr(
        auth_service,
        "get_branch_name_by_user_identifier",
        lambda email: "All",
    )
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)
    monkeypatch.setattr(auth_service, "_set_refresh_cookie", lambda response, token: None)

    response = Response()
    result = asyncio.run(
        auth_service.f5_login_user(
            {
                "user": "MBOND",
                "groups": ["DIRECTORS"],
            },
            response,
        )
    )

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
        },
    )
    monkeypatch.setattr(auth_service, "get_branch_name_by_email", lambda email: "HQ")

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    result = asyncio.run(auth_service.get_current_user_from_token(request))
    assert result["user"]["email"] == "a@example.com"
    assert result["user"]["role"] == "User"
    assert result["user"]["branch"] == "HQ"


def test_get_current_user_from_token_success_f5_path(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "decode_access_token",
        lambda token: {"sub": "MRM468", "role": "Admin,Director,Underwriter"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for F5 user id"),
    )

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    result = asyncio.run(auth_service.get_current_user_from_token(request))
    assert result["user"]["id"] == "MRM468"
    assert result["user"]["email"] == ""
    assert result["user"]["role"] == "Admin,Director,Underwriter"
    assert result["user"]["branch"] is None


def test_get_current_user_from_token_keeps_underwriter_for_mbond(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "decode_access_token",
        lambda token: {"sub": "MBOND", "role": "Admin,Director,Underwriter"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for F5 user id"),
    )

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    result = asyncio.run(auth_service.get_current_user_from_token(request))
    assert result["user"]["id"] == "MBOND"
    assert result["user"]["email"] == ""
    assert result["user"]["role"] == "Admin,Director,Underwriter"


def test_get_current_user_from_token_success_f5_path_branch_mapping(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "decode_access_token",
        lambda token: {"sub": "JHOULE", "role": "Director"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for F5 user id"),
    )
    monkeypatch.setattr(
        auth_service,
        "get_branch_name_by_user_identifier",
        lambda user_id: "All",
    )

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.SESSION_COOKIE_NAME: "token"}

    result = asyncio.run(auth_service.get_current_user_from_token(request))
    assert result["user"]["id"] == "JHOULE"
    assert result["user"]["email"] == ""
    assert result["user"]["branch"] == "All"


def test_get_branch_name_by_email_returns_none_when_lookup_fails(monkeypatch):
    monkeypatch.setattr(
        auth_service,
        "run_raw_query",
        lambda query, params: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )

    assert auth_service.get_branch_name_by_email("mbond@hanover.com") is None


def test_get_branch_name_by_user_identifier_uses_user_id(monkeypatch):
    captured = {}

    def fake_run_raw_query(query, params):
        captured["query"] = query
        captured["params"] = params
        return [{"BranchName": "Northeast"}]

    monkeypatch.setattr(auth_service, "run_raw_query", fake_run_raw_query)

    result = auth_service.get_branch_name_by_user_identifier("MRM468")

    assert result == "Northeast"
    assert "LOWER(UserID) = ?" in captured["query"]
    assert captured["params"] == ["mrm468"]


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


def test_refresh_user_token_success_f5_path(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        auth_service,
        "decode_refresh_token",
        lambda token: {"sub": "MRM468", "role": "Admin,Director,Underwriter"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for F5 user id"),
    )
    monkeypatch.setattr(
        auth_service,
        "create_access_token",
        lambda uid, role: captured.__setitem__("role", role) or "newtoken",
    )
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.REFRESH_COOKIE_NAME: "refresh-token"}
    response = Response()

    result = asyncio.run(auth_service.refresh_user_token(request, response, token=None))
    assert result == {"message": "Token refreshed", "token": "newtoken"}
    assert captured["role"] == "Admin,Director,Underwriter"


def test_refresh_user_token_keeps_underwriter_for_mbond(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        auth_service,
        "decode_refresh_token",
        lambda token: {"sub": "MBOND", "role": "Admin,Director,Underwriter"},
    )
    monkeypatch.setattr(
        auth_service,
        "get_user_by_id",
        lambda user_id: pytest.fail("DB lookup should not happen for F5 user id"),
    )
    monkeypatch.setattr(
        auth_service,
        "create_access_token",
        lambda uid, role: captured.__setitem__("role", role) or "newtoken",
    )
    monkeypatch.setattr(auth_service, "_set_session_cookie", lambda response, token: None)

    request = Request({"type": "http", "headers": [], "query_string": b""})
    request._cookies = {auth_service.REFRESH_COOKIE_NAME: "refresh-token"}
    response = Response()

    result = asyncio.run(auth_service.refresh_user_token(request, response, token=None))
    assert result == {"message": "Token refreshed", "token": "newtoken"}
    assert captured["role"] == "Admin,Director,Underwriter"


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
