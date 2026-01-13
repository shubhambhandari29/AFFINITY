import pytest
from fastapi import HTTPException, Request, Response

from services import auth_service as svc


class DummyCursor:
    def __init__(self):
        self.executed = None

    def execute(self, query, params):
        self.executed = (query.strip(), params)


class DummyConn:
    def __init__(self):
        self.cursor_obj = DummyCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def make_request(cookies: str | None = None) -> Request:
    headers = []
    if cookies:
        headers.append((b"cookie", cookies.encode()))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "method": "GET",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": headers,
        "client": ("testclient", 5000),
        "server": ("testserver", 80),
        "scheme": "http",
        "app": None,
    }
    return Request(scope, receive=None)


@pytest.mark.anyio
async def test_login_user_success(monkeypatch):
    user_record = {
        "ID": 1,
        "FirstName": "Test",
        "LastName": "User",
        "Email": "test@example.com",
        "Role": "admin",
        "BranchName": "NY",
        "Password": "hashed",
    }

    monkeypatch.setattr(svc, "get_user_by_email", lambda _: user_record)
    monkeypatch.setattr(svc, "verify_password", lambda provided, stored: True)
    monkeypatch.setattr(svc, "create_access_token", lambda user: "token123")

    response = Response()
    result = await svc.login_user({"email": "test@example.com", "password": "pw"}, response)

    assert result["token"] == "token123"
    assert "session=" in response.headers.get("set-cookie", "")


@pytest.mark.anyio
async def test_login_user_missing_fields():
    response = Response()
    with pytest.raises(HTTPException):
        await svc.login_user({"email": ""}, response)


@pytest.mark.anyio
async def test_get_current_user_from_token(monkeypatch):
    request = make_request("session=abc")
    monkeypatch.setattr(svc, "decode_access_token", lambda token: {"user": {"email": "x"}})

    result = await svc.get_current_user_from_token(request)
    assert result["user"]["email"] == "x"


@pytest.mark.anyio
async def test_get_current_user_missing_token():
    request = make_request()
    with pytest.raises(HTTPException):
        await svc.get_current_user_from_token(request)


@pytest.mark.anyio
async def test_logout_user():
    response = Response()
    result = await svc.logout_user(response)
    assert result == {"message": "Logged out successfully"}
    assert "session=" in response.headers.get("set-cookie", "")


@pytest.mark.anyio
async def test_refresh_user_token(monkeypatch):
    request = make_request("session=old")
    monkeypatch.setattr(svc, "decode_access_token", lambda token: {"user": {"email": "x"}})
    monkeypatch.setattr(svc, "create_access_token", lambda user: "newtoken")

    response = Response()
    result = await svc.refresh_user_token(request, response, token=None)
    assert result["token"] == "newtoken"
    assert "session=newtoken" in response.headers.get("set-cookie", "")


def test_get_user_by_email(monkeypatch):
    monkeypatch.setattr(
        svc, "run_raw_query", lambda query, params: [{"ID": 1, "Email": params[0]}]
    )
    result = svc.get_user_by_email("user@example.com")
    assert result["Email"] == "user@example.com"


def test_get_user_by_email_not_found(monkeypatch):
    monkeypatch.setattr(svc, "run_raw_query", lambda *args, **kwargs: [])
    assert svc.get_user_by_email("missing@example.com") is None


def test_persist_hashed_password(monkeypatch):
    conn = DummyConn()

    def fake_db_connection():
        return conn

    monkeypatch.setattr(svc, "db_connection", fake_db_connection)
    svc._persist_hashed_password(1, "hash")

    assert conn.cursor_obj.executed[0].startswith("UPDATE tblUsers")
    assert conn.committed is True
