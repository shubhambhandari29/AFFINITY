from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from core.config import settings
from services import graph_auth_service


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class FakeAsyncClient:
    def __init__(self, responses: list[FakeResponse], calls: list[tuple[str, dict | None]]):
        self._responses = responses
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, headers=None, params=None):
        self.calls.append((url, params))
        if not self._responses:
            raise AssertionError("Unexpected request made to fake Graph client")
        return self._responses.pop(0)


def _mock_httpx(monkeypatch, responses: list[FakeResponse], calls: list[tuple[str, dict | None]]):
    class FakeHttpxModule:
        def AsyncClient(self, timeout):
            assert timeout == 30.0
            return FakeAsyncClient(responses, calls)

    monkeypatch.setattr(graph_auth_service, "_require_httpx", lambda: FakeHttpxModule())


def test_login_with_f5_identifier_missing_header():
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(graph_auth_service.login_with_f5_identifier(None))

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Missing X-User-ID header"}


def test_login_with_f5_identifier_upn_happy_path(monkeypatch):
    monkeypatch.setattr(graph_auth_service, "_get_graph_access_token", lambda: "graph-token")
    monkeypatch.setattr(settings, "F5_ALLOWED_GROUP_NAMES", ["SAC_Admin"], raising=False)
    monkeypatch.setattr(settings, "F5_GROUP_ROLE_MAP", {"SAC_Admin": "admin"}, raising=False)

    calls: list[tuple[str, dict | None]] = []
    responses = [
        FakeResponse(
            200,
            {
                "id": "user-1",
                "displayName": "Shubham Bhandari",
                "userPrincipalName": "shubhambhandari@hanover.com",
                "mail": "shubhambhandari@hanover.com",
                "employeeId": "SXB640",
            },
        ),
        FakeResponse(
            200,
            {
                "value": [
                    {"id": "g-1", "displayName": "SAC_Admin"},
                    {"id": "g-2", "displayName": "RandomGroup"},
                ]
            },
        ),
    ]
    _mock_httpx(monkeypatch, responses, calls)

    result = asyncio.run(graph_auth_service.login_with_f5_identifier("shubhambhandari@hanover.com"))

    assert result["lookup_method"] == "id_or_upn"
    assert result["resolved_user"]["employee_id"] == "SXB640"
    assert result["group_names"] == ["RandomGroup", "SAC_Admin"]
    assert result["relevant_group_names"] == ["SAC_Admin"]
    assert result["roles"] == ["admin"]
    assert result["group_filter_applied"] is True
    assert calls[0][0].endswith("/users/shubhambhandari%40hanover.com")
    assert calls[1][0].endswith("/users/user-1/transitiveMemberOf/microsoft.graph.group")


def test_login_with_f5_identifier_employee_id_fallback(monkeypatch):
    monkeypatch.setattr(graph_auth_service, "_get_graph_access_token", lambda: "graph-token")
    monkeypatch.setattr(settings, "F5_ALLOWED_GROUP_NAMES", [], raising=False)
    monkeypatch.setattr(settings, "F5_GROUP_ROLE_MAP", {}, raising=False)

    calls: list[tuple[str, dict | None]] = []
    responses = [
        FakeResponse(404, {"error": {"message": "Resource not found"}}),
        FakeResponse(
            200,
            {
                "value": [
                    {
                        "id": "user-1",
                        "displayName": "Shubham Bhandari",
                        "userPrincipalName": "shubhambhandari@hanover.com",
                        "mail": "shubhambhandari@hanover.com",
                        "employeeId": "SXB640",
                    }
                ]
            },
        ),
        FakeResponse(200, {"value": [{"id": "g-3", "displayName": "SAC_Director"}]}),
    ]
    _mock_httpx(monkeypatch, responses, calls)

    result = asyncio.run(graph_auth_service.login_with_f5_identifier("SXB640"))

    assert result["lookup_method"] == "employee_id"
    assert result["resolved_user"]["id"] == "user-1"
    assert result["relevant_group_names"] == ["SAC_Director"]
    assert result["group_filter_applied"] is False
    assert "employeeId eq 'SXB640'" in str(calls[1][1]["$filter"])


def test_login_with_f5_identifier_ambiguous_employee_id(monkeypatch):
    monkeypatch.setattr(graph_auth_service, "_get_graph_access_token", lambda: "graph-token")

    calls: list[tuple[str, dict | None]] = []
    responses = [
        FakeResponse(404, {"error": {"message": "Resource not found"}}),
        FakeResponse(
            200,
            {
                "value": [
                    {"id": "user-1", "displayName": "User 1"},
                    {"id": "user-2", "displayName": "User 2"},
                ]
            },
        ),
    ]
    _mock_httpx(monkeypatch, responses, calls)

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(graph_auth_service.login_with_f5_identifier("SXB640"))

    assert excinfo.value.status_code == 409
    assert "Multiple users found via employee_id lookup" in str(excinfo.value.detail["error"])
