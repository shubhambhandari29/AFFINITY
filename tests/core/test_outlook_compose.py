from __future__ import annotations

import pytest
from fastapi import HTTPException

from core import outlook_compose


def test_build_compose_link_requires_recipients():
    with pytest.raises(HTTPException) as excinfo:
        outlook_compose.build_compose_link([], "Subject", "Body")

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == {"error": "Provide recipients to build the compose link"}


def test_build_compose_link_filters_invalid_and_duplicates(monkeypatch):
    class DummySettings:
        OUTLOOK_COMPOSE_BASE_URL = "https://outlook.test/compose"

    monkeypatch.setattr(outlook_compose, "settings", DummySettings)

    result = outlook_compose.build_compose_link(
        ["a@example.com", "", "a@example.com", "b@example.com"],
        "Hello",
        "Body",
    )

    assert result["recipients"] == ["a@example.com", "b@example.com"]
    assert result["filtered_out"] == 2
    assert result["total"] == 4
    assert "to=a%40example.com%3Bb%40example.com" in result["url"]


def test_build_compose_link_rejects_all_invalid(monkeypatch):
    class DummySettings:
        OUTLOOK_COMPOSE_BASE_URL = "https://outlook.test/compose"

    monkeypatch.setattr(outlook_compose, "settings", DummySettings)

    with pytest.raises(HTTPException) as excinfo:
        outlook_compose.build_compose_link(["", ""], "Hello", "Body")

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail["error"] == "No valid email recipients found"
