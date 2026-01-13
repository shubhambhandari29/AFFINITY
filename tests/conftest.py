from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from services.auth_service import get_current_user_from_token


@pytest.fixture
def make_test_client():
    def _create(router):
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_current_user_from_token] = lambda: {"sub": "test-user"}
        return TestClient(app)

    return _create
