from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlencode

import pytest
from starlette.requests import Request


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def request_factory():
    def _make(params=None):
        query_params = params or {}
        query_string = urlencode(query_params, doseq=True).encode()
        scope = {"type": "http", "query_string": query_string, "headers": []}
        return Request(scope)

    return _make
