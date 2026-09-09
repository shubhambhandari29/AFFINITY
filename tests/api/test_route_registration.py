"""Ensure every API module is connected to the application."""

import importlib
from collections import Counter
from pathlib import Path

from fastapi import APIRouter
from fastapi.routing import APIRoute

from app import app


def test_every_api_endpoint_is_registered_once():
    root = Path(__file__).resolve().parents[2]
    registered = [route for route in app.routes if isinstance(route, APIRoute)]
    checked = 0
    for path in sorted((root / "api").rglob("*.py")):
        module_name = ".".join(path.relative_to(root).with_suffix("").parts)
        module = importlib.import_module(module_name)
        for router in vars(module).values():
            if not isinstance(router, APIRouter):
                continue
            for endpoint in router.routes:
                if not isinstance(endpoint, APIRoute):
                    continue
                for method in endpoint.methods:
                    matches = [
                        route for route in registered
                        if route.endpoint is endpoint.endpoint
                        and method in route.methods
                        and route.path.endswith(endpoint.path)
                    ]
                    assert len(matches) == 1, (
                        f"{module_name}: {method} {endpoint.path} "
                        f"has {len(matches)} application registrations"
                    )
                    checked += 1
    assert checked > 0


def test_no_duplicate_api_method_and_path():
    counts = Counter(
        (method, route.path)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    )
    assert not {key: count for key, count in counts.items() if count > 1}
