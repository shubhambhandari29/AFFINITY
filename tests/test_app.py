from fastapi.testclient import TestClient

import app as app_module


def test_home_and_health_endpoints(monkeypatch):
    # Avoid creating real log handlers during import
    monkeypatch.setattr(app_module, "configure_logging", lambda: None)
    client = TestClient(app_module.app)

    home = client.get("/")
    health = client.get("/health")

    assert home.status_code == 200
    assert home.json() == "Welcome to SAC"
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
