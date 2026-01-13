from api.sac import loss_run_distribution


def test_get_loss_run_distribution(make_test_client, monkeypatch):
    captured = {}

    async def fake_get(params):
        captured["params"] = params
        return [{"CustomerNum": "1"}]

    monkeypatch.setattr(loss_run_distribution, "get_distribution_service", fake_get)
    client = make_test_client(loss_run_distribution.router)

    response = client.get("/?CustomerNum=1")

    assert response.status_code == 200
    assert response.json() == [{"CustomerNum": "1"}]
    assert captured["params"] == {"CustomerNum": "1"}


def test_upsert_loss_run_distribution(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(rows):
        captured["rows"] = rows
        return {"count": len(rows)}

    monkeypatch.setattr(loss_run_distribution, "upsert_distribution_service", fake_upsert)
    client = make_test_client(loss_run_distribution.router)

    payload = [{"CustomerNum": "1", "EMailAddress": "a@example.com"}]
    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"count": 1}
    assert len(captured["rows"]) == 1
    assert captured["rows"][0]["CustomerNum"] == "1"
    assert captured["rows"][0]["EMailAddress"] == "a@example.com"


def test_delete_loss_run_distribution(make_test_client, monkeypatch):
    captured = {}

    async def fake_delete(rows):
        captured["rows"] = rows
        return {"deleted": len(rows)}

    monkeypatch.setattr(loss_run_distribution, "delete_distribution_service", fake_delete)
    client = make_test_client(loss_run_distribution.router)

    payload = [{"CustomerNum": "1", "EMailAddress": "a@example.com"}]
    response = client.post("/delete", json=payload)

    assert response.status_code == 200
    assert response.json() == {"deleted": 1}
    assert len(captured["rows"]) == 1
    assert captured["rows"][0]["CustomerNum"] == "1"
    assert captured["rows"][0]["EMailAddress"] == "a@example.com"
