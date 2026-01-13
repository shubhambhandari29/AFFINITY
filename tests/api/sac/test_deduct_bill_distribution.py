from api.sac import deduct_bill_distribution


def test_get_deduct_bill_distribution(make_test_client, monkeypatch):
    captured = {}

    async def fake_get(params):
        captured["params"] = params
        return [{"CustomerNum": "1"}]

    monkeypatch.setattr(deduct_bill_distribution, "get_distribution_service", fake_get)
    client = make_test_client(deduct_bill_distribution.router)

    response = client.get("/?CustomerNum=1")

    assert response.status_code == 200
    assert response.json() == [{"CustomerNum": "1"}]
    assert captured["params"] == {"CustomerNum": "1"}


def test_upsert_deduct_bill_distribution(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(rows):
        captured["rows"] = rows
        return {"count": len(rows)}

    monkeypatch.setattr(
        deduct_bill_distribution, "upsert_distribution_service", fake_upsert
    )
    client = make_test_client(deduct_bill_distribution.router)

    payload = [{"CustomerNum": "1", "EMailAddress": "d@example.com"}]
    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"count": 1}
    assert len(captured["rows"]) == 1
    assert captured["rows"][0]["CustomerNum"] == "1"
    assert captured["rows"][0]["EMailAddress"] == "d@example.com"


def test_delete_deduct_bill_distribution(make_test_client, monkeypatch):
    captured = {}

    async def fake_delete(rows):
        captured["rows"] = rows
        return {"deleted": len(rows)}

    monkeypatch.setattr(
        deduct_bill_distribution, "delete_distribution_service", fake_delete
    )
    client = make_test_client(deduct_bill_distribution.router)

    payload = [{"CustomerNum": "1", "EMailAddress": "d@example.com"}]
    response = client.post("/delete", json=payload)

    assert response.status_code == 200
    assert response.json() == {"deleted": 1}
    assert len(captured["rows"]) == 1
    assert captured["rows"][0]["CustomerNum"] == "1"
    assert captured["rows"][0]["EMailAddress"] == "d@example.com"
