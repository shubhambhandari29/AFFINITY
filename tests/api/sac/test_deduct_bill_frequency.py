from api.sac import deduct_bill_frequency


def test_get_deduct_bill_frequency(make_test_client, monkeypatch):
    captured = {}

    async def fake_get(params):
        captured["params"] = params
        return [{"CustomerNum": "1", "MthNum": 1}]

    monkeypatch.setattr(deduct_bill_frequency, "get_frequency_service", fake_get)
    client = make_test_client(deduct_bill_frequency.router)

    response = client.get("/?CustomerNum=1")

    assert response.status_code == 200
    assert response.json() == [{"CustomerNum": "1", "MthNum": 1}]
    assert captured["params"] == {"CustomerNum": "1"}


def test_upsert_deduct_bill_frequency(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(items):
        captured["items"] = items
        return {"count": len(items)}

    monkeypatch.setattr(deduct_bill_frequency, "upsert_frequency_service", fake_upsert)
    client = make_test_client(deduct_bill_frequency.router)

    payload = [{"CustomerNum": "1", "MthNum": 1, "Frequency": 10}]
    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"count": 1}
    assert len(captured["items"]) == 1
    assert captured["items"][0]["CustomerNum"] == "1"
    assert captured["items"][0]["MthNum"] == 1
