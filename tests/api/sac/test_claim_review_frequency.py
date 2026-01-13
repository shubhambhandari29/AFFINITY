from api.sac import claim_review_frequency


def test_get_claim_review_frequency(make_test_client, monkeypatch):
    captured = {}

    async def fake_get(params):
        captured["params"] = params
        return [{"CustomerNum": "1", "MthNum": 1}]

    monkeypatch.setattr(claim_review_frequency, "get_frequency_service", fake_get)
    client = make_test_client(claim_review_frequency.router)

    response = client.get("/?CustomerNum=1&MthNum=2")

    assert response.status_code == 200
    assert response.json() == [{"CustomerNum": "1", "MthNum": 1}]
    assert captured["params"] == {"CustomerNum": "1", "MthNum": "2"}


def test_upsert_claim_review_frequency(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(items):
        captured["items"] = items
        return {"count": len(items)}

    monkeypatch.setattr(claim_review_frequency, "upsert_frequency_service", fake_upsert)
    client = make_test_client(claim_review_frequency.router)

    payload = [{"CustomerNum": "1", "MthNum": 1, "Frequency": 3}]
    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"count": 1}
    assert len(captured["items"]) == 1
    assert captured["items"][0]["CustomerNum"] == "1"
    assert captured["items"][0]["MthNum"] == 1
