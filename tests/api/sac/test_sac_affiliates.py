from api.sac import sac_affiliates


def test_get_affiliates(make_test_client, monkeypatch):
    captured = {}

    async def fake_service(params):
        captured["params"] = params
        return [{"AffiliateName": "Alpha"}]

    monkeypatch.setattr(sac_affiliates, "get_affiliates_service", fake_service)
    client = make_test_client(sac_affiliates.router)

    response = client.get("/?CustomerNum=1001")

    assert response.status_code == 200
    assert response.json() == [{"AffiliateName": "Alpha"}]
    assert captured["params"] == {"CustomerNum": "1001"}


def test_upsert_affiliates_transforms_payload(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(data):
        captured["data"] = data
        return {"count": len(data)}

    monkeypatch.setattr(sac_affiliates, "upsert_affiliates_service", fake_upsert)
    client = make_test_client(sac_affiliates.router)

    payload = [
        {"CustomerNum": "1001", "AffiliateName": "Alpha"},
        {"CustomerNum": "1002", "AffiliateName": "Beta", "Email": "beta@example.com"},
    ]
    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"count": 2}
    assert len(captured["data"]) == 2
    assert captured["data"][0]["CustomerNum"] == "1001"
    assert captured["data"][1]["CustomerNum"] == "1002"
