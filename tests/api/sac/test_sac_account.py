from api.sac import sac_account


def test_get_sac_account_forwards_query_params(make_test_client, monkeypatch):
    captured = {}

    async def fake_service(params):
        captured["params"] = params
        return [{"CustomerNum": "C1"}]

    monkeypatch.setattr(sac_account, "get_sac_account_service", fake_service)
    client = make_test_client(sac_account.router)

    response = client.get("/?CustomerNum=abc&Stage=Active")

    assert response.status_code == 200
    assert response.json() == [{"CustomerNum": "C1"}]
    assert captured["params"] == {"CustomerNum": "abc", "Stage": "Active"}


def test_upsert_sac_account_dumps_payload(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(payload):
        captured["payload"] = payload
        return {"status": "ok"}

    monkeypatch.setattr(sac_account, "upsert_sac_account_service", fake_upsert)
    client = make_test_client(sac_account.router)

    payload = {"CustomerNum": "C1", "Stage": "Bound", "ServLevel": "Gold"}
    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert captured["payload"] == payload
