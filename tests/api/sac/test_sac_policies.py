from api.sac import sac_policies


def test_get_sac_policies(make_test_client, monkeypatch):
    captured = {}

    async def fake_get(params):
        captured["params"] = params
        return [{"PolicyNum": "P1"}]

    monkeypatch.setattr(sac_policies, "get_sac_policies_service", fake_get)
    client = make_test_client(sac_policies.router)

    response = client.get("/?CustomerNum=1")

    assert response.status_code == 200
    assert response.json() == [{"PolicyNum": "P1"}]
    assert captured["params"] == {"CustomerNum": "1"}


def test_upsert_sac_policies(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(payload):
        captured["payload"] = payload
        return {"status": "ok"}

    monkeypatch.setattr(sac_policies, "upsert_sac_policies_service", fake_upsert)
    client = make_test_client(sac_policies.router)

    payload = {"CustomerNum": "1", "PolicyNum": "P1", "PolMod": "A"}
    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert captured["payload"] == payload


def test_update_field_for_all_policies(make_test_client, monkeypatch):
    captured = {}

    async def fake_update(data):
        captured["data"] = data
        return {"updated": True}

    monkeypatch.setattr(
        sac_policies, "update_field_for_all_policies_service", fake_update
    )
    client = make_test_client(sac_policies.router)

    payload = {
        "fieldName": "AccountName",
        "fieldValue": "Bound",
        "updateVia": "CustomerNum",
        "updateViaValue": "1",
    }
    response = client.post("/update_field_for_all_policies", json=payload)

    assert response.status_code == 200
    assert response.json() == {"updated": True}
    assert captured["data"] == payload


def test_get_premium(make_test_client, monkeypatch):
    captured = {}

    async def fake_premium(params):
        captured["params"] = params
        return {"Premium": 1234}

    monkeypatch.setattr(sac_policies, "get_premium_service", fake_premium)
    client = make_test_client(sac_policies.router)

    response = client.get("/get_premium?CustomerNum=1")

    assert response.status_code == 200
    assert response.json() == {"Premium": 1234}
    assert captured["params"] == {"CustomerNum": "1"}
