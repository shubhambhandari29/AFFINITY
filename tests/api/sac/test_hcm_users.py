from api.sac import hcm_users


def test_get_hcm_users(make_test_client, monkeypatch):
    captured = {}

    async def fake_get(params):
        captured["params"] = params
        return [{"CustNum": "1001"}]

    monkeypatch.setattr(hcm_users, "get_hcm_users_service", fake_get)
    client = make_test_client(hcm_users.router)

    response = client.get("/?CustomerNum=1001")

    assert response.status_code == 200
    assert response.json() == [{"CustNum": "1001"}]
    assert captured["params"] == {"CustomerNum": "1001"}


def test_upsert_hcm_users(make_test_client, monkeypatch):
    captured = {}

    async def fake_upsert(records):
        captured["records"] = records
        return {"count": len(records)}

    monkeypatch.setattr(hcm_users, "upsert_hcm_users_service", fake_upsert)
    client = make_test_client(hcm_users.router)

    payload = [
        {"CustomerNum": "1001", "UserID": "u1", "Email": "a@example.com"},
        {"CustomerNum": "1002", "UserID": "u2"},
    ]

    response = client.post("/upsert", json=payload)

    assert response.status_code == 200
    assert response.json() == {"count": 2}
    assert len(captured["records"]) == 2
    assert captured["records"][0]["CustomerNum"] == "1001"
    assert captured["records"][1]["CustomerNum"] == "1002"
