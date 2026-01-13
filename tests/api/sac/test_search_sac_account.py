from api.sac import search_sac_account


def test_search_sac_account(make_test_client, monkeypatch):
    captured = {}

    async def fake_search(term):
        captured["term"] = term
        return [{"CustomerName": "ACME"}]

    monkeypatch.setattr(
        search_sac_account, "get_sac_account_records_service", fake_search
    )
    client = make_test_client(search_sac_account.router)

    response = client.get("/?search_by=acme")

    assert response.status_code == 200
    assert response.json() == [{"CustomerName": "ACME"}]
    assert captured["term"] == "acme"
