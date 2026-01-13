from api import dropdowns


def test_get_dropdown_forwards_name(make_test_client, monkeypatch):
    captured = {}

    async def fake_service(name):
        captured["name"] = name
        return [{"value": 1}]

    monkeypatch.setattr(dropdowns, "get_dropdown_values_service", fake_service)
    client = make_test_client(dropdowns.router)

    response = client.get("/sac_1")

    assert response.status_code == 200
    assert response.json() == [{"value": 1}]
    assert captured["name"] == "sac_1"
