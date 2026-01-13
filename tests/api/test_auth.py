from api import auth as auth_router


def test_login_calls_service(make_test_client, monkeypatch):
    captured = {}

    async def fake_login(payload, response):
        captured["payload"] = payload
        captured["response"] = response
        return {"token": "abc"}

    monkeypatch.setattr(auth_router, "login_user", fake_login)
    client = make_test_client(auth_router.router)

    response = client.post("/login", json={"email": "user@example.com", "password": "pw"})

    assert response.status_code == 200
    assert response.json() == {"token": "abc"}
    assert captured["payload"] == {"email": "user@example.com", "password": "pw"}


def test_get_current_user(make_test_client, monkeypatch):
    captured = {}

    async def fake_get_current(request):
        captured["request"] = request
        return {"user": "ok"}

    monkeypatch.setattr(auth_router, "get_current_user_from_token", fake_get_current)
    client = make_test_client(auth_router.router)

    response = client.get("/me")

    assert response.status_code == 200
    assert response.json() == {"user": "ok"}
    assert captured["request"].url.path == "/me"


def test_logout_route(make_test_client, monkeypatch):
    captured = {}

    async def fake_logout(response):
        captured["response"] = response
        return {"message": "bye"}

    monkeypatch.setattr(auth_router, "logout_user", fake_logout)
    client = make_test_client(auth_router.router)

    response = client.post("/logout")

    assert response.status_code == 200
    assert response.json() == {"message": "bye"}
    assert captured["response"] is not None


def test_refresh_token_uses_oauth_token(make_test_client, monkeypatch):
    captured = {}

    async def fake_refresh(request, response, token):
        captured["request"] = request
        captured["token"] = token
        return {"token": "new"}

    monkeypatch.setattr(auth_router, "refresh_user_token", fake_refresh)
    client = make_test_client(auth_router.router)

    response = client.post(
        "/refresh_token",
        headers={"Authorization": "Bearer sometoken"},
    )

    assert response.status_code == 200
    assert response.json() == {"token": "new"}
    assert captured["token"] == "sometoken"
