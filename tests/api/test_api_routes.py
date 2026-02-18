from __future__ import annotations

import asyncio
import importlib

import pytest
from fastapi import Response


class DummyModel:
    def __init__(self, data):
        self._data = data

    def model_dump(self):
        return dict(self._data)


GET_REQUEST_ENDPOINTS = [
    ("api.sac.claim_review_distribution", "get_distribution", "get_distribution_service"),
    ("api.sac.deduct_bill_distribution", "get_distribution", "get_distribution_service"),
    ("api.sac.loss_run_distribution", "get_distribution", "get_distribution_service"),
    ("api.sac.claim_review_frequency", "get_frequency", "get_frequency_service"),
    ("api.sac.deduct_bill_frequency", "get_frequency", "get_frequency_service"),
    ("api.sac.loss_run_frequency", "get_frequency", "get_frequency_service"),
    ("api.sac.hcm_users", "get_hcm_users", "get_hcm_users_service"),
    ("api.sac.sac_account", "get_sac_account", "get_sac_account_service"),
    ("api.sac.sac_account_associations", "get_associations", "get_associations_service"),
    ("api.sac.sac_affiliates", "get_affiliates", "get_affiliates_service"),
    ("api.sac.sac_policies", "get_sac_policies", "get_sac_policies_service"),
    ("api.sac.sac_policies", "get_premium", "get_premium_service"),
    ("api.affinity.affinity_program", "get_affinity_program", "get_affinity_program_service"),
    ("api.affinity.affinity_agents", "get_affinity_agents", "get_affinity_agents_service"),
    ("api.affinity.affinity_policy_types", "get_affinity_policy_types", "get_affinity_policy_types_service"),
    ("api.affinity.claim_review_distribution", "get_distribution", "get_distribution_service"),
    ("api.affinity.loss_run_distribution", "get_distribution", "get_distribution_service"),
    ("api.affinity.policy_type_distribution", "get_distribution", "get_distribution_service"),
    ("api.affinity.claim_review_frequency", "get_frequency", "get_frequency_service"),
    ("api.affinity.loss_run_frequency", "get_frequency", "get_frequency_service"),
]

QUERY_ENDPOINTS = [
    ("api.sac.search_sac_account", "get_sac_account_records", "get_sac_account_records_service"),
    ("api.affinity.search_affinity_program", "get_affinity_program_records", "get_affinity_program_records_service"),
]

LIST_PAYLOAD_ENDPOINTS = [
    ("api.sac.claim_review_distribution", "upsert_distribution", "upsert_distribution_service"),
    ("api.sac.claim_review_distribution", "delete_distribution", "delete_distribution_service"),
    ("api.sac.deduct_bill_distribution", "upsert_distribution", "upsert_distribution_service"),
    ("api.sac.deduct_bill_distribution", "delete_distribution", "delete_distribution_service"),
    ("api.sac.loss_run_distribution", "upsert_distribution", "upsert_distribution_service"),
    ("api.sac.loss_run_distribution", "delete_distribution", "delete_distribution_service"),
    ("api.sac.claim_review_frequency", "upsert_frequency", "upsert_frequency_service"),
    ("api.sac.deduct_bill_frequency", "upsert_frequency", "upsert_frequency_service"),
    ("api.sac.loss_run_frequency", "upsert_frequency", "upsert_frequency_service"),
    ("api.sac.hcm_users", "upsert_hcm_users", "upsert_hcm_users_service"),
    ("api.sac.sac_affiliates", "upsert_affiliates", "upsert_affiliates_service"),
    ("api.affinity.claim_review_distribution", "upsert_distribution", "upsert_distribution_service"),
    ("api.affinity.claim_review_distribution", "delete_distribution", "delete_distribution_service"),
    ("api.affinity.loss_run_distribution", "upsert_distribution", "upsert_distribution_service"),
    ("api.affinity.loss_run_distribution", "delete_distribution", "delete_distribution_service"),
    ("api.affinity.policy_type_distribution", "upsert_distribution", "upsert_distribution_service"),
    ("api.affinity.claim_review_frequency", "upsert_frequency", "upsert_frequency_service"),
    ("api.affinity.loss_run_frequency", "upsert_frequency", "upsert_frequency_service"),
]

SINGLE_PAYLOAD_ENDPOINTS = [
    ("api.sac.sac_account", "upsert_sac_account", "upsert_sac_account_service"),
    ("api.sac.sac_account_associations", "add_associations", "add_associations_service"),
    ("api.sac.sac_account_associations", "delete_associations", "delete_associations_service"),
    ("api.sac.sac_policies", "upsert_sac_policies", "upsert_sac_policies_service"),
    ("api.sac.sac_policies", "update_field_for_all_policies", "update_field_for_all_policies_service"),
    ("api.affinity.affinity_program", "upsert_affinity_program", "upsert_affinity_program_service"),
    ("api.affinity.affinity_policy_types", "upsert_affinity_policy_types", "upsert_affinity_policy_types_service"),
]


@pytest.mark.parametrize("module_path, func_name, service_attr", GET_REQUEST_ENDPOINTS)
def test_get_endpoints_pass_query_params(request_factory, monkeypatch, module_path, func_name, service_attr):
    module = importlib.import_module(module_path)
    captured = {}

    async def fake_service(params):
        captured["params"] = params
        return {"ok": module_path}

    monkeypatch.setattr(module, service_attr, fake_service)
    request = request_factory({"foo": "bar"})

    result = asyncio.run(getattr(module, func_name)(request))

    assert captured["params"] == {"foo": "bar"}
    assert result == {"ok": module_path}


@pytest.mark.parametrize("module_path, func_name, service_attr", QUERY_ENDPOINTS)
def test_query_endpoints_pass_search_by(monkeypatch, module_path, func_name, service_attr):
    module = importlib.import_module(module_path)
    captured = {}

    async def fake_service(search_by):
        captured["search_by"] = search_by
        return {"ok": search_by}

    monkeypatch.setattr(module, service_attr, fake_service)

    result = asyncio.run(getattr(module, func_name)(search_by="AccountName"))

    assert captured["search_by"] == "AccountName"
    assert result == {"ok": "AccountName"}


@pytest.mark.parametrize("module_path, func_name, service_attr", LIST_PAYLOAD_ENDPOINTS)
def test_list_payload_endpoints(monkeypatch, module_path, func_name, service_attr):
    module = importlib.import_module(module_path)
    captured = {}

    async def fake_service(rows):
        captured["rows"] = rows
        return {"count": len(rows)}

    monkeypatch.setattr(module, service_attr, fake_service)
    payload = [DummyModel({"id": 1}), DummyModel({"id": 2})]

    result = asyncio.run(getattr(module, func_name)(payload))

    assert captured["rows"] == [{"id": 1}, {"id": 2}]
    assert result == {"count": 2}


@pytest.mark.parametrize("module_path, func_name, service_attr", SINGLE_PAYLOAD_ENDPOINTS)
def test_single_payload_endpoints(monkeypatch, module_path, func_name, service_attr):
    module = importlib.import_module(module_path)
    captured = {}

    async def fake_service(data):
        captured["data"] = data
        return {"ok": True}

    monkeypatch.setattr(module, service_attr, fake_service)
    payload = DummyModel({"id": 99})

    result = asyncio.run(getattr(module, func_name)(payload))

    assert captured["data"] == {"id": 99}
    assert result == {"ok": True}


def test_affinity_agents_upsert_handles_list_and_single(monkeypatch):
    module = importlib.import_module("api.affinity.affinity_agents")
    captured = {"rows": None}

    async def fake_service(rows):
        captured["rows"] = rows
        return {"ok": True}

    monkeypatch.setattr(module, "upsert_affinity_agents_service", fake_service)

    result = asyncio.run(module.upsert_affinity_agents([DummyModel({"id": 1})]))
    assert captured["rows"] == [{"id": 1}]
    assert result == {"ok": True}

    result = asyncio.run(module.upsert_affinity_agents(DummyModel({"id": 2})))
    assert captured["rows"] == [{"id": 2}]
    assert result == {"ok": True}


def test_dropdown_endpoints(monkeypatch):
    module = importlib.import_module("api.dropdowns")
    captured = {"get": None, "upsert": None, "delete": None}

    async def fake_get(name):
        captured["get"] = name
        return {"name": name}

    async def fake_upsert(name, payload):
        captured["upsert"] = (name, payload)
        return {"count": len(payload)}

    async def fake_delete(name, payload):
        captured["delete"] = (name, payload)
        return {"count": len(payload)}

    monkeypatch.setattr(module, "get_dropdown_values_service", fake_get)
    monkeypatch.setattr(module, "upsert_dropdown_values_service", fake_upsert)
    monkeypatch.setattr(module, "delete_dropdown_values_service", fake_delete)

    assert asyncio.run(module.get_dropdown("LossCtl")) == {"name": "LossCtl"}
    assert asyncio.run(module.upsert_dropdown("LossCtl", [{"id": 1}])) == {"count": 1}
    assert asyncio.run(module.delete_dropdown("LossCtl", [{"id": 2}])) == {"count": 1}
    assert captured["get"] == "LossCtl"
    assert captured["upsert"] == ("LossCtl", [{"id": 1}])
    assert captured["delete"] == ("LossCtl", [{"id": 2}])


def test_auth_endpoints(monkeypatch, request_factory):
    module = importlib.import_module("api.auth")
    captured = {"login": None, "me": None, "logout": None, "refresh": None}

    async def fake_login(payload, response):
        captured["login"] = (payload, response)
        return {"ok": "login"}

    async def fake_me(request):
        captured["me"] = request
        return {"ok": "me"}

    async def fake_logout(response):
        captured["logout"] = response
        return {"ok": "logout"}

    async def fake_refresh(request, response, token):
        captured["refresh"] = (request, response, token)
        return {"ok": "refresh"}

    monkeypatch.setattr(module, "login_user", fake_login)
    monkeypatch.setattr(module, "get_current_user_from_token", fake_me)
    monkeypatch.setattr(module, "logout_user", fake_logout)
    monkeypatch.setattr(module, "refresh_user_token", fake_refresh)

    response = Response()
    request = request_factory()

    assert asyncio.run(module.login(DummyModel({"user": "u"}), response)) == {"ok": "login"}
    assert asyncio.run(module.get_current_user(request)) == {"ok": "me"}
    assert asyncio.run(module.logout(response)) == {"ok": "logout"}
    assert asyncio.run(module.refresh_token(request, response, token="abc")) == {"ok": "refresh"}

    assert captured["login"][0] == {"user": "u"}
    assert captured["logout"] is response
    assert captured["refresh"][2] == "abc"


def test_outlook_compose_endpoint(monkeypatch):
    module = importlib.import_module("api.outlook_compose")
    captured = {}

    def fake_build_compose_link(*, recipients, subject, body):
        captured["args"] = (recipients, subject, body)
        return "mailto:link"

    monkeypatch.setattr(module, "build_compose_link", fake_build_compose_link)

    class DummyPayload:
        recipients = ["user@example.com"]
        subject = "Hello"
        body = "World"

    result = asyncio.run(module.build_compose_link_handler(DummyPayload()))
    assert result == "mailto:link"
    assert captured["args"] == (["user@example.com"], "Hello", "World")
