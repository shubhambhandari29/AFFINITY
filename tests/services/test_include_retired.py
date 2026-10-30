import asyncio
import importlib

import pytest
from fastapi import HTTPException


@pytest.fixture(params=[
    ("hcm.hcm_account_service", "get_hcm_account", None),
    ("sac.sac_account_service", "get_sac_account", "BranchName"),
    ("affinity.affinity_program_service", "get_affinity_program", "BranchVal"),
])
def account_service(request):
    module_name, function_name, branch_field = request.param
    module = importlib.import_module(f"services.{module_name}")
    return module, getattr(module, function_name), branch_field


@pytest.mark.parametrize("flag", [None, "false", "true"])
@pytest.mark.parametrize("branch", [None, "NY, LA", " , & "])
def test_include_retired_filter(monkeypatch, account_service, flag, branch):
    module, service, branch_field = account_service
    if branch_field is None and branch is not None:
        pytest.skip("HCM does not use branch prefix searches")
    records = [
        {"CustomerNum": "1223", "Stage": " Retired "},
        {"CustomerNum": "1223", "Stage": "Admin"},
    ]
    calls = []

    async def fake_fetch(*, table, filters):
        assert filters == {"CustomerNum": "1223"}
        calls.append("fetch")
        return [dict(row) for row in records]

    async def fake_query(query, params):
        assert "include_retired" not in query
        assert params == ["1223", "NY%", "LA%"]
        calls.append("query")
        return [dict(row) for row in records]

    monkeypatch.setattr(module, "fetch_records_async", fake_fetch)
    if branch_field is not None:
        monkeypatch.setattr(module, "run_raw_query_async", fake_query)
    params = {"CustomerNum": "1223"}
    if flag is not None:
        params["include_retired"] = flag
    if branch is not None:
        params[branch_field] = branch
    original = dict(params)
    result = asyncio.run(service(params))
    assert result == (records if flag == "true" else records[1:])
    assert params == original
    assert calls == (["query"] if branch == "NY, LA" else ["fetch"])


def test_invalid_include_retired(monkeypatch, account_service):
    module, service, _ = account_service

    async def unexpected_fetch(**kwargs):
        pytest.fail("Invalid flag must not reach the database")

    monkeypatch.setattr(module, "fetch_records_async", unexpected_fetch)
    with pytest.raises(HTTPException) as error:
        asyncio.run(service({"include_retired": "invalid"}))
    assert error.value.status_code == 400
