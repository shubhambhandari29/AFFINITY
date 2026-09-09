"""Service contracts for invalid filters and unavailable persistence."""

import asyncio
from importlib import import_module
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

READ_CASES = [
    ("affinity.affinity_agents_service", "get_affinity_agents"),
    ("affinity.affinity_program_service", "get_affinity_program"),
    ("affinity.affinity_policy_types_service", "get_affinity_policy_types"),
    ("affinity.claim_review_distribution_service", "get_distribution"),
    ("affinity.loss_run_distribution_service", "get_distribution"),
    ("affinity.policy_type_distribution_service", "get_distribution"),
    ("affinity.claim_review_frequency_service", "get_frequency"),
    ("affinity.loss_run_frequency_service", "get_frequency"),
    ("hcm.hcm_account_service", "get_hcm_account"),
    ("hcm.hcm_users_service", "get_hcm_users"),
    ("hcm.hcm_account_associations_service", "get_associations"),
    ("sac.sac_account_service", "get_sac_account"),
    ("sac.hcm_users_service", "get_hcm_users"),
    ("sac.sac_account_associations_service", "get_associations"),
    ("sac.sac_affiliates_service", "get_affiliates"),
    ("sac.sac_policies_service", "get_sac_policies"),
    ("sac.sac_policies_service", "get_premium"),
    ("sac.sac_policies_service", "get_underwriter_details"),
    ("sac.claim_review_distribution_service", "get_distribution"),
    ("sac.loss_run_distribution_service", "get_distribution"),
    ("sac.deduct_bill_distribution_service", "get_distribution"),
    ("sac.claim_review_frequency_service", "get_frequency"),
    ("sac.loss_run_frequency_service", "get_frequency"),
    ("sac.deduct_bill_frequency_service", "get_frequency"),
]


@pytest.mark.parametrize("module_name,method", READ_CASES)
def test_invalid_filter_returns_400_without_querying_database(monkeypatch, module_name, method):
    module = import_module("services." + module_name)
    queries = []
    for name in ("fetch_records_async", "run_raw_query_async"):
        if hasattr(module, name):
            query = AsyncMock()
            monkeypatch.setattr(module, name, query)
            queries.append(query)
    with pytest.raises(HTTPException) as error:
        asyncio.run(getattr(module, method)({"bad-column": "value"}))
    assert error.value.status_code == 400
    assert "bad-column" in error.value.detail["error"]
    for query in queries:
        query.assert_not_awaited()


@pytest.mark.parametrize("module_name,method", READ_CASES)
def test_database_read_failure_returns_500_with_cause(monkeypatch, module_name, method):
    module = import_module("services." + module_name)
    failure = RuntimeError("database unavailable")
    queries = []
    for name in ("fetch_records_async", "run_raw_query_async"):
        if hasattr(module, name):
            query = AsyncMock(side_effect=failure)
            monkeypatch.setattr(module, name, query)
            queries.append(query)
    filters = {"ParentAccount": "001"} if method == "get_associations" else {"CustomerNum": "001"}
    if module_name.startswith("affinity."):
        filters = {"ProgramName": "Example"}
    with pytest.raises(HTTPException) as error:
        asyncio.run(getattr(module, method)(filters))
    assert error.value.status_code == 500
    assert error.value.detail == {"error": "database unavailable"}
    assert error.value.__cause__ is failure
    assert sum(query.await_count for query in queries) == 1


DISTRIBUTIONS = [
    "affinity.claim_review_distribution_service",
    "affinity.loss_run_distribution_service",
    "affinity.policy_type_distribution_service",
    "sac.claim_review_distribution_service",
    "sac.loss_run_distribution_service",
    "sac.deduct_bill_distribution_service",
]


@pytest.mark.parametrize("module_name", DISTRIBUTIONS)
@pytest.mark.parametrize(
    "method,dependency",
    [
        ("upsert_distribution", "merge_upsert_records_async"),
        ("delete_distribution", "delete_records_async"),
    ],
)
def test_distribution_write_failure_returns_500(monkeypatch, module_name, method, dependency):
    module = import_module("services." + module_name)
    failure = RuntimeError("write failed")
    query = AsyncMock(side_effect=failure)
    monkeypatch.setattr(module, dependency, query)
    with pytest.raises(HTTPException) as error:
        asyncio.run(
            getattr(module, method)(
                [
                    {
                        "ProgramName": "Example",
                        "CustomerNum": "001",
                        "EMailAddress": "test@example.com",
                        "PolicyType": "Liability",
                        "RecipCat": "Agent",
                        "DistVia": "Email",
                        "AttnTo": "Test User",
                    }
                ]
            )
        )
    assert error.value.status_code == 500
    assert error.value.detail == {"error": "write failed"}
    assert error.value.__cause__ is failure
    query.assert_awaited_once()
