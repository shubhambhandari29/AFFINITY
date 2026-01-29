import logging

from fastapi import HTTPException

from core.db_helpers import run_raw_query_async

logger = logging.getLogger(__name__)


async def get_cct_policy_statuses(customer_num: str):
    if not customer_num:
        raise HTTPException(status_code=400, detail={"error": "customer_num is required"})

    query = """
        SELECT tblPolicies.PolicyStatus
        FROM tblPolicies
        GROUP BY tblPolicies.PolicyStatus, tblPolicies.CustomerNum
        HAVING tblPolicies.CustomerNum = ?
        ORDER BY tblPolicies.PolicyStatus;
    """

    try:
        return await run_raw_query_async(query, [customer_num])
    except Exception as exc:
        logger.warning(f"Error fetching policy statuses for {customer_num} - {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc


async def get_cct_policy_numbers(customer_num: str):
    if not customer_num:
        raise HTTPException(status_code=400, detail={"error": "customer_num is required"})

    query = """
        SELECT tblPolicies.PolicyNum
        FROM tblPolicies
        GROUP BY tblPolicies.PolicyNum, tblPolicies.CustomerNum
        HAVING tblPolicies.CustomerNum = ?
        ORDER BY tblPolicies.PolicyNum;
    """

    try:
        return await run_raw_query_async(query, [customer_num])
    except Exception as exc:
        logger.warning(f"Error fetching policy numbers for {customer_num} - {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc
