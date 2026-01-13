import logging
import time
from copy import deepcopy
from typing import Any

from fastapi import HTTPException

from core.db_helpers import run_raw_query_async

logger = logging.getLogger(__name__)

DropdownQuery = str | tuple[str, list[Any]]

DROPDOWN_CACHE_TTL_SECONDS = 300
_DROPDOWN_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}

_DROPDOWN_QUERIES: dict[str, DropdownQuery] = {
    "SAC_Contact1": """
        SELECT SACName
        FROM tblMGTUsers
        ORDER BY SACName
    """,
    "SAC_Contact2": """
        SELECT SACName, EmpTitle, TelNum, EMailID
        FROM tblMGTUsers
        ORDER BY SACName
    """,
    "AcctOwner": """
        SELECT SACName, EMailID, EmpTitle, TelNum, TelExt, LANID
        FROM tblMGTUsers
        ORDER BY SACName
    """,
    "CreatedBy": """
        SELECT SACName
        FROM tblMGTUsers
        ORDER BY SACName
    """,
    "LossCtlRep1": """
        SELECT RepName, LCEmail, LCTel, LAN_ID
        FROM tblLossCtrl
        ORDER BY RepName
    """,
    "LossCtlRep2": (
        """
        SELECT RepName, Active, LCEmail
        FROM tblLossCtrl
        WHERE Active = ?
        ORDER BY RepName
        """,
        ["Yes"],
    ),
    "RiskSolMgr": """
        SELECT RepName, LCEmail, LAN_ID
        FROM tblLossCtrl
        ORDER BY RepName
    """,
    "BranchName": """
        SELECT BranchName, ReportingBranch
        FROM tblBranch
        ORDER BY BranchName
    """,
    "ServLevel": """
        SELECT [service Level], [Dollar Threshold]
        FROM tblServiceLevel
        ORDER BY SortNum
    """,
    "Underwriters": """
        SELECT [UW Last], [UW Email]
        FROM tblUnderwriters
        ORDER BY [UW Last]
    """,
    "EDW_AGENT_LIST": """
        SELECT Agent_Code, Agent_Name
        FROM tblEDW_AGENT_LIST
        ORDER BY Agent_Code
    """,
}


def _get_cached_dropdown(cache_key: str) -> list[dict[str, Any]] | None:
    entry = _DROPDOWN_CACHE.get(cache_key)
    if not entry:
        return None

    expires_at, payload = entry
    if time.monotonic() >= expires_at:
        _DROPDOWN_CACHE.pop(cache_key, None)
        return None

    return deepcopy(payload)


def _set_cached_dropdown(cache_key: str, payload: list[dict[str, Any]]) -> None:
    expires_at = time.monotonic() + DROPDOWN_CACHE_TTL_SECONDS
    _DROPDOWN_CACHE[cache_key] = (expires_at, deepcopy(payload))


async def get_dropdown_values(name: str) -> list[dict[str, Any]]:
    normalized_name = name.strip()

    if not normalized_name:
        raise HTTPException(status_code=400, detail={"error": "Dropdown type is required"})

    cache_key = normalized_name
    if normalized_name.lower() == "all":
        cache_key = "all"

    cached = _get_cached_dropdown(cache_key)
    if cached is not None:
        return cached

    if normalized_name.lower() == "all":
        result = await get_all_dropdowns()
        _set_cached_dropdown(cache_key, result)
        return result

    query_def = _DROPDOWN_QUERIES.get(normalized_name)

    if query_def:
        query, params = _normalize_query_definition(query_def)
        try:
            result = await run_raw_query_async(query, params)
            _set_cached_dropdown(cache_key, result)
            return result
        except Exception as exc:
            logger.warning(f"Error fetching dropdown '{name}': {exc}")
            raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc

    result = await _fetch_dynamic_dropdown(normalized_name)
    _set_cached_dropdown(cache_key, result)
    return result


def _normalize_query_definition(query_def: DropdownQuery) -> tuple[str, list[Any]]:
    if isinstance(query_def, str):
        return query_def, []

    query, params = query_def
    return query, list(params)


async def _fetch_dynamic_dropdown(dd_type: str) -> list[dict[str, Any]]:
    query = """
        SELECT DD_Value, DD_SortOrder
        FROM tblDropDowns
        WHERE DD_Type = ?
        ORDER BY COALESCE(DD_SortOrder, 0), DD_Value
    """
    try:
        return await run_raw_query_async(query, [dd_type])
    except Exception as exc:
        logger.warning(f"Error fetching dynamic dropdown '{dd_type}': {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc


async def get_all_dropdowns() -> list[dict[str, Any]]:
    query = """
        SELECT DD_Type, DD_Value, DD_SortOrder
        FROM tblDropDowns
        ORDER BY DD_Type, COALESCE(DD_SortOrder, 0), DD_Value
    """
    try:
        return await run_raw_query_async(query, [])
    except Exception as exc:
        logger.warning(f"Error fetching all dropdowns: {exc}")
        raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc