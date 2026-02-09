# services/sac/sac_account_service.py

import logging
import re
from typing import Any

from fastapi import HTTPException

from core.date_utils import (
    format_records_dates,
    normalize_payload_dates,
)
from core.db_helpers import (
    fetch_records_async,
    insert_records_async,
    merge_upsert_records_async,
    run_raw_query_async,
    sanitize_filters,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblAcctSpecial"
PRIMARY_KEY = "AcctSpecialKey"
ALLOWED_FILTERS = {
    "AcctSpecialKey",
    "CustomerNum",
    "CustomerName",
    "Stage",
    "IsSubmitted",
    "ServLevel",
    "AcctOwner",
    "BranchName",
}
_DATE_FIELDS = {
    "OnBoardDate",
    "DateCreated",
    "DiscDate",
    "TermDate",
    "DateNotif",
    "RenewLetterDt",
    "NCMStartDt",
    "NCMEndDt",
    "EffectiveDate"
}


async def get_sac_account(query_params: dict[str, Any]):
    try:
        filters = sanitize_filters(query_params, ALLOWED_FILTERS)
        branch_filter = filters.pop("BranchName", None)

        if not branch_filter:
            records = await fetch_records_async(table=TABLE_NAME, filters=filters)
            if not records:
                return []
            return format_records_dates(records, fields=_DATE_FIELDS)

        branch_terms = [term for term in re.split(r"[ ,&]+", str(branch_filter)) if term.strip()]

        # Fall back to simple filtering if nothing usable came from the branch filter.
        if not branch_terms:
            records = await fetch_records_async(table=TABLE_NAME, filters=filters)
            if not records:
                return []
            return format_records_dates(records, fields=_DATE_FIELDS)

        clauses: list[str] = []
        params: list[Any] = []

        for key, value in filters.items():
            clauses.append(f"{key} = ?")
            params.append(value)

        branch_clauses = ["BranchName LIKE ?" for _ in branch_terms]
        clauses.append(f"({' OR '.join(branch_clauses)})")
        params.extend(f"{term}%" for term in branch_terms)

        query = f"SELECT * FROM {TABLE_NAME}"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        records = await run_raw_query_async(query, list(params))
        if not records:
            return []
        return format_records_dates(records, fields=_DATE_FIELDS)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching SAC account List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_sac_account(data: dict[str, Any]):
    try:
        normalized_data = normalize_payload_dates(data, fields=_DATE_FIELDS)
        pk_value = normalized_data.get(PRIMARY_KEY)
        if pk_value in (None, ""):
            sanitized = {k: v for k, v in normalized_data.items() if k != PRIMARY_KEY}
            return await insert_records_async(table=TABLE_NAME, records=[sanitized])

        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=[normalized_data],
            key_columns=[PRIMARY_KEY],
            exclude_key_columns_from_insert=True,
        )
    except Exception as e:
        logger.warning(f"Upsert failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
