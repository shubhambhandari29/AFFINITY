import logging
import re
from typing import Any

from fastapi import HTTPException

from core.date_utils import format_records_dates, normalize_payload_dates
from core.db_helpers import (
    fetch_records_async,
    merge_upsert_records_async,
    run_raw_query_async,
    sanitize_filters,
)
from services.validations.affinity_validations import validate_affinity_program_payload

logger = logging.getLogger(__name__)

TABLE_NAME = "tblAcctAffinityProgram"
PRIMARY_KEY = "AcctAffinityProgramKey"
KEY_COLUMNS = ["ProgramName"]


async def get_affinity_program(query_params: dict[str, Any]):
    """
    Fetch account(s) from tblAcctAffinityProgram.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    try:
        filters = sanitize_filters(query_params)
        branch_filter = filters.pop("BranchVal", None)

        if not branch_filter:
            records = await fetch_records_async(table=TABLE_NAME, filters=filters)
            return format_records_dates(records)

        branch_terms = [term for term in re.split(r"[ ,&]+", str(branch_filter)) if term.strip()]

        if not branch_terms:
            records = await fetch_records_async(table=TABLE_NAME, filters=filters)
            return format_records_dates(records)

        clauses: list[str] = []
        params: list[Any] = []

        for key, value in filters.items():
            clauses.append(f"{key} = ?")
            params.append(value)

        branch_clauses = ["BranchVal LIKE ?" for _ in branch_terms]
        clauses.append(f"({' OR '.join(branch_clauses)})")
        params.extend(f"{term}%" for term in branch_terms)

        query = f"SELECT * FROM {TABLE_NAME}"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        records = await run_raw_query_async(query, list(params))
        return format_records_dates(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching Affinity Program List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_affinity_program(data: dict[str, Any]):
    """
    Update row if already exists, else insert row into tblAcctAffinityProgram.
    """

    try:
        errors = validate_affinity_program_payload(data)
        if errors:
            raise HTTPException(status_code=400, detail={"errors": errors})
        normalized = normalize_payload_dates(data)
        pk_value = normalized.get(PRIMARY_KEY)
        if pk_value not in (None, ""):
            return await merge_upsert_records_async(
                table=TABLE_NAME,
                data_list=[normalized],
                key_columns=[PRIMARY_KEY],
                exclude_key_columns_from_insert=True,
            )

        sanitized = {k: v for k, v in normalized.items() if k != PRIMARY_KEY}
        return await merge_upsert_records_async(
            table=TABLE_NAME,
            data_list=[sanitized],
            key_columns=KEY_COLUMNS,
        )
    except Exception as e:
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
