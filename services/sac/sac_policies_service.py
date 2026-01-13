import logging
from typing import Any

from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool

from core.date_utils import format_records_dates, normalize_payload_dates, parse_date_input
from core.db_helpers import (
    _ensure_safe_identifier,
    fetch_records_async,
    insert_records_async,
    merge_upsert_records_async,
    run_raw_query_async,
    sanitize_filters,
)
from core.models.sac_policies import normalize_money_string
from db import db_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "tblPolicies"
PRIMARY_KEY = "PK_Number"
ALLOWED_FILTERS = {"CustomerNum", "PolicyNum", "PolMod","PK_Number"}
PREMIUM_ALLOWED_FILTERS = {"CustomerNum", "PolicyNum", "PolMod", "PolicyStatus"}


async def _lookup_pk_number(record: dict[str, Any]) -> int | None:
    """
    Fetch the latest PK_Number for a policy identified by its natural key fields.
    Used after inserts to return the new identity value.
    """
    try:
        query = """
            SELECT TOP 1 PK_Number
            FROM tblPolicies
            WHERE CustomerNum = ? AND PolicyNum = ? AND PolMod = ?
            ORDER BY PK_Number DESC
        """
        params = [record["CustomerNum"], record["PolicyNum"], record["PolMod"]]
        rows = await run_raw_query_async(query, params)
        return rows[0]["PK_Number"] if rows else None
    except Exception as exc:
        logger.warning("Failed to fetch PK_Number for new policy row: %s", exc)
        return None


async def get_sac_policies(query_params: dict[str, Any]):
    try:
        filters = sanitize_filters(query_params, ALLOWED_FILTERS)
        records = await fetch_records_async(table=TABLE_NAME, filters=filters)
        formatted = format_records_dates(records)
        for record in formatted:
            if "PremiumAmt" in record:
                record["PremiumAmt"] = normalize_money_string(record.get("PremiumAmt"))
        return formatted
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching SAC policies List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def upsert_sac_policies(data: dict[str, Any]):
    try:
        normalized = normalize_payload_dates(data)
        pk_value = normalized.get(PRIMARY_KEY)
        pk_response: int | None = None

        if pk_value in (None, ""):
            sanitized_record = {k: v for k, v in normalized.items() if k != PRIMARY_KEY}
            if sanitized_record:
                await insert_records_async(table=TABLE_NAME, records=[sanitized_record])
                pk_response = await _lookup_pk_number(sanitized_record)
        else:
            existing = await fetch_records_async(table=TABLE_NAME, filters={PRIMARY_KEY: pk_value})
            existing_row = existing[0] if existing else None

            # If incoming mod differs from stored mod, treat this as a "new mod" clone and insert
            existing_mod = None
            if existing_row and existing_row.get("PolMod") is not None:
                existing_mod = str(existing_row.get("PolMod"))
            incoming_mod = None
            if normalized.get("PolMod") is not None:
                incoming_mod = str(normalized.get("PolMod"))

            if existing_row is None:
                logger.info("PK_Number %s not found; inserting new policy row", pk_value)
                sanitized_record = {k: v for k, v in normalized.items() if k != PRIMARY_KEY}
                if sanitized_record:
                    await insert_records_async(table=TABLE_NAME, records=[sanitized_record])
                    pk_response = await _lookup_pk_number(sanitized_record)
            elif incoming_mod is not None and incoming_mod != existing_mod:
                logger.info(
                    "Detected new mod for policy PK_Number %s (old %s -> new %s); inserting clone",
                    pk_value,
                    existing_mod,
                    incoming_mod,
                )
                sanitized_record = {k: v for k, v in normalized.items() if k != PRIMARY_KEY}
                if sanitized_record:
                    await insert_records_async(table=TABLE_NAME, records=[sanitized_record])
                    pk_response = await _lookup_pk_number(sanitized_record)
            else:
                await merge_upsert_records_async(
                    table=TABLE_NAME,
                    data_list=[normalized],
                    key_columns=[PRIMARY_KEY],
                    exclude_key_columns_from_insert=True,
                )
                pk_response = pk_value

        return {"message": "Transaction successful", "count": 1, "pk": pk_response}
    except Exception as e:
        logger.warning(f"Upsert failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def update_field_for_all_policies(data: dict[str, Any]):
    field_name = data.get("fieldName")
    update_via = data.get("updateVia")

    if not field_name or not update_via:
        raise HTTPException(status_code=400, detail={"error": "Field name and update via are required"})
    if "fieldValue" not in data or "updateViaValue" not in data:
        raise HTTPException(status_code=400, detail={"error": "Missing required values"})

    try:
        _ensure_safe_identifier(field_name)
        _ensure_safe_identifier(update_via)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    field_value = data["fieldValue"]
    if isinstance(field_value, str):
        parsed = parse_date_input(field_value)
        field_value = parsed

    query = f"""
        UPDATE {TABLE_NAME}
        SET {field_name} = ?
        WHERE {update_via} = ?
    """

    def _execute_update():
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (field_value, data["updateViaValue"]))
            conn.commit()
        return {"message": "Update successful"}

    try:
        return await run_in_threadpool(_execute_update)
    except Exception as e:
        logger.error(f"Error updating policies field: {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def get_premium(query_params: dict[str, Any]):
    """
    Fetch sum of premium of all active policies from tblPolicies.
    """

    try:
        filters_input = dict(query_params)
        filters_input["PolicyStatus"] = "Active"
        filters = sanitize_filters(filters_input, PREMIUM_ALLOWED_FILTERS)

        clauses = []
        params = []
        for key, value in filters.items():
            clauses.append(f"{key} = ?")
            params.append(value)

        query = "SELECT COALESCE(SUM(PremiumAmt), 0) AS Premium FROM tblPolicies"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        rows = await run_raw_query_async(query, params)
        premium_value = rows[0]["Premium"] if rows else 0
        return premium_value

    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"Error fetching SAC policy premium - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e