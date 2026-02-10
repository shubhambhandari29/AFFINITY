import logging
from typing import Any

from fastapi import HTTPException

from core.db_helpers import (
    delete_records_async,
    fetch_records_async,
    insert_records_async,
    run_raw_query_async,
    sanitize_filters,
)

logger = logging.getLogger(__name__)

TABLE_NAME = "tblSACAccountAssociations"
ALLOWED_FILTERS = {"ParentAccount"}


def _normalize_children(children: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for child in children:
        if child is None:
            continue
        child_value = str(child).strip()
        if not child_value:
            continue
        if child_value in seen:
            continue
        seen.add(child_value)
        normalized.append(child_value)
    return normalized


async def add_associations(payload: dict[str, Any]):
    try:
        parent_account = payload.get("parent_account")
        parent_account = str(parent_account).strip() if parent_account is not None else ""
        if not parent_account:
            raise HTTPException(
                status_code=400, detail={"error": "parent_account is required"}
            )

        child_accounts = payload.get("child_account")
        if not isinstance(child_accounts, list):
            raise HTTPException(
                status_code=400, detail={"error": "child_account must be a list"}
            )

        normalized_children = [
            child
            for child in _normalize_children(child_accounts)
            if child != parent_account
        ]
        if not normalized_children:
            return {"message": "No new associations to add", "count": 0}

        parents_to_check = [parent_account, *normalized_children]
        existing_pairs: set[tuple[str, str]] = set()
        for parent in parents_to_check:
            existing = await fetch_records_async(
                table=TABLE_NAME, filters={"ParentAccount": parent}
            )
            for row in existing:
                associated = row.get("AssociatedAccount")
                associated = str(associated).strip() if associated is not None else ""
                if associated:
                    existing_pairs.add((parent, associated))

        pairs: list[tuple[str, str]] = []
        for child in normalized_children:
            pairs.append((parent_account, child))
            pairs.append((child, parent_account))

        to_insert = [
            {"ParentAccount": parent, "AssociatedAccount": child}
            for parent, child in pairs
            if (parent, child) not in existing_pairs
        ]

        if not to_insert:
            return {"message": "No new associations to add", "count": 0}

        return await insert_records_async(table=TABLE_NAME, records=to_insert)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"SAC account associations add failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def delete_associations(payload: dict[str, Any]):
    try:
        parent_account = payload.get("parent_account")
        parent_account = str(parent_account).strip() if parent_account is not None else ""
        if not parent_account:
            raise HTTPException(
                status_code=400, detail={"error": "parent_account is required"}
            )

        child_accounts = payload.get("child_account")
        if not isinstance(child_accounts, list):
            raise HTTPException(
                status_code=400, detail={"error": "child_account must be a list"}
            )

        normalized_children = [
            child
            for child in _normalize_children(child_accounts)
            if child != parent_account
        ]
        if not normalized_children:
            return {"message": "No data provided for deletion", "count": 0}

        pairs: list[tuple[str, str]] = []
        for child in normalized_children:
            pairs.append((parent_account, child))
            pairs.append((child, parent_account))

        data_list = [
            {"ParentAccount": parent, "AssociatedAccount": child} for parent, child in pairs
        ]
        return await delete_records_async(
            table=TABLE_NAME,
            data_list=data_list,
            key_columns=["ParentAccount", "AssociatedAccount"],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"SAC account associations delete failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


async def get_associations(query_params: dict[str, Any]):
    try:
        filters = sanitize_filters(query_params, ALLOWED_FILTERS)
        parent_account = filters.get("ParentAccount")
        if not parent_account:
            raise HTTPException(status_code=400, detail={"error": "ParentAccount is required"})

        query = """
            SELECT
                assoc.ParentAccount,
                parent.CustomerName AS ParentCustomerName,
                parent.AcctStatus AS ParentAcctStatus,
                assoc.AssociatedAccount,
                child.CustomerName AS AssociatedCustomerName,
                child.AcctStatus AS AssociatedAcctStatus
            FROM tblSACAccountAssociations AS assoc
            LEFT JOIN tblAcctSpecial AS parent
                ON assoc.ParentAccount = parent.CustomerNum
            LEFT JOIN tblAcctSpecial AS child
                ON assoc.AssociatedAccount = child.CustomerNum
            WHERE assoc.ParentAccount = ?
            ORDER BY assoc.AssociatedAccount
        """
        return await run_raw_query_async(query, [parent_account])
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc
    except Exception as e:
        logger.warning(f"SAC account associations fetch failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
