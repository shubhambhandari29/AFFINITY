import logging

from fastapi import HTTPException

from core.date_utils import format_records_dates
from core.db_helpers import run_raw_query_async

logger = logging.getLogger(__name__)

SEARCH_QUERIES = {
    "AccountName": """
        SELECT
            tblHcmAccount.CustomerName AS [Customer Name],
            tblHcmAccount.CustomerNum AS [Customer Number],
            tblHcmAccount.OnBoardDate AS [On Board Date],
            tblHcmAccount.AcctStatus AS [Account Status]
        FROM tblHcmAccount
        WHERE tblHcmAccount.Stage = 'Admin' AND tblHcmAccount.IsSubmitted = 1
        GROUP BY
            tblHcmAccount.CustomerName,
            tblHcmAccount.CustomerNum,
            tblHcmAccount.OnBoardDate,
            tblHcmAccount.AcctStatus
        ORDER BY tblHcmAccount.CustomerName;
    """,
    "CustomerNum": """
        SELECT
            tblHcmAccount.CustomerNum AS [Customer Number],
            tblHcmAccount.CustomerName AS [Customer Name],
            tblHcmAccount.OnBoardDate AS [On Board Date],
            tblHcmAccount.AcctStatus AS [Account Status]
        FROM tblHcmAccount
        WHERE tblHcmAccount.Stage = 'Admin' AND tblHcmAccount.IsSubmitted = 1
        GROUP BY
            tblHcmAccount.CustomerNum,
            tblHcmAccount.CustomerName,
            tblHcmAccount.OnBoardDate,
            tblHcmAccount.AcctStatus
        ORDER BY tblHcmAccount.CustomerNum;
    """,
}


async def search_hcm_account_records(search_by: str):
    if search_by not in SEARCH_QUERIES:
        raise HTTPException(status_code=400, detail={"error": "Invalid search type"})

    try:
        records = await run_raw_query_async(SEARCH_QUERIES[search_by])
        return format_records_dates(records)
    except Exception as e:
        logger.warning(f"HCM account search failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
