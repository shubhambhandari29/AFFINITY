import logging

from fastapi import HTTPException

from core.date_utils import format_records_dates
from core.db_helpers import run_raw_query_async

logger = logging.getLogger(__name__)

# Map frontend "search by" values to actual SQL queries
SEARCH_QUERIES = {
    "AccountName": """
        SELECT
            tblAcctSpecial.CustomerName AS [Customer Name],
            tblAcctSpecial.CustomerNum AS [Customer Number],
            tblAcctSpecial.OnBoardDate AS [On Board Date],
            tblAcctSpecial.ServLevel AS [Service Level]
        FROM tblAcctSpecial
        WHERE tblAcctSpecial.Stage = 'Admin' AND tblAcctSpecial.IsSubmitted = 1
        GROUP BY
            tblAcctSpecial.CustomerName,
            tblAcctSpecial.CustomerNum,
            tblAcctSpecial.OnBoardDate,
            tblAcctSpecial.ServLevel
        ORDER BY tblAcctSpecial.CustomerName;
    """,
    "CustomerNum": """
        SELECT
            tblAcctSpecial.CustomerNum AS [Customer Number],
            tblAcctSpecial.CustomerName AS [Customer Name],
            tblAcctSpecial.OnBoardDate AS [On Board Date],
            tblAcctSpecial.ServLevel AS [Service Level]
        FROM tblAcctSpecial
        WHERE tblAcctSpecial.Stage = 'Admin' AND tblAcctSpecial.IsSubmitted = 1
        GROUP BY
            tblAcctSpecial.CustomerNum,
            tblAcctSpecial.CustomerName,
            tblAcctSpecial.OnBoardDate,
            tblAcctSpecial.ServLevel
        ORDER BY tblAcctSpecial.CustomerNum;
    """,
    "PolicyNum": """
        SELECT
            tblPOLICIES.PolicyNum AS [Policy Number],
            tblAcctSpecial.CustomerNum AS [Customer Number],
            tblAcctSpecial.CustomerName AS [Customer Name],
            tblAcctSpecial.OnBoardDate AS [On Board Date],
            tblAcctSpecial.ServLevel AS [Service Level]
        FROM tblPOLICIES
        INNER JOIN tblAcctSpecial
            ON tblPOLICIES.CustomerNum = tblAcctSpecial.CustomerNum
        WHERE tblAcctSpecial.Stage = 'Admin' AND tblAcctSpecial.IsSubmitted = 1
        GROUP BY
            tblPOLICIES.PolicyNum,
            tblAcctSpecial.CustomerNum,
            tblAcctSpecial.CustomerName,
            tblAcctSpecial.OnBoardDate,
            tblAcctSpecial.ServLevel
        HAVING tblPOLICIES.PolicyNum IS NOT NULL
        ORDER BY tblPOLICIES.PolicyNum;
    """,
    "ProducerCode": """
        SELECT
            tblPolicies.AgentCode AS [Producer Code],
            tblPolicies.AgentName AS Producer,
            tblPolicies.AccountName AS [Customer Name],
            tblPolicies.InceptDate AS [Inception Date],
            tblPolicies.CustomerNum AS [Customer Number]
        FROM tblPolicies
        GROUP BY
            tblPolicies.AgentCode,
            tblPolicies.AgentName,
            tblPolicies.AccountName,
            tblPolicies.InceptDate,
            tblPolicies.CustomerNum
        HAVING tblPolicies.AgentCode IS NOT NULL
        ORDER BY tblPolicies.AgentCode;
    """,
    "PolicyNameInsured": """
        SELECT
            tblPolicies.AcctOnPolicyName AS [Name Insured on Policy],
            tblAcctSpecial.CustomerName AS [Customer Name],
            tblAcctSpecial.AcctOwner AS [Account Owner],
            tblPolicies.CustomerNum AS [Customer Number]
        FROM tblPolicies
        INNER JOIN tblAcctSpecial
            ON tblPolicies.CustomerNum = tblAcctSpecial.CustomerNum
        WHERE tblAcctSpecial.Stage = 'Admin' AND tblAcctSpecial.IsSubmitted = 1
        GROUP BY
            tblPolicies.AcctOnPolicyName,
            tblAcctSpecial.CustomerName,
            tblAcctSpecial.AcctOwner,
            tblPolicies.CustomerNum
        HAVING tblPolicies.AcctOnPolicyName IS NOT NULL
        ORDER BY tblPolicies.AcctOnPolicyName;
    """,
    "AffiliateName": """
    SELECT tblAffiliates.AffiliateName AS [Affiliate Name],
      tblAcctSpecial.CustomerName AS [Customer Name],
        tblAcctSpecial.CustomerNum AS [Cust Number],
          tblAcctSpecial.OnBoardDate,
            tblAcctSpecial.ServLevel AS [Service Level]
              FROM
                tblAffiliates INNER JOIN tblAcctSpecial
                  ON tblAffiliates.CustomerNum=tblAcctSpecial.CustomerNum
                    GROUP BY 
                    tblAffiliates.AffiliateName, tblAcctSpecial.CustomerName, tblAcctSpecial.CustomerNum, tblAcctSpecial.OnBoardDate, tblAcctSpecial.ServLevel
                      ORDER BY tblAffiliates.AffiliateName;""",
}


async def search_sac_account_records(search_by: str):
    if search_by not in SEARCH_QUERIES:
        raise HTTPException(status_code=400, detail={"error": "Invalid search type"})

    try:
        query = SEARCH_QUERIES[search_by]
        records = await run_raw_query_async(query)
        return format_records_dates(records)
    except Exception as e:
        logger.warning(f"Search failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e
