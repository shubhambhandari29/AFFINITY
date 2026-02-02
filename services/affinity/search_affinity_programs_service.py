import logging

from fastapi import HTTPException

from core.date_utils import format_records_dates
from core.db_helpers import run_raw_query_async

logger = logging.getLogger(__name__)

# Map frontend "search by" values to actual SQL queries
SEARCH_QUERIES = {
    "ProgramName": """
        SELECT
            tblAcctAffinityProgram.ProgramName AS [Program Name],
            tblAcctAffinityProgram.OnBoardDt AS [On Board Date]
        FROM tblAcctAffinityProgram
        WHERE tblAcctAffinityProgram.Stage = 'Admin' AND tblAcctAffinityProgram.IsSubmitted = 1
        ORDER BY tblAcctAffinityProgram.ProgramName;
    """,
    "ProducerCode": """
        SELECT
            tblAffinityAgents.AgentCode AS [Agent Code],
            tblAffinityAgents.AgentName AS [Agent Name],
            tblAcctAffinityProgram.ProgramName AS [Program Name],
            tblAcctAffinityProgram.OnBoardDt AS [On Board Date]
        FROM tblAcctAffinityProgram LEFT JOIN tblAffinityAgents
        ON tblAcctAffinityProgram.ProgramName=tblAffinityAgents.ProgramName
        WHERE tblAffinityAgents.AgentCode IS NOT NULL
        AND tblAcctAffinityProgram.Stage = 'Admin' 
        AND tblAcctAffinityProgram.IsSubmitted = 1
        ORDER BY tblAcctAffinityProgram.ProgramName;
    """,
}


async def search_affinity_program_records(search_by: str):
    """
    Executes the appropriate query based on `search_by` key.
    Returns rows as a list of dicts.
    """
    if search_by not in SEARCH_QUERIES:
        raise HTTPException(status_code=400, detail={"error": "Invalid search type"})

    try:
        query = SEARCH_QUERIES[search_by]
        records = await run_raw_query_async(query)
        return format_records_dates(records)
    except Exception as e:
        logger.warning(f"Error running search for {search_by} - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)}) from e


search_affinity_porgram_records = search_affinity_program_records
