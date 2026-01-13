from db import conn
import pandas as pd
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

# Map frontend "search by" values to actual SQL queries
SEARCH_QUERIES = {
    "ProgramName": """
        SELECT 
            tblAcctAffinityProgram.ProgramName AS [Program Name], 
            tblAcctAffinityProgram.OnBoardDt AS [On Board Date]
        FROM tblAcctAffinityProgram
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
        ORDER BY tblAcctAffinityProgram.ProgramName;
    """
}

async def search_affinity_porgram_records(search_by: str):
    """
    Executes the appropriate query based on `search_by` key.
    Returns rows as a list of dicts.
    """
    if search_by not in SEARCH_QUERIES:
            raise HTTPException(status_code=500, detail={"error": f"Invalid search type: {search_by}"})
    
    try:
        query = SEARCH_QUERIES[search_by]
        df = pd.read_sql(query, conn)

        # Replace NaN with None for JSON compatibility
        df = df.astype(object).where(pd.notna(df), None)

        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning(f"Error running search for {search_by} - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})
