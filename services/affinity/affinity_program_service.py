from db import conn
import pandas as pd
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

async def get_affinity_program(query_params: dict):
    """
    Fetch account(s) from tblAcctAffinityProgram.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    try:
        base_query = "SELECT * FROM tblAcctAffinityProgram"
        filters = []
        params = []
        for key, value in query_params.items():
            filters.append(f"{key} = ?")
            params.append(value)
        where_clause = " WHERE " + " AND ".join(filters)
        query = base_query + where_clause

        if filters:
            where_clause = " WHERE " + " AND ".join(filters)
            query = base_query + where_clause
        else:
            query = base_query

        df = pd.read_sql(query, conn, params=params)
        
        df = df.astype(object).where(pd.notna(df), None)   # replacing NaN with null
        result = df.to_dict(orient="records")
        return result
    except Exception as e:
        logger.warning(f"Error fetching Affinity Program List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})
    
    

async def upsert_affinity_program(data):
    """
    Update row if already exists, else insert row into tblAcctAffinityProgram
    """

    try:
        cursor = conn.cursor()

        # Assuming `ProgramName` is the primary key / unique identifier
        merge_query = f"""
        MERGE INTO tblAcctAffinityProgram AS target
        USING (SELECT {", ".join(['? AS ' + col for col in data.keys()])}) AS source
        ON target.ProgramName = source.ProgramName
        WHEN MATCHED THEN
            UPDATE SET {", ".join([f"{col} = source.{col}" for col in data.keys() if col != 'ProgramName'])}
        WHEN NOT MATCHED THEN
            INSERT ({", ".join(data.keys())})
            VALUES ({", ".join(['source.' + col for col in data.keys()])});
        """

        values = list(data.values())
        cursor.execute(merge_query, values)
        conn.commit()

        return {"message": "Transaction successful"}
    except Exception as e:
        conn.rollback()
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})