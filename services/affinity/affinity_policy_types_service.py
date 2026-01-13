from db import conn
import pandas as pd
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

async def get_affinity_policy_types(query_params: dict):
    """
    Fetch account(s) from tblAffinityPolicyType.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """
    query_params.update({'PrimaryAgt': 'Yes'})

    try:
        base_query = """SELECT * FROM tblAffinityPolicyType LEFT JOIN tblAffinityAgents 
                        ON tblAffinityPolicyType.ProgramName = tblAffinityAgents.ProgramName"""
        filters = []
        params = []
        for key, value in query_params.items():
            table = "tblAffinityAgents" if key == "PrimaryAgt" else "tblAffinityPolicyType"
            filters.append(f"{table}.{key} = ?")
            params.append(value)
        where_clause = " WHERE " + " AND ".join(filters)
        query = base_query + where_clause

        if filters:
            where_clause = " WHERE " + " AND ".join(filters)
            query = base_query + where_clause
        else:
            query = base_query
        
        print(query)

        df = pd.read_sql(query, conn, params=params)
        df = df.loc[:, ~df.columns.duplicated()]    # remove duplicate PK_Number
        
        df = df.astype(object).where(pd.notna(df), None)   # replacing NaN with null
        result = df.to_dict(orient="records")
        return result
    except Exception as e:
        logger.warning(f"Error fetching Affinity Program List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})
    
    

async def upsert_affinity_policy_types(data):
    """
    Update row if already exists, else insert row into tblAffinityPolicyType
    """

    try:
        cursor = conn.cursor()

        # Assuming `ProgramName` is the primary key / unique identifier
        merge_query = f"""
        MERGE INTO tblAffinityPolicyType AS target
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