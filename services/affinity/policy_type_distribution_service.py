from db import conn
import pandas as pd
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

async def get_distribution(query_params: dict):
    """
    Fetch account(s) from tblDIST_PolicyTypeScheduler_AFF.
    If query_params is provided, filters by given key/value.
    Returns a list of dicts (records).
    """

    try:
        base_query = "SELECT * FROM tblDIST_PolicyTypeScheduler_AFF"
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
        logger.warning(f"Error fetching Loss Run Distribution List - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})
    

async def upsert_distribution(data_list):
    """
    Takes an array of maps as data
    Update row if already exists, else insert row into tblDIST_PolicyTypeScheduler_AFF
    """

    try:
        cursor = conn.cursor()

        for data in data_list:
            print(data)
            # Assuming ProgramName + EMailAddress is the primary key / unique identifier
            merge_query = f"""
            MERGE INTO tblDIST_PolicyTypeScheduler_AFF AS target
            USING (SELECT {", ".join(['? AS ' + col for col in data.keys()])}) AS source
            ON target.ProgramName = source.ProgramName AND target.EMailAddress = source.EMailAddress
            WHEN MATCHED THEN
                UPDATE SET {", ".join([f"{col} = source.{col}" for col in data.keys() if col != 'ProgramName'])}
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(data.keys())})
                VALUES ({", ".join(['source.' + col for col in data.keys()])});
            """

            print(merge_query)
            values = list(data.values())
            cursor.execute(merge_query, values)

        conn.commit()
        return {"message": "Transaction successful", "count": len(data_list)}
    except Exception as e:
        conn.rollback()
        logger.warning(f"Insert/Update failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})

async def delete_distribution(data_list):
    """
    Takes an array of maps as data
    Deletes matching rows from tblDIST_PolicyTypeScheduler_AFF
    Matching is based on ProgramName + EMailAddress
    """

    try:
        cursor = conn.cursor()

        for data in data_list:
            if "ProgramName" not in data or "EMailAddress" not in data:
                raise ValueError("Both ProgramName and EMailAddress are required for deletion")

            delete_query = """
                DELETE FROM tblDIST_PolicyTypeScheduler_AFF
                WHERE ProgramName = ? AND EMailAddress = ?
            """
            cursor.execute(delete_query, (data["ProgramName"], data["EMailAddress"]))

        conn.commit()
        return {"message": "Deletion successful", "count": len(data_list)}
    except Exception as e:
        conn.rollback()
        logger.warning(f"Deletion failed - {str(e)}")
        raise HTTPException(status_code=500, detail={"error": str(e)})