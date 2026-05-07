from pathlib import Path

import pandas as pd
import pyodbc


FILE_NAME = "EDW_AGENT_LIST.csv"
SHEET_NAME = "EDW_AGENT_LIST"

CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=clms-preprd-sqlmanagedinstance.3b98dc354c37.database.windows.net;"
    "Database=CLMAA_SpecialAccounts;"
    "Authentication=ActiveDirectoryIntegrated;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)


def load_rows(file_path: Path) -> list[tuple[str, str]]:
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, dtype={"Agent_Code": str, "Agent_Name": str})
    else:
        df = pd.read_excel(
            file_path,
            sheet_name=SHEET_NAME,
            dtype={"Agent_Code": str, "Agent_Name": str},
        )

    df = df[["Agent_Code", "Agent_Name"]].fillna("")
    df["Agent_Code"] = df["Agent_Code"].astype(str).str.strip()
    df["Agent_Name"] = df["Agent_Name"].astype(str).str.strip()
    df = df[(df["Agent_Code"] != "") & (df["Agent_Name"] != "")]

    return list(df.itertuples(index=False, name=None))


def main() -> None:
    file_path = Path(__file__).with_name(FILE_NAME)
    rows = load_rows(file_path)

    conn = pyodbc.connect(CONN_STR)
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM [dbo].[tblEDW_AGENT_LIST];")
        cursor.execute("DBCC CHECKIDENT ('tblEDW_AGENT_LIST', RESEED, 0);")
        cursor.executemany(
            "INSERT INTO [dbo].[tblEDW_AGENT_LIST] ([Agent_Code], [Agent_name]) VALUES (?, ?);",
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    print(f"Inserted {len(rows)} rows into tblEDW_AGENT_LIST.")


if __name__ == "__main__":
    main()
