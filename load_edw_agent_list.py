import argparse
import csv
import re
from typing import Iterable

from db import get_raw_connection


_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _sanitize_value(value: str) -> str:
    cleaned = value.replace("\x00", "")
    cleaned = _CONTROL_CHARS_RE.sub("", cleaned)
    return cleaned.strip()


def _iter_rows(csv_path: str, encoding: str) -> Iterable[tuple[str, str]]:
    with open(csv_path, newline="", encoding=encoding, errors="replace") as handle:
        reader = csv.DictReader(handle)
        fieldnames = {name.strip() for name in (reader.fieldnames or [])}
        required = {"Agent_Code", "Agent_Name"}
        if not required.issubset(fieldnames):
            missing = ", ".join(sorted(required - fieldnames))
            raise ValueError(f"CSV is missing required columns: {missing}")

        for row in reader:
            agent_code = _sanitize_value(row.get("Agent_Code") or "")
            agent_name = _sanitize_value(row.get("Agent_Name") or "")
            if not agent_code and not agent_name:
                continue
            yield agent_code, agent_name


def _format_error(row: tuple[str, str], error: Exception) -> str:
    clean_error = str(error).replace("\n", " ")
    return f"Agent_Code={row[0]!r}, Agent_Name={row[1]!r}, Error={clean_error}"


def load_agents(
    csv_path: str,
    batch_size: int,
    encoding: str,
) -> tuple[int, list[str]]:
    sql = "INSERT INTO tblEDW_AGENT_LIST (Agent_Code, Agent_Name) VALUES (?, ?)"
    total = 0
    batch: list[tuple[str, str]] = []
    errors: list[str] = []

    conn = get_raw_connection()
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        for row in _iter_rows(csv_path, encoding):
            batch.append(row)
            if len(batch) >= batch_size:
                try:
                    cursor.executemany(sql, batch)
                    conn.commit()
                    total += len(batch)
                    batch.clear()
                except Exception:
                    conn.rollback()
                    for item in batch:
                        try:
                            cursor.execute(sql, item)
                            conn.commit()
                            total += 1
                        except Exception as row_exc:
                            conn.rollback()
                            errors.append(_format_error(item, row_exc))
                    batch.clear()

        if batch:
            try:
                cursor.executemany(sql, batch)
                conn.commit()
                total += len(batch)
            except Exception:
                conn.rollback()
                for item in batch:
                    try:
                        cursor.execute(sql, item)
                        conn.commit()
                        total += 1
                    except Exception as row_exc:
                        conn.rollback()
                        errors.append(_format_error(item, row_exc))
            batch.clear()
    finally:
        conn.close()

    return total, errors


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load EDW agent list CSV into tblEDW_AGENT_LIST",
    )
    parser.add_argument(
        "--csv",
        default="EDW_AGENT_LIST.csv",
        help="Path to the CSV file",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows to insert per batch",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="CSV file encoding (default: utf-8-sig)",
    )
    args = parser.parse_args()

    inserted, errors = load_agents(
        args.csv,
        args.batch_size,
        args.encoding,
    )
    print(f"Inserted {inserted} rows into tblEDW_AGENT_LIST")
    if errors:
        print("Rows that failed to insert:")
        for entry in errors:
            print(entry)


if __name__ == "__main__":
    main()
