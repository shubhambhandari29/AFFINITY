from __future__ import annotations

from pathlib import Path

import pandas as pd

from graph import TARGET_GROUPS, get_graph_access_token, get_user_groups

EXCEL_FILE_NAME = "UW Details.xlsx"
SHEET_NAME = "sheet1"
EMAIL_COLUMN_NAME = "UW Email"


def load_emails_from_excel(file_path: Path) -> list[str]:
    df = pd.read_excel(file_path, sheet_name=SHEET_NAME)
    if EMAIL_COLUMN_NAME not in df.columns:
        raise ValueError(
            f"Column '{EMAIL_COLUMN_NAME}' not found in sheet '{SHEET_NAME}'. "
            f"Available columns: {list(df.columns)}"
        )

    emails: list[str] = []
    for value in df[EMAIL_COLUMN_NAME].dropna():
        email = str(value).strip()
        if email:
            emails.append(email)

    return list(dict.fromkeys(emails))


def main() -> None:
    excel_path = Path(__file__).with_name(EXCEL_FILE_NAME)
    emails = load_emails_from_excel(excel_path)
    access_token = get_graph_access_token()

    matched_users: list[tuple[str, list[str]]] = []
    no_group_users: list[str] = []
    error_users: list[tuple[str, str]] = []

    for email in emails:
        try:
            groups = get_user_groups(email, access_token)
            matched_group_names = sorted(
                {
                    str(group.get("displayName") or "").strip()
                    for group in groups
                    if str(group.get("displayName") or "").strip() in TARGET_GROUPS
                }
            )
            if matched_group_names:
                matched_users.append((email, matched_group_names))
            else:
                no_group_users.append(email)
        except Exception as exc:
            error_users.append((email, str(exc)))

    print("Matched users:")
    if matched_users:
        for email, group_names in matched_users:
            print(f"{email} | {', '.join(group_names)}")
    else:
        print("None")

    print("\nUsers with no matching groups:")
    if no_group_users:
        for email in no_group_users:
            print(email)
    else:
        print("None")

    print("\nUsers with errors:")
    if error_users:
        for email, error in error_users:
            print(f"{email} | {error}")
    else:
        print("None")


if __name__ == "__main__":
    main()
