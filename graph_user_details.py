from __future__ import annotations

import argparse
import json
from typing import Any
from urllib.parse import quote

import requests

from graph import GRAPH_BASE_URL, get_graph_access_token

USER_SELECT_FIELDS = (
    "id,"
    "displayName,"
    "givenName,"
    "surname,"
    "mail,"
    "userPrincipalName,"
    "otherMails,"
    "proxyAddresses,"
    "accountEnabled,"
    "jobTitle,"
    "department,"
    "officeLocation,"
    "mobilePhone,"
    "businessPhones"
)


def get_user_details(user_id: str, access_token: str) -> dict[str, Any]:
    url = f"{GRAPH_BASE_URL}/users/{quote(user_id, safe='')}?$select={USER_SELECT_FIELDS}"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 404:
        raise LookupError(f"User '{user_id}' was not found in Microsoft Graph.")
    response.raise_for_status()
    return response.json()


def extract_email_candidates(user_details: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add_candidate(value: Any) -> None:
        if not isinstance(value, str):
            return
        normalized = value.strip()
        if "@" not in normalized:
            return
        lowered = normalized.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        candidates.append(normalized)

    add_candidate(user_details.get("mail"))
    add_candidate(user_details.get("userPrincipalName"))

    for value in user_details.get("otherMails") or []:
        add_candidate(value)

    primary_proxy_addresses: list[str] = []
    secondary_proxy_addresses: list[str] = []
    for value in user_details.get("proxyAddresses") or []:
        if not isinstance(value, str):
            continue
        if value.startswith("SMTP:"):
            primary_proxy_addresses.append(value[5:])
        elif value.startswith("smtp:"):
            secondary_proxy_addresses.append(value[5:])

    for value in primary_proxy_addresses + secondary_proxy_addresses:
        add_candidate(value)

    return candidates


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Microsoft Graph user details from an Entra object ID, UPN, or email."
    )
    parser.add_argument("--user-id", required=True, help="User ID to look up in Graph")
    args = parser.parse_args()

    try:
        access_token = get_graph_access_token()
        user_details = get_user_details(args.user_id, access_token)
    except LookupError as exc:
        raise SystemExit(str(exc)) from exc
    except requests.HTTPError as exc:
        response = exc.response
        status_code = response.status_code if response is not None else "unknown"
        response_text = response.text if response is not None else str(exc)
        raise SystemExit(f"Graph lookup failed [{status_code}]: {response_text}") from exc
    except Exception as exc:
        raise SystemExit(f"Lookup failed: {exc}") from exc

    email_candidates = extract_email_candidates(user_details)
    payload = {
        "lookup_input": args.user_id,
        "resolved_email": email_candidates[0] if email_candidates else None,
        "email_candidates": email_candidates,
        "user": user_details,
    }

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
