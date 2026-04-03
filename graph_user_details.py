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
    "mailNickname,"
    "userPrincipalName,"
    "otherMails,"
    "proxyAddresses,"
    "accountEnabled,"
    "jobTitle,"
    "department,"
    "officeLocation,"
    "mobilePhone,"
    "businessPhones,"
    "onPremisesSamAccountName,"
    "onPremisesUserPrincipalName,"
    "onPremisesSyncEnabled"
)


def escape_odata_string(value: str) -> str:
    return value.replace("'", "''")


def get_headers(access_token: str, *, advanced_query: bool = False) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    if advanced_query:
        headers["ConsistencyLevel"] = "eventual"
    return headers


def get_user_by_direct_path(user_id: str, access_token: str) -> dict[str, Any] | None:
    url = f"{GRAPH_BASE_URL}/users/{quote(user_id, safe='')}?$select={USER_SELECT_FIELDS}"
    response = requests.get(url, headers=get_headers(access_token), timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def build_filter_clauses(user_id: str) -> list[str]:
    escaped = escape_odata_string(user_id)
    clauses = [
        f"id eq '{escaped}'",
        f"userPrincipalName eq '{escaped}'",
        f"mail eq '{escaped}'",
        f"mailNickname eq '{escaped}'",
        f"onPremisesSamAccountName eq '{escaped}'",
        f"onPremisesUserPrincipalName eq '{escaped}'",
        f"otherMails/any(x:x eq '{escaped}')",
    ]

    if "@" in user_id:
        clauses.extend(
            [
                f"proxyAddresses/any(x:x eq 'SMTP:{escaped}')",
                f"proxyAddresses/any(x:x eq 'smtp:{escaped}')",
            ]
        )

    return clauses


def list_users_by_filter(user_id: str, access_token: str) -> list[dict[str, Any]]:
    clauses = build_filter_clauses(user_id)
    url = (
        f"{GRAPH_BASE_URL}/users"
        f"?$filter={' or '.join(clauses)}"
        f"&$select={USER_SELECT_FIELDS}"
        "&$count=true"
        "&$top=10"
    )

    response = requests.get(url, headers=get_headers(access_token, advanced_query=True), timeout=30)
    response.raise_for_status()
    payload = response.json()
    return payload.get("value", [])


def normalize_email(value: str) -> str:
    return value.strip().lower()


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


def score_user_match(user_id: str, user_details: dict[str, Any]) -> tuple[int, str]:
    normalized_user_id = normalize_email(user_id) if "@" in user_id else user_id.strip().lower()

    match_checks: list[tuple[int, str, Any]] = [
        (1, "id", user_details.get("id")),
        (2, "userPrincipalName", user_details.get("userPrincipalName")),
        (3, "mail", user_details.get("mail")),
        (4, "mailNickname", user_details.get("mailNickname")),
        (5, "onPremisesSamAccountName", user_details.get("onPremisesSamAccountName")),
        (
            6,
            "onPremisesUserPrincipalName",
            user_details.get("onPremisesUserPrincipalName"),
        ),
    ]

    for score, field_name, field_value in match_checks:
        if isinstance(field_value, str) and field_value.strip().lower() == normalized_user_id:
            return score, field_name

    for field_name in ("otherMails",):
        for value in user_details.get(field_name) or []:
            if isinstance(value, str) and value.strip().lower() == normalized_user_id:
                return 7, field_name

    for value in user_details.get("proxyAddresses") or []:
        if not isinstance(value, str):
            continue
        if value.startswith(("SMTP:", "smtp:")) and value[5:].strip().lower() == normalized_user_id:
            return 8, "proxyAddresses"

    return 999, "unknown"


def resolve_user(user_id: str, access_token: str) -> tuple[dict[str, Any], str, str]:
    direct_match = get_user_by_direct_path(user_id, access_token)
    if direct_match is not None:
        return direct_match, "direct_path", "id_or_userPrincipalName"

    filtered_matches = list_users_by_filter(user_id, access_token)
    if not filtered_matches:
        raise LookupError(
            f"User '{user_id}' was not found in Microsoft Graph. "
            "Tried direct /users/{id} lookup plus filtered search on "
            "mail, userPrincipalName, mailNickname, onPremisesSamAccountName, "
            "onPremisesUserPrincipalName, otherMails, and proxyAddresses."
        )

    ranked_matches = sorted(
        (score_user_match(user_id, item), item) for item in filtered_matches
    )
    best_score, best_match_field = ranked_matches[0][0]
    best_match = ranked_matches[0][1]

    if best_score == 999:
        best_match_field = "filtered_search"

    return best_match, "filtered_search", best_match_field


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Microsoft Graph user details from an Entra object ID, "
            "UPN, email, alias, or synced account name."
        )
    )
    parser.add_argument("--user-id", required=True, help="User identifier to look up in Graph")
    args = parser.parse_args()

    try:
        access_token = get_graph_access_token()
        user_details, lookup_strategy, matched_by = resolve_user(args.user_id, access_token)
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
        "lookup_strategy": lookup_strategy,
        "matched_by": matched_by,
        "resolved_email": email_candidates[0] if email_candidates else None,
        "email_candidates": email_candidates,
        "user": user_details,
    }

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
