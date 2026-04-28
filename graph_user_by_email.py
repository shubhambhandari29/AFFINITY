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
    "userType,"
    "otherMails,"
    "proxyAddresses,"
    "accountEnabled,"
    "jobTitle,"
    "department,"
    "companyName,"
    "officeLocation,"
    "mobilePhone,"
    "businessPhones,"
    "employeeId,"
    "employeeType,"
    "city,"
    "state,"
    "country,"
    "postalCode,"
    "streetAddress,"
    "usageLocation,"
    "preferredLanguage,"
    "createdDateTime,"
    "lastPasswordChangeDateTime,"
    "onPremisesSamAccountName,"
    "onPremisesUserPrincipalName,"
    "onPremisesSyncEnabled,"
    "onPremisesDomainName,"
    "onPremisesDistinguishedName,"
    "onPremisesSecurityIdentifier,"
    "onPremisesLastSyncDateTime,"
    "onPremisesImmutableId,"
    "employeeOrgData,"
    "identities,"
    "imAddresses,"
    "faxNumber,"
    "ageGroup,"
    "consentProvidedForMinor,"
    "legalAgeGroupClassification,"
    "creationType,"
    "externalUserState,"
    "externalUserStateChangeDateTime,"
    "signInSessionsValidFromDateTime,"
    "showInAddressList"
)


def escape_odata_string(value: str) -> str:
    return value.replace("'", "''")


def get_headers(access_token: str, *, advanced_query: bool = False) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    if advanced_query:
        headers["ConsistencyLevel"] = "eventual"
    return headers


def build_email_filter(email: str) -> str:
    escaped = escape_odata_string(email.strip())
    return " or ".join(
        [
            f"mail eq '{escaped}'",
            f"userPrincipalName eq '{escaped}'",
            f"otherMails/any(x:x eq '{escaped}')",
            f"proxyAddresses/any(x:x eq 'SMTP:{escaped}')",
            f"proxyAddresses/any(x:x eq 'smtp:{escaped}')",
        ]
    )


def score_match(email: str, user: dict[str, Any]) -> tuple[int, str]:
    normalized_email = email.strip().lower()

    checks: list[tuple[int, str, Any]] = [
        (1, "mail", user.get("mail")),
        (2, "userPrincipalName", user.get("userPrincipalName")),
    ]

    for score, field_name, value in checks:
        if isinstance(value, str) and value.strip().lower() == normalized_email:
            return score, field_name

    for value in user.get("otherMails") or []:
        if isinstance(value, str) and value.strip().lower() == normalized_email:
            return 3, "otherMails"

    for value in user.get("proxyAddresses") or []:
        if isinstance(value, str) and value.lower() == f"smtp:{normalized_email}":
            return 4, "proxyAddresses"

    return 999, "unknown"


def find_user_by_email(email: str, access_token: str) -> tuple[str, str]:
    direct_url = f"{GRAPH_BASE_URL}/users/{quote(email.strip(), safe='')}?$select=id"
    direct_response = requests.get(direct_url, headers=get_headers(access_token), timeout=30)
    if direct_response.status_code == 200:
        direct_payload = direct_response.json()
        return str(direct_payload["id"]), "userPrincipalName"
    if direct_response.status_code not in (400, 404):
        direct_response.raise_for_status()

    filter_query = build_email_filter(email)
    url = (
        f"{GRAPH_BASE_URL}/users"
        f"?$filter={filter_query}"
        "&$select=id,mail,userPrincipalName,otherMails,proxyAddresses"
        "&$count=true"
        "&$top=10"
    )
    response = requests.get(url, headers=get_headers(access_token, advanced_query=True), timeout=30)
    response.raise_for_status()

    matches = response.json().get("value", [])
    if not matches:
        raise LookupError(f"User not found for email '{email}'.")

    ranked_matches = sorted(matches, key=lambda match: score_match(email, match))
    best_match = ranked_matches[0]
    best_score, matched_by = score_match(email, best_match)

    if best_score == 999:
        matched_by = "filtered_search"

    return str(best_match["id"]), matched_by


def get_user_details(user_id: str, access_token: str) -> dict[str, Any]:
    url = f"{GRAPH_BASE_URL}/users/{quote(user_id, safe='')}?$select={USER_SELECT_FIELDS}"
    response = requests.get(url, headers=get_headers(access_token), timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Microsoft Graph user details from an email address."
    )
    parser.add_argument("--email", required=True, help="User email address")
    args = parser.parse_args()

    try:
        access_token = get_graph_access_token()
        user_id, matched_by = find_user_by_email(args.email, access_token)
        user_details = get_user_details(user_id, access_token)
    except LookupError as exc:
        raise SystemExit(str(exc)) from exc
    except requests.HTTPError as exc:
        response = exc.response
        status_code = response.status_code if response is not None else "unknown"
        response_text = response.text if response is not None else str(exc)
        raise SystemExit(f"Graph lookup failed [{status_code}]: {response_text}") from exc
    except Exception as exc:
        raise SystemExit(f"Lookup failed: {exc}") from exc

    payload = {
        "lookup_input": args.email,
        "matched_by": matched_by,
        "id": user_details.get("id"),
        "displayName": user_details.get("displayName"),
        "mail": user_details.get("mail"),
        "userPrincipalName": user_details.get("userPrincipalName"),
        "user": user_details,
    }

    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
