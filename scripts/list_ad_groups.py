#!/usr/bin/env python3
"""List all Microsoft Entra ID groups for a user by email address."""

import argparse
import json
import os
import sys
from typing import Any
from urllib.parse import quote

import requests
from azure.core.exceptions import AzureError
from azure.identity import DefaultAzureCredential

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
REQUEST_TIMEOUT_SECONDS = 30


class GraphAPIError(RuntimeError):
    """Raised when Microsoft Graph returns an unsuccessful response."""


def _error_detail(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or response.reason

    error = payload.get("error", {})
    return error.get("message") or response.text.strip() or response.reason


def _get_json(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
) -> dict[str, Any]:
    response = session.get(
        url,
        headers=headers,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if not response.ok:
        raise GraphAPIError(
            f"Microsoft Graph returned HTTP {response.status_code}: {_error_detail(response)}"
        )
    return response.json()


def find_user_id(
    session: requests.Session,
    headers: dict[str, str],
    email: str,
) -> str:
    escaped_email = email.replace("'", "''")
    payload = _get_json(
        session,
        f"{GRAPH_BASE_URL}/users",
        headers,
        params={
            "$filter": (
                f"userPrincipalName eq '{escaped_email}' or mail eq '{escaped_email}'"
            ),
            "$select": "id,userPrincipalName,mail",
            "$top": "2",
        },
    )
    users = payload.get("value", [])
    if not users:
        raise GraphAPIError(f"No Microsoft Entra ID user found for {email!r}")
    if len(users) > 1:
        raise GraphAPIError(f"More than one Microsoft Entra ID user matched {email!r}")
    return users[0]["id"]


def list_transitive_groups(
    session: requests.Session,
    headers: dict[str, str],
    user_id: str,
) -> list[dict[str, Any]]:
    url = (
        f"{GRAPH_BASE_URL}/users/{quote(user_id, safe='')}"
        "/transitiveMemberOf/microsoft.graph.group"
    )
    params: dict[str, str] | None = {
        "$select": "id,displayName,mail,securityEnabled,groupTypes",
        "$top": "999",
        "$count": "true",
    }
    groups: dict[str, dict[str, Any]] = {}

    while url:
        payload = _get_json(session, url, headers, params)
        for group in payload.get("value", []):
            group_id = group.get("id")
            if group_id:
                groups[group_id] = group

        next_url = payload.get("@odata.nextLink")
        if next_url and not next_url.startswith(f"{GRAPH_BASE_URL}/"):
            raise GraphAPIError("Microsoft Graph returned an unexpected pagination URL")
        url = next_url
        params = None

    return sorted(
        groups.values(),
        key=lambda group: ((group.get("displayName") or "").casefold(), group["id"]),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="List all direct and nested Microsoft Entra ID groups for a user."
    )
    parser.add_argument("email", help="User email address or user principal name")
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print full group details as JSON instead of one group name per line",
    )
    parser.add_argument(
        "--managed-identity-client-id",
        default=os.getenv("AZURE_CLIENT_ID"),
        help=(
            "Client ID of a user-assigned managed identity. Defaults to AZURE_CLIENT_ID; "
            "omit for a system-assigned identity."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    credential = DefaultAzureCredential(
        managed_identity_client_id=args.managed_identity_client_id,
        exclude_interactive_browser_credential=True,
    )

    try:
        token = credential.get_token(GRAPH_SCOPE)
        headers = {
            "Authorization": f"Bearer {token.token}",
            "Accept": "application/json",
            "ConsistencyLevel": "eventual",
        }
        with requests.Session() as session:
            user_id = find_user_id(session, headers, args.email.strip())
            groups = list_transitive_groups(session, headers, user_id)
    except (AzureError, GraphAPIError, requests.RequestException, KeyError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        credential.close()

    if args.as_json:
        print(json.dumps(groups, indent=2))
    else:
        for group in groups:
            print(group.get("displayName") or group["id"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
