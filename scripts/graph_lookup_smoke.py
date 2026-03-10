from __future__ import annotations

import argparse
import json
import sys
from urllib.parse import quote

import httpx
from azure.identity import ManagedIdentityCredential

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
USER_SELECT = "id,displayName,userPrincipalName,mail,employeeId"
GROUP_SELECT = "id,displayName"

# Update these to your actual 4 AD group names.
GROUP_ROLE_MAP = {
    "SAC_Underwriter": "underwriter",
    "SAC_Director": "director",
    "SAC_Admin": "admin",
    "SAC_CCIT_User": "ccit_user",
}


def get_graph_access_token() -> str:
    credential = ManagedIdentityCredential()
    token = credential.get_token(GRAPH_SCOPE)
    return token.token


def parse_graph_error(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    error = payload.get("error")
    if isinstance(error, dict) and error.get("message"):
        return str(error["message"])
    body = response.text.strip()
    return body or "Unknown Graph error"


def graph_get(
    client: httpx.Client, url: str, token: str, params: dict[str, str] | None = None
) -> dict:
    response = client.get(
        url,
        params=params,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=30.0,
    )
    if response.status_code >= 400:
        message = parse_graph_error(response)
        raise RuntimeError(f"Graph call failed [{response.status_code}] {url}: {message}")
    return response.json()


def resolve_user(client: httpx.Client, token: str, raw_user_id: str) -> tuple[dict, str]:
    user_id = raw_user_id.strip()
    direct_url = f"{GRAPH_BASE_URL}/users/{quote(user_id, safe='')}"
    direct_response = client.get(
        direct_url,
        params={"$select": USER_SELECT},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=30.0,
    )
    if direct_response.status_code == 200:
        return direct_response.json(), "id_or_upn"
    if direct_response.status_code not in {400, 404}:
        message = parse_graph_error(direct_response)
        raise RuntimeError(
            f"Direct /users/{{id-or-upn}} lookup failed [{direct_response.status_code}]: {message}"
        )

    escaped = user_id.replace("'", "''")
    user_list = graph_get(
        client,
        f"{GRAPH_BASE_URL}/users",
        token,
        params={
            "$filter": f"employeeId eq '{escaped}'",
            "$select": USER_SELECT,
            "$top": "2",
        },
    )
    matches = user_list.get("value", [])
    if len(matches) == 1:
        return matches[0], "employee_id"
    if len(matches) > 1:
        raise RuntimeError("More than one user matched employeeId. Please check directory data.")

    raise RuntimeError("No Entra user found from direct, UPN, or employeeId lookup.")


def get_user_groups(client: httpx.Client, token: str, user_object_id: str) -> list[dict]:
    groups: list[dict] = []
    next_url = (
        f"{GRAPH_BASE_URL}/users/{quote(user_object_id, safe='')}"
        "/transitiveMemberOf/microsoft.graph.group"
    )
    params: dict[str, str] | None = {"$select": GROUP_SELECT}

    while next_url:
        payload = graph_get(client, next_url, token, params=params)
        groups.extend(payload.get("value", []))
        next_url = payload.get("@odata.nextLink")
        params = None

    return groups


def map_roles(group_names: list[str]) -> list[str]:
    roles: list[str] = []
    for name in group_names:
        role = GROUP_ROLE_MAP.get(name)
        if role and role not in roles:
            roles.append(role)
    return roles


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke test for Microsoft Graph group lookup via App Service managed identity."
    )
    parser.add_argument(
        "--user-id",
        required=True,
        help="User identifier to test (UPN/GUID or employeeId like SXB640).",
    )
    args = parser.parse_args()

    try:
        token = get_graph_access_token()
        with httpx.Client() as client:
            user, lookup_mode = resolve_user(client, token, args.user_id)
            user_object_id = str(user.get("id", "")).strip()
            if not user_object_id:
                raise RuntimeError("Resolved user object does not include id.")
            groups = get_user_groups(client, token, user_object_id)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    group_names = sorted(
        {
            str(group.get("displayName", "")).strip()
            for group in groups
            if str(group.get("displayName", "")).strip()
        },
        key=str.casefold,
    )
    output = {
        "input_user_id": args.user_id,
        "lookup_mode": lookup_mode,
        "resolved_user": {
            "id": user.get("id"),
            "displayName": user.get("displayName"),
            "userPrincipalName": user.get("userPrincipalName"),
            "mail": user.get("mail"),
            "employeeId": user.get("employeeId"),
        },
        "group_count": len(groups),
        "group_names": group_names,
        "mapped_roles": map_roles(group_names),
    }
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
