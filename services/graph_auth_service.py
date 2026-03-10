from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from fastapi import HTTPException

from core.config import settings

logger = logging.getLogger(__name__)

GRAPH_USER_SELECT = "id,displayName,userPrincipalName,mail,employeeId"
GRAPH_GROUP_SELECT = "id,displayName"


@dataclass(slots=True)
class GraphCallError(Exception):
    status_code: int
    message: str


def _require_httpx():
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover - defensive for misconfigured env
        raise HTTPException(
            status_code=500,
            detail={"error": "Missing dependency: httpx"},
        ) from exc
    return httpx


def _get_graph_access_token() -> str:
    try:
        from azure.identity import ManagedIdentityCredential
    except ImportError as exc:  # pragma: no cover - defensive for misconfigured env
        raise HTTPException(
            status_code=500,
            detail={"error": "Missing dependency: azure-identity"},
        ) from exc

    try:
        credential = ManagedIdentityCredential()
        token = credential.get_token(settings.GRAPH_SCOPE)
        return token.token
    except Exception as exc:
        logger.error("Failed to acquire managed identity token for Graph: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "Unable to acquire Graph token from managed identity"},
        ) from exc


def _normalize_group(group: dict[str, Any]) -> dict[str, str]:
    return {
        "id": str(group.get("id", "")).strip(),
        "display_name": str(group.get("displayName", "")).strip(),
    }


def _dedupe_groups(groups: list[dict[str, Any]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for group in groups:
        normalized = _normalize_group(group)
        key = normalized["id"] or normalized["display_name"].casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _extract_graph_error(response) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    error = payload.get("error")
    if isinstance(error, dict):
        message = str(error.get("message", "")).strip()
        if message:
            return message
    text = response.text.strip()
    return text[:400] if text else "Unknown Graph error"


async def _graph_get(client, url: str, headers: dict[str, str], params: dict[str, str] | None = None):
    response = await client.get(url, headers=headers, params=params)
    if response.status_code < 400:
        return response
    raise GraphCallError(
        status_code=response.status_code,
        message=_extract_graph_error(response),
    )


def _escape_odata_string(value: str) -> str:
    return value.replace("'", "''")


async def _query_single_user(
    client,
    headers: dict[str, str],
    filter_expression: str,
    lookup_method: str,
) -> tuple[dict[str, Any], str] | None:
    users_url = f"{settings.GRAPH_BASE_URL}/users"
    response = await _graph_get(
        client,
        users_url,
        headers,
        params={
            "$filter": filter_expression,
            "$select": GRAPH_USER_SELECT,
            "$top": "2",
        },
    )

    users = response.json().get("value", [])
    if not users:
        return None
    if len(users) > 1:
        raise GraphCallError(
            status_code=409,
            message=f"Multiple users found via {lookup_method} lookup",
        )
    return users[0], lookup_method


async def _resolve_user(client, headers: dict[str, str], user_identifier: str) -> tuple[dict[str, Any], str]:
    direct_url = f"{settings.GRAPH_BASE_URL}/users/{quote(user_identifier, safe='')}"
    try:
        direct_response = await _graph_get(
            client,
            direct_url,
            headers,
            params={"$select": GRAPH_USER_SELECT},
        )
        return direct_response.json(), "id_or_upn"
    except GraphCallError as exc:
        if exc.status_code not in {400, 404}:
            raise

    escaped_identifier = _escape_odata_string(user_identifier)

    employee_lookup = await _query_single_user(
        client,
        headers,
        filter_expression=f"employeeId eq '{escaped_identifier}'",
        lookup_method="employee_id",
    )
    if employee_lookup:
        return employee_lookup

    if "@" in user_identifier:
        mail_lookup = await _query_single_user(
            client,
            headers,
            filter_expression=f"mail eq '{escaped_identifier}'",
            lookup_method="mail",
        )
        if mail_lookup:
            return mail_lookup

    raise GraphCallError(
        status_code=404,
        message="No Entra user found for provided identifier",
    )


async def _get_user_transitive_groups(
    client,
    headers: dict[str, str],
    user_object_id: str,
) -> list[dict[str, str]]:
    groups: list[dict[str, Any]] = []
    next_url = (
        f"{settings.GRAPH_BASE_URL}/users/{quote(user_object_id, safe='')}"
        "/transitiveMemberOf/microsoft.graph.group"
    )
    params: dict[str, str] | None = {"$select": GRAPH_GROUP_SELECT}

    while next_url:
        response = await _graph_get(client, next_url, headers, params=params)
        payload = response.json()
        groups.extend(payload.get("value", []))
        next_url = payload.get("@odata.nextLink")
        params = None

    return _dedupe_groups(groups)


def _filter_relevant_groups(groups: list[dict[str, str]]) -> list[dict[str, str]]:
    allowed_group_names = {name.casefold() for name in settings.F5_ALLOWED_GROUP_NAMES}
    if not allowed_group_names:
        return groups
    return [group for group in groups if group["display_name"].casefold() in allowed_group_names]


def _map_roles(groups: list[dict[str, str]]) -> list[str]:
    mapping = {name.casefold(): role for name, role in settings.F5_GROUP_ROLE_MAP.items()}
    roles: list[str] = []
    for group in groups:
        role = mapping.get(group["display_name"].casefold())
        if role and role not in roles:
            roles.append(role)
    return roles


async def login_with_f5_identifier(user_identifier: str | None) -> dict[str, Any]:
    if not user_identifier or not user_identifier.strip():
        raise HTTPException(status_code=400, detail={"error": "Missing X-User-ID header"})

    identifier = user_identifier.strip()
    access_token = _get_graph_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    httpx = _require_httpx()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            user, lookup_method = await _resolve_user(client, headers, identifier)
            user_object_id = str(user.get("id", "")).strip()
            if not user_object_id:
                raise GraphCallError(status_code=502, message="Resolved user does not contain an id")
            groups = await _get_user_transitive_groups(client, headers, user_object_id)
    except GraphCallError as exc:
        if exc.status_code in {400, 404, 409}:
            raise HTTPException(status_code=exc.status_code, detail={"error": exc.message}) from exc
        logger.error("Graph lookup failed: %s", exc.message, exc_info=True)
        raise HTTPException(status_code=502, detail={"error": exc.message}) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Unexpected Graph lookup failure: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail={"error": "Unexpected Graph lookup failure"}) from exc

    relevant_groups = _filter_relevant_groups(groups)
    roles = _map_roles(relevant_groups)

    group_names = sorted(
        {group["display_name"] for group in groups if group["display_name"]},
        key=str.casefold,
    )
    relevant_group_names = sorted(
        {group["display_name"] for group in relevant_groups if group["display_name"]},
        key=str.casefold,
    )

    return {
        "input_identifier": identifier,
        "lookup_method": lookup_method,
        "resolved_user": {
            "id": user.get("id"),
            "display_name": user.get("displayName"),
            "user_principal_name": user.get("userPrincipalName"),
            "mail": user.get("mail"),
            "employee_id": user.get("employeeId"),
        },
        "group_count": len(groups),
        "group_names": group_names,
        "relevant_group_count": len(relevant_groups),
        "relevant_group_names": relevant_group_names,
        "roles": roles,
        "group_filter_applied": bool(settings.F5_ALLOWED_GROUP_NAMES),
    }
