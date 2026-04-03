import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from fastapi import HTTPException, Request, Response

from core.config import settings
from core.db_helpers import run_raw_query
from core.jwt_handler import (
    ACCESS_TOKEN_VALIDITY,
    REFRESH_TOKEN_VALIDITY,
    create_access_token,
    create_refresh_token,
    decode_access_token,
    decode_refresh_token,
)

logger = logging.getLogger(__name__)

GRAPH_SCOPE = "https://graph.microsoft.com/.default"
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GROUP_ROLE_PRIORITY = {
    "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_ADMIN": ("Admin", 1),
    "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_DIRECTORS": ("Director", 2),
    "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_UNDERWRITERS": ("Underwriter", 3),
    "AZURE_SECURE_ROLE_CLAIMS_PROD_SACAPP_CCT": ("CCT_User", 4),
}
FULL_ROLE_EXCEPTION_EMAIL = "mbond@hanover.com"

SESSION_COOKIE_NAME = "session"
REFRESH_COOKIE_NAME = "refresh_session"
COOKIE_BASE_OPTIONS = {
    "httponly": True,
    "secure": settings.SECURE_COOKIE,
    "samesite": settings.SAME_SITE,
}
ACCESS_COOKIE_OPTIONS = {
    **COOKIE_BASE_OPTIONS,
    "path": "/",
}
REFRESH_COOKIE_OPTIONS = {
    **COOKIE_BASE_OPTIONS,
    "path": "/auth/refresh",
}
LEGACY_REFRESH_COOKIE_PATHS = (
    "/auth/refresh_token",
    "/auth",
    "/",
)


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=ACCESS_TOKEN_VALIDITY * 60,
        expires=datetime.now(UTC) + timedelta(minutes=ACCESS_TOKEN_VALIDITY),
        **ACCESS_COOKIE_OPTIONS,
    )


def _set_refresh_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=REFRESH_COOKIE_NAME,
        value=token,
        max_age=REFRESH_TOKEN_VALIDITY * 60,
        expires=datetime.now(UTC) + timedelta(minutes=REFRESH_TOKEN_VALIDITY),
        **REFRESH_COOKIE_OPTIONS,
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path=ACCESS_COOKIE_OPTIONS["path"],
        **COOKIE_BASE_OPTIONS,
    )


def _clear_refresh_cookie(response: Response) -> None:
    refresh_paths = dict.fromkeys(
        (
            REFRESH_COOKIE_OPTIONS["path"],
            *LEGACY_REFRESH_COOKIE_PATHS,
        )
    )
    for path in refresh_paths:
        response.delete_cookie(
            key=REFRESH_COOKIE_NAME,
            path=path,
            **COOKIE_BASE_OPTIONS,
        )


def _clear_auth_cookies(response: Response) -> None:
    _clear_session_cookie(response)
    _clear_refresh_cookie(response)


def _build_db_user_payload(user_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": user_record["ID"],
        "first_name": user_record["FirstName"],
        "last_name": user_record["LastName"],
        "email": user_record["Email"],
        "role": user_record["Role"],
        "branch": user_record["BranchName"],
    }


def _build_f5_user_payload(user_id: str, role: str | None) -> dict[str, Any]:
    normalized_user_id = str(user_id).strip()
    return {
        "id": normalized_user_id,
        "first_name": "",
        "last_name": "",
        "email": normalized_user_id if "@" in normalized_user_id else "",
        "role": role,
        "branch": _resolve_branch_name(normalized_user_id, role),
    }


def _create_login_response(
    response: Response,
    *,
    token_subject: str | int,
    user: dict[str, Any],
    refresh_role: str | None = None,
) -> dict[str, Any]:
    token = create_access_token(token_subject, user.get("role"))
    refresh_token = create_refresh_token(token_subject, refresh_role)

    _set_session_cookie(response, token)
    _set_refresh_cookie(response, refresh_token)

    return {"message": "Sign in successful", "user": user, "token": token}


def _get_graph_access_token() -> str:
    try:
        from azure.identity import ManagedIdentityCredential
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise HTTPException(
            status_code=500, detail={"error": "Missing dependency: azure-identity"}
        ) from exc

    try:
        credential = ManagedIdentityCredential()
        return credential.get_token(GRAPH_SCOPE).token
    except Exception as exc:
        logger.error("Managed identity token acquisition failed: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "Unable to get Graph access token"},
        ) from exc


def _get_user_groups_from_graph(user_id: str) -> list[dict[str, Any]]:
    try:
        import requests
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise HTTPException(
            status_code=500, detail={"error": "Missing dependency: requests"}
        ) from exc

    token = _get_graph_access_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = (
        f"{GRAPH_BASE_URL}/users/{quote(user_id, safe='')}"
        "/transitiveMemberOf/microsoft.graph.group?$select=id,displayName"
    )

    groups: list[dict[str, Any]] = []
    while url:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail={"error": "User not found in Entra ID"})
        if response.status_code >= 400:
            logger.error("Graph groups lookup failed [%s]: %s", response.status_code, response.text)
            raise HTTPException(
                status_code=502,
                detail={"error": "Failed to fetch AD groups from Graph"},
            )
        payload = response.json()
        groups.extend(payload.get("value", []))
        url = payload.get("@odata.nextLink")

    return groups


def _resolve_role_from_groups(groups: list[dict[str, Any]]) -> str | None:
    matched: list[tuple[int, str, str]] = []
    for group in groups:
        group_name = str(group.get("displayName", "")).strip()
        role_priority = GROUP_ROLE_PRIORITY.get(group_name)
        if role_priority:
            role, priority = role_priority
            matched.append((priority, group_name, role))

    if not matched:
        return None

    matched.sort(key=lambda item: item[0])
    ordered_roles = list(dict.fromkeys(role for _, _, role in matched))
    return ",".join(ordered_roles)


def _normalize_graph_role(email: str | None, role: str | None) -> str | None:
    normalized_email = str(email or "").strip().lower()
    roles = [item.strip() for item in str(role or "").split(",") if item.strip()]
    unique_roles = list(dict.fromkeys(roles))

    if {"Admin", "Director", "Underwriter"}.issubset(
        set(unique_roles)
    ) and normalized_email != FULL_ROLE_EXCEPTION_EMAIL:
        unique_roles = [item for item in unique_roles if item != "Underwriter"]

    return ",".join(unique_roles) or None


def _resolve_branch_name(email: str | None, role: str | None) -> str | None:
    normalized_email = str(email or "").strip().lower()
    if not normalized_email:
        return None

    roles = {item.strip() for item in str(role or "").split(",") if item.strip()}
    if "Director" in roles:
        return get_branch_name_by_email(normalized_email)

    return None


# -------------------------
# DB HELPERS FOR AUTH USER
# -------------------------


def get_user_by_email(email: str) -> dict[str, Any] | None:
    """
    Fetch a single active user by email.
    Returns: dict or None
    """

    query = """
        SELECT *
        FROM tblUsers
        WHERE active = 1 AND email = ?
    """

    try:
        results = run_raw_query(query, [email])
    except Exception as e:
        logger.error(f"DB error fetching user by email {email}: {e}")
        raise

    if len(results) == 1:
        return results[0]

    return None


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    """
    Fetch a single active user by id.
    Returns: dict or None
    """
    query = """
        SELECT *
        FROM tblUsers
        WHERE active = 1 AND id = ?
    """

    try:
        results = run_raw_query(query, [user_id])
    except Exception as e:
        logger.error(f"DB error fetching user by id {user_id}: {e}")
        raise

    if len(results) == 1:
        return results[0]

    return None


def get_email_by_user_identifier(user_identifier: str) -> str | None:
    normalized_user_identifier = str(user_identifier or "").strip().lower()
    if not normalized_user_identifier:
        return None

    query = """
        SELECT TOP 1 Email
        FROM tblUsers
        WHERE active = 1 AND LOWER(UserID) = ?
    """

    try:
        results = run_raw_query(query, [normalized_user_identifier])
    except Exception as exc:
        logger.error("DB error fetching email by UserID %s: %s", user_identifier, exc)
        raise

    if not results:
        return None

    email = results[0].get("Email")
    if not email:
        return None

    return str(email).strip()


def get_branch_name_by_email(email: str) -> str | None:
    normalized_email = str(email or "").strip().lower()
    if not normalized_email:
        return None

    query = """
        SELECT TOP 1 BranchName
        FROM tblBranchMapping
        WHERE LOWER(Email) = ?
    """

    try:
        results = run_raw_query(query, [normalized_email])
    except Exception as exc:
        logger.warning("tblBranchMapping lookup failed for %s: %s", normalized_email, exc)
        return None

    if not results:
        return None

    return results[0].get("BranchName")


# -------------------------
# AUTH SERVICE FUNCTIONS
# -------------------------


async def login_user(login_data: dict[str, Any], response: Response):
    """
    Validates user email/password.
    Sets HTTP-only cookie.
    Returns user profile + token.
    """

    email = login_data.get("email")
    password = login_data.get("password")
    if not email or not password:
        logger.warning("Login attempt with missing data")
        raise HTTPException(status_code=400, detail={"error": "Missing email or password"})

    user_record = get_user_by_email(email)
    if not user_record:
        logger.warning(f"Login failed: user not found ({email})")
        raise HTTPException(status_code=404, detail={"error": "User not found"})

    stored_password = user_record.get("Password")
    if stored_password is None or password != str(stored_password):
        logger.warning(f"Login failed: wrong password ({email})")
        raise HTTPException(status_code=401, detail={"error": "Wrong password"})

    user = _build_db_user_payload(user_record)
    result = _create_login_response(response, token_subject=user["id"], user=user)

    logger.info(f"User {email} logged in successfully")
    return result


async def f5_login_user(login_data: dict[str, Any], response: Response):
    """
    Validates user by Entra AD group membership via Graph API.
    Sets HTTP-only cookie.
    Returns user profile + token.
    """

    user_identifier = str(login_data.get("user") or "").strip()
    if not user_identifier:
        logger.warning("F5 login attempt with missing user")
        raise HTTPException(status_code=400, detail={"error": "Missing user"})

    email = get_email_by_user_identifier(user_identifier)
    if not email:
        logger.warning("F5 login failed: no email found for UserID %s", user_identifier)
        raise HTTPException(
            status_code=404,
            detail={"error": "User email not found for provided user id"},
        )

    groups = _get_user_groups_from_graph(email)
    role = _normalize_graph_role(email, _resolve_role_from_groups(groups))
    if not role:
        logger.warning("F5 login failed: no SAC role groups matched (%s)", email)
        raise HTTPException(
            status_code=403,
            detail={
                "error": (
                    "You are not an authorized user. For getting access, "
                    "share an email to mbond@hanover.com for next steps."
                )
            },
        )

    user = _build_f5_user_payload(email, role)
    result = _create_login_response(
        response,
        token_subject=email,
        user=user,
        refresh_role=role,
    )

    logger.info("User %s resolved to %s and logged in successfully via F5", user_identifier, email)
    return result


async def get_current_user_from_token(request: Request):
    """
    Validates JWT from cookie.
    Returns decoded user.
    """

    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail={"error": "Not authenticated"})

    try:
        payload = decode_access_token(token)
        user_id = payload.get("sub")
        if not user_id and isinstance(payload.get("user"), dict):
            user_id = payload["user"].get("id")
        if not user_id:
            raise HTTPException(status_code=401, detail={"error": "Invalid token"})
        user_record = None
        if str(user_id).isdigit():
            user_record = get_user_by_id(int(str(user_id)))

        if user_record:
            user = _build_db_user_payload(user_record)
        else:
            role = _normalize_graph_role(str(user_id), payload.get("role"))
            user = _build_f5_user_payload(str(user_id), role)
    except Exception as e:
        logger.error(f"Token decode failed: {e}")
        raise HTTPException(status_code=401, detail={"error": "Invalid token"}) from e

    return {"message": "User authenticated", "user": user, "token": token}


async def logout_user(response: Response):
    """
    Deletes auth cookies.
    """
    _clear_auth_cookies(response)
    logger.info("User logged out successfully")
    return {"message": "Logged out successfully"}


async def refresh_user_token(request: Request, response: Response, token: str | None):
    """
    Creates a new access token using a valid refresh token.
    """

    refresh_token = request.cookies.get(REFRESH_COOKIE_NAME)
    if not refresh_token:
        # Backward-compatible fallback for non-cookie clients.
        refresh_token = token

    if not refresh_token:
        raise HTTPException(status_code=401, detail={"error": "No refresh token found"})

    try:
        payload = decode_refresh_token(refresh_token)
        user_id = payload.get("sub")
        if not user_id and isinstance(payload.get("user"), dict):
            user_id = payload["user"].get("id")
        if not user_id:
            raise HTTPException(status_code=401, detail={"error": "Invalid refresh token"})
        role = payload.get("role")
        user_record = None
        if str(user_id).isdigit():
            user_record = get_user_by_id(int(str(user_id)))
            if user_record:
                role = user_record.get("Role")
        else:
            role = _normalize_graph_role(str(user_id), role)
        if not role and not user_record:
            raise HTTPException(status_code=401, detail={"error": "Invalid refresh token"})
    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        _clear_auth_cookies(response)
        raise HTTPException(status_code=401, detail={"error": "Invalid refresh token"}) from e

    # Fixed refresh token strategy: only issue a new access token.
    new_token = create_access_token(user_id, role)

    _set_session_cookie(response, new_token)

    refreshed_user_identifier = user_record["Email"] if user_record else str(user_id)
    logger.info("Token refreshed for user %s", refreshed_user_identifier)

    return {"message": "Token refreshed", "token": new_token}
