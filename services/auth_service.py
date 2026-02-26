import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, Request, Response

from core.config import settings
from core.db_helpers import run_raw_query
from core.jwt_handler import (
    ACCESS_TOKEN_VALIDITY,
    create_access_token,
    decode_access_token,
)

logger = logging.getLogger(__name__)

ROLE_PRIORITY = ("admin", "director", "underwriter")
SESSION_COOKIE_NAME = "session"
COOKIE_OPTIONS = {
    "httponly": True,
    "secure": settings.SECURE_COOKIE,
    "samesite": settings.SAME_SITE,
    "path": "/",
}


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=ACCESS_TOKEN_VALIDITY * 60,
        expires=datetime.now(UTC) + timedelta(minutes=ACCESS_TOKEN_VALIDITY),
        **COOKIE_OPTIONS,
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(key=SESSION_COOKIE_NAME, path=COOKIE_OPTIONS["path"])


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


# -------------------------
# AUTH SERVICE FUNCTIONS
# -------------------------


def _is_local_environment() -> bool:
    return settings.ENVIRONMENT.strip().lower() == "local"


def _normalize_claim_value(value: str) -> str:
    return value.strip().lower()


def _build_user_payload(
    user_record: dict[str, Any],
    role_override: str | None = None,
) -> dict[str, Any]:
    role = role_override if role_override is not None else user_record["Role"]
    return {
        "id": user_record["ID"],
        "first_name": user_record["FirstName"],
        "last_name": user_record["LastName"],
        "email": user_record["Email"],
        "role": role,
        "branch": user_record["BranchName"],
    }


def _extract_user_id_from_payload(payload: dict[str, Any]) -> int | None:
    user_id = payload.get("sub")
    if not user_id and isinstance(payload.get("user"), dict):
        user_id = payload["user"].get("id")
    if isinstance(user_id, int):
        return user_id
    if isinstance(user_id, str) and user_id.isdigit():
        return int(user_id)
    return None


def _parse_f5_groups(raw_groups: str | None) -> list[str]:
    if not raw_groups:
        return []

    delimiter = settings.F5_GROUPS_DELIMITER or ","
    normalized_groups = raw_groups.replace(";", ",")
    if delimiter != ",":
        normalized_groups = normalized_groups.replace(delimiter, ",")

    return [group.strip() for group in normalized_groups.split(",") if group.strip()]


def _f5_group_to_role_mapping() -> dict[str, str]:
    mapping: dict[str, str] = {}
    if settings.F5_UNDERWRITER_GROUP:
        mapping[_normalize_claim_value(settings.F5_UNDERWRITER_GROUP)] = "underwriter"
    if settings.F5_DIRECTOR_GROUP:
        mapping[_normalize_claim_value(settings.F5_DIRECTOR_GROUP)] = "director"
    if settings.F5_ADMIN_GROUP:
        mapping[_normalize_claim_value(settings.F5_ADMIN_GROUP)] = "admin"

    for role in ROLE_PRIORITY:
        mapping.setdefault(role, role)

    return mapping


def _resolve_role_from_f5_groups(groups: list[str]) -> str:
    mapping = _f5_group_to_role_mapping()
    resolved_roles = {
        mapping[normalized]
        for normalized in (_normalize_claim_value(group) for group in groups)
        if normalized in mapping
    }

    for role in ROLE_PRIORITY:
        if role in resolved_roles:
            return role

    raise HTTPException(status_code=403, detail={"error": "Unauthorized group"})


def _resolve_user_record_for_identity(identity: str) -> dict[str, Any] | None:
    identity_value = identity.strip()
    if not identity_value:
        return None

    user_record = get_user_by_email(identity_value)
    if user_record:
        return user_record

    if identity_value.isdigit():
        return get_user_by_id(int(identity_value))

    return None


async def _login_with_local_credentials(login_data: dict[str, Any], response: Response):
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

    # Fetch user from DB
    user_record = get_user_by_email(email)

    if not user_record:
        logger.warning(f"Login failed: user not found ({email})")
        raise HTTPException(status_code=404, detail={"error": "User not found"})
    stored_password = str(user_record.get("Password", ""))
    if password != stored_password:
        logger.warning(f"Login failed: wrong password ({email})")
        raise HTTPException(status_code=401, detail={"error": "Wrong password"})

    user = _build_user_payload(user_record)

    # Create JWT
    token = create_access_token(user["id"], user.get("role"))

    # Set cookie
    _set_session_cookie(response, token)

    logger.info(f"User {email} logged in successfully")

    return {"message": "Sign in successful", "user": user, "token": token}


async def _login_with_f5_headers(request: Request | None, response: Response):
    """
    Validates identity headers inserted by F5 APM.
    Sets HTTP-only cookie.
    Returns user profile + token.
    """

    if request is None:
        raise HTTPException(status_code=400, detail={"error": "Missing request context"})

    identity = request.headers.get(settings.F5_USER_HEADER)
    if not identity:
        logger.warning("F5 login failed: missing user header")
        raise HTTPException(status_code=401, detail={"error": "Not authenticated"})

    groups = _parse_f5_groups(request.headers.get(settings.F5_GROUPS_HEADER))
    if not groups:
        logger.warning(f"F5 login failed: missing groups header for user {identity}")
        raise HTTPException(status_code=403, detail={"error": "Unauthorized group"})

    role = _resolve_role_from_f5_groups(groups)
    user_record = _resolve_user_record_for_identity(identity)
    if not user_record:
        logger.warning(f"F5 login failed: user not found ({identity})")
        raise HTTPException(status_code=404, detail={"error": "User not found"})

    user = _build_user_payload(user_record, role_override=role)
    token = create_access_token(user["id"], user.get("role"))
    _set_session_cookie(response, token)

    logger.info(f"User {identity} logged in through F5 as {role}")
    return {"message": "Sign in successful", "user": user, "token": token}


async def login_user(
    login_data: dict[str, Any] | None,
    response: Response,
    request: Request | None = None,
):
    if _is_local_environment():
        return await _login_with_local_credentials(login_data or {}, response)
    return await _login_with_f5_headers(request, response)


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
        user_id = _extract_user_id_from_payload(payload)
        if not user_id:
            raise HTTPException(status_code=401, detail={"error": "Invalid token"})
        user_record = get_user_by_id(user_id)
        if not user_record:
            raise HTTPException(status_code=401, detail={"error": "Invalid token"})
        user = _build_user_payload(user_record, role_override=payload.get("role"))
    except Exception as e:
        logger.error(f"Token decode failed: {e}")
        raise HTTPException(status_code=401, detail={"error": "Invalid token"}) from e

    return {"message": "User authenticated", "user": user, "token": token}


async def logout_user(response: Response):
    """
    Deletes the session cookie.
    """
    _clear_session_cookie(response)
    logger.info("User logged out successfully")
    return {"message": "Logged out successfully"}


async def refresh_user_token(request: Request, response: Response, token: str | None):
    """
    Creates a new token using an existing valid token.
    """

    # If dependency didn't supply token, check cookie
    if not token:
        token = request.cookies.get(SESSION_COOKIE_NAME)

    if not token:
        raise HTTPException(status_code=401, detail={"error": "No token found"})

    try:
        payload = decode_access_token(token)
        user_id = _extract_user_id_from_payload(payload)
        if not user_id:
            raise HTTPException(status_code=401, detail={"error": "Invalid token"})
        user_record = get_user_by_id(user_id)
        if not user_record:
            raise HTTPException(status_code=401, detail={"error": "Invalid token"})
    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        raise HTTPException(status_code=401, detail={"error": "Invalid token"}) from e

    # Generate new token
    new_token = create_access_token(user_id, payload.get("role"))

    _set_session_cookie(response, new_token)

    logger.info(f"Token refreshed for user {user_record['Email']}")

    return {"message": "Token refreshed", "token": new_token}
