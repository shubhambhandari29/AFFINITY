import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, Request, Response

from core.config import settings
from core.db_helpers import run_raw_query
from core.encrypt import hash_password, verify_password
from core.jwt_handler import (
    ACCESS_TOKEN_VALIDITY,
    create_access_token,
    decode_access_token,
)
from db import db_connection

logger = logging.getLogger(__name__)

SESSION_COOKIE_NAME = "session"
COOKIE_OPTIONS = {
    "httponly": True,
    "secure": settings.SECURE_COOKIE,
    "samesite": settings.SAME_SITE,
    "path": "/",
}

_GROUP_SPLIT_RE = re.compile(r"[;,]")


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


def _get_header_value(request: Request, header_name: str) -> str | None:
    value = request.headers.get(header_name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _normalize_group_name(value: str) -> str:
    cleaned = value.strip()
    lowered = cleaned.lower()
    if lowered.startswith("cn="):
        cleaned = cleaned.split(",", 1)[0][3:]
    if "\\" in cleaned:
        cleaned = cleaned.split("\\")[-1]
    if "/" in cleaned:
        cleaned = cleaned.split("/")[-1]
    return cleaned.strip().lower()


def _parse_f5_groups(groups_header: str | None) -> list[str]:
    if not groups_header:
        return []
    groups: list[str] = []
    for part in _GROUP_SPLIT_RE.split(groups_header):
        normalized = _normalize_group_name(part)
        if normalized:
            groups.append(normalized)
    return groups


def _map_f5_groups_to_role(groups: list[str]) -> str | None:
    if not groups:
        return None
    admin_group = _normalize_group_name(settings.F5_GROUP_ADMIN)
    director_group = _normalize_group_name(settings.F5_GROUP_DIRECTOR)
    underwriter_group = _normalize_group_name(settings.F5_GROUP_UNDERWRITER)

    if admin_group in groups:
        return settings.F5_GROUP_ADMIN
    if director_group in groups:
        return settings.F5_GROUP_DIRECTOR
    if underwriter_group in groups:
        return settings.F5_GROUP_UNDERWRITER
    return None


def _get_f5_identity(request: Request) -> dict[str, str] | None:
    if not settings.F5_AUTH_ENABLED:
        return None

    user_id = _get_header_value(request, settings.F5_HEADER_USER_ID)
    if not user_id:
        return None

    if settings.F5_SHARED_SECRET:
        secret = _get_header_value(request, settings.F5_SHARED_SECRET_HEADER)
        if secret != settings.F5_SHARED_SECRET:
            logger.warning("F5 header auth failed: shared secret mismatch")
            raise HTTPException(status_code=401, detail={"error": "Not authenticated"})

    return {
        "user_id": user_id,
        "groups": _get_header_value(request, settings.F5_HEADER_GROUPS) or "",
        "email": _get_header_value(request, settings.F5_HEADER_EMAIL) or "",
        "first_name": _get_header_value(request, settings.F5_HEADER_FIRST_NAME) or "",
        "last_name": _get_header_value(request, settings.F5_HEADER_LAST_NAME) or "",
    }


def _resolve_user_for_f5(identity: dict[str, str]) -> dict[str, Any] | None:
    email = identity.get("email") or identity.get("user_id") or ""
    if not email:
        return None
    return get_user_by_email(email)


def _build_user_payload(
    user_record: dict[str, Any],
    role_override: str | None = None,
    first_name_override: str | None = None,
    last_name_override: str | None = None,
    email_override: str | None = None,
) -> dict[str, Any]:
    return {
        "id": user_record["ID"],
        "first_name": user_record.get("FirstName") or first_name_override,
        "last_name": user_record.get("LastName") or last_name_override,
        "email": user_record.get("Email") or email_override,
        "role": role_override or user_record.get("Role"),
        "branch": user_record.get("BranchName"),
    }


def _issue_token_for_user(
    user: dict[str, Any],
    response: Response | None,
    role_override: str | None = None,
) -> str:
    token = create_access_token(user["id"], role_override or user.get("role"))
    if response is not None:
        _set_session_cookie(response, token)
    return token


def _authenticate_f5_identity(
    identity: dict[str, str],
    response: Response | None,
) -> tuple[dict[str, Any], str]:
    user_record = _resolve_user_for_f5(identity)
    if not user_record:
        logger.warning(f"F5 login failed: user not found ({identity.get('user_id')})")
        raise HTTPException(status_code=404, detail={"error": "User not found"})

    groups = _parse_f5_groups(identity.get("groups"))
    role = _map_f5_groups_to_role(groups)
    if not role:
        logger.warning(
            f"F5 login failed: no matching role for user {identity.get('user_id')}"
        )
        raise HTTPException(status_code=403, detail={"error": "Forbidden"})
    user = _build_user_payload(
        user_record,
        role_override=role,
        first_name_override=identity.get("first_name"),
        last_name_override=identity.get("last_name"),
        email_override=identity.get("email"),
    )
    token = _issue_token_for_user(user, response, role)
    return user, token


def _persist_hashed_password(user_id: int, new_hash: str) -> None:
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE tblUsers SET password = ? WHERE id = ?",
            (new_hash, user_id),
        )
        conn.commit()


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

    # Fetch user from DB
    user_record = get_user_by_email(email)

    if not user_record:
        logger.warning(f"Login failed: user not found ({email})")
        raise HTTPException(status_code=404, detail={"error": "User not found"})
    stored_password = str(user_record.get("Password", ""))
    password_valid = False
    needs_rehash = False

    try:
        password_valid = verify_password(password, stored_password)
    except ValueError:
        if password == stored_password:
            password_valid = True
            needs_rehash = True
        else:
            password_valid = False

    if not password_valid:
        logger.warning(f"Login failed: wrong password ({email})")
        raise HTTPException(status_code=401, detail={"error": "Wrong password"})

    if needs_rehash:
        try:
            new_hash = hash_password(password)
            _persist_hashed_password(user_record["ID"], new_hash)
            logger.info(f"Rehashed legacy password for user {email}")
        except Exception as exc:
            logger.error(f"Failed to rehash password for user {email}: {exc}", exc_info=True)

    # Prepare user payload (only safe fields)
    user = _build_user_payload(user_record)

    # Create JWT + set cookie
    token = _issue_token_for_user(user, response)

    logger.info(f"User {email} logged in successfully")

    return {"message": "Sign in successful", "user": user, "token": token}


async def login_user_from_f5_headers(request: Request, response: Response):
    """
    Validates user from F5-injected headers.
    Returns user profile + token or None if headers are missing.
    """

    identity = _get_f5_identity(request)
    if not identity:
        if settings.F5_ENFORCE:
            raise HTTPException(status_code=401, detail={"error": "Not authenticated"})
        return None

    user, token = _authenticate_f5_identity(identity, response)
    logger.info(f"User {user.get('email', identity.get('user_id'))} logged in via F5")
    return {"message": "Sign in successful", "user": user, "token": token}


async def get_current_user_from_token(
    request: Request,
    response: Response | None = None,
):
    """
    Validates JWT from cookie.
    Returns decoded user.
    """

    identity = _get_f5_identity(request)
    if identity:
        user, token = _authenticate_f5_identity(identity, response)
        return {"message": "User authenticated", "user": user, "token": token}
    if settings.F5_AUTH_ENABLED and settings.F5_ENFORCE:
        raise HTTPException(status_code=401, detail={"error": "Not authenticated"})

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
        user_record = get_user_by_id(user_id)
        if not user_record:
            raise HTTPException(status_code=401, detail={"error": "Invalid token"})
        user = _build_user_payload(user_record)
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

    identity = _get_f5_identity(request)
    if identity:
        _, new_token = _authenticate_f5_identity(identity, response)
        return {"message": "Token refreshed", "token": new_token}
    if settings.F5_AUTH_ENABLED and settings.F5_ENFORCE:
        raise HTTPException(status_code=401, detail={"error": "Not authenticated"})

    # If dependency didn't supply token, check cookie
    if not token:
        token = request.cookies.get(SESSION_COOKIE_NAME)

    if not token:
        raise HTTPException(status_code=401, detail={"error": "No token found"})

    try:
        payload = decode_access_token(token)
        user_id = payload.get("sub")
        if not user_id and isinstance(payload.get("user"), dict):
            user_id = payload["user"].get("id")
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
