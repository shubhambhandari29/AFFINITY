import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, Request, Response

from core.config import settings
from core.db_helpers import run_raw_query
from core.encrypt import hash_password, verify_password
from core.jwt_handler import (
    ACCESS_TOKEN_VALIDITY,
    REFRESH_TOKEN_VALIDITY,
    create_access_token,
    create_refresh_token,
    decode_access_token,
    decode_refresh_token,
)
from db import db_connection

logger = logging.getLogger(__name__)

SESSION_COOKIE_NAME = "session"
REFRESH_COOKIE_NAME = "refresh_session"
ACCESS_COOKIE_OPTIONS = {
    "httponly": True,
    "secure": settings.SECURE_COOKIE,
    "samesite": settings.SAME_SITE,
    "path": "/",
}
REFRESH_COOKIE_OPTIONS = {
    "httponly": True,
    "secure": settings.SECURE_COOKIE,
    "samesite": settings.SAME_SITE,
    "path": "/auth/refresh_token",
}


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
    response.delete_cookie(key=SESSION_COOKIE_NAME, path=ACCESS_COOKIE_OPTIONS["path"])


def _clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(key=REFRESH_COOKIE_NAME, path=REFRESH_COOKIE_OPTIONS["path"])


def _clear_auth_cookies(response: Response) -> None:
    _clear_session_cookie(response)
    _clear_refresh_cookie(response)


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
    user = {
        "id": user_record["ID"],
        "first_name": user_record["FirstName"],
        "last_name": user_record["LastName"],
        "email": user_record["Email"],
        "role": user_record["Role"],
        "branch": user_record["BranchName"],
    }

    # Create JWT pair
    token = create_access_token(user["id"], user.get("role"))
    refresh_token = create_refresh_token(user["id"])

    # Set cookies
    _set_session_cookie(response, token)
    _set_refresh_cookie(response, refresh_token)

    logger.info(f"User {email} logged in successfully")

    return {"message": "Sign in successful", "user": user, "token": token}


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
        user_record = get_user_by_id(user_id)
        if not user_record:
            raise HTTPException(status_code=401, detail={"error": "Invalid token"})
        user = {
            "id": user_record["ID"],
            "first_name": user_record["FirstName"],
            "last_name": user_record["LastName"],
            "email": user_record["Email"],
            "role": user_record["Role"],
            "branch": user_record["BranchName"],
        }
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
        user_record = get_user_by_id(user_id)
        if not user_record:
            raise HTTPException(status_code=401, detail={"error": "Invalid refresh token"})
    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        _clear_auth_cookies(response)
        raise HTTPException(status_code=401, detail={"error": "Invalid refresh token"}) from e

    # Fixed refresh token strategy: only issue a new access token.
    new_token = create_access_token(user_id, user_record.get("Role"))

    _set_session_cookie(response, new_token)

    logger.info(f"Token refreshed for user {user_record['Email']}")

    return {"message": "Token refreshed", "token": new_token}
