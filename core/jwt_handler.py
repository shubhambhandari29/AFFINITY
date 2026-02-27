from datetime import UTC, datetime, timedelta

import jwt
from fastapi import HTTPException

from core.config import settings

SECRET_KEY = settings.SECRET_KEY
ACCESS_TOKEN_VALIDITY = settings.ACCESS_TOKEN_VALIDITY
REFRESH_TOKEN_VALIDITY = settings.REFRESH_TOKEN_VALIDITY


def create_access_token(user_id, role=None):
    expire = datetime.now(UTC) + timedelta(minutes=ACCESS_TOKEN_VALIDITY)
    payload = {"sub": str(user_id), "exp": expire, "type": "access"}
    if role is not None:
        payload["role"] = role
    encoded_jwt = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return encoded_jwt


def create_refresh_token(user_id):
    expire = datetime.now(UTC) + timedelta(minutes=REFRESH_TOKEN_VALIDITY)
    payload = {"sub": str(user_id), "exp": expire, "type": "refresh"}
    encoded_jwt = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return encoded_jwt


def decode_access_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        token_type = payload.get("type")
        if token_type and token_type != "access":
            raise HTTPException(status_code=403, detail="Invalid token")
        return payload
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=403, detail="Invalid token") from exc


def decode_refresh_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=403, detail="Invalid refresh token")
        return payload
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Refresh token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=403, detail="Invalid refresh token") from exc
