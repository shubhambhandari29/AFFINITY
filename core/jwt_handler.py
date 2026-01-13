
from datetime import UTC, datetime, timedelta

import jwt
from fastapi import HTTPException

from core.config import settings

SECRET_KEY = settings.SECRET_KEY
ACCESS_TOKEN_VALIDITY = settings.ACCESS_TOKEN_VALIDITY


def create_access_token(user_id, role=None):
    expire = datetime.now(UTC) + timedelta(minutes=ACCESS_TOKEN_VALIDITY)
    payload = {"sub": str(user_id), "exp": expire}
    if role is not None:
        payload["role"] = role
    encoded_jwt = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return encoded_jwt


def decode_access_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=403, detail="Invalid token") from exc