import bcrypt


def hash_password(password: str) -> str:
    """
    Hashes a plain-text password using bcrypt.

    Use this when creating or updating user passwords.
    """
    if password is None:
        raise ValueError("Password cannot be None")

    # bcrypt works with bytes
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    # Store as UTF-8 string in DB
    return hashed.decode("utf-8")


def verify_password(entered_password: str, stored_password: str) -> bool:
    """
    Verifies the entered password against a bcrypt hash.
    Raises ValueError if the stored value is not a bcrypt hash so callers can
    decide how to handle legacy data.
    """

    if entered_password is None or stored_password is None:
        return False

    if not stored_password.startswith("$2"):
        raise ValueError("Stored password is not a bcrypt hash")
    if len(stored_password) < 60:
        raise ValueError("Stored password hash is malformed")

    try:
        return bcrypt.checkpw(
            entered_password.encode("utf-8"),
            stored_password.encode("utf-8"),
        )
    except Exception:
        return False
