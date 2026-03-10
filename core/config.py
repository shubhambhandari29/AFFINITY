import os

from dotenv import load_dotenv

# Load .env values
load_dotenv()

def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() == "true"


def _parse_origins(value: str | None) -> list[str]:
    if not value:
        return []
    origins: list[str] = []
    for origin in value.split(","):
        cleaned = origin.strip().rstrip("/")
        if cleaned:
            origins.append(cleaned)
    return origins


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    items: list[str] = []
    for raw_item in value.split(","):
        item = raw_item.strip()
        if item:
            items.append(item)
    return items


def _parse_mapping(value: str | None) -> dict[str, str]:
    if not value:
        return {}
    mapping: dict[str, str] = {}
    for raw_entry in value.split(","):
        entry = raw_entry.strip()
        if not entry or ":" not in entry:
            continue
        key, mapped_value = entry.split(":", 1)
        key = key.strip()
        mapped_value = mapped_value.strip()
        if key and mapped_value:
            mapping[key] = mapped_value
    return mapping


class Settings:

    # Feature flags / environment toggles
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "local")

    # Database configuration
    DB_SERVER: str | None = os.getenv("DB_SERVER")
    DB_NAME: str | None = os.getenv("DB_NAME")
    DB_DRIVER: str = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
    DB_AUTH: str | None = os.getenv("DB_AUTH")

    # Azure AD app credentials (used with SSO login)
    AZURE_TENANT_ID: str | None = os.getenv("AZURE_TENANT_ID")
    AZURE_CLIENT_ID: str | None = os.getenv("AZURE_CLIENT_ID")
    AZURE_CLIENT_SECRET: str | None = os.getenv("AZURE_CLIENT_SECRET")

    # Microsoft Graph settings used by F5 header-based login
    GRAPH_BASE_URL: str = os.getenv("GRAPH_BASE_URL", "https://graph.microsoft.com/v1.0").rstrip(
        "/"
    )
    GRAPH_SCOPE: str = os.getenv("GRAPH_SCOPE", "https://graph.microsoft.com/.default")
    F5_ALLOWED_GROUP_NAMES: list[str] = _parse_csv(os.getenv("F5_ALLOWED_GROUP_NAMES"))
    F5_GROUP_ROLE_MAP: dict[str, str] = _parse_mapping(os.getenv("F5_GROUP_ROLE_MAP"))

    # JWT / Auth config (still used for existing flows)
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    ACCESS_TOKEN_VALIDITY: int = int(os.getenv("ACCESS_TOKEN_VALIDITY"))
    REFRESH_TOKEN_VALIDITY: int = int(os.getenv("REFRESH_TOKEN_VALIDITY", "10080"))

    # CORS settings
    ALLOWED_ORIGINS: list[str] = list(dict.fromkeys(_parse_origins(os.getenv("FRONTEND_URL"))))

    # Cookie settings
    SECURE_COOKIE: bool = _as_bool(os.getenv("SECURE_COOKIE"))
    SAME_SITE: str = os.getenv("SAME_SITE")

    # Outlook compose settings
    OUTLOOK_COMPOSE_BASE_URL: str = os.getenv("OUTLOOK_COMPOSE_BASE_URL")


settings = Settings()
