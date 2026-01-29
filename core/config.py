import os

from dotenv import load_dotenv

# Load .env values
load_dotenv()

def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() == "true"


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

    # JWT / Auth config (still used for existing flows)
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    ACCESS_TOKEN_VALIDITY: int = int(os.getenv("ACCESS_TOKEN_VALIDITY"))

    # CORS settings
    ALLOWED_ORIGINS: list = [os.getenv("FRONTEND_URL")]

    # Cookie settings
    SECURE_COOKIE: bool = _as_bool(os.getenv("SECURE_COOKIE"))
    SAME_SITE: str = os.getenv("SAME_SITE")

    # Outlook compose settings
    OUTLOOK_COMPOSE_ENABLED: bool = _as_bool(os.getenv("OUTLOOK_COMPOSE_ENABLED"), True)
    OUTLOOK_COMPOSE_BASE_URL: str = os.getenv(
        "OUTLOOK_COMPOSE_BASE_URL",
        "https://outlook.office.com/mail/deeplink/compose",
    )
    OUTLOOK_COMPOSE_MAX_RECIPIENTS: int = int(os.getenv("OUTLOOK_COMPOSE_MAX_RECIPIENTS", "50"))
    OUTLOOK_COMPOSE_ALLOWED_DOMAINS: str | None = os.getenv("OUTLOOK_COMPOSE_ALLOWED_DOMAINS")
    OUTLOOK_COMPOSE_SUBJECT_TEMPLATE: str = os.getenv(
        "OUTLOOK_COMPOSE_SUBJECT_TEMPLATE",
        "Loss Run Report Distribution",
    )
    OUTLOOK_COMPOSE_BODY_TEMPLATE: str = os.getenv(
        "OUTLOOK_COMPOSE_BODY_TEMPLATE",
        "Hi,\n\nPlease see the loss run report.\n\nThanks,",
    )

settings = Settings()
