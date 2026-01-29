from urllib.parse import urlencode

from fastapi import HTTPException

from core.config import settings


def build_compose_link(
    recipients: list[str] | None,
    subject: str | None,
    body: str | None,
):
    if not recipients:
        raise HTTPException(
            status_code=400,
            detail={"error": "Provide recipients to build the compose link"},
        )

    params = urlencode(
        {
            "to": ";".join(recipients),
            "subject": subject or "",
            "body": body or "",
        }
    )
    compose_url = f"{settings.OUTLOOK_COMPOSE_BASE_URL}?{params}"

    return {
        "url": compose_url,
        "recipients": recipients,
        "invalid_emails": [],
        "filtered_out": 0,
        "total": len(recipients),
    }
