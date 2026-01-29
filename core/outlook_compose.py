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

    deduped: list[str] = []
    seen: set[str] = set()
    filtered_out = 0
    for email in recipients:
        if not email:
            filtered_out += 1
            continue
        if email in seen:
            filtered_out += 1
            continue
        seen.add(email)
        deduped.append(email)

    if not deduped:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "No valid email recipients found",
                "filtered_out": filtered_out,
                "total": len(recipients),
            },
        )

    params = urlencode(
        {
            "to": ";".join(deduped),
            "subject": subject or "",
            "body": body or "",
        }
    )
    compose_url = f"{settings.OUTLOOK_COMPOSE_BASE_URL}?{params}"

    return {
        "url": compose_url,
        "recipients": deduped,
        "invalid_emails": [],
        "filtered_out": filtered_out,
        "total": len(recipients),
    }
