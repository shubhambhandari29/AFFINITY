from datetime import datetime

from services.hcm import hcm_account_service


def test_normalize_hcm_account_payload_converts_datetime_fields():
    payload = {
        "DateCreated": "2026-08-03",
        "OnBoardDate": "2026-08-03",
        "EffectiveDate": "2026-08-04",
        "DiscDate": "2026-08-05",
        "TermDate": "",
    }

    normalized = hcm_account_service._normalize_hcm_account_payload(payload)

    assert normalized["DateCreated"] == datetime(2026, 8, 3, 0, 0)
    assert normalized["OnBoardDate"] == datetime(2026, 8, 3, 0, 0)
    assert normalized["EffectiveDate"] == datetime(2026, 8, 4, 0, 0)
    assert normalized["DiscDate"] == datetime(2026, 8, 5, 0, 0)
    assert normalized["TermDate"] is None
