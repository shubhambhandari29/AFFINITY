import pytest
from pydantic import ValidationError

from core.models.auth import LoginRequest
from core.models.claim_review_distribution import ClaimReviewDistributionEntry
from core.models.frequency import FrequencyEntry


def test_frequency_entry_validates_month_bounds():
    entry = FrequencyEntry(CustomerNum="1001", MthNum=5, Frequency=10)
    assert entry.MthNum == 5

    with pytest.raises(ValidationError):
        FrequencyEntry(CustomerNum="1001", MthNum=13)


def test_distribution_entry_allows_extra_fields():
    entry = ClaimReviewDistributionEntry(
        CustomerNum="1001", EMailAddress="test@example.com", Notes="Extra"
    )

    dumped = entry.model_dump()
    assert dumped["Notes"] == "Extra"


def test_login_request_validates_email():
    model = LoginRequest(email="user@example.com", password="pw")
    assert model.email == "user@example.com"

    with pytest.raises(ValidationError):
        LoginRequest(email="bad-email", password="pw")
