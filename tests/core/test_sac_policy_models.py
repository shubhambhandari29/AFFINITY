from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models.sac_policies import SacPolicyUpsert, normalize_money_string


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", ""),
        ("   ", "   "),
        (100, "100"),
        (12.5, "12.5"),
        (Decimal("120.5000"), "120.5"),
        (" 0012.5000 ", "12.5"),
        ("0.000", "0"),
        ("1E+3", "1000"),
        ("-2.500", "-2.5"),
        ("pending quote", "pending quote"),
        (True, "True"),
    ],
)
def test_premium_normalization_preserves_notes_and_numeric_value(value, expected):
    assert normalize_money_string(value) == expected
    policy = SacPolicyUpsert(CustomerNum="001", PolicyNum="P1", PolMod="1", PremiumAmt=value)
    assert policy.PremiumAmt == expected


@pytest.mark.parametrize("field", ["CustomerNum", "PolicyNum", "PolMod"])
def test_policy_rejects_empty_natural_key(field):
    payload = dict(CustomerNum="001", PolicyNum="P1", PolMod="1")
    payload[field] = ""
    with pytest.raises(ValidationError):
        SacPolicyUpsert(**payload)
