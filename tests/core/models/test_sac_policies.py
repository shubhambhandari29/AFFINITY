from decimal import Decimal

from core.models.sac_policies import SacPolicyUpsert, normalize_money_string


def test_normalize_money_string_trims_trailing_zeroes():
    assert normalize_money_string(12345.0) == "12345"
    assert normalize_money_string(Decimal("12345.5000")) == "12345.5"
    assert normalize_money_string("") == ""
    assert normalize_money_string(None) is None


def test_normalize_money_string_preserves_non_numeric_strings():
    assert normalize_money_string("N/A") == "N/A"


def test_sac_policy_upsert_accepts_numeric_premium():
    payload = SacPolicyUpsert(
        CustomerNum="1", PolicyNum="P1", PolMod="00", PremiumAmt=12345.0
    )
    assert payload.PremiumAmt == "12345"
