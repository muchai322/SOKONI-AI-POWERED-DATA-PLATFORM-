import pytest

from common.transforms import customer_sk, normalise_payment, sale_revenue, store_or_unknown


@pytest.mark.parametrize("raw, expected", [
    ("M-Pesa", "M-Pesa"),
    ("mpesa", "M-Pesa"),
    (" M-PESA ", "M-Pesa"),
    ("CASH", "Cash"),
    ("card", "Card"),
    ("Bitcoin", None),
    (None, None),
])
def test_payment_names_are_standardised(raw, expected):
    assert normalise_payment(raw) == expected


def test_surrogate_key_is_stable_across_runs():
    assert customer_sk("C000123", "2025-06-01") == customer_sk("C000123", "2025-06-01")


def test_surrogate_key_differs_between_versions():
    assert customer_sk("C000123", "2025-01-01") != customer_sk("C000123", "2025-06-01")


def test_surrogate_key_is_64_char_hex():
    key = customer_sk("C000123", "2025-06-01")
    assert len(key) == 64
    assert all(c in "0123456789abcdef" for c in key)


def test_revenue_is_quantity_times_price():
    assert sale_revenue(3, 140.0) == 420.0


def test_revenue_rounds_to_cents():
    assert sale_revenue(3, 10.333) == 31.0


def test_blank_store_becomes_unknown():
    assert store_or_unknown(None) == "UNKNOWN"
    assert store_or_unknown("") == "UNKNOWN"


def test_real_store_is_kept():
    assert store_or_unknown("S05") == "S05"