import hashlib

PAYMENT_ALIASES = {
    "M-PESA": "M-Pesa",
    "MPESA": "M-Pesa",
    "CASH": "Cash",
    "CARD": "Card",
}


def normalise_payment(raw):
    """Map messy payment spellings to the three standard names. Unknown values return None."""
    if raw is None:
        return None
    return PAYMENT_ALIASES.get(raw.strip().upper())


def customer_sk(customer_id, valid_from):
    """Surrogate key for one customer version: SHA-256 of 'customer_id|valid_from'.
    Matches Spark's sha2(concat_ws('|', ...), 256), so the same version gets the same key."""
    return hashlib.sha256(f"{customer_id}|{valid_from}".encode("utf-8")).hexdigest()


def sale_revenue(quantity, unit_price):
    """Revenue for one sale line, rounded to cents."""
    return round(quantity * unit_price, 2)


def store_or_unknown(store_id):
    """Blank store IDs become the UNKNOWN member, so their sales still join to dim_store."""
    return store_id if store_id else "UNKNOWN"