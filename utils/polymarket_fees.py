"""
Polymarket fee helpers.

Fee schedule (effective Apr 2026):
    fee = theta * contracts * p * (1 - p)
with banker's rounding to cents for charged/credited totals.
"""

from decimal import Decimal, ROUND_HALF_EVEN


def bankers_round_cents(amount: float) -> float:
    """Round to nearest cent using round-half-to-even."""
    return float(Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))


def polymarket_fee_raw(theta: float, contracts: float, price: float) -> float:
    """Compute unrounded fee/rebate."""
    clamped_price = max(0.01, min(0.99, float(price)))
    return float(theta) * max(0.0, float(contracts)) * clamped_price * (1.0 - clamped_price)


def polymarket_fee(
    theta: float,
    contracts: float,
    price: float,
    round_to_cents: bool = True,
) -> float:
    """Compute fee/rebate, optionally using exchange cent rounding."""
    raw = polymarket_fee_raw(theta=theta, contracts=contracts, price=price)
    return bankers_round_cents(raw) if round_to_cents else raw
