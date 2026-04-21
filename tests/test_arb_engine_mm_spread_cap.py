"""
Tests for the MM spread cap and configurable cooldown added during the
post-go-live tuning pass.

Wide-spread (>~25c) prediction markets are almost always dead books. Quoting
into them spams orders that never fill and racks up cancel/replace churn.
"""

from datetime import datetime, timedelta

import pytest

from core.arb_engine import ArbConfig, ArbEngine
from polymarket_client.models import OrderBook, PriceLevel, TokenType


def _book(*, yes_bid: float, yes_ask: float) -> OrderBook:
    yes = MagicTokenBook(best_bid=yes_bid, best_ask=yes_ask)
    no = MagicTokenBook(best_bid=None, best_ask=None)
    book = OrderBook.__new__(OrderBook)
    book.market_id = "m1"
    book.yes = yes
    book.no = no
    return book


class MagicTokenBook:
    """Lightweight stand-in for the per-token order book."""
    def __init__(self, best_bid, best_ask):
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
        self.bids = [PriceLevel(price=best_bid, size=10.0)] if best_bid is not None else []
        self.asks = [PriceLevel(price=best_ask, size=10.0)] if best_ask is not None else []


def _engine(**overrides) -> ArbEngine:
    cfg = ArbConfig(
        min_edge=0.005,
        bundle_arb_enabled=True,
        min_spread=0.01,
        mm_max_spread=overrides.get("mm_max_spread", 0.15),
        mm_cooldown_seconds=overrides.get("mm_cooldown_seconds", 5.0),
        mm_enabled=True,
        tick_size=0.01,
        default_order_size=1,
        min_order_size=1,
        max_order_size=1,
    )
    return ArbEngine(cfg)


def test_mm_signal_emitted_for_normal_spread():
    engine = _engine(mm_max_spread=0.15)
    book = _book(yes_bid=0.40, yes_ask=0.50)
    signal = engine._check_mm_token("m1", book.yes, TokenType.YES)
    assert signal is not None


def test_mm_signal_skipped_for_pathologically_wide_spread():
    engine = _engine(mm_max_spread=0.15)
    # 80c spread — clearly a dead book.
    book = _book(yes_bid=0.10, yes_ask=0.90)
    signal = engine._check_mm_token("m1", book.yes, TokenType.YES)
    assert signal is None


def test_mm_max_spread_zero_disables_cap():
    engine = _engine(mm_max_spread=0.0)
    book = _book(yes_bid=0.10, yes_ask=0.90)
    signal = engine._check_mm_token("m1", book.yes, TokenType.YES)
    assert signal is not None


def test_mm_cooldown_uses_configured_value():
    engine = _engine(mm_cooldown_seconds=30.0)
    book = _book(yes_bid=0.40, yes_ask=0.50)
    sig1 = engine._check_mm_token("m1", book.yes, TokenType.YES)
    assert sig1 is not None

    # Second call inside the cooldown window must be suppressed.
    sig2 = engine._check_mm_token("m1", book.yes, TokenType.YES)
    assert sig2 is None

    # The cooldown entry should be ~30s in the future, not the hardcoded 5s.
    cooldown_until = engine._opportunity_cooldown["mm_m1_yes"]
    delta = cooldown_until - datetime.utcnow()
    assert delta > timedelta(seconds=20)
