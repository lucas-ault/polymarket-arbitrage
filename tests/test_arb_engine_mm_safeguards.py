"""
Tests for MM safeguards against adverse selection.

After observing real losses on long-tail NHL futures (positions bought at
5-21% probability that decayed toward zero as outcomes resolved against us),
two new defenses were added:

- mm_min_price / mm_max_price: skip MM at extreme prices (the adverse-
  selection zone in event futures).
- inventory probe: don't quote a side we already have inventory on.
"""

from core.arb_engine import ArbConfig, ArbEngine
from polymarket_client.models import PriceLevel, TokenType


class _StubBook:
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
        mm_max_spread=overrides.get("mm_max_spread", 1.0),
        mm_min_price=overrides.get("mm_min_price", 0.10),
        mm_max_price=overrides.get("mm_max_price", 0.90),
        mm_cooldown_seconds=overrides.get("mm_cooldown_seconds", 5.0),
        mm_enabled=True,
        tick_size=0.01,
        default_order_size=1,
        min_order_size=1,
        max_order_size=1,
    )
    return ArbEngine(cfg)


def test_mm_skipped_at_low_price_long_tail_market():
    """Long-tail futures at 5-15% are the adverse-selection zone."""
    engine = _engine(mm_min_price=0.10, mm_max_price=0.90)
    # Midpoint 0.06 — long-tail event future, exactly the case that lost
    # the most real money.
    book = _StubBook(best_bid=0.05, best_ask=0.07)
    assert engine._check_mm_token("nhl_cup_loser", book, TokenType.YES) is None


def test_mm_skipped_at_high_price():
    """Symmetric: don't MM at 90+ either (selling tail of same trade)."""
    engine = _engine(mm_min_price=0.10, mm_max_price=0.90)
    book = _StubBook(best_bid=0.93, best_ask=0.95)
    assert engine._check_mm_token("near_resolution", book, TokenType.YES) is None


def test_mm_emitted_in_central_price_band():
    engine = _engine(mm_min_price=0.10, mm_max_price=0.90)
    # Midpoint 0.45 — squarely in the safe band.
    book = _StubBook(best_bid=0.42, best_ask=0.48)
    assert engine._check_mm_token("close_election", book, TokenType.YES) is not None


def test_mm_price_band_disabled_with_default_bounds():
    """mm_min_price=0 and mm_max_price=1.0 should disable the gate."""
    engine = _engine(mm_min_price=0.0, mm_max_price=1.0)
    # Same low-midpoint book that the price-band test rejects, but with
    # enough room (4c spread) for the inner spread/tick checks to pass.
    book = _StubBook(best_bid=0.03, best_ask=0.07)
    assert engine._check_mm_token("any_market", book, TokenType.YES) is not None


def test_inventory_probe_blocks_quoting_when_holding_position():
    """If we already hold this leg, don't pile on more (anti-adverse-selection)."""
    engine = _engine()
    book = _StubBook(best_bid=0.42, best_ask=0.48)

    # Without probe: signal emits.
    assert engine._check_mm_token("m1", book, TokenType.YES) is not None

    # With probe returning a non-zero position: suppressed.
    engine.set_position_probe(lambda mid, tt: 5.0)
    # Re-emit by clearing the cooldown that the prior call set.
    engine._opportunity_cooldown.clear()
    assert engine._check_mm_token("m1", book, TokenType.YES) is None


def test_inventory_probe_zero_position_does_not_block():
    engine = _engine()
    engine.set_position_probe(lambda mid, tt: 0.0)
    book = _StubBook(best_bid=0.42, best_ask=0.48)
    assert engine._check_mm_token("m1", book, TokenType.YES) is not None


def test_inventory_probe_exception_is_safe():
    """A buggy probe must not crash the strategy loop."""
    engine = _engine()
    def _broken_probe(mid, tt):
        raise RuntimeError("portfolio not ready")
    engine.set_position_probe(_broken_probe)
    book = _StubBook(best_bid=0.42, best_ask=0.48)
    # Falls back to no-position assumption -> signal emits.
    assert engine._check_mm_token("m1", book, TokenType.YES) is not None
