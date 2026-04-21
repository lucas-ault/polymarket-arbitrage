"""Tests for the bundle-arb diagnostic counters.

These counters answer the operator question "why are we generating zero
signals?" by distinguishing books with insufficient data (one-sided) from
books that are healthy but tightly priced.
"""

from __future__ import annotations

import pytest

from polymarket_client.models import (
    OrderBook,
    OrderBookSide,
    PriceLevel,
    TokenOrderBook,
    TokenType,
)
from core.arb_engine import ArbEngine, ArbConfig


def _book(
    *,
    yes_bid: float | None,
    yes_ask: float | None,
    no_bid: float | None,
    no_ask: float | None,
    size: float = 100.0,
) -> OrderBook:
    yes_bids = (
        OrderBookSide(levels=[PriceLevel(price=yes_bid, size=size)])
        if yes_bid is not None
        else OrderBookSide()
    )
    yes_asks = (
        OrderBookSide(levels=[PriceLevel(price=yes_ask, size=size)])
        if yes_ask is not None
        else OrderBookSide()
    )
    no_bids = (
        OrderBookSide(levels=[PriceLevel(price=no_bid, size=size)])
        if no_bid is not None
        else OrderBookSide()
    )
    no_asks = (
        OrderBookSide(levels=[PriceLevel(price=no_ask, size=size)])
        if no_ask is not None
        else OrderBookSide()
    )
    return OrderBook(
        market_id="m1",
        yes=TokenOrderBook(token_type=TokenType.YES, bids=yes_bids, asks=yes_asks),
        no=TokenOrderBook(token_type=TokenType.NO, bids=no_bids, asks=no_asks),
    )


@pytest.fixture
def engine() -> ArbEngine:
    return ArbEngine(
        ArbConfig(
            min_edge=0.005,
            bundle_arb_enabled=True,
            mm_enabled=False,
            fee_theta_taker=0.0,
            fee_theta_maker=0.0,
        )
    )


def test_one_sided_book_increments_missing_leg(engine: ArbEngine):
    """A book missing the NO side should be counted as 'missing leg', not 'no edge'."""
    book = _book(yes_bid=0.40, yes_ask=0.45, no_bid=None, no_ask=None)

    result = engine._check_bundle_arbitrage("m1", book)

    assert result is None
    assert engine.stats.bundle_scans_total == 1
    assert engine.stats.bundle_skipped_missing_leg == 1
    assert engine.stats.bundle_skipped_no_edge == 0


def test_tight_two_sided_book_increments_no_edge(engine: ArbEngine):
    """A healthy book whose total_ask ~= total_bid ~= 1.0 should be 'no edge'."""
    # YES + NO asks = 1.02 (no long opp), bids = 0.98 (no short opp).
    book = _book(yes_bid=0.49, yes_ask=0.51, no_bid=0.49, no_ask=0.51)

    result = engine._check_bundle_arbitrage("m1", book)

    assert result is None
    assert engine.stats.bundle_scans_total == 1
    assert engine.stats.bundle_skipped_missing_leg == 0
    assert engine.stats.bundle_skipped_no_edge == 1


def test_best_observed_gross_edges_track_max(engine: ArbEngine):
    """best_gross_long/short should reflect the closest-to-arb book seen so far."""
    # Tight book: total_ask = 1.02 -> gross_long = -0.02
    engine._check_bundle_arbitrage(
        "m1", _book(yes_bid=0.49, yes_ask=0.51, no_bid=0.49, no_ask=0.51)
    )
    # Tighter book: total_ask = 1.005 -> gross_long = -0.005 (closer to arb)
    engine._check_bundle_arbitrage(
        "m1", _book(yes_bid=0.495, yes_ask=0.50, no_bid=0.495, no_ask=0.505)
    )
    # Even-wider book (total_ask = 1.10) - should NOT lower best_gross_long
    engine._check_bundle_arbitrage(
        "m1", _book(yes_bid=0.40, yes_ask=0.55, no_bid=0.40, no_ask=0.55)
    )

    # Best gross long is from the tightest two-sided book (closer to zero is better).
    assert engine.stats.bundle_best_gross_long == pytest.approx(-0.005, abs=1e-9)
    # Best gross short tracks max(total_bid - 1). All bids are < 0.5 so short edge is negative.
    assert engine.stats.bundle_best_gross_short < 0


def test_real_arb_opportunity_increments_signals_not_no_edge(engine: ArbEngine):
    """A book with a real bundle-long edge should fire a signal, not a no-edge skip."""
    # total_ask = 0.95 (5c gross edge, well above 0.5c min_edge with 0 fees)
    book = _book(yes_bid=0.40, yes_ask=0.45, no_bid=0.45, no_ask=0.50)

    result = engine._check_bundle_arbitrage("m1", book)

    assert result is not None
    assert engine.stats.bundle_scans_total == 1
    assert engine.stats.bundle_skipped_missing_leg == 0
    assert engine.stats.bundle_skipped_no_edge == 0
    assert engine.stats.bundle_opportunities_detected == 1
