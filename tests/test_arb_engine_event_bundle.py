from __future__ import annotations

from datetime import datetime

from core.arb_engine import ArbConfig, ArbEngine
from polymarket_client.models import (
    Market,
    MarketState,
    OpportunityType,
    OrderBook,
    OrderBookSide,
    PriceLevel,
    TokenOrderBook,
    TokenType,
)


def _state(*, market_id: str, event_id: str, yes_bid: float, yes_ask: float) -> MarketState:
    book = OrderBook(
        market_id=market_id,
        yes=TokenOrderBook(
            token_type=TokenType.YES,
            bids=OrderBookSide(levels=[PriceLevel(price=yes_bid, size=100.0)]),
            asks=OrderBookSide(levels=[PriceLevel(price=yes_ask, size=100.0)]),
        ),
        no=TokenOrderBook(
            token_type=TokenType.NO,
            bids=OrderBookSide(levels=[PriceLevel(price=1.0 - yes_ask, size=100.0)]),
            asks=OrderBookSide(levels=[PriceLevel(price=1.0 - yes_bid, size=100.0)]),
        ),
    )
    return MarketState(
        market=Market(
            market_id=market_id,
            condition_id=market_id,
            question=f"Outcome {market_id}",
            event_id=event_id,
            active=True,
            volume_24h=100000.0,
        ),
        order_book=book,
        timestamp=datetime.utcnow(),
    )


def test_event_bundle_long_signal_emits_multileg_yes_orders():
    engine = ArbEngine(
        ArbConfig(
            min_edge=0.01,
            bundle_arb_enabled=False,
            event_bundle_arb_enabled=True,
            mm_enabled=False,
            fee_theta_taker=0.0,
        )
    )

    assert engine.analyze(_state(market_id="m1", event_id="e1", yes_bid=0.38, yes_ask=0.40)) == []
    signals = engine.analyze(_state(market_id="m2", event_id="e1", yes_bid=0.42, yes_ask=0.45))

    assert len(signals) == 1
    signal = signals[0]
    assert signal.opportunity is not None
    assert signal.opportunity.opportunity_type == OpportunityType.EVENT_BUNDLE_LONG
    assert signal.opportunity.edge > 0
    assert len(signal.orders) == 2
    assert {o["market_id"] for o in signal.orders} == {"m1", "m2"}
    assert {o["strategy_tag"] for o in signal.orders} == {"event_bundle_arb"}


def test_event_bundle_long_respects_min_edge_threshold():
    engine = ArbEngine(
        ArbConfig(
            min_edge=0.02,
            bundle_arb_enabled=False,
            event_bundle_arb_enabled=True,
            mm_enabled=False,
            fee_theta_taker=0.0,
        )
    )

    engine.analyze(_state(market_id="m1", event_id="e1", yes_bid=0.49, yes_ask=0.50))
    signals = engine.analyze(_state(market_id="m2", event_id="e1", yes_bid=0.49, yes_ask=0.50))
    assert signals == []
