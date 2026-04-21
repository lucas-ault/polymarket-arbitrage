"""
Tests for execution queue behavior and backpressure.
"""

import pytest

from core.execution import ExecutionConfig, ExecutionEngine
from core.portfolio import Portfolio
from core.risk_manager import RiskConfig, RiskManager
from polymarket_client.models import Order, OrderSide, Signal, TokenType


class DummyClient:
    async def place_order(self, *args, **kwargs):
        raise RuntimeError("not used in this test")

    async def cancel_order(self, *args, **kwargs):
        return None


def _build_engine(max_queue: int) -> ExecutionEngine:
    return ExecutionEngine(
        client=DummyClient(),
        risk_manager=RiskManager(RiskConfig(trade_only_high_volume=False)),
        portfolio=Portfolio(initial_balance=1000.0),
        config=ExecutionConfig(
            dry_run=True,
            max_signal_queue_size=max_queue,
        ),
    )


def test_submit_signal_nowait_drops_when_queue_full():
    engine = _build_engine(max_queue=1)
    signal_a = Signal(signal_id="s1", action="place_orders", market_id="m1")
    signal_b = Signal(signal_id="s2", action="place_orders", market_id="m2")

    assert engine.submit_signal_nowait(signal_a) is True
    assert engine.submit_signal_nowait(signal_b) is False
    assert engine.stats.signal_queue_drops == 1
    assert engine.signal_queue_size == 1
    assert engine.stats.max_signal_queue_depth == 1


@pytest.mark.asyncio
async def test_submit_signal_tracks_queue_depth():
    engine = _build_engine(max_queue=3)
    await engine.submit_signal(Signal(signal_id="s1", action="place_orders", market_id="m1"))
    await engine.submit_signal(Signal(signal_id="s2", action="place_orders", market_id="m1"))

    assert engine.signal_queue_size == 2
    assert engine.stats.max_signal_queue_depth == 2


@pytest.mark.asyncio
async def test_cancel_order_passes_market_slug_for_live_cancel():
    class CapturingClient(DummyClient):
        def __init__(self) -> None:
            self.calls = []

        async def cancel_order(self, order_id, market_slug=None):
            self.calls.append((order_id, market_slug))
            return None

    client = CapturingClient()
    engine = ExecutionEngine(
        client=client,
        risk_manager=RiskManager(RiskConfig(trade_only_high_volume=False)),
        portfolio=Portfolio(initial_balance=1000.0),
        config=ExecutionConfig(dry_run=False),
    )
    engine._open_orders["ord-1"] = Order(
        order_id="ord-1",
        market_id="m-1",
        market_slug="slug-1",
        token_type=TokenType.YES,
        side=OrderSide.BUY,
        price=0.5,
        size=1.0,
    )

    cancelled = await engine.cancel_order("ord-1")

    assert cancelled is True
    assert client.calls == [("ord-1", "slug-1")]


def test_build_cancel_signal_filters_market_strategy_and_token():
    engine = _build_engine(max_queue=3)
    engine._track_order(Order(
        order_id="ord-yes-mm",
        market_id="m-1",
        token_type=TokenType.YES,
        side=OrderSide.BUY,
        price=0.5,
        size=1.0,
        strategy_tag="market_making",
    ))
    engine._track_order(Order(
        order_id="ord-no-mm",
        market_id="m-1",
        token_type=TokenType.NO,
        side=OrderSide.BUY,
        price=0.5,
        size=1.0,
        strategy_tag="market_making",
    ))
    engine._track_order(Order(
        order_id="ord-other",
        market_id="m-1",
        token_type=TokenType.YES,
        side=OrderSide.BUY,
        price=0.5,
        size=1.0,
        strategy_tag="bundle_arb",
    ))

    signal = engine.build_cancel_signal(
        market_id="m-1",
        strategy_tag="market_making",
        token_type=TokenType.YES,
    )

    assert signal is not None
    assert signal.action == "cancel_orders"
    assert signal.cancel_order_ids == ["ord-yes-mm"]
