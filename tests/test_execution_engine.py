"""
Tests for execution queue behavior and backpressure.
"""

from datetime import datetime, timedelta

import pytest

from core.execution import ExecutionConfig, ExecutionEngine
from core.portfolio import Portfolio
from core.risk_manager import RiskConfig, RiskManager
from polymarket_client.models import Order, OrderSide, Signal, TokenType


class DummyClient:
    async def place_order(self, *args, **kwargs):
        raise RuntimeError("not used in this test")

    async def preview_order(self, *args, **kwargs):
        return {"ok": True}

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
        strategy_tag="market_making",
    )
    cancelled_strategies = []
    engine.set_cancel_callback(cancelled_strategies.append)

    cancelled = await engine.cancel_order("ord-1")

    assert cancelled is True
    assert client.calls == [("ord-1", "slug-1")]
    assert cancelled_strategies == ["market_making"]


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


@pytest.mark.asyncio
async def test_group_cancel_signal_resolves_open_orders_at_execution_time():
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
    engine._track_order(Order(
        order_id="ord-buy",
        market_id="m-1",
        market_slug="slug-1",
        token_type=TokenType.YES,
        side=OrderSide.BUY,
        price=0.5,
        size=1.0,
        strategy_tag="market_making",
        quote_group_id="group-a",
    ))
    engine._track_order(Order(
        order_id="ord-sell",
        market_id="m-1",
        market_slug="slug-1",
        token_type=TokenType.YES,
        side=OrderSide.SELL,
        price=0.51,
        size=1.0,
        strategy_tag="market_making",
        quote_group_id="group-a",
    ))
    engine._track_order(Order(
        order_id="ord-newer",
        market_id="m-1",
        market_slug="slug-1",
        token_type=TokenType.YES,
        side=OrderSide.BUY,
        price=0.49,
        size=1.0,
        strategy_tag="market_making",
        quote_group_id="group-b",
    ))

    signal = engine.build_cancel_signal(
        market_id="m-1",
        strategy_tag="market_making",
        token_type=TokenType.YES,
        quote_group_id="group-a",
    )

    assert signal is not None
    assert signal.cancel_order_ids == []
    await engine._handle_cancel_orders(signal)

    assert client.calls == [("ord-buy", "slug-1"), ("ord-sell", "slug-1")]
    assert "ord-newer" in engine._open_orders


@pytest.mark.asyncio
async def test_handle_place_orders_counts_unplaceable_skip():
    engine = _build_engine(max_queue=3)
    engine._unplaceable_until["m-1"] = datetime.utcnow() + timedelta(seconds=60)

    signal = Signal(
        signal_id="s1",
        action="place_orders",
        market_id="m-1",
        orders=[
            {
                "token_type": TokenType.YES,
                "side": OrderSide.BUY,
                "price": 0.5,
                "size": 1.0,
            }
        ],
    )

    await engine._handle_place_orders(signal)

    assert engine.stats.signals_rejected == 1
    assert engine.stats.unplaceable_signal_skips == 1
    assert engine.unplaceable_market_count == 1


@pytest.mark.asyncio
async def test_ioc_taker_orders_use_short_timeout_and_stats():
    class CapturingClient(DummyClient):
        async def place_order(self, **kwargs):
            return Order(
                order_id="ioc-1",
                market_id=kwargs["market_id"],
                token_type=kwargs["token_type"],
                side=kwargs["side"],
                price=kwargs["price"],
                size=kwargs["size"],
                strategy_tag=kwargs.get("strategy_tag", ""),
                order_type=kwargs.get("order_type", "ORDER_TYPE_LIMIT"),
                time_in_force=kwargs.get("time_in_force", "TIME_IN_FORCE_GOOD_TILL_CANCEL"),
            )

    engine = ExecutionEngine(
        client=CapturingClient(),
        risk_manager=RiskManager(RiskConfig(trade_only_high_volume=False)),
        portfolio=Portfolio(initial_balance=1000.0),
        config=ExecutionConfig(dry_run=False),
    )
    signal = Signal(
        signal_id="s-ioc",
        action="place_orders",
        market_id="m-1",
        orders=[
            {
                "token_type": TokenType.YES,
                "side": OrderSide.BUY,
                "price": 0.5,
                "size": 1.0,
                "strategy_tag": "taker_entry",
                "order_type": "ORDER_TYPE_LIMIT",
                "time_in_force": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
            }
        ],
    )

    await engine._handle_place_orders(signal)

    assert engine.stats.taker_orders_attempted == 1
    assert engine.open_order_count == 1
    assert engine._order_timeouts_seconds["ioc-1"] == 2.0


@pytest.mark.asyncio
async def test_live_taker_order_preview_rejection_blocks_placement():
    class RejectingPreviewClient(DummyClient):
        def __init__(self) -> None:
            self.place_calls = 0

        async def preview_order(self, *args, **kwargs):
            raise RuntimeError("preview rejected")

        async def place_order(self, **kwargs):
            self.place_calls += 1
            return Order(
                order_id="should-not-place",
                market_id=kwargs["market_id"],
                token_type=kwargs["token_type"],
                side=kwargs["side"],
                price=kwargs["price"],
                size=kwargs["size"],
                strategy_tag=kwargs.get("strategy_tag", ""),
            )

    client = RejectingPreviewClient()
    engine = ExecutionEngine(
        client=client,
        risk_manager=RiskManager(RiskConfig(trade_only_high_volume=False)),
        portfolio=Portfolio(initial_balance=1000.0),
        config=ExecutionConfig(dry_run=False),
    )
    signal = Signal(
        signal_id="s-preview-reject",
        action="place_orders",
        market_id="m-1",
        orders=[
            {
                "token_type": TokenType.YES,
                "side": OrderSide.BUY,
                "price": 0.5,
                "size": 2.0,
                "strategy_tag": "taker_entry",
                "order_type": "ORDER_TYPE_LIMIT",
                "time_in_force": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
            }
        ],
    )

    await engine._handle_place_orders(signal)

    assert client.place_calls == 0
    assert engine.stats.orders_previewed == 1
    assert engine.stats.preview_rejections == 1
    assert engine.stats.signals_rejected == 1


@pytest.mark.asyncio
async def test_multi_market_signal_places_each_leg_on_its_market():
    class CapturingClient(DummyClient):
        def __init__(self) -> None:
            self.markets = []

        async def place_order(self, **kwargs):
            self.markets.append(kwargs["market_id"])
            return Order(
                order_id=f"leg-{len(self.markets)}",
                market_id=kwargs["market_id"],
                token_type=kwargs["token_type"],
                side=kwargs["side"],
                price=kwargs["price"],
                size=kwargs["size"],
                strategy_tag=kwargs.get("strategy_tag", ""),
            )

    client = CapturingClient()
    engine = ExecutionEngine(
        client=client,
        risk_manager=RiskManager(RiskConfig(trade_only_high_volume=False)),
        portfolio=Portfolio(initial_balance=1000.0),
        config=ExecutionConfig(dry_run=False),
    )
    signal = Signal(
        signal_id="s-event-bundle",
        action="place_orders",
        market_id="m-primary",
        orders=[
            {
                "market_id": "m-1",
                "token_type": TokenType.YES,
                "side": OrderSide.BUY,
                "price": 0.42,
                "size": 1.0,
                "strategy_tag": "event_bundle_arb",
            },
            {
                "market_id": "m-2",
                "token_type": TokenType.YES,
                "side": OrderSide.BUY,
                "price": 0.44,
                "size": 1.0,
                "strategy_tag": "event_bundle_arb",
            },
        ],
    )

    await engine._handle_place_orders(signal)

    assert client.markets == ["m-1", "m-2"]
    assert engine.open_order_count == 2


@pytest.mark.asyncio
async def test_urgent_exit_close_position_falls_back_to_ioc_order():
    class CapturingClient(DummyClient):
        def __init__(self) -> None:
            self.close_calls = 0
            self.place_calls = 0

        async def close_position(self, *args, **kwargs):
            self.close_calls += 1
            raise RuntimeError("close-position unavailable")

        async def place_order(self, **kwargs):
            self.place_calls += 1
            return Order(
                order_id="urg-1",
                market_id=kwargs["market_id"],
                token_type=kwargs["token_type"],
                side=kwargs["side"],
                price=kwargs["price"],
                size=kwargs["size"],
                strategy_tag=kwargs.get("strategy_tag", ""),
                order_type=kwargs.get("order_type", "ORDER_TYPE_LIMIT"),
                time_in_force=kwargs.get("time_in_force", "TIME_IN_FORCE_GOOD_TILL_CANCEL"),
            )

    risk = RiskManager(
        RiskConfig(
            trade_only_high_volume=False,
            allow_urgent_exit_after_kill_switch=True,
            allow_urgent_exit_on_stale_data=True,
        )
    )
    risk.update_position("m-1", TokenType.YES, 1.0, 0.5)
    client = CapturingClient()
    engine = ExecutionEngine(
        client=client,
        risk_manager=risk,
        portfolio=Portfolio(initial_balance=1000.0),
        config=ExecutionConfig(dry_run=False),
    )
    signal = Signal(
        signal_id="s-urgent",
        action="place_orders",
        market_id="m-1",
        orders=[
            {
                "token_type": TokenType.YES,
                "side": OrderSide.SELL,
                "price": 0.5,
                "size": 1.0,
                "strategy_tag": "urgent_exit",
                "order_type": "ORDER_TYPE_LIMIT",
                "time_in_force": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
                "close_position": True,
                "slippage_ticks": 3,
            }
        ],
    )

    await engine._handle_place_orders(signal)

    assert client.close_calls == 1
    assert client.place_calls == 1
    assert engine.stats.urgent_exit_attempted == 1
