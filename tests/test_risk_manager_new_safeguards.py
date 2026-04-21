"""
Tests for the new RiskManager safeguards added during the live-readiness pass.

Specifically:
- market staleness gating
- kill-switch callback (auto_unwind_on_breach)
- volume gating via DataFeed-style register_market_state
"""

import asyncio
from datetime import datetime, timedelta

import pytest

from core.risk_manager import RiskConfig, RiskManager
from polymarket_client.models import Order, OrderSide, OrderStatus, TokenType


def _order(market_id: str = "m1", side: OrderSide = OrderSide.BUY) -> Order:
    return Order(
        order_id="o1",
        market_id=market_id,
        token_type=TokenType.YES,
        side=side,
        price=0.5,
        size=10.0,
        status=OrderStatus.PENDING,
    )


@pytest.fixture
def base_config() -> RiskConfig:
    return RiskConfig(
        max_position_per_market=100.0,
        max_global_exposure=10_000.0,
        max_daily_loss=100.0,
        max_drawdown_pct=0.5,
        trade_only_high_volume=False,
        kill_switch_enabled=True,
        max_market_staleness_seconds=2.0,
        auto_unwind_on_breach=True,
    )


def test_stale_market_rejection(base_config):
    rm = RiskManager(base_config)
    stale_ts = datetime.utcnow() - timedelta(seconds=10)
    rm._market_freshness["m1"] = stale_ts  # noqa: SLF001 — direct seed for test

    assert rm.check_order(_order("m1")) is False
    assert rm.get_summary()["stale_market_rejections"] == 1


def test_fresh_market_passes(base_config):
    rm = RiskManager(base_config)
    rm._market_freshness["m1"] = datetime.utcnow()  # noqa: SLF001
    assert rm.check_order(_order("m1")) is True


def test_no_freshness_record_does_not_block(base_config):
    """If we've never registered a market, gating should not block; the order
    will still be subject to other risk checks but should not be rejected
    purely because we lack freshness data."""
    rm = RiskManager(base_config)
    assert rm.check_order(_order("never_seen")) is True


@pytest.mark.asyncio
async def test_kill_switch_callback_invoked(base_config):
    rm = RiskManager(base_config)
    fired = asyncio.Event()
    received_reasons: list[str] = []

    async def cb(reason: str):
        received_reasons.append(reason)
        fired.set()

    rm.set_kill_switch_callback(cb)
    rm._trigger_kill_switch("test breach")  # noqa: SLF001

    await asyncio.wait_for(fired.wait(), timeout=1.0)
    assert rm.state.kill_switch_triggered
    assert received_reasons == ["test breach"]


@pytest.mark.asyncio
async def test_kill_switch_callback_only_fires_once(base_config):
    rm = RiskManager(base_config)
    call_count = {"n": 0}

    async def cb(_reason: str):
        call_count["n"] += 1

    rm.set_kill_switch_callback(cb)
    rm._trigger_kill_switch("first")  # noqa: SLF001
    rm._trigger_kill_switch("second")  # noqa: SLF001

    await asyncio.sleep(0.05)
    assert call_count["n"] == 1


def test_register_market_state_updates_volume_and_freshness(base_config):
    """register_market_state should populate both caches so the risk manager
    can gate by volume and staleness without explicit setup."""

    class _Market:
        def __init__(self):
            self.market_id = "m1"
            self.volume_24h = 50_000.0

    class _State:
        def __init__(self):
            self.market = _Market()
            self.timestamp = datetime.utcnow()

    rm = RiskManager(base_config)
    rm.register_market_state(_State())

    assert rm._market_volumes["m1"] == 50_000.0  # noqa: SLF001
    assert "m1" in rm._market_freshness  # noqa: SLF001
