"""
Tests for execution queue behavior and backpressure.
"""

import pytest

from core.execution import ExecutionConfig, ExecutionEngine
from core.portfolio import Portfolio
from core.risk_manager import RiskConfig, RiskManager
from polymarket_client.models import Signal


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
