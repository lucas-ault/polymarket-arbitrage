from __future__ import annotations

from datetime import datetime

import pytest

from core.portfolio import Portfolio
from core.auto_take_profit import AutoTakeProfitConfig, AutoTakeProfitMonitor
from dashboard.integration import DashboardIntegration
from dashboard.server import dashboard_state
from polymarket_client.models import Market, MarketState, OrderBook, TokenType


class _DummyClient:
    def get_runtime_stats(self) -> dict:
        return {}


class _DummyFeed:
    def __init__(self, state: MarketState):
        self._state = state
        self.client = _DummyClient()
        self.market_ids = [state.market.market_id]
        self.update_count = 0
        self.is_running = True

    def consume_changed_market_states(self, limit=None):
        return {}

    def get_all_market_states(self):
        return {self._state.market.market_id: self._state}

    def get_market_state(self, market_id: str):
        if market_id == self._state.market.market_id:
            return self._state
        return None

    def get_market(self, market_id: str):
        if market_id == self._state.market.market_id:
            return self._state.market
        return None

    def get_staleness_summary(self):
        return {
            "avg_staleness_seconds": 0.0,
            "max_staleness_seconds": 0.0,
            "p95_staleness_seconds": 0.0,
        }

    def get_runtime_stats(self):
        return {
            "avg_state_update_ms": 0.0,
            "avg_callback_ms": 0.0,
            "stream_reconnects": 0.0,
            "stream_errors": 0.0,
        }


@pytest.mark.asyncio
async def test_update_state_serializes_portfolio_positions():
    dashboard_state.markets = {}
    dashboard_state.portfolio = {}
    dashboard_state.operational = {}

    market = Market(
        market_id="m1",
        condition_id="m1",
        question="Will Team A win?",
        yes_price=0.55,
        no_price=0.45,
    )
    state = MarketState(
        market=market,
        order_book=OrderBook(market_id="m1"),
        timestamp=datetime.utcnow(),
    )
    feed = _DummyFeed(state)

    portfolio = Portfolio()
    portfolio.seed_position("m1", TokenType.YES, 2.0, 0.40)

    integration = DashboardIntegration(data_feed=feed, portfolio=portfolio)
    await integration._update_state()

    positions = dashboard_state.portfolio["positions"]
    assert len(positions) == 1
    assert positions[0]["market_id"] == "m1"
    assert positions[0]["question"] == "Will Team A win?"
    assert positions[0]["token_type"] == "yes"
    assert positions[0]["size"] == 2.0
    assert positions[0]["avg_entry_price"] == 0.40


@pytest.mark.asyncio
async def test_update_state_includes_take_profit_status():
    dashboard_state.markets = {}
    dashboard_state.portfolio = {}
    dashboard_state.operational = {}

    market = Market(
        market_id="m1",
        condition_id="m1",
        question="Will Team A win?",
        yes_price=0.55,
        no_price=0.45,
    )
    state = MarketState(
        market=market,
        order_book=OrderBook(market_id="m1"),
        timestamp=datetime.utcnow(),
    )
    state.order_book.yes.bids.levels = []
    state.order_book.yes.asks.levels = []
    state.order_book.no.bids.levels = []
    state.order_book.no.asks.levels = []
    feed = _DummyFeed(state)

    portfolio = Portfolio()
    portfolio.seed_position("m1", TokenType.YES, 2.0, 0.40)

    class _Exec:
        def get_open_orders(self, market_id=None):
            return []

    monitor = AutoTakeProfitMonitor(
        AutoTakeProfitConfig(enabled=True, min_net_profit_usd=0.20, cooldown_seconds=15.0),
        _Exec(),
        portfolio,
        fee_theta_taker=0.05,
    )
    state.order_book.yes.bids.levels = [type("L", (), {"price": 0.45, "size": 1.0})()]
    monitor.maybe_submit_for_market("m1", state)

    integration = DashboardIntegration(
        data_feed=feed,
        portfolio=portfolio,
        auto_take_profit_monitor=monitor,
    )
    await integration._update_state()

    positions = dashboard_state.portfolio["positions"]
    assert len(positions) == 1
    assert positions[0]["take_profit_threshold_usd"] == 0.20
    assert "take_profit_progress_pct" in positions[0]
    assert "take_profit_net_profit_est" in positions[0]
