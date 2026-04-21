import pytest
from types import SimpleNamespace

from run_with_dashboard import TradingBotWithDashboard
from dashboard.server import dashboard_state
from polymarket_client.models import OrderSide, TokenType, Trade


def _market_state(
    best_bid_yes=None,
    best_ask_yes=None,
    best_bid_no=None,
    best_ask_no=None,
    yes_price=0.0,
    no_price=0.0,
):
    return SimpleNamespace(
        order_book=SimpleNamespace(
            best_bid_yes=best_bid_yes,
            best_ask_yes=best_ask_yes,
            best_bid_no=best_bid_no,
            best_ask_no=best_ask_no,
        ),
        market=SimpleNamespace(
            yes_price=yes_price,
            no_price=no_price,
        ),
    )


def test_estimate_token_mark_prices_prefers_midpoint():
    state = _market_state(
        best_bid_yes=0.42,
        best_ask_yes=0.46,
        best_bid_no=0.54,
        best_ask_no=0.58,
    )
    bot = TradingBotWithDashboard.__new__(TradingBotWithDashboard)

    prices = bot._estimate_token_mark_prices(state)

    assert prices is not None
    yes_price, no_price = prices
    assert yes_price == 0.44
    assert no_price == 0.56


def test_estimate_token_mark_prices_uses_complement_when_one_side_missing():
    state = _market_state(best_bid_yes=0.61, best_ask_yes=0.63)
    bot = TradingBotWithDashboard.__new__(TradingBotWithDashboard)

    prices = bot._estimate_token_mark_prices(state)

    assert prices is not None
    yes_price, no_price = prices
    assert yes_price == 0.62
    assert no_price == 0.38


def test_live_fill_bootstrap_marks_existing_trades_as_seen():
    bot = TradingBotWithDashboard.__new__(TradingBotWithDashboard)
    bot._seen_trade_ids = set()
    bot._live_fill_bootstrapped = False

    existing = [
        Trade(
            trade_id="t-1",
            order_id="o-1",
            market_id="m-1",
            token_type=TokenType.YES,
            side=OrderSide.BUY,
            price=0.5,
            size=1.0,
        ),
        Trade(
            trade_id="t-2",
            order_id="o-2",
            market_id="m-1",
            token_type=TokenType.YES,
            side=OrderSide.BUY,
            price=0.5,
            size=1.0,
        ),
    ]

    for trade in existing:
        if trade.trade_id:
            bot._seen_trade_ids.add(trade.trade_id)
    bot._live_fill_bootstrapped = True

    assert bot._live_fill_bootstrapped is True
    assert bot._seen_trade_ids == {"t-1", "t-2"}


@pytest.mark.asyncio
async def test_start_matching_task_uses_latest_market_snapshot():
    dashboard_state.cross_platform = {}
    bot = TradingBotWithDashboard.__new__(TradingBotWithDashboard)
    bot.data_feed = SimpleNamespace(_markets={"m1": object(), "m2": object()})
    bot._kalshi_markets = [object()]
    bot._matching_task = None
    bot.config = SimpleNamespace(
        mode=SimpleNamespace(cross_platform_match_start_delay_seconds=0.0)
    )

    seen = {}

    async def _fake_load_cached():
        seen["loaded_cached"] = True

    async def _fake_run_matching(polymarket_markets, *, reason="startup"):
        seen["count"] = len(polymarket_markets)
        seen["reason"] = reason

    bot._load_cached_matches = _fake_load_cached
    bot._run_matching_background = _fake_run_matching

    started = await bot._start_matching_task(
        reason="periodic refresh",
        load_cached=True,
        apply_start_delay=False,
    )
    await bot._matching_task

    assert started is True
    assert seen["loaded_cached"] is True
    assert seen["count"] == 2
    assert seen["reason"] == "periodic refresh"
    assert dashboard_state.cross_platform["polymarket_markets"] == 2
