from types import SimpleNamespace

from run_with_dashboard import TradingBotWithDashboard
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
