from types import SimpleNamespace

from core.auto_take_profit import AutoTakeProfitConfig, AutoTakeProfitMonitor
from core.portfolio import Portfolio
from polymarket_client.models import Order, OrderSide, OrderStatus, Signal, TokenType


def _market_state(
    best_bid_yes=None,
    best_ask_yes=None,
    best_bid_no=None,
    best_ask_no=None,
):
    return SimpleNamespace(
        order_book=SimpleNamespace(
            best_bid_yes=best_bid_yes,
            best_ask_yes=best_ask_yes,
            best_bid_no=best_bid_no,
            best_ask_no=best_ask_no,
        )
    )


class FakeExecutionEngine:
    def __init__(self, open_orders=None):
        self._open_orders = list(open_orders or [])
        self.submitted: list[Signal] = []

    def get_open_orders(self, market_id=None):
        if market_id is None:
            return list(self._open_orders)
        return [order for order in self._open_orders if order.market_id == market_id]

    def build_cancel_signal(
        self,
        *,
        market_id,
        strategy_tag=None,
        token_type=None,
        quote_group_id="",
        priority=9,
    ):
        matching = [
            order
            for order in self.get_open_orders(market_id=market_id)
            if (not strategy_tag or order.strategy_tag == strategy_tag)
            and (token_type is None or order.token_type == token_type)
        ]
        if not matching:
            return None
        return Signal(
            signal_id="cancel-1",
            action="cancel_orders",
            market_id=market_id,
            cancel_order_ids=[order.order_id for order in matching],
            priority=priority,
        )

    def submit_signal_nowait(self, signal):
        self.submitted.append(signal)
        return True


def test_auto_take_profit_triggers_for_profitable_position():
    portfolio = Portfolio()
    portfolio.seed_position("m1", TokenType.YES, size=1.0, avg_entry_price=0.40)
    execution = FakeExecutionEngine(
        open_orders=[
            Order(
                order_id="mm-1",
                market_id="m1",
                token_type=TokenType.YES,
                side=OrderSide.BUY,
                price=0.39,
                size=1.0,
                status=OrderStatus.OPEN,
                strategy_tag="market_making",
            )
        ]
    )
    monitor = AutoTakeProfitMonitor(
        AutoTakeProfitConfig(enabled=True, min_net_profit_usd=0.20, cooldown_seconds=15.0),
        execution,
        portfolio,
        fee_theta_taker=0.05,
    )

    triggered = monitor.maybe_submit_for_market(
        "m1",
        _market_state(best_bid_yes=0.70, best_ask_yes=0.71, best_bid_no=0.29, best_ask_no=0.30),
    )

    assert triggered == 1
    assert len(execution.submitted) == 2
    assert execution.submitted[0].action == "cancel_orders"
    place_signal = execution.submitted[1]
    assert place_signal.orders[0]["side"] == OrderSide.SELL
    assert place_signal.orders[0]["token_type"] == TokenType.YES
    assert place_signal.orders[0]["price"] == 0.70
    assert place_signal.orders[0]["strategy_tag"] == "take_profit"


def test_auto_take_profit_respects_net_profit_threshold():
    portfolio = Portfolio()
    portfolio.seed_position("m1", TokenType.YES, size=1.0, avg_entry_price=0.40)
    execution = FakeExecutionEngine()
    monitor = AutoTakeProfitMonitor(
        AutoTakeProfitConfig(enabled=True, min_net_profit_usd=0.20, cooldown_seconds=15.0),
        execution,
        portfolio,
        fee_theta_taker=0.05,
    )

    triggered = monitor.maybe_submit_for_market(
        "m1",
        _market_state(best_bid_yes=0.60, best_ask_yes=0.61, best_bid_no=0.39, best_ask_no=0.40),
    )

    assert triggered == 0
    assert execution.submitted == []


def test_auto_take_profit_skips_when_exit_order_already_open():
    portfolio = Portfolio()
    portfolio.seed_position("m1", TokenType.YES, size=1.0, avg_entry_price=0.40)
    execution = FakeExecutionEngine(
        open_orders=[
            Order(
                order_id="tp-1",
                market_id="m1",
                token_type=TokenType.YES,
                side=OrderSide.SELL,
                price=0.70,
                size=1.0,
                status=OrderStatus.OPEN,
                strategy_tag="take_profit",
            )
        ]
    )
    monitor = AutoTakeProfitMonitor(
        AutoTakeProfitConfig(enabled=True, min_net_profit_usd=0.20, cooldown_seconds=15.0),
        execution,
        portfolio,
        fee_theta_taker=0.05,
    )

    triggered = monitor.maybe_submit_for_market(
        "m1",
        _market_state(best_bid_yes=0.70, best_ask_yes=0.71, best_bid_no=0.29, best_ask_no=0.30),
    )

    assert triggered == 0
    assert execution.submitted == []
