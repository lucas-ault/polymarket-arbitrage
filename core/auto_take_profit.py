"""
Auto take-profit monitor for live positions.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from core.portfolio import Portfolio, PortfolioPosition
from core.execution import ExecutionEngine
from polymarket_client.models import OrderSide, Signal, TokenType
from utils.polymarket_fees import polymarket_fee


logger = logging.getLogger(__name__)


@dataclass
class AutoTakeProfitConfig:
    """Configuration for auto take-profit exits."""

    enabled: bool = False
    min_net_profit_usd: float = 0.20
    cooldown_seconds: float = 15.0


class AutoTakeProfitMonitor:
    """Monitors held positions and submits take-profit exits."""

    def __init__(
        self,
        config: AutoTakeProfitConfig,
        execution_engine: ExecutionEngine,
        portfolio: Portfolio,
        *,
        fee_theta_taker: float,
    ) -> None:
        self.config = config
        self.execution_engine = execution_engine
        self.portfolio = portfolio
        self.fee_theta_taker = float(fee_theta_taker)
        self._cooldowns_until: dict[tuple[str, TokenType], datetime] = {}
        self._last_statuses: dict[tuple[str, TokenType], dict[str, Any]] = {}

    def maybe_submit_for_market(self, market_id: str, market_state) -> int:
        """Submit take-profit exits for profitable positions in a market."""
        if not self.config.enabled or not market_state:
            return 0

        triggered = 0
        now = datetime.utcnow()
        positions = self.portfolio.get_all_positions().get(market_id, {}) or {}
        active_keys: set[tuple[str, TokenType]] = set()
        for token_type, position in positions.items():
            size = float(getattr(position, "size", 0.0) or 0.0)
            if size == 0:
                continue

            cooldown_key = (market_id, token_type)
            active_keys.add(cooldown_key)
            cooldown_until = self._cooldowns_until.get(cooldown_key)

            exit_side = OrderSide.SELL if size > 0 else OrderSide.BUY
            exit_price = self._exit_price(market_state, token_type, exit_side)
            if exit_price is None or exit_price <= 0:
                self._last_statuses.pop(cooldown_key, None)
                continue

            net_profit = self._estimate_net_exit_profit(position, exit_price)
            self._last_statuses[cooldown_key] = self._build_status(
                market_id=market_id,
                token_type=token_type,
                position=position,
                exit_price=exit_price,
                net_profit=net_profit,
                cooldown_until=cooldown_until,
                has_open_take_profit=self._has_open_take_profit_order(market_id, token_type),
            )

            if cooldown_until is not None and now < cooldown_until:
                continue

            if self._has_open_take_profit_order(market_id, token_type):
                continue

            if net_profit < self.config.min_net_profit_usd:
                continue

            cancel_signal = self.execution_engine.build_cancel_signal(
                market_id=market_id,
                strategy_tag="market_making",
                token_type=token_type,
                priority=35,
            )
            if cancel_signal is not None:
                self.execution_engine.submit_signal_nowait(cancel_signal)

            place_signal = Signal(
                signal_id=f"sig_take_profit_{uuid.uuid4().hex[:12]}",
                action="place_orders",
                market_id=market_id,
                orders=[
                    {
                        "token_type": token_type,
                        "side": exit_side,
                        "price": exit_price,
                        "size": abs(size),
                        "strategy_tag": "take_profit",
                    }
                ],
                priority=30,
            )
            if self.execution_engine.submit_signal_nowait(place_signal):
                self._cooldowns_until[cooldown_key] = now + timedelta(
                    seconds=max(1.0, float(self.config.cooldown_seconds))
                )
                triggered += 1
                logger.info(
                    "Auto take-profit triggered: %s/%s | size=%.2f | entry=%.4f | exit=%.4f | net_est=$%.2f",
                    market_id,
                    token_type.value,
                    abs(size),
                    float(getattr(position, "avg_entry_price", 0.0) or 0.0),
                    exit_price,
                    net_profit,
                )
        for key in list(self._last_statuses.keys()):
            if key[0] == market_id and key not in active_keys:
                self._last_statuses.pop(key, None)
        return triggered

    def get_position_status(
        self,
        market_id: str,
        token_type: TokenType,
    ) -> Optional[dict[str, Any]]:
        """Return the latest cached take-profit status for a position."""
        status = self._last_statuses.get((market_id, token_type))
        return dict(status) if status is not None else None

    def get_close_candidates(self, limit: int = 3) -> list[dict[str, Any]]:
        """Return positions nearest to their configured take-profit threshold."""
        candidates = [
            status for status in self._last_statuses.values()
            if not bool(status.get("triggered"))
            and not bool(status.get("has_open_take_profit_order"))
            and float(status.get("size_abs", 0.0) or 0.0) > 0
            and status.get("exit_price") is not None
        ]
        candidates.sort(
            key=lambda status: (
                float(status.get("distance_usd", float("inf"))),
                -float(status.get("progress_pct", 0.0)),
            )
        )
        return [dict(item) for item in candidates[: max(0, int(limit))]]

    def _has_open_take_profit_order(self, market_id: str, token_type: TokenType) -> bool:
        """Return True when a take-profit order already exists for this leg."""
        for order in self.execution_engine.get_open_orders(market_id=market_id):
            if order.token_type != token_type:
                continue
            if order.strategy_tag == "take_profit":
                return True
        return False

    @staticmethod
    def _exit_price(market_state, token_type: TokenType, side: OrderSide) -> Optional[float]:
        """Pick a realistic exit price from the current top-of-book."""
        order_book = getattr(market_state, "order_book", None)
        if order_book is None:
            return None
        if token_type == TokenType.YES:
            return (
                float(order_book.best_bid_yes)
                if side == OrderSide.SELL and order_book.best_bid_yes is not None
                else float(order_book.best_ask_yes)
                if side == OrderSide.BUY and order_book.best_ask_yes is not None
                else None
            )
        return (
            float(order_book.best_bid_no)
            if side == OrderSide.SELL and order_book.best_bid_no is not None
            else float(order_book.best_ask_no)
            if side == OrderSide.BUY and order_book.best_ask_no is not None
            else None
        )

    def _estimate_net_exit_profit(self, position: PortfolioPosition, exit_price: float) -> float:
        """Estimate full-exit PnL after entry and exit fees."""
        size = abs(float(getattr(position, "size", 0.0) or 0.0))
        if size <= 0:
            return 0.0
        entry_price = float(getattr(position, "avg_entry_price", 0.0) or 0.0)
        if entry_price <= 0:
            return 0.0

        if float(getattr(position, "size", 0.0) or 0.0) > 0:
            gross = size * (float(exit_price) - entry_price)
        else:
            gross = size * (entry_price - float(exit_price))

        entry_fee = polymarket_fee(
            theta=self.fee_theta_taker,
            contracts=size,
            price=entry_price,
            round_to_cents=False,
        )
        exit_fee = polymarket_fee(
            theta=self.fee_theta_taker,
            contracts=size,
            price=float(exit_price),
            round_to_cents=False,
        )
        return gross - entry_fee - exit_fee

    def _build_status(
        self,
        *,
        market_id: str,
        token_type: TokenType,
        position: PortfolioPosition,
        exit_price: float,
        net_profit: float,
        cooldown_until: Optional[datetime],
        has_open_take_profit: bool,
    ) -> dict[str, Any]:
        """Build a dashboard/log-friendly take-profit status payload."""
        threshold = float(self.config.min_net_profit_usd)
        size = float(getattr(position, "size", 0.0) or 0.0)
        size_abs = abs(size)
        entry_price = float(getattr(position, "avg_entry_price", 0.0) or 0.0)
        distance_usd = threshold - float(net_profit)
        progress_pct = 100.0
        if threshold > 0:
            progress_pct = max(0.0, min(100.0, (float(net_profit) / threshold) * 100.0))
        return {
            "market_id": market_id,
            "token_type": token_type.value,
            "size": size,
            "size_abs": size_abs,
            "entry_price": entry_price,
            "exit_price": float(exit_price),
            "net_profit_est": float(net_profit),
            "threshold_usd": threshold,
            "distance_usd": distance_usd,
            "progress_pct": progress_pct,
            "triggered": float(net_profit) >= threshold,
            "close_to_take_profit": distance_usd <= max(0.05, threshold * 0.25),
            "has_open_take_profit_order": bool(has_open_take_profit),
            "cooldown_seconds_remaining": (
                max(0.0, (cooldown_until - datetime.utcnow()).total_seconds())
                if cooldown_until is not None
                else 0.0
            ),
        }
