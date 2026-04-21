"""
Risk Manager Module
====================

Enforces position limits, loss limits, and other risk constraints.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Optional, Set, Union

from polymarket_client.models import MarketState, Order, OrderSide, TokenType, Trade


logger = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    """Configuration for risk management."""
    # Position limits
    max_position_per_market: float = 200.0  # Max notional per market
    max_global_exposure: float = 5000.0  # Max total exposure
    
    # Loss limits
    max_daily_loss: float = 500.0
    max_drawdown_pct: float = 0.10  # 10% max drawdown from peak
    min_peak_pnl_for_drawdown: float = 1.0  # Ignore drawdown until peak exceeds this
    
    # Market filters
    trade_only_high_volume: bool = True
    min_24h_volume: float = 10000.0
    # Reject orders on a market whose order book has not refreshed within this
    # many seconds. 0 disables the staleness gate (e.g. for backtests).
    max_market_staleness_seconds: float = 5.0
    
    # Whitelist/blacklist
    whitelist: list[str] = field(default_factory=list)
    blacklist: list[str] = field(default_factory=list)
    
    # Kill switch
    kill_switch_enabled: bool = True
    # When true, the risk manager invokes the on_kill_switch callback on first
    # trip. The intent is to cancel resting orders so we are not exposed to
    # adverse fills while we figure out what happened.
    auto_unwind_on_breach: bool = False
    # Allow reduce-only urgent exits to continue after kill switch triggers.
    allow_urgent_exit_after_kill_switch: bool = True
    # Allow reduce-only urgent exits even when market freshness is stale.
    allow_urgent_exit_on_stale_data: bool = True


KillSwitchCallback = Callable[[str], Union[None, Awaitable[None]]]


@dataclass
class RiskState:
    """Current risk state."""
    daily_pnl: float = 0.0
    peak_pnl: float = 0.0
    current_drawdown: float = 0.0
    global_exposure: float = 0.0
    kill_switch_triggered: bool = False
    kill_switch_reason: str = ""
    last_check: datetime = field(default_factory=datetime.utcnow)


class RiskManager:
    """
    Risk management system.
    
    Validates orders against risk limits and monitors overall exposure.
    Can trigger a kill switch to stop all trading.
    """
    
    def __init__(self, config: RiskConfig):
        self.config = config
        self.state = RiskState()
        
        # Per-market exposure tracking
        self._market_exposure: dict[str, float] = {}
        self._market_positions: dict[str, dict[TokenType, float]] = {}
        self._market_prices: dict[str, dict[TokenType, float]] = {}
        
        # Volume cache
        self._market_volumes: dict[str, float] = {}
        # Per-market last-update timestamp for staleness gating.
        self._market_freshness: dict[str, datetime] = {}
        # Counters for observability. Tests and the dashboard read these.
        self._stale_market_rejections: int = 0
        self._volume_rejections: int = 0
        # Optional callback fired the first time the kill switch trips. Used by
        # the runtime to cancel resting orders when auto_unwind_on_breach=True.
        self._on_kill_switch: Optional[KillSwitchCallback] = None
        self._kill_switch_callback_fired: bool = False
        
        # Trading session tracking
        self._session_start = datetime.utcnow()
        self._session_trades: list[Trade] = []
        
        logger.info(
            f"RiskManager initialized | "
            f"max_per_market={config.max_position_per_market} | "
            f"max_global={config.max_global_exposure} | "
            f"max_daily_loss={config.max_daily_loss} | "
            f"auto_unwind={config.auto_unwind_on_breach}"
        )

    def set_kill_switch_callback(self, callback: Optional[KillSwitchCallback]) -> None:
        """Register a callback fired the first time the kill switch trips.

        The callback receives the trip reason. It can be sync or async; async
        callbacks are scheduled on the running event loop.
        """
        self._on_kill_switch = callback
    
    def check_order(self, order: Order) -> bool:
        """
        Check if an order passes all risk checks.
        
        Returns True if the order is allowed, False otherwise.
        """
        if self._is_urgent_exit_order(order):
            return self._check_urgent_exit(order)

        # Kill switch check
        if self.state.kill_switch_triggered:
            logger.warning(f"Order rejected: kill switch triggered ({self.state.kill_switch_reason})")
            return False
        
        # Market blacklist check
        if order.market_id in self.config.blacklist:
            logger.warning(f"Order rejected: market {order.market_id} is blacklisted")
            return False
        
        # Whitelist check (if whitelist is non-empty)
        if self.config.whitelist and order.market_id not in self.config.whitelist:
            logger.warning(f"Order rejected: market {order.market_id} not in whitelist")
            return False
        
        # Volume check
        if self.config.trade_only_high_volume:
            market_volume = self._market_volumes.get(order.market_id, 0)
            if market_volume < self.config.min_24h_volume:
                self._volume_rejections += 1
                logger.warning(
                    f"Order rejected: market {order.market_id} volume "
                    f"({market_volume:.0f}) below minimum ({self.config.min_24h_volume})"
                )
                return False
        
        # Stale-market check: if we have a freshness entry for this market,
        # require it to be within max_market_staleness_seconds. We intentionally
        # only enforce this when the market is registered, so backtests / unit
        # tests that never register freshness keep working.
        if self.config.max_market_staleness_seconds > 0:
            last_seen = self._market_freshness.get(order.market_id)
            if last_seen is not None:
                staleness = (datetime.utcnow() - last_seen).total_seconds()
                if staleness > self.config.max_market_staleness_seconds:
                    self._stale_market_rejections += 1
                    logger.warning(
                        "Order rejected: market %s data is stale (%.2fs > %.2fs)",
                        order.market_id,
                        staleness,
                        self.config.max_market_staleness_seconds,
                    )
                    return False
        
        # Per-market exposure check (count only incremental absolute exposure).
        current_market_exposure = self._market_exposure.get(order.market_id, 0.0)
        incremental_exposure = self._estimate_incremental_exposure(order)
        projected_exposure = current_market_exposure + incremental_exposure
        
        if projected_exposure > self.config.max_position_per_market:
            logger.warning(
                f"Order rejected: would exceed market limit | "
                f"current={current_market_exposure:.2f} + order={incremental_exposure:.2f} = "
                f"{projected_exposure:.2f} > {self.config.max_position_per_market}"
            )
            return False
        
        # Global exposure check
        projected_global = self.state.global_exposure + incremental_exposure
        if projected_global > self.config.max_global_exposure:
            logger.warning(
                f"Order rejected: would exceed global limit | "
                f"current={self.state.global_exposure:.2f} + order={incremental_exposure:.2f} = "
                f"{projected_global:.2f} > {self.config.max_global_exposure}"
            )
            return False
        
        # Daily loss check
        if self.state.daily_pnl < -self.config.max_daily_loss:
            logger.warning(
                f"Order rejected: daily loss limit exceeded | "
                f"daily_pnl={self.state.daily_pnl:.2f} < -{self.config.max_daily_loss}"
            )
            if self.config.kill_switch_enabled:
                self._trigger_kill_switch("Daily loss limit exceeded")
            return False
        
        # Drawdown check
        if self.state.current_drawdown > self.config.max_drawdown_pct:
            logger.warning(
                f"Order rejected: drawdown limit exceeded | "
                f"drawdown={self.state.current_drawdown:.2%} > {self.config.max_drawdown_pct:.2%}"
            )
            if self.config.kill_switch_enabled:
                self._trigger_kill_switch("Drawdown limit exceeded")
            return False
        
        return True

    def _is_urgent_exit_order(self, order: Order) -> bool:
        return str(getattr(order, "strategy_tag", "") or "") == "urgent_exit"

    def _check_urgent_exit(self, order: Order) -> bool:
        """Allow tagged urgent exits when they are reduce-only."""
        if self.state.kill_switch_triggered and not self.config.allow_urgent_exit_after_kill_switch:
            logger.warning("Urgent exit rejected: kill-switch bypass disabled")
            return False

        if not self._is_reduce_only_order(order):
            logger.warning(
                "Urgent exit rejected: order is not reduce-only for market %s",
                order.market_id,
            )
            return False

        if self.config.max_market_staleness_seconds > 0 and not self.config.allow_urgent_exit_on_stale_data:
            last_seen = self._market_freshness.get(order.market_id)
            if last_seen is not None:
                staleness = (datetime.utcnow() - last_seen).total_seconds()
                if staleness > self.config.max_market_staleness_seconds:
                    logger.warning(
                        "Urgent exit rejected: market %s stale and bypass disabled (%.2fs > %.2fs)",
                        order.market_id,
                        staleness,
                        self.config.max_market_staleness_seconds,
                    )
                    return False

        return True

    def _is_reduce_only_order(self, order: Order) -> bool:
        return self._estimate_incremental_exposure(order) <= 0
    
    def update_position(
        self,
        market_id: str,
        token_type: TokenType,
        size_delta: float,
        price: float
    ) -> None:
        """Update position tracking after a trade."""
        market_positions = self._market_positions.setdefault(market_id, {})
        market_prices = self._market_prices.setdefault(market_id, {})

        current_size = float(market_positions.get(token_type, 0.0))
        market_positions[token_type] = current_size + float(size_delta)
        market_prices[token_type] = max(0.0, float(price))

        self._recalculate_market_exposure(market_id)
        self._recalculate_global_exposure()
        
        self.state.last_check = datetime.utcnow()
    
    def update_from_fill(self, trade: Trade) -> None:
        """Update risk state from a trade fill."""
        size_delta = trade.size if trade.side == OrderSide.BUY else -trade.size
        self.update_position(trade.market_id, trade.token_type, size_delta, trade.price)
        self._session_trades.append(trade)
    
    def update_pnl(self, realized_pnl: float, unrealized_pnl: float) -> None:
        """Update PnL tracking."""
        total_pnl = realized_pnl + unrealized_pnl
        self.state.daily_pnl = total_pnl
        
        # Update peak and drawdown
        if total_pnl > self.state.peak_pnl:
            self.state.peak_pnl = total_pnl
        
        if self.state.peak_pnl > max(0.0, self.config.min_peak_pnl_for_drawdown):
            self.state.current_drawdown = (self.state.peak_pnl - total_pnl) / self.state.peak_pnl
        else:
            self.state.current_drawdown = 0.0
        
        # Check for limit breaches
        if total_pnl < -self.config.max_daily_loss:
            if self.config.kill_switch_enabled and not self.state.kill_switch_triggered:
                self._trigger_kill_switch("Daily loss limit exceeded")
        
        if self.state.current_drawdown > self.config.max_drawdown_pct:
            if self.config.kill_switch_enabled and not self.state.kill_switch_triggered:
                self._trigger_kill_switch("Drawdown limit exceeded")
    
    def update_market_volume(self, market_id: str, volume_24h: float) -> None:
        """Update cached 24h volume for a market."""
        try:
            self._market_volumes[market_id] = float(volume_24h)
        except (TypeError, ValueError):
            return
    
    def set_market_volumes(self, volumes: dict[str, float]) -> None:
        """Bulk update market volumes."""
        for market_id, volume in volumes.items():
            self.update_market_volume(market_id, volume)
    
    def register_market_state(self, state: "MarketState") -> None:
        """Record liquidity / freshness from a MarketState update.

        Called by the data feed on every market update so that risk gates can
        cite real, current values instead of relying on the runtime to
        explicitly populate volume caches.
        """
        market = getattr(state, "market", None)
        if market is None:
            return
        market_id = getattr(market, "market_id", None)
        if not market_id:
            return
        volume = getattr(market, "volume_24h", None)
        if volume is not None:
            self.update_market_volume(market_id, float(volume))
        self._market_freshness[market_id] = getattr(state, "timestamp", datetime.utcnow())
    
    def _trigger_kill_switch(self, reason: str) -> None:
        """Trigger the kill switch to stop trading."""
        already_tripped = self.state.kill_switch_triggered
        self.state.kill_switch_triggered = True
        self.state.kill_switch_reason = reason
        logger.critical(f"KILL SWITCH TRIGGERED: {reason}")
        if already_tripped or self._kill_switch_callback_fired:
            return
        if not self.config.auto_unwind_on_breach:
            return
        callback = self._on_kill_switch
        if callback is None:
            return
        self._kill_switch_callback_fired = True
        try:
            result = callback(reason)
        except Exception as exc:
            logger.error("Kill-switch callback raised synchronously: %s", exc)
            return
        if result is None:
            return
        # Async callback: schedule on the running loop if there is one.
        try:
            import asyncio

            loop = asyncio.get_running_loop()
            loop.create_task(result)  # type: ignore[arg-type]
        except RuntimeError:
            # No running loop (sync caller). Best-effort: run to completion.
            try:
                import asyncio

                asyncio.run(result)  # type: ignore[arg-type]
            except Exception as exc:
                logger.error("Failed to execute async kill-switch callback: %s", exc)
    
    def reset_kill_switch(self) -> None:
        """Reset the kill switch (use with caution)."""
        self.state.kill_switch_triggered = False
        self.state.kill_switch_reason = ""
        self._kill_switch_callback_fired = False
        logger.warning("Kill switch reset")
    
    def within_global_limits(self) -> bool:
        """Check if currently within global risk limits."""
        if self.state.kill_switch_triggered:
            return False
        if self.state.daily_pnl < -self.config.max_daily_loss:
            return False
        if self.state.current_drawdown > self.config.max_drawdown_pct:
            return False
        if self.state.global_exposure > self.config.max_global_exposure:
            return False
        return True
    
    def get_market_exposure(self, market_id: str) -> float:
        """Get current exposure for a market."""
        return self._market_exposure.get(market_id, 0.0)
    
    def get_available_exposure(self, market_id: str) -> float:
        """Get remaining available exposure for a market."""
        current = self._market_exposure.get(market_id, 0.0)
        return max(0, self.config.max_position_per_market - current)
    
    def get_global_available(self) -> float:
        """Get remaining global exposure capacity."""
        return max(0, self.config.max_global_exposure - self.state.global_exposure)
    
    def reset_daily_stats(self) -> None:
        """Reset daily statistics (call at start of trading day)."""
        self.state.daily_pnl = 0.0
        self.state.peak_pnl = 0.0
        self.state.current_drawdown = 0.0
        self._session_start = datetime.utcnow()
        self._session_trades = []
        logger.info("Daily stats reset")
    
    def get_summary(self) -> dict:
        """Get a summary of current risk state."""
        return {
            "global_exposure": self.state.global_exposure,
            "max_global_exposure": self.config.max_global_exposure,
            "utilization_pct": (self.state.global_exposure / self.config.max_global_exposure * 100
                               if self.config.max_global_exposure > 0 else 0),
            "daily_pnl": self.state.daily_pnl,
            "max_daily_loss": self.config.max_daily_loss,
            "peak_pnl": self.state.peak_pnl,
            "current_drawdown_pct": self.state.current_drawdown * 100,
            "max_drawdown_pct": self.config.max_drawdown_pct * 100,
            "kill_switch_triggered": self.state.kill_switch_triggered,
            "kill_switch_reason": self.state.kill_switch_reason,
            "markets_with_exposure": len([m for m, e in self._market_exposure.items() if e > 0]),
            "session_trade_count": len(self._session_trades),
            "within_limits": self.within_global_limits(),
            "auto_unwind_on_breach": self.config.auto_unwind_on_breach,
            "stale_market_rejections": self._stale_market_rejections,
            "volume_rejections": self._volume_rejections,
            "max_market_staleness_seconds": self.config.max_market_staleness_seconds,
            "trade_only_high_volume": self.config.trade_only_high_volume,
            "min_24h_volume": self.config.min_24h_volume,
            "tracked_market_volumes": len(self._market_volumes),
            "tracked_market_freshness": len(self._market_freshness),
        }
    
    def add_to_blacklist(self, market_id: str) -> None:
        """Add a market to the blacklist."""
        if market_id not in self.config.blacklist:
            self.config.blacklist.append(market_id)
            logger.info(f"Market {market_id} added to blacklist")
    
    def remove_from_blacklist(self, market_id: str) -> None:
        """Remove a market from the blacklist."""
        if market_id in self.config.blacklist:
            self.config.blacklist.remove(market_id)
            logger.info(f"Market {market_id} removed from blacklist")

    @staticmethod
    def _signed_order_size(side: OrderSide, size: float) -> float:
        return float(size) if side == OrderSide.BUY else -float(size)

    def _estimate_incremental_exposure(self, order: Order) -> float:
        market_positions = self._market_positions.get(order.market_id, {})
        current_size = float(market_positions.get(order.token_type, 0.0))
        signed_size = self._signed_order_size(order.side, order.size)
        projected_size = current_size + signed_size
        current_abs = abs(current_size)
        projected_abs = abs(projected_size)
        incremental_contracts = max(0.0, projected_abs - current_abs)
        return incremental_contracts * float(order.price)

    def _recalculate_market_exposure(self, market_id: str) -> None:
        market_positions = self._market_positions.get(market_id, {})
        market_prices = self._market_prices.get(market_id, {})
        total = 0.0
        for token_type, size in market_positions.items():
            price = float(market_prices.get(token_type, 0.0))
            total += abs(float(size)) * max(0.0, price)
        self._market_exposure[market_id] = total

    def _recalculate_global_exposure(self) -> None:
        self.state.global_exposure = sum(self._market_exposure.values())

