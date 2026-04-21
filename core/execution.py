"""
Execution Engine Module
========================

Handles order placement, cancellation, and management.
Consumes signals from the ArbEngine and interfaces with the API.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from polymarket_client.api import PolymarketClient
from polymarket_client.models import (
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    Signal,
    TokenType,
    Trade,
)
from core.risk_manager import RiskManager
from core.portfolio import Portfolio
from utils.logging_utils import trade_logger


logger = logging.getLogger(__name__)


@dataclass
class ExecutionConfig:
    """Configuration for the execution engine."""
    slippage_tolerance: float = 0.02  # Max allowed price slippage
    order_timeout_seconds: float = 60.0  # Cancel unfilled orders after this time
    # MM quotes decay much faster than arbitrage legs. Keep them on-book for a
    # much shorter window so stale maker quotes don't sit for a full minute.
    mm_order_timeout_seconds: float = 12.0
    max_retries: int = 3
    retry_delay: float = 0.5
    enable_slippage_check: bool = True
    dry_run: bool = True
    max_signal_queue_size: int = 5000
    # When the API rejects an order with a permanent error (e.g. "market not
    # found" / 404), suppress further attempts on that market for this many
    # seconds. Prevents the bot from hammering closed/non-placeable markets
    # discovered by the public catalog. 0 disables.
    unplaceable_market_skip_seconds: float = 300.0


@dataclass
class ExecutionStats:
    """Statistics for the execution engine."""
    orders_placed: int = 0
    orders_filled: int = 0
    orders_cancelled: int = 0
    orders_rejected: int = 0
    total_notional: float = 0.0
    signals_processed: int = 0
    signals_rejected: int = 0
    slippage_rejections: int = 0
    unplaceable_signal_skips: int = 0
    signal_queue_drops: int = 0
    max_signal_queue_depth: int = 0
    taker_orders_attempted: int = 0
    taker_orders_filled: int = 0
    taker_orders_rejected: int = 0
    urgent_exit_attempted: int = 0
    urgent_exit_rejected: int = 0


class ExecutionEngine:
    """
    Order execution engine.
    
    Consumes trading signals and places/manages orders through the
    Polymarket API. Enforces risk limits and handles slippage checks.
    """
    
    def __init__(
        self,
        client: PolymarketClient,
        risk_manager: RiskManager,
        portfolio: Portfolio,
        config: ExecutionConfig,
    ):
        self.client = client
        self.risk_manager = risk_manager
        self.portfolio = portfolio
        self.config = config
        self.stats = ExecutionStats()
        
        # Track open orders
        self._open_orders: dict[str, Order] = {}
        self._order_timestamps: dict[str, datetime] = {}
        self._order_timeouts_seconds: dict[str, float] = {}
        
        # Order tracking by market and strategy
        self._orders_by_market: dict[str, list[str]] = {}
        self._orders_by_strategy: dict[str, list[str]] = {}
        self._cancel_in_flight: set[str] = set()

        # Markets whose orders the API has permanently rejected. We skip new
        # signals for them until this timestamp passes. Keeps the log readable
        # and stops us burning rate-limit budget on dead markets.
        self._unplaceable_until: dict[str, datetime] = {}
        self._last_unplaceable_skip_log: dict[str, datetime] = {}
        
        # Signal queue
        self._signal_queue: asyncio.Queue[Signal] = asyncio.Queue(maxsize=self.config.max_signal_queue_size)
        self._processing_task: Optional[asyncio.Task] = None
        self._timeout_task: Optional[asyncio.Task] = None
        self._running = False
        
        logger.info(f"ExecutionEngine initialized (dry_run={config.dry_run})")
    
    async def start(self) -> None:
        """Start the execution engine."""
        if self._running:
            return
        
        self._running = True
        self._processing_task = asyncio.create_task(
            self._process_signals(),
            name="signal_processor"
        )
        
        # Start order timeout monitor
        self._timeout_task = asyncio.create_task(self._monitor_order_timeouts(), name="order_timeout_monitor")
        
        logger.info("ExecutionEngine started")
    
    async def stop(self) -> None:
        """Stop the execution engine."""
        self._running = False
        
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
        
        if self._timeout_task:
            self._timeout_task.cancel()
            try:
                await self._timeout_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all open orders
        await self.cancel_all_orders()
        
        logger.info("ExecutionEngine stopped")
    
    async def submit_signal(self, signal: Signal) -> None:
        """Submit a signal for processing."""
        await self._signal_queue.put(signal)
        self.stats.max_signal_queue_depth = max(
            self.stats.max_signal_queue_depth,
            self._signal_queue.qsize(),
        )
        logger.debug(f"Signal queued: {signal.signal_id}")
    
    def submit_signal_nowait(self, signal: Signal) -> bool:
        """
        Try to submit a signal without scheduling extra tasks.
        
        Returns True when queued, False when queue is full.
        """
        try:
            self._signal_queue.put_nowait(signal)
        except asyncio.QueueFull:
            self.stats.signal_queue_drops += 1
            return False
        
        self.stats.max_signal_queue_depth = max(
            self.stats.max_signal_queue_depth,
            self._signal_queue.qsize(),
        )
        return True
    
    async def _process_signals(self) -> None:
        """Main signal processing loop."""
        while self._running:
            try:
                # Get next signal with timeout
                try:
                    signal = await asyncio.wait_for(
                        self._signal_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                try:
                    await self._execute_signal(signal)
                    self.stats.signals_processed += 1
                finally:
                    self._signal_queue.task_done()
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Signal processing error: {e}")
    
    async def _execute_signal(self, signal: Signal) -> None:
        """Execute a single trading signal."""
        logger.info(f"Executing signal: {signal.signal_id} ({signal.action})")
        
        if signal.is_place:
            await self._handle_place_orders(signal)
        elif signal.is_cancel:
            await self._handle_cancel_orders(signal)
        else:
            logger.warning(f"Unknown signal action: {signal.action}")
    
    def _is_market_unplaceable(self, market_id: str) -> bool:
        """Return True if we've recently been rejected on this market."""
        until = self._unplaceable_until.get(market_id)
        if not until:
            return False
        if datetime.utcnow() >= until:
            self._unplaceable_until.pop(market_id, None)
            return False
        return True

    def _mark_market_unplaceable(self, market_id: str, reason: str) -> None:
        """Suppress further order attempts on a market for the configured TTL."""
        ttl = max(0.0, float(self.config.unplaceable_market_skip_seconds))
        if ttl <= 0:
            return
        self._unplaceable_until[market_id] = datetime.utcnow() + timedelta(seconds=ttl)
        logger.warning(
            "Marking market %s as unplaceable for %.0fs (%s)",
            market_id, ttl, reason,
        )

    async def _handle_place_orders(self, signal: Signal) -> None:
        """Handle a place_orders signal."""
        if self._is_market_unplaceable(signal.market_id):
            # Drop quietly — the warning was already logged when we first
            # decided to skip this market. Counts as a rejection so dashboards
            # still see the pressure.
            self.stats.signals_rejected += len(signal.orders) or 1
            self.stats.unplaceable_signal_skips += 1
            until = self._unplaceable_until.get(signal.market_id)
            now = datetime.utcnow()
            last_logged = self._last_unplaceable_skip_log.get(signal.market_id)
            if last_logged is None or (now - last_logged).total_seconds() >= 30.0:
                remaining = (
                    max(0.0, (until - now).total_seconds())
                    if until is not None
                    else 0.0
                )
                logger.info(
                    "Skipping place_orders for market %s: marked unplaceable for another %.0fs",
                    signal.market_id,
                    remaining,
                )
                self._last_unplaceable_skip_log[signal.market_id] = now
            return

        for order_spec in signal.orders:
            strategy_tag = ""
            try:
                # Extract order parameters
                token_type = order_spec["token_type"]
                side = order_spec["side"]
                price = order_spec["price"]
                size = order_spec["size"]
                strategy_tag = order_spec.get("strategy_tag", "")
                quote_group_id = order_spec.get("quote_group_id", "")
                order_type = str(order_spec.get("order_type", "ORDER_TYPE_LIMIT"))
                time_in_force = str(
                    order_spec.get("time_in_force", "TIME_IN_FORCE_GOOD_TILL_CANCEL")
                )
                use_close_position = bool(order_spec.get("close_position", False))
                slippage_ticks = order_spec.get("slippage_ticks")
                
                # Check slippage if enabled
                if self.config.enable_slippage_check and signal.opportunity:
                    if not self._check_slippage(signal.opportunity, order_spec):
                        self.stats.slippage_rejections += 1
                        self._note_strategy_rejection(strategy_tag)
                        logger.warning(f"Order rejected due to slippage: {order_spec}")
                        trade_logger.log_order_rejected(
                            market_id=signal.market_id,
                            side=side.value,
                            token=token_type.value,
                            price=price,
                            size=size,
                            strategy=strategy_tag,
                            reason="slippage_check_failed",
                        )
                        continue
                
                # Check risk limits
                proposed_order = Order(
                    order_id="temp",
                    market_id=signal.market_id,
                    token_type=token_type,
                    side=side,
                    price=price,
                    size=size,
                    strategy_tag=strategy_tag,
                    quote_group_id=quote_group_id,
                    order_type=order_type,
                    time_in_force=time_in_force,
                )
                
                if not self.risk_manager.check_order(proposed_order):
                    self.stats.signals_rejected += 1
                    self._note_strategy_rejection(strategy_tag)
                    logger.warning(f"Order rejected by risk manager: {order_spec}")
                    trade_logger.log_order_rejected(
                        market_id=signal.market_id,
                        side=side.value,
                        token=token_type.value,
                        price=price,
                        size=size,
                        strategy=strategy_tag,
                        reason="risk_manager_rejected",
                    )
                    continue
                
                if use_close_position:
                    if await self._close_position(
                        market_id=signal.market_id,
                        strategy_tag=strategy_tag or "urgent_exit",
                        side=side,
                        token_type=token_type,
                        size=size,
                        current_price=price,
                        slippage_ticks=int(slippage_ticks) if slippage_ticks is not None else None,
                    ):
                        if strategy_tag == "urgent_exit":
                            self.stats.urgent_exit_attempted += 1
                        self.stats.orders_placed += 1
                        self.stats.total_notional += float(price) * float(size)
                        continue

                # Place the order
                order = await self._place_order(
                    market_id=signal.market_id,
                    token_type=token_type,
                    side=side,
                    price=price,
                    size=size,
                    strategy_tag=strategy_tag,
                    order_type=order_type,
                    time_in_force=time_in_force,
                )
                
                if order:
                    order.quote_group_id = quote_group_id
                    self._track_order(order)
                    self._note_strategy_placement(order.strategy_tag)
                    self.stats.orders_placed += 1
                    self.stats.total_notional += order.notional
                    
            except Exception as e:
                logger.error(f"Failed to place order: {e}")
                self._note_strategy_rejection(strategy_tag)
                self.stats.orders_rejected += 1
    
    async def _handle_cancel_orders(self, signal: Signal) -> None:
        """Handle a cancel_orders signal."""
        order_ids = signal.cancel_order_ids or self._resolve_cancel_order_ids(signal)
        for order_id in order_ids:
            try:
                await self.cancel_order(order_id)
            except Exception as e:
                logger.error(f"Failed to cancel order {order_id}: {e}")
    
    def _check_slippage(self, opportunity, order_spec: dict) -> bool:
        """
        Check if current prices have slipped too far from signal generation.
        
        Returns True if within tolerance, False if slippage exceeded.
        """
        # Compare intended price vs opportunity snapshot
        intended_price = order_spec["price"]
        side = order_spec["side"]
        token_type = order_spec["token_type"]
        
        if token_type == TokenType.YES:
            snapshot_bid = opportunity.best_bid_yes
            snapshot_ask = opportunity.best_ask_yes
        else:
            snapshot_bid = opportunity.best_bid_no
            snapshot_ask = opportunity.best_ask_no
        
        if snapshot_bid is None or snapshot_ask is None:
            return True  # Can't check, allow
        
        if side == OrderSide.BUY:
            # For buys, check if ask hasn't moved up too much
            slippage = (intended_price - snapshot_ask) / snapshot_ask if snapshot_ask > 0 else 0
        else:
            # For sells, check if bid hasn't moved down too much
            slippage = (snapshot_bid - intended_price) / snapshot_bid if snapshot_bid > 0 else 0
        
        return abs(slippage) <= self.config.slippage_tolerance
    
    # Substrings we treat as permanent placement errors. Retrying these is
    # pointless and just burns rate-limit budget.
    _PERMANENT_ORDER_ERRORS: tuple[str, ...] = (
        "market not found",
        "not found",
        "market is closed",
        "market closed",
        "market is resolved",
        "invalid market",
        "404",
    )

    @classmethod
    def _is_permanent_order_error(cls, exc: BaseException) -> bool:
        msg = str(exc).lower()
        return any(token in msg for token in cls._PERMANENT_ORDER_ERRORS)

    async def _place_order(
        self,
        market_id: str,
        token_type: TokenType,
        side: OrderSide,
        price: float,
        size: float,
        strategy_tag: str = "",
        order_type: str = "ORDER_TYPE_LIMIT",
        time_in_force: str = "TIME_IN_FORCE_GOOD_TILL_CANCEL",
    ) -> Optional[Order]:
        """Place an order through the API with retry logic."""
        last_error: Optional[BaseException] = None

        for attempt in range(self.config.max_retries):
            try:
                order = await self.client.place_order(
                    market_id=market_id,
                    token_type=token_type,
                    side=side,
                    price=price,
                    size=size,
                    strategy_tag=strategy_tag,
                    order_type=order_type,
                    time_in_force=time_in_force,
                )

                logger.info(
                    f"Order placed: {order.order_id} | "
                    f"{side.value} {size:.2f} {token_type.value} @ {price:.4f} | "
                    f"type={order.order_type} tif={order.time_in_force}"
                )
                trade_logger.log_order_placed(
                    order_id=order.order_id,
                    market_id=market_id,
                    side=side.value,
                    token=token_type.value,
                    price=price,
                    size=size,
                    strategy=strategy_tag,
                )

                return order

            except Exception as e:
                last_error = e
                if self._is_permanent_order_error(e):
                    logger.warning(
                        "Order placement permanently rejected for market %s: %s",
                        market_id, e,
                    )
                    self._mark_market_unplaceable(market_id, str(e))
                    return None

                logger.warning(
                    "Order placement attempt %d/%d failed for market %s: %s",
                    attempt + 1, self.config.max_retries, market_id, e,
                )
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay)

        logger.error(
            "Order placement failed after %d attempts for market %s: %s",
            self.config.max_retries, market_id, last_error,
        )
        return None

    async def _close_position(
        self,
        *,
        market_id: str,
        strategy_tag: str,
        side: OrderSide,
        token_type: TokenType,
        size: float,
        current_price: float,
        slippage_ticks: Optional[int],
    ) -> bool:
        """Attempt exchange close-position endpoint for urgent exits."""
        try:
            await self.client.close_position(
                market_id=market_id,
                slippage_ticks=slippage_ticks,
                current_price=current_price,
            )
            logger.info("Close-position submitted: market=%s", market_id)
            trade_logger.log_order_placed(
                order_id=f"close_position_{uuid.uuid4().hex[:10]}",
                market_id=market_id,
                side=side.value,
                token=token_type.value,
                price=current_price,
                size=size,
                strategy=strategy_tag,
            )
            return True
        except Exception as exc:
            logger.warning(
                "Close-position failed for %s, falling back to order placement: %s",
                market_id,
                exc,
            )
            return False

    def _note_strategy_placement(self, strategy_tag: str) -> None:
        if strategy_tag == "taker_entry":
            self.stats.taker_orders_attempted += 1
        elif strategy_tag == "urgent_exit":
            self.stats.urgent_exit_attempted += 1

    def _note_strategy_rejection(self, strategy_tag: str) -> None:
        if strategy_tag == "taker_entry":
            self.stats.taker_orders_rejected += 1
        elif strategy_tag == "urgent_exit":
            self.stats.urgent_exit_rejected += 1
    
    def _track_order(self, order: Order) -> None:
        """Add order to tracking structures."""
        self._open_orders[order.order_id] = order
        self._order_timestamps[order.order_id] = datetime.utcnow()
        timeout_seconds = float(self.config.order_timeout_seconds)
        if order.strategy_tag == "market_making":
            timeout_seconds = max(
                0.5,
                float(getattr(self.config, "mm_order_timeout_seconds", timeout_seconds) or timeout_seconds),
            )
        elif order.is_ioc_or_fok:
            timeout_seconds = 2.0
        self._order_timeouts_seconds[order.order_id] = timeout_seconds
        
        # Track by market
        if order.market_id not in self._orders_by_market:
            self._orders_by_market[order.market_id] = []
        self._orders_by_market[order.market_id].append(order.order_id)
        
        # Track by strategy
        if order.strategy_tag:
            if order.strategy_tag not in self._orders_by_strategy:
                self._orders_by_strategy[order.strategy_tag] = []
            self._orders_by_strategy[order.strategy_tag].append(order.order_id)
    
    def _untrack_order(self, order_id: str) -> None:
        """Remove order from tracking structures."""
        if order_id in self._open_orders:
            order = self._open_orders[order_id]
            del self._open_orders[order_id]
            
            if order_id in self._order_timestamps:
                del self._order_timestamps[order_id]
            if order_id in self._order_timeouts_seconds:
                del self._order_timeouts_seconds[order_id]
            
            # Remove from market tracking
            if order.market_id in self._orders_by_market:
                if order_id in self._orders_by_market[order.market_id]:
                    self._orders_by_market[order.market_id].remove(order_id)
            
            # Remove from strategy tracking
            if order.strategy_tag and order.strategy_tag in self._orders_by_strategy:
                if order_id in self._orders_by_strategy[order.strategy_tag]:
                    self._orders_by_strategy[order.strategy_tag].remove(order_id)
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a specific order."""
        if order_id in self._cancel_in_flight:
            logger.debug("Cancel already in flight for %s", order_id)
            return False

        tracked_order = self._open_orders.get(order_id)
        if tracked_order is None:
            logger.debug("Skipping cancel for untracked order %s", order_id)
            return False

        self._cancel_in_flight.add(order_id)
        try:
            market_slug = None
            if tracked_order:
                market_slug = tracked_order.market_slug or tracked_order.market_id
            await self.client.cancel_order(order_id, market_slug=market_slug)
            self._untrack_order(order_id)
            self.stats.orders_cancelled += 1
            logger.info(f"Order cancelled: {order_id}")
            trade_logger.log_order_cancelled(order_id=order_id, reason="cancel_request")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
        finally:
            self._cancel_in_flight.discard(order_id)
    
    async def cancel_all_orders(self, market_id: Optional[str] = None) -> int:
        """Cancel all open orders, optionally for a specific market."""
        if market_id:
            order_ids = list(self._orders_by_market.get(market_id, []))
        else:
            order_ids = list(self._open_orders.keys())
        
        cancelled = 0
        for order_id in order_ids:
            if await self.cancel_order(order_id):
                cancelled += 1
        
        logger.info(f"Cancelled {cancelled} orders")
        return cancelled

    def build_cancel_signal(
        self,
        *,
        market_id: str,
        strategy_tag: Optional[str] = None,
        token_type: Optional[TokenType] = None,
        quote_group_id: str = "",
        priority: int = 9,
    ) -> Optional[Signal]:
        """Create a cancel signal for currently open tracked orders."""
        if quote_group_id:
            return Signal(
                signal_id=f"sig_cancel_{uuid.uuid4().hex[:12]}",
                action="cancel_orders",
                market_id=market_id,
                cancel_strategy_tag=strategy_tag or "",
                cancel_quote_group_id=quote_group_id,
                cancel_token_type=token_type,
                priority=priority,
            )

        order_ids: list[str] = []
        for order in self.get_open_orders(market_id=market_id):
            if strategy_tag and order.strategy_tag != strategy_tag:
                continue
            if token_type is not None and order.token_type != token_type:
                continue
            order_ids.append(order.order_id)

        if not order_ids:
            return None

        return Signal(
            signal_id=f"sig_cancel_{uuid.uuid4().hex[:12]}",
            action="cancel_orders",
            market_id=market_id,
            cancel_order_ids=order_ids,
            cancel_strategy_tag=strategy_tag or "",
            cancel_token_type=token_type,
            priority=priority,
        )

    def _resolve_cancel_order_ids(self, signal: Signal) -> list[str]:
        """Resolve filtered cancel signals to the currently open order ids."""
        order_ids: list[str] = []
        for order in self.get_open_orders(market_id=signal.market_id):
            if signal.cancel_strategy_tag and order.strategy_tag != signal.cancel_strategy_tag:
                continue
            if signal.cancel_token_type is not None and order.token_type != signal.cancel_token_type:
                continue
            if signal.cancel_quote_group_id and order.quote_group_id != signal.cancel_quote_group_id:
                continue
            order_ids.append(order.order_id)
        return order_ids
    
    async def cancel_orders_by_strategy(self, strategy_tag: str) -> int:
        """Cancel all orders for a specific strategy."""
        order_ids = list(self._orders_by_strategy.get(strategy_tag, []))
        
        cancelled = 0
        for order_id in order_ids:
            if await self.cancel_order(order_id):
                cancelled += 1
        
        return cancelled
    
    async def _monitor_order_timeouts(self) -> None:
        """Monitor and cancel orders that have timed out."""
        while self._running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                now = datetime.utcnow()
                
                timed_out = [
                    order_id for order_id, timestamp in self._order_timestamps.items()
                    if now - timestamp > timedelta(
                        seconds=float(
                            self._order_timeouts_seconds.get(
                                order_id,
                                self.config.order_timeout_seconds,
                            )
                        )
                    )
                ]
                
                for order_id in timed_out:
                    logger.info(f"Order timed out: {order_id}")
                    await self.cancel_order(order_id)
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Order timeout monitor error: {e}")
    
    def handle_fill(self, trade: Trade) -> None:
        """Handle a trade fill notification."""
        order_id = trade.order_id
        
        if order_id in self._open_orders:
            order = self._open_orders[order_id]
            order.filled_size += trade.size
            order.updated_at = datetime.utcnow()
            
            if order.remaining_size <= 0:
                order.status = OrderStatus.FILLED
                self._untrack_order(order_id)
                self.stats.orders_filled += 1
                if order.strategy_tag == "taker_entry":
                    self.stats.taker_orders_filled += 1
            else:
                order.status = OrderStatus.PARTIALLY_FILLED
        
        # Update portfolio
        self.portfolio.update_from_fill(trade)
        
        # Update risk manager
        self.risk_manager.update_from_fill(trade)
        pnl = self.portfolio.get_pnl()
        trade_logger.log_order_filled(
            trade_id=trade.trade_id,
            order_id=trade.order_id,
            market_id=trade.market_id,
            side=trade.side.value,
            token=trade.token_type.value,
            price=trade.price,
            size=trade.size,
            fee=trade.fee,
            pnl_total=pnl.get("total_pnl"),
            pnl_realized=pnl.get("realized_pnl"),
            pnl_unrealized=pnl.get("unrealized_pnl"),
        )
        
        logger.info(
            f"Fill: {trade.trade_id} | "
            f"{trade.side.value} {trade.size:.2f} {trade.token_type.value} @ {trade.price:.4f}"
        )
    
    def get_open_orders(self, market_id: Optional[str] = None) -> list[Order]:
        """Get all open orders, optionally filtered by market."""
        if market_id:
            order_ids = self._orders_by_market.get(market_id, [])
            return [self._open_orders[oid] for oid in order_ids if oid in self._open_orders]
        return list(self._open_orders.values())
    
    def get_stats(self) -> ExecutionStats:
        """Get execution statistics."""
        return self.stats

    @property
    def unplaceable_market_count(self) -> int:
        """Return number of markets currently blacklisted for placement."""
        now = datetime.utcnow()
        expired = [market_id for market_id, until in self._unplaceable_until.items() if now >= until]
        for market_id in expired:
            self._unplaceable_until.pop(market_id, None)
            self._last_unplaceable_skip_log.pop(market_id, None)
        return len(self._unplaceable_until)
    
    @property
    def open_order_count(self) -> int:
        """Get number of open orders."""
        return len(self._open_orders)
    
    @property
    def signal_queue_size(self) -> int:
        """Get current queued signal count."""
        return self._signal_queue.qsize()

