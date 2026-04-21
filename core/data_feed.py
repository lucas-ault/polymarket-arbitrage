"""
Data Feed Module
=================

Maintains real-time in-memory state of order books and positions
for all monitored markets.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Callable, Optional

from polymarket_client.api import PolymarketClient
from polymarket_client.models import (
    Market,
    MarketState,
    OrderBook,
    Position,
    TokenType,
)


logger = logging.getLogger(__name__)


class DataFeed:
    """
    Real-time data feed manager.
    
    Subscribes to order book updates via WebSocket and periodically
    refreshes positions via REST API. Provides a unified view of
    market state for the trading engine.
    """
    
    def __init__(
        self,
        client: PolymarketClient,
        market_ids: list[str],
        position_refresh_interval: float = 5.0,
        on_update: Optional[Callable[[str, MarketState], None]] = None,
        config = None,
    ):
        self.client = client
        self.market_ids = market_ids
        self.position_refresh_interval = position_refresh_interval
        self.on_update = on_update
        self.config = config
        
        # In-memory state
        self._markets: dict[str, Market] = {}
        self._order_books: dict[str, OrderBook] = {}
        self._positions: dict[str, dict[TokenType, Position]] = {}
        self._market_states: dict[str, MarketState] = {}
        
        # Tasks
        self._orderbook_task: Optional[asyncio.Task] = None
        self._position_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Statistics
        self._update_count = 0
        self._last_update: dict[str, datetime] = {}
        self._dirty_market_ids: set[str] = set()
        self._stream_reconnects = 0
        self._stream_errors = 0
        self._state_updates = 0
        self._state_update_latency_ms_total = 0.0
        self._state_update_latency_ms_max = 0.0
        self._callback_count = 0
        self._callback_error_count = 0
        self._callback_latency_ms_total = 0.0
        self._callback_latency_ms_max = 0.0
        self._started_at: Optional[datetime] = None
    
    async def start(self) -> None:
        """
        Start the data feed.
        
        Connects to order book streams and starts position refresh loop.
        """
        if self._running:
            logger.warning("DataFeed already running")
            return
        
        self._running = True
        self._started_at = datetime.utcnow()
        logger.info(f"Starting DataFeed for {len(self.market_ids)} markets")
        
        # Fetch initial market info
        await self._fetch_markets()
        
        # Fetch initial positions
        await self._refresh_positions()
        
        # Start streaming order books
        self._orderbook_task = asyncio.create_task(
            self._stream_orderbooks(),
            name="orderbook_stream"
        )
        
        # Start position refresh loop
        self._position_task = asyncio.create_task(
            self._position_refresh_loop(),
            name="position_refresh"
        )
        
        logger.info("DataFeed started successfully")
    
    async def stop(self) -> None:
        """Stop the data feed."""
        self._running = False
        
        if self._orderbook_task:
            self._orderbook_task.cancel()
            try:
                await self._orderbook_task
            except asyncio.CancelledError:
                pass
        
        if self._position_task:
            self._position_task.cancel()
            try:
                await self._position_task
            except asyncio.CancelledError:
                pass
        
        logger.info("DataFeed stopped")
    
    async def _fetch_markets(self) -> None:
        """Fetch market information for all monitored markets."""
        try:
            if not self.market_ids:
                # Discover markets if none specified - bias towards tradable/liquid markets first.
                discovery_filters = {
                    "active": True,
                    "closed": False,
                    "archived": False,
                    "includeHidden": False,
                    "limit": 300,
                    "orderBy": ["volumeNum", "liquidityNum"],
                    "orderDirection": "desc",
                    "volumeNumMin": 500,
                    "liquidityNumMin": 200,
                }
                markets = await self.client.list_markets(discovery_filters)
                if not markets:
                    # Fall back to looser filter set.
                    markets = await self.client.list_markets(
                        {"active": True, "closed": False, "archived": False, "limit": 300}
                    )

                # Prefer liquid/active markets first so price feeds are meaningful.
                liquid_markets = [
                    market for market in markets
                    if (
                        (market.volume_24h > 0)
                        or (market.liquidity > 0)
                        or (market.best_bid > 0)
                        or (market.best_ask > 0)
                        or (market.yes_price > 0)
                        or (market.no_price > 0)
                    )
                ]
                selected_markets = liquid_markets if liquid_markets else markets
                selected_markets = selected_markets[:200]

                # Store markets directly from the list - no need to re-fetch!
                for market in selected_markets:
                    self._markets[market.market_id] = market

                self.market_ids = [m.market_id for m in selected_markets]
                logger.info(
                    "Discovered %s markets (%s liquid); monitoring %s",
                    len(markets),
                    len(liquid_markets),
                    len(self.market_ids),
                )
            else:
                # Only fetch if specific market_ids were provided
                for market_id in self.market_ids:
                    market = await self.client.get_market(market_id)
                    self._markets[market_id] = market
                
        except Exception as e:
            logger.error(f"Failed to fetch markets: {e}")
            raise
    
    async def _stream_orderbooks(self) -> None:
        """Stream order book updates."""
        # Use simulation for demo/screenshots, real data for production
        # Check config.mode.data_mode (set in config.yaml)
        use_simulation = getattr(self.config, 'use_simulation', False)
        
        monitor_cfg = getattr(self.config, "monitoring", None)
        active_batch_size = getattr(monitor_cfg, "orderbook_active_batch_size", 500)
        markets_per_request_batch = getattr(monitor_cfg, "orderbook_request_batch_size", 20)
        request_concurrency = getattr(monitor_cfg, "orderbook_fetch_concurrency", 12)
        request_delay = getattr(monitor_cfg, "orderbook_request_delay_seconds", 0.0)
        batch_delay = getattr(monitor_cfg, "orderbook_batch_delay_seconds", 0.15)
        rotation_delay = getattr(monitor_cfg, "orderbook_rotation_delay_seconds", 1.25)
        
        while self._running:
            try:
                async for market_id, orderbook in self.client.stream_orderbook(
                    self.market_ids,
                    use_simulation=use_simulation,
                    active_batch_size=active_batch_size,
                    markets_per_request_batch=markets_per_request_batch,
                    request_concurrency=request_concurrency,
                    request_delay=request_delay,
                    batch_delay=batch_delay,
                    rotation_delay=rotation_delay,
                ):
                    if not self._running:
                        break
                    
                    self._order_books[market_id] = orderbook
                    self._last_update[market_id] = datetime.utcnow()
                    self._update_count += 1
                    self._dirty_market_ids.add(market_id)
                    
                    # Update market state
                    update_start = time.perf_counter()
                    self._update_market_state(market_id)
                    update_duration_ms = (time.perf_counter() - update_start) * 1000
                    self._state_updates += 1
                    self._state_update_latency_ms_total += update_duration_ms
                    self._state_update_latency_ms_max = max(
                        self._state_update_latency_ms_max,
                        update_duration_ms,
                    )
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._stream_errors += 1
                logger.error(f"Order book stream error: {e}")
                if self._running:
                    self._stream_reconnects += 1
                    await asyncio.sleep(1)  # Brief delay before reconnecting
    
    async def _position_refresh_loop(self) -> None:
        """Periodically refresh positions."""
        while self._running:
            try:
                await asyncio.sleep(self.position_refresh_interval)
                await self._refresh_positions()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Position refresh error: {e}")
    
    async def _refresh_positions(self) -> None:
        """Fetch current positions from API."""
        try:
            self._positions = await self.client.get_positions()
            logger.debug(f"Refreshed positions for {len(self._positions)} markets")
            
            # Update market states with new positions
            for market_id in self._positions:
                if market_id in self._order_books:
                    self._update_market_state(market_id)
                    
        except Exception as e:
            logger.warning(f"Failed to refresh positions: {e}")
    
    def _update_market_state(self, market_id: str) -> None:
        """Update the complete market state for a market."""
        if market_id not in self._markets:
            return
        
        state = MarketState(
            market=self._markets.get(market_id, Market(market_id=market_id, condition_id=market_id, question="")),
            order_book=self._order_books.get(market_id, OrderBook(market_id=market_id)),
            positions=self._positions.get(market_id, {}),
            open_orders=[],  # Will be populated by execution engine
            timestamp=datetime.utcnow(),
        )
        
        self._market_states[market_id] = state
        
        # Notify callback if set
        if self.on_update:
            try:
                callback_start = time.perf_counter()
                self.on_update(market_id, state)
                callback_duration_ms = (time.perf_counter() - callback_start) * 1000
                self._callback_count += 1
                self._callback_latency_ms_total += callback_duration_ms
                self._callback_latency_ms_max = max(
                    self._callback_latency_ms_max,
                    callback_duration_ms,
                )
            except Exception as e:
                self._callback_error_count += 1
                logger.error(f"Update callback error for {market_id}: {e}")
    
    def get_market_state(self, market_id: str) -> Optional[MarketState]:
        """
        Get the latest state snapshot for a market.
        
        Returns None if the market hasn't been loaded yet.
        """
        return self._market_states.get(market_id)
    
    def get_all_market_states(self) -> dict[str, MarketState]:
        """Get all current market states."""
        return self._market_states.copy()
    
    def consume_changed_market_states(self, limit: Optional[int] = None) -> dict[str, MarketState]:
        """
        Get and clear recently changed market states.
        
        This allows callers (dashboard) to process incremental updates rather
        than rebuilding every market on each tick.
        """
        if not self._dirty_market_ids:
            return {}
        
        changed_ids = list(self._dirty_market_ids)
        if limit is not None:
            changed_ids = changed_ids[:limit]
        
        for market_id in changed_ids:
            self._dirty_market_ids.discard(market_id)
        
        return {
            market_id: self._market_states[market_id]
            for market_id in changed_ids
            if market_id in self._market_states
        }
    
    def get_order_book(self, market_id: str) -> Optional[OrderBook]:
        """Get the latest order book for a market."""
        return self._order_books.get(market_id)
    
    def get_position(self, market_id: str, token_type: TokenType) -> Optional[Position]:
        """Get position for a specific market and token."""
        market_positions = self._positions.get(market_id, {})
        return market_positions.get(token_type)
    
    def get_positions(self, market_id: str) -> dict[TokenType, Position]:
        """Get all positions for a market."""
        return self._positions.get(market_id, {})
    
    def get_market(self, market_id: str) -> Optional[Market]:
        """Get market information."""
        return self._markets.get(market_id)
    
    @property
    def update_count(self) -> int:
        """Get total number of order book updates received."""
        return self._update_count
    
    @property
    def is_running(self) -> bool:
        """Check if the data feed is running."""
        return self._running
    
    def get_staleness(self, market_id: str) -> Optional[float]:
        """
        Get time since last update for a market (in seconds).
        Returns None if never updated.
        """
        if market_id not in self._last_update:
            return None
        return (datetime.utcnow() - self._last_update[market_id]).total_seconds()
    
    def get_staleness_summary(self) -> dict[str, float]:
        """Return aggregate staleness metrics for observability."""
        if not self._last_update:
            return {
                "avg_staleness_seconds": 0.0,
                "max_staleness_seconds": 0.0,
                "p95_staleness_seconds": 0.0,
            }
        
        staleness_values = sorted(
            (datetime.utcnow() - ts).total_seconds()
            for ts in self._last_update.values()
        )
        count = len(staleness_values)
        avg = sum(staleness_values) / count
        p95_index = max(0, min(count - 1, int(count * 0.95) - 1))
        
        return {
            "avg_staleness_seconds": avg,
            "max_staleness_seconds": staleness_values[-1],
            "p95_staleness_seconds": staleness_values[p95_index],
        }
    
    def get_runtime_stats(self) -> dict[str, float]:
        """Expose runtime timings/counters for dashboards and profiling."""
        uptime_seconds = 0.0
        if self._started_at:
            uptime_seconds = (datetime.utcnow() - self._started_at).total_seconds()
        
        avg_state_ms = (
            self._state_update_latency_ms_total / self._state_updates
            if self._state_updates else 0.0
        )
        avg_callback_ms = (
            self._callback_latency_ms_total / self._callback_count
            if self._callback_count else 0.0
        )
        return {
            "feed_uptime_seconds": uptime_seconds,
            "stream_reconnects": float(self._stream_reconnects),
            "stream_errors": float(self._stream_errors),
            "state_updates": float(self._state_updates),
            "avg_state_update_ms": avg_state_ms,
            "max_state_update_ms": self._state_update_latency_ms_max,
            "callback_count": float(self._callback_count),
            "callback_errors": float(self._callback_error_count),
            "avg_callback_ms": avg_callback_ms,
            "max_callback_ms": self._callback_latency_ms_max,
            "dirty_market_count": float(len(self._dirty_market_ids)),
        }
    
    async def wait_for_data(self, timeout: float = 10.0) -> bool:
        """
        Wait until data is available for all markets.
        
        Returns True if data is available, False on timeout.
        """
        start = datetime.utcnow()
        while (datetime.utcnow() - start).total_seconds() < timeout:
            if all(m in self._order_books for m in self.market_ids):
                return True
            await asyncio.sleep(0.1)
        return False

