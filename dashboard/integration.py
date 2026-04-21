"""
Dashboard Integration
======================

Integrates the dashboard with the trading bot components.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Optional

from dashboard.server import dashboard_state

logger = logging.getLogger(__name__)


class DashboardIntegration:
    """
    Integrates the trading bot with the dashboard.
    
    Updates the dashboard state with live data from the bot.
    """
    
    def __init__(
        self,
        data_feed=None,
        arb_engine=None,
        execution_engine=None,
        risk_manager=None,
        portfolio=None,
        profit_telemetry=None,
        mode: str = "dry_run",
    ):
        self.data_feed = data_feed
        self.arb_engine = arb_engine
        self.execution_engine = execution_engine
        self.risk_manager = risk_manager
        self.portfolio = portfolio
        self.profit_telemetry = profit_telemetry
        
        dashboard_state.mode = mode
        dashboard_state.is_running = False
        
        self._update_task: Optional[asyncio.Task] = None
        self._running = False
        self._loop_count = 0
        self._ws_market_limit = 250
        self._full_market_refresh_interval = 30
    
    async def start(self, update_interval: float = 1.0) -> None:
        """Start the dashboard integration."""
        self._running = True
        dashboard_state.is_running = True
        
        self._update_task = asyncio.create_task(
            self._update_loop(update_interval)
        )
        
        logger.info("Dashboard integration started")
    
    async def stop(self) -> None:
        """Stop the dashboard integration."""
        self._running = False
        dashboard_state.is_running = False
        
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Dashboard integration stopped")
    
    async def _update_loop(self, interval: float) -> None:
        """Periodically update the dashboard state."""
        while self._running:
            try:
                await self._update_state()
                await self._broadcast_update()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Dashboard update error: {e}")
                await asyncio.sleep(interval)
    
    async def _update_state(self) -> None:
        """Update the dashboard state from bot components."""
        loop_started_at = time.perf_counter()
        
        # Update markets
        if self.data_feed:
            changed_states = self.data_feed.consume_changed_market_states(limit=5000)
            if not changed_states and not dashboard_state.markets:
                changed_states = self.data_feed.get_all_market_states()
            
            for market_id, state in changed_states.items():
                ob = state.order_book
                dashboard_state.markets[market_id] = {
                    "market_id": market_id,
                    "question": state.market.question[:80] if state.market.question else market_id,
                    "best_bid_yes": ob.best_bid_yes,
                    "best_ask_yes": ob.best_ask_yes,
                    "best_bid_no": ob.best_bid_no,
                    "best_ask_no": ob.best_ask_no,
                    "total_ask": ob.total_ask,
                    "total_bid": ob.total_bid,
                    "spread_yes": ob.yes.spread if ob.yes else None,
                    "spread_no": ob.no.spread if ob.no else None,
                }
        
        # Update portfolio
        if self.portfolio:
            summary = self.portfolio.get_summary()
            dashboard_state.portfolio = summary
        
        # Update risk
        if self.risk_manager:
            dashboard_state.risk = self.risk_manager.get_summary()
        
        # Update orders
        if self.execution_engine:
            orders = self.execution_engine.get_open_orders()
            dashboard_state.orders = [
                {
                    "order_id": o.order_id,
                    "market_id": o.market_id,
                    "side": o.side.value,
                    "token_type": o.token_type.value,
                    "price": o.price,
                    "size": o.size,
                    "filled_size": o.filled_size,
                    "status": o.status.value,
                }
                for o in orders
            ]
            
            # Update stats
            stats = self.execution_engine.get_stats()
            dashboard_state.stats = {
                "orders_placed": stats.orders_placed,
                "orders_filled": stats.orders_filled,
                "orders_cancelled": stats.orders_cancelled,
                "orders_rejected": stats.orders_rejected,
                "signals_rejected": stats.signals_rejected,
                "slippage_rejections": stats.slippage_rejections,
                "signals_processed": stats.signals_processed,
            }
        
        # Update arb stats and timing
        if self.arb_engine:
            arb_stats = self.arb_engine.get_stats()
            dashboard_state.stats.update({
                "bundle_opportunities": arb_stats.bundle_opportunities_detected,
                "mm_opportunities": arb_stats.mm_opportunities_detected,
                "signals_generated": arb_stats.signals_generated,
            })
            
            # Update opportunity timing stats
            dashboard_state.timing = self.arb_engine.get_timing_stats()
        
        # Update operational stats
        if self.data_feed:
            if self._loop_count % self._full_market_refresh_interval == 0:
                # Keep state in sync in case any market is missed by incremental flow.
                for market_id, state in self.data_feed.get_all_market_states().items():
                    if market_id not in dashboard_state.markets:
                        ob = state.order_book
                        dashboard_state.markets[market_id] = {
                            "market_id": market_id,
                            "question": state.market.question[:80] if state.market.question else market_id,
                            "best_bid_yes": ob.best_bid_yes,
                            "best_ask_yes": ob.best_ask_yes,
                            "best_bid_no": ob.best_bid_no,
                            "best_ask_no": ob.best_ask_no,
                            "total_ask": ob.total_ask,
                            "total_bid": ob.total_bid,
                            "spread_yes": ob.yes.spread if ob.yes else None,
                            "spread_no": ob.no.spread if ob.no else None,
                        }
            
            markets_with_data = len([
                m for m in dashboard_state.markets.values()
                if m.get("best_bid_yes") is not None or m.get("best_ask_yes") is not None
            ])
            staleness = self.data_feed.get_staleness_summary()
            feed_stats = self.data_feed.get_runtime_stats()
            client_stats = self.data_feed.client.get_runtime_stats()
            queue_size = self.execution_engine.signal_queue_size if self.execution_engine else 0
            dashboard_state.operational = {
                "total_markets": len(self.data_feed.market_ids),
                "markets_with_orderbooks": len(dashboard_state.markets),
                "markets_with_prices": markets_with_data,
                "orderbook_updates": self.data_feed.update_count,
                "is_streaming": self.data_feed.is_running,
                "signal_queue_size": queue_size,
                "avg_staleness_seconds": staleness["avg_staleness_seconds"],
                "p95_staleness_seconds": staleness["p95_staleness_seconds"],
                "max_staleness_seconds": staleness["max_staleness_seconds"],
                "avg_state_update_ms": feed_stats["avg_state_update_ms"],
                "avg_callback_ms": feed_stats["avg_callback_ms"],
                "stream_reconnects": int(feed_stats["stream_reconnects"]),
                "stream_errors": int(feed_stats["stream_errors"]),
                "market_discovery_batches": int(client_stats.get("market_discovery_batches", 0)),
                "market_discovery_duration_ms": client_stats.get("market_discovery_duration_ms", 0.0),
                "orderbook_rotations": int(client_stats.get("orderbook_rotations", 0)),
                "last_rotation_duration_ms": client_stats.get("last_rotation_duration_ms", 0.0),
                "avg_rotation_duration_ms": client_stats.get("avg_rotation_duration_ms", 0.0),
                "max_rotation_duration_ms": client_stats.get("max_rotation_duration_ms", 0.0),
                "cache_connected": bool(client_stats.get("cache_connected", 0)),
                "cache_reads": int(client_stats.get("cache_reads", 0)),
                "cache_writes": int(client_stats.get("cache_writes", 0)),
                "cache_hits": int(client_stats.get("cache_hits", 0)),
                "cache_misses": int(client_stats.get("cache_misses", 0)),
                "cache_errors": int(client_stats.get("cache_errors", 0)),
                "cache_last_read_ms": client_stats.get("cache_last_read_ms", 0.0),
                "cache_last_write_ms": client_stats.get("cache_last_write_ms", 0.0),
            }
        
        if self.profit_telemetry is not None:
            try:
                dashboard_state.profit_telemetry = self.profit_telemetry.summary()
            except Exception as exc:
                logger.debug("Failed to refresh profit telemetry: %s", exc)
        
        self._loop_count += 1
        dashboard_state.operational["dashboard_update_ms"] = (time.perf_counter() - loop_started_at) * 1000
        dashboard_state.last_update = datetime.utcnow()
    
    async def _broadcast_update(self) -> None:
        """Broadcast update to connected clients."""
        await dashboard_state.broadcast({
            "type": "update",
            "data": dashboard_state.to_dict(include_markets=True, market_limit=self._ws_market_limit),
        })
    
    def add_opportunity(
        self,
        opportunity_type: str,
        market_id: str,
        edge: float,
        **kwargs
    ) -> None:
        """Add an opportunity to the dashboard."""
        opp = {
            "type": opportunity_type,
            "market_id": market_id,
            "edge": edge,
            **kwargs
        }
        dashboard_state.add_opportunity(opp)
    
    def add_signal(
        self,
        action: str,
        market_id: str,
        **kwargs
    ) -> None:
        """Add a signal to the dashboard."""
        signal = {
            "action": action,
            "market_id": market_id,
            **kwargs
        }
        dashboard_state.add_signal(signal)
    
    def add_trade(
        self,
        side: str,
        price: float,
        size: float,
        **kwargs
    ) -> None:
        """Add a trade to the dashboard."""
        trade = {
            "side": side,
            "price": price,
            "size": size,
            **kwargs
        }
        dashboard_state.add_trade(trade)

