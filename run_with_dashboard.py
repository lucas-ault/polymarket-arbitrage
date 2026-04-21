#!/usr/bin/env python3
"""
Run Trading Bot with Dashboard
===============================

Starts the trading bot and web dashboard together.
Supports cross-platform arbitrage between Polymarket and Kalshi.

Usage:
    python run_with_dashboard.py              # Dry run mode
    python run_with_dashboard.py --live       # Live mode
    python run_with_dashboard.py --port 8080  # Custom port
"""

import argparse
import asyncio
import logging
import socket
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import uvicorn

from polymarket_client import PolymarketClient
from kalshi_client import KalshiClient
from core.data_feed import DataFeed
from core.arb_engine import ArbEngine, ArbConfig
from core.execution import ExecutionEngine, ExecutionConfig
from core.risk_manager import RiskManager, RiskConfig
from core.portfolio import Portfolio
from core.cross_platform_arb import CrossPlatformArbEngine
from utils.config_loader import load_config, BotConfig
from utils.logging_utils import setup_logging, trade_logger
from utils.redis_cache import RedisCacheConfig, create_cache_store
from dashboard.server import app, dashboard_state
from dashboard.integration import DashboardIntegration


logger = logging.getLogger(__name__)
MATCHES_CACHE_KEY = "xplat:matches:v1"
MATCHES_CACHE_SCHEMA_VERSION = 1


class TradingBotWithDashboard:
    """Trading bot with integrated dashboard."""
    
    def __init__(self, config: BotConfig, port: int = 8888, host: str = "0.0.0.0"):
        self.config = config
        self.port = port
        self.host = host
        self._running = False
        
        # Components - Polymarket
        self.client = None
        self.data_feed = None
        self.arb_engine = None
        self.execution_engine = None
        self.risk_manager = None
        self.portfolio = None
        self.dashboard_integration = None
        self.cache_store = None
        
        # Components - Kalshi (cross-platform)
        self.kalshi_client = None
        self.cross_platform_engine = None
        self.market_matcher = None
        self._kalshi_markets = []
        self._matched_pairs = []
        self._matching_task = None
        
        # Server
        self._server = None
        self._server_task = None
        self._kalshi_task = None
        self._fill_task = None
        self._live_fill_task = None
        self._portfolio_sync_task = None
        self._stopping = False
        self._seen_trade_ids: set[str] = set()
        self._live_fill_bootstrapped = False
    
    async def start(self) -> None:
        """Start the bot and dashboard."""
        logger.info("=" * 60)
        logger.info("Polymarket + Kalshi Arbitrage Bot")
        logger.info("=" * 60)
        logger.info(f"Mode: {'DRY RUN' if self.config.is_dry_run else 'LIVE'}")
        logger.info(f"Cross-Platform: {'ENABLED' if self.config.mode.cross_platform_enabled else 'DISABLED'}")
        logger.info(f"Dashboard bind: {self.host}:{self.port}")
        logger.info(f"Dashboard (local): http://localhost:{self.port}")
        if self.host in ("0.0.0.0", "::"):
            lan_ip = self._get_lan_ip()
            if lan_ip:
                logger.info(f"Dashboard (LAN): http://{lan_ip}:{self.port}")
        logger.info("=" * 60)
        
        self._running = True
        
        # Initialize Polymarket API client
        cache_config = RedisCacheConfig(
            enabled=self.config.cache.enabled,
            backend=self.config.cache.backend,
            redis_url=self.config.cache.redis_url,
            key_prefix=self.config.cache.key_prefix,
            connect_timeout_seconds=self.config.cache.connect_timeout_seconds,
            op_timeout_seconds=self.config.cache.op_timeout_seconds,
            markets_ttl_seconds=self.config.cache.markets_ttl_seconds,
            matches_ttl_seconds=self.config.cache.matches_ttl_seconds,
        )
        self.cache_store = await create_cache_store(cache_config)
        self.client = PolymarketClient(
            public_url=self.config.api.polymarket_public_url,
            private_url=self.config.api.polymarket_private_url,
            markets_ws_url=self.config.api.polymarket_markets_ws_url,
            private_ws_url=self.config.api.polymarket_private_ws_url,
            key_id=self.config.api.key_id,
            secret_key=self.config.api.secret_key,
            timeout=self.config.api.timeout_seconds,
            max_retries=self.config.api.max_retries,
            retry_delay=self.config.api.retry_delay_seconds,
            dry_run=self.config.is_dry_run,
            use_websocket=self.config.api.use_websocket,
            use_rest_fallback=self.config.api.use_rest_fallback,
            cache_store=self.cache_store,
            markets_cache_ttl_seconds=self.config.cache.markets_ttl_seconds,
        )
        await self.client.connect()
        
        # Initialize Kalshi client (if cross-platform enabled)
        if self.config.mode.cross_platform_enabled and self.config.mode.kalshi_enabled:
            logger.info("Initializing Kalshi client for cross-platform arbitrage...")
            self.kalshi_client = KalshiClient(
                base_url=self.config.api.kalshi_api_url,
                timeout=self.config.api.timeout_seconds,
                max_retries=self.config.api.max_retries,
                dry_run=self.config.is_dry_run,
            )
            
            # Initialize cross-platform arbitrage engine
            self.cross_platform_engine = CrossPlatformArbEngine(
                min_edge=self.config.trading.min_edge,
                polymarket_fee_theta=self.config.trading.fee_theta_taker,
                kalshi_taker_fee=self.config.trading.taker_fee_bps / 10000,
                gas_cost=0.0,
                match_min_similarity=self.config.mode.min_match_similarity,
            )
            self.market_matcher = self.cross_platform_engine.matcher
            
            # Start Kalshi monitoring in background
            self._kalshi_task = asyncio.create_task(self._start_kalshi_monitoring(), name="kalshi_monitoring")
        
        # Initialize portfolio
        initial_balance = (
            self.config.mode.dry_run_initial_balance 
            if self.config.is_dry_run 
            else 0.0
        )
        self.portfolio = Portfolio(initial_balance=initial_balance)
        
        # Initialize risk manager
        self.risk_manager = RiskManager(RiskConfig(
            max_position_per_market=self.config.risk.max_position_per_market,
            max_global_exposure=self.config.risk.max_global_exposure,
            max_daily_loss=self.config.risk.max_daily_loss,
            max_drawdown_pct=self.config.risk.max_drawdown_pct,
            trade_only_high_volume=self.config.risk.trade_only_high_volume,
            min_24h_volume=self.config.risk.min_24h_volume,
            whitelist=self.config.risk.whitelist,
            blacklist=self.config.risk.blacklist,
            kill_switch_enabled=self.config.risk.kill_switch_enabled,
        ))
        
        # Initialize execution engine
        self.execution_engine = ExecutionEngine(
            client=self.client,
            risk_manager=self.risk_manager,
            portfolio=self.portfolio,
            config=ExecutionConfig(
                slippage_tolerance=self.config.trading.slippage_tolerance,
                order_timeout_seconds=self.config.trading.order_timeout_seconds,
                dry_run=self.config.is_dry_run,
            ),
        )
        await self.execution_engine.start()
        
        # Initialize arb engine
        self.arb_engine = ArbEngine(ArbConfig(
            min_edge=self.config.trading.min_edge,
            bundle_arb_enabled=self.config.trading.bundle_arb_enabled,
            min_spread=self.config.trading.min_spread,
            mm_enabled=self.config.trading.mm_enabled,
            tick_size=self.config.trading.tick_size,
            default_order_size=self.config.trading.default_order_size,
            min_order_size=self.config.trading.min_order_size,
            max_order_size=self.config.trading.max_order_size,
            maker_fee_bps=self.config.trading.maker_fee_bps,
            taker_fee_bps=self.config.trading.taker_fee_bps,
            fee_theta_taker=self.config.trading.fee_theta_taker,
            fee_theta_maker=self.config.trading.fee_theta_maker,
        ))
        
        # Initialize data feed
        market_ids = self.config.trading.markets.copy()
        self.data_feed = DataFeed(
            client=self.client,
            market_ids=market_ids,
            position_refresh_interval=30.0 if self.config.is_live else 5.0,
            on_update=self._on_market_update,
            config=self.config,
        )
        await self.data_feed.start()
        
        # Initialize dashboard integration
        self.dashboard_integration = DashboardIntegration(
            data_feed=self.data_feed,
            arb_engine=self.arb_engine,
            execution_engine=self.execution_engine,
            risk_manager=self.risk_manager,
            portfolio=self.portfolio,
            mode="dry_run" if self.config.is_dry_run else "live",
        )
        await self.dashboard_integration.start()
        
        # Start fill simulation for dry run
        if self.config.is_dry_run and self.config.mode.simulate_fills:
            self._fill_task = asyncio.create_task(self._simulate_fills(), name="simulate_fills")
        elif not self.config.is_dry_run:
            # Keep portfolio/risk/dashboard in sync with exchange fills in live mode.
            self._live_fill_task = asyncio.create_task(
                self._poll_live_fills(),
                name="poll_live_fills",
            )
            self._portfolio_sync_task = asyncio.create_task(
                self._sync_live_portfolio_metrics(),
                name="sync_live_portfolio_metrics",
            )
        
        # Start the web server
        await self._start_server()
        
        logger.info("Bot and dashboard started successfully!")
        logger.info(f"Open http://localhost:{self.port} (same device)")
        if self.host in ("0.0.0.0", "::"):
            lan_ip = self._get_lan_ip()
            if lan_ip:
                logger.info(f"From another device: http://{lan_ip}:{self.port}")
    
    async def _start_server(self) -> None:
        """Start the uvicorn server."""
        config = uvicorn.Config(
            app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._server_task = asyncio.create_task(self._server.serve())

    @staticmethod
    def _get_lan_ip() -> str | None:
        """Best-effort LAN IP for cross-device dashboard access."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # No packets are sent; this only resolves the outbound interface.
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
        except OSError:
            return None
        finally:
            sock.close()
    
    def _on_market_update(self, market_id: str, market_state) -> None:
        """Handle market updates."""
        if not self._running:
            return

        self._mark_portfolio_to_market(market_id, market_state)
        
        # Check risk limits
        if not self.risk_manager.within_global_limits():
            return
        
        # Analyze for opportunities
        signals = self.arb_engine.analyze(market_state)
        
        for signal in signals:
            # Add to dashboard
            if signal.opportunity:
                self.dashboard_integration.add_opportunity(
                    opportunity_type=signal.opportunity.opportunity_type.value,
                    market_id=signal.market_id,
                    edge=signal.opportunity.edge,
                    suggested_size=signal.opportunity.suggested_size,
                )
            
            # Submit to execution
            if not self.execution_engine.submit_signal_nowait(signal):
                logger.warning("Execution queue full; dropping dashboard signal %s", signal.signal_id)

    @staticmethod
    def _pick_mark_price(best_bid: float | None, best_ask: float | None, fallback: float | None) -> float | None:
        """Pick a conservative mark price from top-of-book and snapshot fallbacks."""
        bid = float(best_bid) if best_bid is not None else 0.0
        ask = float(best_ask) if best_ask is not None else 0.0
        fb = float(fallback) if fallback is not None else 0.0

        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        if ask > 0:
            return ask
        if bid > 0:
            return bid
        if fb > 0:
            return fb
        return None

    def _estimate_token_mark_prices(self, market_state) -> tuple[float, float] | None:
        """Estimate YES/NO mark prices from the latest market state."""
        if not market_state:
            return None
        order_book = market_state.order_book
        market = market_state.market
        yes_price = self._pick_mark_price(
            order_book.best_bid_yes,
            order_book.best_ask_yes,
            market.yes_price,
        )
        no_price = self._pick_mark_price(
            order_book.best_bid_no,
            order_book.best_ask_no,
            market.no_price,
        )

        if yes_price is None and no_price is not None:
            yes_price = max(0.0, min(1.0, 1.0 - no_price))
        if no_price is None and yes_price is not None:
            no_price = max(0.0, min(1.0, 1.0 - yes_price))
        if yes_price is None or no_price is None:
            return None
        return yes_price, no_price

    def _mark_portfolio_to_market(self, market_id: str, market_state) -> None:
        """Keep portfolio and risk PnL aligned with latest market prices."""
        if not self.portfolio:
            return
        prices = self._estimate_token_mark_prices(market_state)
        if not prices:
            return
        yes_price, no_price = prices
        self.portfolio.update_prices(market_id, yes_price=yes_price, no_price=no_price)
        if self.risk_manager:
            pnl = self.portfolio.get_pnl()
            self.risk_manager.update_pnl(
                realized_pnl=float(pnl.get("realized_pnl", 0.0)),
                unrealized_pnl=float(pnl.get("unrealized_pnl", 0.0)),
            )
    
    async def _simulate_fills(self) -> None:
        """Simulate order fills in dry run mode."""
        import random
        
        while self._running:
            try:
                await asyncio.sleep(2.0)
                
                orders = self.execution_engine.get_open_orders()
                for order in orders:
                    if random.random() < self.config.mode.fill_probability:
                        trade = self.client.simulate_fill(order.order_id)
                        if trade:
                            self.execution_engine.handle_fill(trade)
                            self.dashboard_integration.add_trade(
                                side=trade.side.value,
                                price=trade.price,
                                size=trade.size,
                                market_id=trade.market_id,
                            )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Fill simulation error: {e}")

    async def _poll_live_fills(self) -> None:
        """Poll recent live trades and propagate new fills through local state."""
        if not self.client or not self.execution_engine:
            return
        while self._running:
            try:
                await asyncio.sleep(1.5)
                trades = await self.client.get_trades(limit=200)
                if not trades:
                    continue
                if not self._live_fill_bootstrapped:
                    for trade in trades:
                        if trade.trade_id:
                            self._seen_trade_ids.add(trade.trade_id)
                    self._live_fill_bootstrapped = True
                    logger.info(
                        "Live fill poll bootstrapped with %s historical trades; processing only new fills",
                        len(self._seen_trade_ids),
                    )
                    continue
                # Process oldest first so position/PnL updates are monotonic.
                for trade in sorted(trades, key=lambda t: getattr(t, "timestamp", datetime.utcnow())):
                    if not trade.trade_id or trade.trade_id in self._seen_trade_ids:
                        continue
                    self._seen_trade_ids.add(trade.trade_id)
                    self.execution_engine.handle_fill(trade)
                    if self.dashboard_integration:
                        self.dashboard_integration.add_trade(
                            side=trade.side.value,
                            price=trade.price,
                            size=trade.size,
                            market_id=trade.market_id,
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Live fill poll error: {e}")

    async def _sync_live_portfolio_metrics(self) -> None:
        """Sync exchange portfolio API metrics into dashboard-visible summary."""
        if not self.client or not self.portfolio:
            return
        while self._running:
            try:
                await asyncio.sleep(15.0)
                metrics = await self.client.get_portfolio_metrics(activity_limit=200)
                if not metrics:
                    continue
                self.portfolio.apply_exchange_metrics(metrics)
                pnl = metrics.get("pnl") if isinstance(metrics.get("pnl"), dict) else {}
                if self.risk_manager:
                    self.risk_manager.update_pnl(
                        realized_pnl=float(pnl.get("realized_pnl", 0.0)),
                        unrealized_pnl=float(pnl.get("unrealized_pnl", 0.0)),
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Portfolio sync error: {e}")
    
    async def _start_kalshi_monitoring(self) -> None:
        """Start monitoring Kalshi markets for cross-platform arbitrage."""
        if not self.kalshi_client:
            return
        
        logger.info("Starting Kalshi market monitoring...")
        
        async with self.kalshi_client:
            kalshi_load_started_at = time.perf_counter()
            # Set up dashboard for loading state
            dashboard_state.cross_platform["enabled"] = True
            dashboard_state.cross_platform["matching_status"] = "loading"
            
            # Fetch Kalshi markets with progress updates
            logger.info("Fetching Kalshi markets...")
            
            def on_kalshi_progress(count):
                dashboard_state.cross_platform["kalshi_markets"] = count
            
            self._kalshi_markets = await self.kalshi_client.list_all_markets(
                status="open",
                max_markets=5000,
                on_progress=on_kalshi_progress,
            )
            dashboard_state.cross_platform["kalshi_load_duration_ms"] = (
                time.perf_counter() - kalshi_load_started_at
            ) * 1000
            logger.info(f"✓ Loaded {len(self._kalshi_markets)} Kalshi markets")
            
            # Update dashboard state
            dashboard_state.cross_platform["kalshi_markets"] = len(self._kalshi_markets)
            
            # Wait for at least SOME Polymarket markets to load (start matching quickly!)
            logger.info("Waiting for Polymarket markets...")
            for i in range(30):  # Wait up to 30 seconds
                await asyncio.sleep(1)
                poly_count = len(self.data_feed._markets) if self.data_feed else 0
                
                # Update dashboard with current loading progress
                dashboard_state.cross_platform["polymarket_markets"] = poly_count
                
                # Start matching as soon as we have some markets from both platforms
                if poly_count >= 50:
                    logger.info(f"Got {poly_count} Polymarket markets - starting matching!")
                    break
                    
                if i % 5 == 0:
                    logger.info(f"Polymarket: {poly_count} markets loaded...")
            
            # Match markets between platforms (run in background so dashboard stays responsive)
            if self.data_feed and self._kalshi_markets:
                polymarket_markets = list(self.data_feed._markets.values())
                logger.info(f"Starting background matching: {len(polymarket_markets)} Polymarket x {len(self._kalshi_markets)} Kalshi")
                
                # Set initial status
                dashboard_state.cross_platform["matching_status"] = "starting"
                await self._load_cached_matches()
                start_delay = getattr(self.config.mode, "cross_platform_match_start_delay_seconds", 0.0)
                if start_delay > 0:
                    await asyncio.sleep(start_delay)
                
                # Start matching as a background task (dashboard will show progress)
                self._matching_task = asyncio.create_task(self._run_matching_background(polymarket_markets))
    
    async def _run_matching_background(self, polymarket_markets: list) -> None:
        """Run market matching with live progress updates."""
        try:
            dashboard_state.cross_platform["matching_status"] = "matching"
            total = 0
            dashboard_state.cross_platform["matching_total"] = total
            matching_started_at = time.perf_counter()

            def on_progress(checked, total_comparisons, matches_found):
                dashboard_state.cross_platform["matching_checked"] = checked
                dashboard_state.cross_platform["matching_total"] = total_comparisons
                dashboard_state.cross_platform["matching_progress"] = (
                    int(checked / total_comparisons * 100)
                    if total_comparisons > 0 else 0
                )
                dashboard_state.cross_platform["matched_pairs"] = matches_found

            self._matched_pairs = await self.market_matcher.find_matches(
                polymarket_markets=polymarket_markets,
                kalshi_markets=self._kalshi_markets,
                on_progress=on_progress,
                candidate_limit=self.config.mode.cross_platform_candidate_limit,
            )
            
            dashboard_state.cross_platform["matching_status"] = "complete"
            dashboard_state.cross_platform["matching_progress"] = 100
            dashboard_state.cross_platform["matched_pairs"] = len(self._matched_pairs)
            dashboard_state.cross_platform["matching_duration_ms"] = (
                time.perf_counter() - matching_started_at
            ) * 1000
            await self._persist_cached_matches()
            
            logger.info(f"✓ Matching complete! Found {len(self._matched_pairs)} pairs")
            
            # Prepare matched pairs data for dashboard display
            matched_pairs_display = []
            for pair in self._matched_pairs[:50]:
                matched_pairs_display.append({
                    "poly_question": pair.polymarket_question,
                    "kalshi_title": pair.kalshi_title,
                    "similarity": pair.similarity_score,
                    "category": pair.category,
                })
            
            dashboard_state.cross_platform["matched_pairs_data"] = matched_pairs_display
            
        except Exception as e:
            logger.error(f"Matching error: {e}")
            import traceback
            traceback.print_exc()
            dashboard_state.cross_platform["matching_status"] = "error"

    async def _load_cached_matches(self) -> None:
        """Load previously cached match results to speed up dashboard warm-up."""
        if not self.cache_store or not self.cache_store.is_available():
            return
        payload = await self.cache_store.get_json(MATCHES_CACHE_KEY)
        if not payload or payload.get("schema_version") != MATCHES_CACHE_SCHEMA_VERSION:
            return
        target_similarity = float(self.config.mode.min_match_similarity)
        cached_similarity = float(payload.get("min_similarity", target_similarity))
        cached_pairs = payload.get("pairs", [])
        if cached_similarity < target_similarity:
            # Reuse older cache safely by keeping only pairs that satisfy the
            # stricter current threshold.
            cached_pairs = [
                pair for pair in cached_pairs
                if float(pair.get("similarity_score", 0.0)) >= target_similarity
            ]
            logger.info(
                "Filtered cached matches from min_similarity=%s to %s (%s remaining)",
                cached_similarity,
                target_similarity,
                len(cached_pairs),
            )
        try:
            loaded = self.market_matcher.import_cached_pairs(cached_pairs)
        except Exception as exc:
            logger.warning("Failed to import cached matches: %s", exc)
            return
        if loaded == 0:
            return
        self._matched_pairs = self.market_matcher.get_cached_pairs()
        dashboard_state.cross_platform["matched_pairs"] = loaded
        dashboard_state.cross_platform["matching_status"] = "cached"
        dashboard_state.cross_platform["matched_pairs_data"] = [
            {
                "poly_question": pair.polymarket_question,
                "kalshi_title": pair.kalshi_title,
                "similarity": pair.similarity_score,
                "category": pair.category,
            }
            for pair in self._matched_pairs[:50]
        ]
        logger.info("Loaded %s cached market matches", loaded)

    async def _persist_cached_matches(self) -> None:
        """Persist latest match results for warm starts."""
        if not self.cache_store or not self.cache_store.is_available():
            return
        payload = {
            "schema_version": MATCHES_CACHE_SCHEMA_VERSION,
            "cached_at": datetime.utcnow().isoformat(),
            "pair_count": len(self._matched_pairs),
            "min_similarity": self.config.mode.min_match_similarity,
            "pairs": self.market_matcher.export_cached_pairs(),
        }
        await self.cache_store.set_json(
            MATCHES_CACHE_KEY,
            payload,
            ttl_seconds=self.config.cache.matches_ttl_seconds,
        )
    
    async def stop(self) -> None:
        """Stop everything gracefully."""
        if self._stopping:
            return
        self._stopping = True
        
        logger.info("Shutting down...")
        self._running = False

        async def _cancel_task(task: asyncio.Task | None, name: str, timeout: float = 3.0) -> None:
            if not task:
                return
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except asyncio.CancelledError:
                pass
            except asyncio.TimeoutError:
                logger.warning("Timeout cancelling task %s; continuing shutdown", name)
            except Exception as exc:
                logger.warning("Error while cancelling task %s: %s", name, exc)

        async def _stop_component(component, name: str, timeout: float = 5.0) -> None:
            if not component:
                return
            try:
                await asyncio.wait_for(component.stop(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Timeout stopping %s; continuing shutdown", name)
            except Exception as exc:
                logger.warning("Error while stopping %s: %s", name, exc)

        async def _run_with_timeout(coro, name: str, timeout: float = 5.0) -> None:
            if not coro:
                return
            try:
                await asyncio.wait_for(coro, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Timeout during %s; continuing shutdown", name)
            except Exception as exc:
                logger.warning("Error during %s: %s", name, exc)

        await _cancel_task(self._fill_task, "simulate_fills")
        await _cancel_task(self._live_fill_task, "poll_live_fills")
        await _cancel_task(self._portfolio_sync_task, "sync_live_portfolio_metrics")
        await _cancel_task(self._kalshi_task, "kalshi_monitoring")
        await _cancel_task(self._matching_task, "market_matching")

        await _stop_component(self.dashboard_integration, "dashboard_integration")
        await _stop_component(self.data_feed, "data_feed")
        await _stop_component(self.execution_engine, "execution_engine")
        if self.client:
            await _run_with_timeout(self.client.disconnect(), "client_disconnect")
        
        # Kalshi client is closed via async context manager in _start_kalshi_monitoring
        
        if self._server:
            self._server.should_exit = True
        
        if self._server_task:
            try:
                await asyncio.wait_for(self._server_task, timeout=10.0)
            except asyncio.TimeoutError:
                self._server_task.cancel()
                try:
                    await asyncio.wait_for(self._server_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    logger.warning("Timed out waiting for server task cancellation")
        
        # Final summary
        if self.portfolio:
            summary = self.portfolio.get_summary()
            logger.info("=" * 60)
            logger.info("Final Summary")
            logger.info("=" * 60)
            logger.info(f"Total PnL: ${summary['pnl']['total_pnl']:.2f}")
            logger.info(f"Trades: {summary['total_trades']}")
            logger.info(f"Win Rate: {summary['win_rate']:.1%}")
        
        # Cross-platform summary
        if self.cross_platform_engine:
            cp_stats = self.cross_platform_engine.get_stats()
            logger.info(f"Cross-Platform Opportunities: {cp_stats['total_opportunities']}")
            logger.info(f"Matched Market Pairs: {cp_stats['matched_pairs']}")
        
        logger.info("Shutdown complete")
    
    async def run_forever(self) -> None:
        """Run until interrupted."""
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass


async def main_async(args: argparse.Namespace) -> None:
    """Async main function."""
    # Load config
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    
    # Override mode
    if args.live:
        config.mode.trading_mode = "live"
    elif args.dry_run:
        config.mode.trading_mode = "dry_run"
    if config.is_live and config.mode.simulate_fills:
        logger.warning("simulate_fills is enabled but trading_mode is live; forcing simulate_fills=false")
        config.mode.simulate_fills = False

    # Configure logging from config (with CLI verbose override for console output)
    console_level = "DEBUG" if args.verbose else config.logging.console_level
    setup_logging(
        log_dir=config.logging.log_dir,
        console_level=console_level,
        file_level=config.logging.file_level,
        main_log_file=config.logging.main_log_file,
        trades_log_file=config.logging.trades_log_file,
        opportunities_log_file=config.logging.opportunities_log_file,
        max_size_mb=config.logging.max_log_size_mb,
        backup_count=config.logging.backup_count,
    )
    trade_logger.set_context(
        entrypoint="run_with_dashboard.py",
        config_path=str(Path(args.config).expanduser().resolve()),
        trading_mode=config.mode.trading_mode,
        simulate_fills=config.mode.simulate_fills,
        fill_probability=config.mode.fill_probability,
        min_edge=config.trading.min_edge,
        min_spread=config.trading.min_spread,
        default_order_size=config.trading.default_order_size,
        max_order_size=config.trading.max_order_size,
        trade_only_high_volume=config.risk.trade_only_high_volume,
        min_24h_volume=config.risk.min_24h_volume,
    )
    trade_logger.log_session_start()
    
    # Create and run bot with dashboard
    bot = TradingBotWithDashboard(config, port=args.port, host=args.host)
    
    # Handle shutdown
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()
    stdin_reader_registered = False
    
    def signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    def on_stdin_ready() -> None:
        """Handle terminal input without blocking executor threads."""
        try:
            line = sys.stdin.readline()
        except Exception:
            return
        if line and line.strip().lower() in {"q", "quit", "exit", "stop"}:
            logger.info("Quit command received from terminal.")
            shutdown_event.set()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass
    
    try:
        await bot.start()
        
        if sys.stdin and sys.stdin.isatty():
            try:
                loop.add_reader(sys.stdin.fileno(), on_stdin_ready)
                stdin_reader_registered = True
                logger.info("Type 'q' and press Enter to quit.")
            except (NotImplementedError, OSError, ValueError):
                # Some environments do not support add_reader for stdin.
                logger.info("Quit shortcut unavailable in this terminal; use Ctrl+C to stop.")
        
        # Wait for shutdown
        await shutdown_event.wait()
        
    except KeyboardInterrupt:
        pass
    finally:
        if stdin_reader_registered:
            try:
                loop.remove_reader(sys.stdin.fileno())
            except Exception:
                pass
        await bot.stop()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Polymarket Arbitrage Bot with Live Dashboard"
    )
    
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Config file path"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8888,
        help="Dashboard port (default: 8888)"
    )

    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Dashboard bind host (default: 0.0.0.0, use 127.0.0.1 for local-only)"
    )
    
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run in live mode"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Run in dry-run mode (default)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging"
    )
    
    args = parser.parse_args()
    
    # Run
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nShutdown complete.")


if __name__ == "__main__":
    main()

