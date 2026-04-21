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
from core.auto_take_profit import AutoTakeProfitConfig, AutoTakeProfitMonitor
from core.execution import ExecutionEngine, ExecutionConfig
from core.risk_manager import RiskManager, RiskConfig
from core.portfolio import Portfolio
from core.cross_platform_arb import CrossPlatformArbEngine
from utils.bootstrap import bootstrap_components
from utils.config_loader import load_config, BotConfig
from utils.logging_utils import setup_logging, trade_logger
from utils.profit_telemetry import ProfitTelemetry
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
        self._private_ws_task = None
        self._portfolio_sync_task = None
        self._heartbeat_task = None
        self._stopping = False
        self._seen_trade_ids: set[str] = set()
        self._live_fill_bootstrapped = False
        self.auto_take_profit_monitor: AutoTakeProfitMonitor | None = None
    
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
        
        # Build the shared component graph once so bot-only and bot+dashboard
        # entrypoints stay in lockstep on risk wiring, kill-switch hooks, and
        # data-feed plumbing.
        components = await bootstrap_components(
            self.config,
            on_market_update=self._on_market_update,
        )
        self.cache_store = components.cache_store
        self.client = components.client
        self.portfolio = components.portfolio
        self.risk_manager = components.risk_manager
        self.execution_engine = components.execution_engine
        self.arb_engine = components.arb_engine
        self.data_feed = components.data_feed
        self.profit_telemetry = ProfitTelemetry()
        self.auto_take_profit_monitor = AutoTakeProfitMonitor(
            AutoTakeProfitConfig(
                enabled=bool(self.config.trading.auto_take_profit_enabled),
                min_net_profit_usd=float(
                    self.config.trading.auto_take_profit_profit_threshold_usd
                ),
                cooldown_seconds=float(
                    self.config.trading.auto_take_profit_cooldown_seconds
                ),
                urgent_exit_enabled=bool(self.config.trading.urgent_exit_enabled),
                urgent_exit_use_close_position=bool(
                    self.config.trading.urgent_exit_use_close_position
                ),
                urgent_exit_max_slippage_ticks=int(
                    self.config.trading.urgent_exit_max_slippage_ticks
                ),
            ),
            self.execution_engine,
            self.portfolio,
            fee_theta_taker=float(self.config.trading.fee_theta_taker),
        )
        
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
        
        await self.data_feed.start()
        
        # Initialize dashboard integration
        self.dashboard_integration = DashboardIntegration(
            data_feed=self.data_feed,
            arb_engine=self.arb_engine,
            execution_engine=self.execution_engine,
            risk_manager=self.risk_manager,
            portfolio=self.portfolio,
            profit_telemetry=self.profit_telemetry,
            auto_take_profit_monitor=self.auto_take_profit_monitor,
            mode="dry_run" if self.config.is_dry_run else "live",
            ws_market_limit=(
                None
                if int(getattr(self.config.monitoring, "dashboard_market_limit", 300) or 0) <= 0
                else int(getattr(self.config.monitoring, "dashboard_market_limit", 300))
            ),
        )
        await self.dashboard_integration.start()
        
        # Start fill simulation for dry run
        if self.config.is_dry_run and self.config.mode.simulate_fills:
            self._fill_task = asyncio.create_task(self._simulate_fills(), name="simulate_fills")
        elif not self.config.is_dry_run:
            self._private_ws_task = asyncio.create_task(
                self._consume_live_private_updates(),
                name="consume_live_private_updates",
            )
            # Keep portfolio/risk/dashboard in sync with exchange fills in live mode.
            self._live_fill_task = asyncio.create_task(
                self._poll_live_fills(),
                name="poll_live_fills",
            )
            self._portfolio_sync_task = asyncio.create_task(
                self._sync_live_portfolio_metrics(),
                name="sync_live_portfolio_metrics",
            )

        # Heartbeat: periodically log core counters so an idle bot (e.g. MM
        # disabled + no bundle-arb opportunities) doesn't look frozen between
        # the "Matching complete!" line and the next event.
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name="heartbeat",
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
        if self.auto_take_profit_monitor:
            self.auto_take_profit_monitor.maybe_submit_for_market(market_id, market_state)
        
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
                if self.profit_telemetry:
                    self.profit_telemetry.record_opportunity(
                        market_id=signal.market_id,
                        strategy=signal.opportunity.opportunity_type.value,
                        edge=float(signal.opportunity.edge),
                        fee_bps=self.config.trading.taker_fee_bps,
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
                            if self.profit_telemetry:
                                self.profit_telemetry.record_fill(
                                    market_id=trade.market_id,
                                    price=float(trade.price),
                                    size=float(trade.size),
                                    fee=float(trade.fee),
                                )
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

    async def _heartbeat_loop(self) -> None:
        """Periodically log alive-state and core counters.

        Without this, a correctly-configured bot that simply has no current
        opportunities (e.g. MM disabled + tightly-priced game markets) emits
        zero log lines after startup and looks frozen. The heartbeat surfaces
        the fact that we're still polling, how many markets we're watching,
        how many signals/orders we've produced, and the current PnL view.
        """
        interval = 60.0
        last_signal_count = 0
        last_order_count = 0
        while self._running:
            try:
                await asyncio.sleep(interval)
                if not self._running:
                    break

                markets_tracked = (
                    len(self.data_feed._markets) if self.data_feed else 0
                )
                arb_stats = self.arb_engine.get_stats() if self.arb_engine else None
                exec_stats = (
                    self.execution_engine.get_stats() if self.execution_engine else None
                )
                signal_count = int(getattr(arb_stats, "signals_generated", 0) or 0)
                orders_placed = int(getattr(exec_stats, "orders_placed", 0) or 0)
                orders_filled = int(getattr(exec_stats, "orders_filled", 0) or 0)
                orders_cancelled = int(getattr(exec_stats, "orders_cancelled", 0) or 0)
                orders_rejected = int(getattr(exec_stats, "orders_rejected", 0) or 0)
                signals_rejected = int(getattr(exec_stats, "signals_rejected", 0) or 0)
                unplaceable_skips = int(getattr(exec_stats, "unplaceable_signal_skips", 0) or 0)
                taker_attempted = int(getattr(exec_stats, "taker_orders_attempted", 0) or 0)
                taker_filled = int(getattr(exec_stats, "taker_orders_filled", 0) or 0)
                taker_rejected = int(getattr(exec_stats, "taker_orders_rejected", 0) or 0)
                urgent_exit_attempted = int(getattr(exec_stats, "urgent_exit_attempted", 0) or 0)
                urgent_exit_rejected = int(getattr(exec_stats, "urgent_exit_rejected", 0) or 0)
                open_orders = int(self.execution_engine.open_order_count) if self.execution_engine else 0
                queue_depth = int(self.execution_engine.signal_queue_size) if self.execution_engine else 0
                unplaceable_markets = (
                    int(self.execution_engine.unplaceable_market_count)
                    if self.execution_engine else 0
                )
                taker_detected = int(getattr(arb_stats, "taker_opportunities_detected", 0) or 0)
                mm_metrics = {"mm_eligible_markets": 0, "mm_quoted_markets": 0}
                if self.arb_engine and self.data_feed:
                    mm_metrics = self.arb_engine.get_market_making_metrics(
                        self.data_feed.get_all_market_states(),
                        self.execution_engine.get_open_orders() if self.execution_engine else [],
                    )

                new_signals = signal_count - last_signal_count
                new_orders = orders_placed - last_order_count
                last_signal_count = signal_count
                last_order_count = orders_placed

                pnl = self.portfolio.get_pnl() if self.portfolio else {"total_pnl": 0.0}
                exposure = (
                    self.portfolio.get_total_exposure() if self.portfolio else 0.0
                )

                kill_switch = False
                if self.risk_manager is not None:
                    risk_summary = self.risk_manager.get_summary()
                    kill_switch = bool(risk_summary.get("kill_switch_triggered", False))

                take_profit_suffix = ""
                if self.auto_take_profit_monitor:
                    close_candidates = self.auto_take_profit_monitor.get_close_candidates(limit=2)
                    if close_candidates:
                        candidate_bits = [
                            (
                                f"{item.get('market_id')}/{item.get('token_type')} "
                                f"net=${float(item.get('net_profit_est', 0.0)):.2f} "
                                f"dist=${float(item.get('distance_usd', 0.0)):.2f}"
                            )
                            for item in close_candidates
                        ]
                        take_profit_suffix = " | tp_near=" + "; ".join(candidate_bits)

                logger.info(
                    "heartbeat | markets=%d | signals(total/new60s)=%d/%d | "
                    "orders_placed/filled=%d/%d (new placed in 60s=%d) | "
                    "open/cxl/rej/sigrej=%d/%d/%d/%d | queue=%d | "
                    "mm(pass/quoted)=%d/%d | unplaceable=%d | skip_unplaceable=%d | "
                    "taker(det/att/fill/rej)=%d/%d/%d/%d | "
                    "urgent_exit(att/rej)=%d/%d | "
                    "session PnL=$%.2f | exposure=$%.2f%s%s",
                    markets_tracked,
                    signal_count,
                    new_signals,
                    orders_placed,
                    orders_filled,
                    new_orders,
                    open_orders,
                    orders_cancelled,
                    orders_rejected,
                    signals_rejected,
                    queue_depth,
                    int(mm_metrics.get("mm_eligible_markets", 0)),
                    int(mm_metrics.get("mm_quoted_markets", 0)),
                    unplaceable_markets,
                    unplaceable_skips,
                    taker_detected,
                    taker_attempted,
                    taker_filled,
                    taker_rejected,
                    urgent_exit_attempted,
                    urgent_exit_rejected,
                    float(pnl.get("total_pnl", 0.0)),
                    float(exposure),
                    " | KILL SWITCH ACTIVE" if kill_switch else "",
                    take_profit_suffix,
                )

                # Bundle-arb visibility: when signals=0, show the operator
                # *why*. Distinguishes "no liquid 2-sided books to scan" from
                # "books are healthy but spreads stay tight enough that no
                # mispricing crosses min_edge".
                if arb_stats is not None:
                    scans = int(getattr(arb_stats, "bundle_scans_total", 0) or 0)
                    if scans > 0:
                        missing = int(
                            getattr(arb_stats, "bundle_skipped_missing_leg", 0) or 0
                        )
                        synthetic = int(
                            getattr(arb_stats, "bundle_skipped_synthetic_no", 0) or 0
                        )
                        no_edge = int(
                            getattr(arb_stats, "bundle_skipped_no_edge", 0) or 0
                        )
                        best_long = float(
                            getattr(arb_stats, "bundle_best_gross_long", float("-inf"))
                        )
                        best_short = float(
                            getattr(arb_stats, "bundle_best_gross_short", float("-inf"))
                        )
                        scannable = max(scans - missing - synthetic, 0)
                        scannable_pct = (scannable / scans * 100.0) if scans else 0.0
                        logger.info(
                            "  bundle scans=%d | one-sided=%d | synthetic-NO=%d | "
                            "real-2-sided=%d (%.0f%%) | no-edge=%d | "
                            "best gross long=%s | best gross short=%s | min_edge=%.4f",
                            scans,
                            missing,
                            synthetic,
                            scannable,
                            scannable_pct,
                            no_edge,
                            f"{best_long:+.4f}" if best_long != float("-inf") else "n/a",
                            f"{best_short:+.4f}" if best_short != float("-inf") else "n/a",
                            float(self.arb_engine.config.min_edge)
                            if self.arb_engine is not None
                            else 0.0,
                        )
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                logger.warning("heartbeat error: %s", exc)

    async def _poll_live_fills(self) -> None:
        """Poll recent live trades and propagate new fills through local state."""
        if not self.client or not self.execution_engine:
            return
        while self._running:
            try:
                await asyncio.sleep(10.0)
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
                    self._process_live_trade(trade)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Live fill poll error: {e}")

    def _process_live_trade(self, trade) -> None:
        """Deduplicate and mirror a live fill into local state."""
        if not trade or not getattr(trade, "trade_id", ""):
            return
        if trade.trade_id in self._seen_trade_ids:
            return
        self._seen_trade_ids.add(trade.trade_id)
        self.execution_engine.handle_fill(trade)
        if self.profit_telemetry:
            self.profit_telemetry.record_fill(
                market_id=trade.market_id,
                price=float(trade.price),
                size=float(trade.size),
                fee=float(trade.fee),
            )
        if self.dashboard_integration:
            self.dashboard_integration.add_trade(
                side=trade.side.value,
                price=trade.price,
                size=trade.size,
                market_id=trade.market_id,
            )

    async def _consume_live_private_updates(self) -> None:
        """Consume private websocket events for real-time fills and account state."""
        if not self.client or not self.execution_engine:
            return
        logger.info("Starting private websocket consumer for orders/positions/balance updates")
        while self._running:
            try:
                async for event in self.client.stream_private_updates():
                    if not self._running:
                        break
                    event_type = str(event.get("type") or "")
                    if event_type == "order_update":
                        trade = event.get("trade")
                        if trade is not None:
                            self._process_live_trade(trade)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Private websocket consumer error: %s", exc)
                await asyncio.sleep(1.0)

    async def _sync_live_portfolio_metrics(self) -> None:
        """Sync exchange portfolio API metrics into dashboard-visible summary.

        Polymarket rate-limits ``/v1/portfolio/activities`` aggressively. We
        poll on a 60s cadence with a small activity window and back off to
        120s on 429s instead of dropping into a tight retry loop.
        """
        if not self.client or not self.portfolio:
            return
        base_interval = 60.0
        backoff_interval = 120.0
        next_sleep = base_interval
        while self._running:
            try:
                await asyncio.sleep(next_sleep)
                metrics = await self.client.get_portfolio_metrics(activity_limit=50)
                if not metrics:
                    next_sleep = base_interval
                    continue
                self.portfolio.apply_exchange_metrics(metrics)
                next_sleep = base_interval
            except asyncio.CancelledError:
                break
            except Exception as e:
                msg = str(e).lower()
                if "429" in msg or "too many requests" in msg or "rate" in msg:
                    logger.warning(
                        "Portfolio sync rate-limited; backing off to %.0fs", backoff_interval,
                    )
                    next_sleep = backoff_interval
                else:
                    logger.error(f"Portfolio sync error: {e}")
                    next_sleep = base_interval
    
    async def _start_kalshi_monitoring(self) -> None:
        """Start monitoring Kalshi markets for cross-platform arbitrage."""
        if not self.kalshi_client:
            return
        
        logger.info("Starting Kalshi market monitoring...")
        
        async with self.kalshi_client:
            await self._refresh_kalshi_markets(reason="startup")
            
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
                await self._start_matching_task(
                    reason="startup",
                    load_cached=True,
                    apply_start_delay=True,
                )

            refresh_interval = float(
                getattr(
                    self.config.mode,
                    "cross_platform_refresh_interval_seconds",
                    0.0,
                )
                or 0.0
            )
            while self._running and refresh_interval > 0:
                await asyncio.sleep(refresh_interval)
                if not self._running:
                    return
                await self._refresh_kalshi_markets(reason="periodic refresh")
                await self._start_matching_task(reason="periodic refresh")

    async def _refresh_kalshi_markets(self, *, reason: str) -> None:
        """Fetch the current Kalshi open-market universe."""
        kalshi_load_started_at = time.perf_counter()
        dashboard_state.cross_platform["enabled"] = True
        dashboard_state.cross_platform["matching_status"] = (
            "loading" if reason == "startup" else "refreshing"
        )
        logger.info(
            "Fetching Kalshi markets..."
            if reason == "startup"
            else "Refreshing Kalshi markets for cross-platform matching..."
        )

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
        dashboard_state.cross_platform["kalshi_markets"] = len(self._kalshi_markets)
        logger.info(
            "Loaded %d Kalshi markets%s",
            len(self._kalshi_markets),
            "" if reason == "startup" else f" ({reason})",
        )

    async def _start_matching_task(
        self,
        *,
        reason: str,
        load_cached: bool = False,
        apply_start_delay: bool = False,
    ) -> bool:
        """Launch a cross-platform matching pass unless one is already running."""
        if not self.data_feed or not self._kalshi_markets:
            return False
        if self._matching_task and not self._matching_task.done():
            logger.info("Cross-platform matching already running; skipping %s", reason)
            return False

        polymarket_markets = list(self.data_feed._markets.values())
        if not polymarket_markets:
            logger.info("Skipping %s matching: no Polymarket markets loaded yet", reason)
            return False

        logger.info(
            "Starting background matching (%s): %d Polymarket x %d Kalshi",
            reason,
            len(polymarket_markets),
            len(self._kalshi_markets),
        )
        dashboard_state.cross_platform["matching_status"] = "starting"
        dashboard_state.cross_platform["polymarket_markets"] = len(polymarket_markets)
        if load_cached:
            await self._load_cached_matches()
        if apply_start_delay:
            start_delay = getattr(
                self.config.mode,
                "cross_platform_match_start_delay_seconds",
                0.0,
            )
            if start_delay > 0:
                await asyncio.sleep(start_delay)

        self._matching_task = asyncio.create_task(
            self._run_matching_background(polymarket_markets, reason=reason),
            name=f"cross_platform_match_{reason.replace(' ', '_')}",
        )
        return True
    
    async def _run_matching_background(self, polymarket_markets: list, *, reason: str = "startup") -> None:
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
            
            logger.info(
                "✓ Matching complete%s! Found %d pairs",
                "" if reason == "startup" else f" ({reason})",
                len(self._matched_pairs),
            )
            
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
        await _cancel_task(self._private_ws_task, "consume_live_private_updates")
        await _cancel_task(self._live_fill_task, "poll_live_fills")
        await _cancel_task(self._portfolio_sync_task, "sync_live_portfolio_metrics")
        await _cancel_task(self._kalshi_task, "kalshi_monitoring")
        await _cancel_task(self._matching_task, "market_matching")
        await _cancel_task(self._heartbeat_task, "heartbeat")

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
        
        # Final summary. Distinguish between *this session* (what the bot did
        # while running) and *account lifetime* (what the exchange reports for
        # the wallet, independent of this process). Earlier versions conflated
        # the two and made it look like trades had been placed when none had.
        if self.portfolio:
            summary = self.portfolio.get_summary()
            session_trades = 0
            session_orders_placed = 0
            if self.execution_engine is not None:
                exec_stats = self.execution_engine.get_stats()
                session_trades = int(getattr(exec_stats, "orders_filled", 0) or 0)
                session_orders_placed = int(getattr(exec_stats, "orders_placed", 0) or 0)

            logger.info("=" * 60)
            logger.info("Final Summary")
            logger.info("=" * 60)
            logger.info("This session:")
            logger.info(
                "  Orders placed: %s | Fills: %s",
                session_orders_placed,
                session_trades,
            )
            local_pnl = summary.get("local_pnl", summary["pnl"])
            logger.info(
                "  Session PnL (this process only): $%.2f realized, $%.2f unrealized",
                float(local_pnl.get("realized_pnl", 0.0)),
                float(local_pnl.get("unrealized_pnl", 0.0)),
            )
            if summary.get("source") == "exchange" or "exchange_pnl" in summary:
                ex = summary.get("exchange_pnl", summary["pnl"])
                logger.info("Account lifetime (from Polymarket):")
                logger.info(
                    "  Total PnL: $%.2f (realized $%.2f, unrealized $%.2f)",
                    float(ex.get("total_pnl", 0.0)),
                    float(ex.get("realized_pnl", 0.0)),
                    float(ex.get("unrealized_pnl", 0.0)),
                )
                logger.info(
                    "  Lifetime trades: %s | Lifetime win rate: %.1f%%",
                    int(summary.get("total_trades", 0)),
                    float(summary.get("win_rate", 0.0)) * 100,
                )

        # Cross-platform summary
        if self.cross_platform_engine:
            cp_stats = self.cross_platform_engine.get_stats()
            logger.info(f"Cross-Platform Opportunities: {cp_stats['total_opportunities']}")
            logger.info(f"Matched Market Pairs: {cp_stats['matched_pairs']}")
        
        if self.profit_telemetry is not None:
            tele = self.profit_telemetry.summary()
            logger.info("-" * 60)
            logger.info(
                "Profit telemetry: opportunities=%s fills=%s fill_rate=%.2f%% notional=$%.2f fees=$%.2f",
                tele["total_opportunities"],
                tele["total_fills"],
                tele["fill_rate"] * 100,
                tele["total_fill_notional"],
                tele["total_fees_paid"],
            )
            for strategy, row in tele["per_strategy"].items():
                logger.info(
                    "  [%s] opp=%s avg_edge=%.4f post_fee=%.4f",
                    strategy,
                    int(row["opportunities"]),
                    row["avg_edge"],
                    row["avg_post_fee_edge"],
                )
        
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

