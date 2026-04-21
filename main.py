#!/usr/bin/env python3
"""
Polymarket Arbitrage Trading Bot
=================================

Main entry point for the trading bot.

Usage:
    python main.py                      # Run in dry-run mode (default)
    python main.py --live               # Run in live mode
    python main.py --backtest           # Run backtest
    python main.py --config my.yaml     # Use custom config file
"""

import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from polymarket_client import PolymarketClient
from core.data_feed import DataFeed
from core.arb_engine import ArbEngine, ArbConfig
from core.auto_take_profit import AutoTakeProfitConfig, AutoTakeProfitMonitor
from core.execution import ExecutionEngine, ExecutionConfig
from core.risk_manager import RiskManager, RiskConfig
from core.portfolio import Portfolio
from utils.bootstrap import bootstrap_components
from utils.config_loader import load_config, BotConfig
from utils.logging_utils import setup_logging, performance_logger, trade_logger
from utils.profit_telemetry import ProfitTelemetry
from utils.redis_cache import RedisCacheConfig, create_cache_store


logger = logging.getLogger(__name__)


class TradingBot:
    """
    Main trading bot orchestrator.
    
    Coordinates all components and manages the trading lifecycle.
    """
    
    def __init__(self, config: BotConfig):
        self.config = config
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # Components (initialized in start())
        self.client: Optional[PolymarketClient] = None
        self.data_feed: Optional[DataFeed] = None
        self.arb_engine: Optional[ArbEngine] = None
        self.execution_engine: Optional[ExecutionEngine] = None
        self.risk_manager: Optional[RiskManager] = None
        self.portfolio: Optional[Portfolio] = None
        self.cache_store = None
        
        # Statistics
        self._start_time: Optional[datetime] = None
        self._update_count = 0
        self._signal_count = 0
        self.profit_telemetry: Optional[ProfitTelemetry] = None
        self._seen_trade_ids: set[str] = set()
        self._live_fill_bootstrapped: bool = False
        self.auto_take_profit_monitor: Optional[AutoTakeProfitMonitor] = None
    
    async def start(self) -> None:
        """Initialize and start all components."""
        logger.info("=" * 60)
        logger.info("Polymarket Arbitrage Bot Starting")
        logger.info("=" * 60)
        logger.info(f"Mode: {'DRY RUN' if self.config.is_dry_run else 'LIVE'}")
        logger.info(f"Markets: {self.config.trading.markets or 'Auto-discover'}")
        
        self._start_time = datetime.utcnow()
        self._running = True
        
        # Build the shared component graph (cache, client, portfolio, risk,
        # execution, arb engine, data feed) via the central bootstrap helper.
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
            ),
            self.execution_engine,
            self.portfolio,
            fee_theta_taker=float(self.config.trading.fee_theta_taker),
        )
        
        await self.data_feed.start()
        
        # Wait for initial data
        logger.info("Waiting for market data...")
        if not await self.data_feed.wait_for_data(timeout=30.0):
            logger.warning("Timeout waiting for initial data, proceeding anyway")
        
        logger.info("Bot started successfully!")
        logger.info("-" * 60)
        
        # Start monitoring loop
        asyncio.create_task(self._monitoring_loop(), name="monitoring_loop")
        
        # Start fill simulation for dry run; for live, sync exchange state.
        if self.config.is_dry_run and self.config.mode.simulate_fills:
            asyncio.create_task(self._simulate_fills(), name="simulate_fills")
        elif not self.config.is_dry_run:
            asyncio.create_task(
                self._consume_live_private_updates(),
                name="consume_live_private_updates",
            )
            asyncio.create_task(self._poll_live_fills(), name="poll_live_fills")
            asyncio.create_task(self._sync_live_portfolio_metrics(), name="sync_live_portfolio")
    
    def _on_market_update(self, market_id: str, market_state) -> None:
        """Callback for market state updates."""
        self._update_count += 1
        
        # Mark portfolio to latest mid before evaluating risk so unrealized PnL
        # tracks the live book even in headless mode.
        self._mark_portfolio_to_market(market_id, market_state)
        if self.auto_take_profit_monitor:
            self.auto_take_profit_monitor.maybe_submit_for_market(market_id, market_state)
        
        # Check risk limits
        if not self.risk_manager.within_global_limits():
            logger.warning("Risk limits exceeded, skipping analysis")
            return
        
        # Analyze for opportunities
        signals = self.arb_engine.analyze(market_state)
        
        for signal in signals:
            self._signal_count += 1
            if self.profit_telemetry and signal.opportunity:
                self.profit_telemetry.record_opportunity(
                    market_id=signal.market_id,
                    strategy=signal.opportunity.opportunity_type.value,
                    edge=signal.opportunity.edge,
                    fee_bps=self.config.trading.taker_fee_bps,
                )
            if not self.execution_engine.submit_signal_nowait(signal):
                logger.warning("Execution queue is full, dropping signal %s", signal.signal_id)
    
    @staticmethod
    def _pick_mark_price(best_bid, best_ask, fallback) -> Optional[float]:
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
    
    def _mark_portfolio_to_market(self, market_id: str, market_state) -> None:
        if not self.portfolio:
            return
        order_book = market_state.order_book
        market = market_state.market
        yes_price = self._pick_mark_price(order_book.best_bid_yes, order_book.best_ask_yes, market.yes_price)
        no_price = self._pick_mark_price(order_book.best_bid_no, order_book.best_ask_no, market.no_price)
        if yes_price is None and no_price is not None:
            yes_price = max(0.0, min(1.0, 1.0 - no_price))
        if no_price is None and yes_price is not None:
            no_price = max(0.0, min(1.0, 1.0 - yes_price))
        if yes_price is None or no_price is None:
            return
        self.portfolio.update_prices(market_id, yes_price=yes_price, no_price=no_price)
        if self.risk_manager:
            pnl = self.portfolio.get_pnl()
            self.risk_manager.update_pnl(
                realized_pnl=float(pnl.get("realized_pnl", 0.0)),
                unrealized_pnl=float(pnl.get("unrealized_pnl", 0.0)),
            )
    
    async def _poll_live_fills(self) -> None:
        """Mirror exchange trades into local execution / portfolio state."""
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
                for trade in sorted(trades, key=lambda t: getattr(t, "timestamp", datetime.utcnow())):
                    self._process_live_trade(trade)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Live fill poll error: {exc}")

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
                    if str(event.get("type") or "") == "order_update":
                        trade = event.get("trade")
                        if trade is not None:
                            self._process_live_trade(trade)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Private websocket consumer error: %s", exc)
                await asyncio.sleep(1.0)
    
    async def _sync_live_portfolio_metrics(self) -> None:
        """Pull authoritative portfolio metrics from the exchange periodically.

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
            except Exception as exc:
                msg = str(exc).lower()
                if "429" in msg or "too many requests" in msg or "rate" in msg:
                    logger.warning(
                        "Portfolio sync rate-limited; backing off to %.0fs", backoff_interval,
                    )
                    next_sleep = backoff_interval
                else:
                    logger.error(f"Portfolio sync error: {exc}")
                    next_sleep = base_interval
    
    async def _monitoring_loop(self) -> None:
        """Periodic monitoring and logging."""
        interval = self.config.monitoring.snapshot_interval
        
        while self._running:
            try:
                await asyncio.sleep(interval)
                
                # Log portfolio snapshot
                pnl = self.portfolio.get_pnl()
                exposure = self.portfolio.get_total_exposure()
                positions = len(self.portfolio.get_all_positions())
                open_orders = self.execution_engine.open_order_count
                
                performance_logger.log_snapshot(pnl, exposure, positions, open_orders)
                
                # Update risk manager
                self.risk_manager.update_pnl(
                    pnl["realized_pnl"],
                    pnl["unrealized_pnl"]
                )
                
                # Log statistics
                arb_stats = self.arb_engine.get_stats()
                exec_stats = self.execution_engine.get_stats()
                risk_summary = self.risk_manager.get_summary()
                
                logger.info(
                    f"Stats | Updates: {self._update_count} | "
                    f"Signals: {self._signal_count} | "
                    f"Orders: {exec_stats.orders_placed} placed, {exec_stats.orders_filled} filled | "
                    f"PnL: ${pnl['total_pnl']:.2f}"
                )
                
                if risk_summary["kill_switch_triggered"]:
                    logger.critical("KILL SWITCH ACTIVE - Trading halted")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
    
    async def _simulate_fills(self) -> None:
        """Simulate order fills in dry run mode."""
        import random
        
        while self._running:
            try:
                await asyncio.sleep(2.0)  # Check every 2 seconds
                
                # Get open orders
                orders = self.execution_engine.get_open_orders()
                
                for order in orders:
                    # Random chance of fill
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
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Fill simulation error: {e}")
    
    async def stop(self) -> None:
        """Stop all components gracefully."""
        logger.info("Shutting down...")
        self._running = False
        
        if self.data_feed:
            await self.data_feed.stop()
        
        if self.execution_engine:
            await self.execution_engine.stop()
        
        if self.client:
            await self.client.disconnect()
        
        # Final summary
        if self.portfolio:
            summary = self.portfolio.get_summary()
            logger.info("=" * 60)
            logger.info("Final Portfolio Summary")
            logger.info("=" * 60)
            logger.info(f"Total PnL: ${summary['pnl']['total_pnl']:.2f}")
            logger.info(f"  Realized: ${summary['pnl']['realized_pnl']:.2f}")
            logger.info(f"  Unrealized: ${summary['pnl']['unrealized_pnl']:.2f}")
            logger.info(f"Total Trades: {summary['total_trades']}")
            logger.info(f"Win Rate: {summary['win_rate']:.1%}")
            logger.info(f"Total Volume: ${summary['total_volume']:.2f}")
        
        if self.arb_engine:
            stats = self.arb_engine.get_stats()
            logger.info("-" * 60)
            logger.info(f"Bundle Opportunities: {stats.bundle_opportunities_detected}")
            logger.info(f"MM Opportunities: {stats.mm_opportunities_detected}")
            logger.info(f"Signals Generated: {stats.signals_generated}")
        
        if self.profit_telemetry is not None:
            tele = self.profit_telemetry.summary()
            logger.info("-" * 60)
            logger.info("Profit Telemetry")
            logger.info(
                "  Opportunities=%s Fills=%s Fill rate=%.2f%%",
                tele["total_opportunities"],
                tele["total_fills"],
                tele["fill_rate"] * 100,
            )
            logger.info(
                "  Notional=$%.2f Fees paid=$%.2f Net=$%.2f",
                tele["total_fill_notional"],
                tele["total_fees_paid"],
                tele["total_fill_notional"] - tele["total_fees_paid"],
            )
            for strategy, row in tele["per_strategy"].items():
                logger.info(
                    "  [%s] opp=%s avg_edge=%.4f post_fee=%.4f min=%.4f max=%.4f",
                    strategy,
                    int(row["opportunities"]),
                    row["avg_edge"],
                    row["avg_post_fee_edge"],
                    row["min_edge"],
                    row["max_edge"],
                )
        
        logger.info("=" * 60)
        logger.info("Bot stopped")
        
        self._shutdown_event.set()
    
    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()


async def run_backtest(config: BotConfig, duration: float = 300.0) -> None:
    """Run a backtest simulation."""
    from utils.backtest import BacktestConfig, BacktestEngine, run_backtest as _run_backtest
    
    logger.info("Starting backtest mode...")
    
    # Create components
    portfolio = Portfolio(initial_balance=config.mode.dry_run_initial_balance)
    
    risk_manager = RiskManager(RiskConfig(
        max_position_per_market=config.risk.max_position_per_market,
        max_global_exposure=config.risk.max_global_exposure,
        max_daily_loss=config.risk.max_daily_loss,
        max_drawdown_pct=config.risk.max_drawdown_pct,
    ))
    
    arb_engine = ArbEngine(ArbConfig(
        min_edge=config.trading.min_edge,
        bundle_arb_enabled=config.trading.bundle_arb_enabled,
        min_spread=config.trading.min_spread,
        mm_enabled=config.trading.mm_enabled,
        tick_size=config.trading.tick_size,
        default_order_size=config.trading.default_order_size,
        min_order_size=config.trading.min_order_size,
        max_order_size=config.trading.max_order_size,
        maker_fee_bps=config.trading.maker_fee_bps,
        taker_fee_bps=config.trading.taker_fee_bps,
        fee_theta_taker=config.trading.fee_theta_taker,
        fee_theta_maker=config.trading.fee_theta_maker,
    ))
    
    # Use placeholder client for execution
    client = PolymarketClient(dry_run=True)
    await client.connect()
    
    execution_engine = ExecutionEngine(
        client=client,
        risk_manager=risk_manager,
        portfolio=portfolio,
        config=ExecutionConfig(dry_run=True),
    )
    await execution_engine.start()
    
    # Run backtest
    backtest_config = BacktestConfig(
        initial_balance=config.mode.dry_run_initial_balance,
        simulate_fills=True,
        fill_probability=config.mode.fill_probability,
    )
    
    # Generate market IDs
    market_ids = config.trading.markets or [f"market_{i}" for i in range(3)]
    
    result = await _run_backtest(
        config=backtest_config,
        market_ids=market_ids,
        arb_engine=arb_engine,
        execution_engine=execution_engine,
        risk_manager=risk_manager,
        portfolio=portfolio,
        duration_seconds=duration,
    )
    
    await execution_engine.stop()
    await client.disconnect()
    
    return result


async def main_async(args: argparse.Namespace) -> None:
    """Async main function."""
    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    
    # Override mode from command line
    if args.live:
        config.mode.trading_mode = "live"
    elif args.dry_run:
        config.mode.trading_mode = "dry_run"

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
        entrypoint="main.py",
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
    
    # Run backtest if requested
    if args.backtest:
        await run_backtest(config, duration=args.backtest_duration)
        return
    
    # Create and run the bot
    bot = TradingBot(config)
    
    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(bot.stop())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass
    
    try:
        await bot.start()
        await bot.wait_for_shutdown()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await bot.stop()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        await bot.stop()
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Polymarket Arbitrage Trading Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    Run in dry-run mode
  python main.py --live             Run in live trading mode
  python main.py --backtest         Run backtest simulation
  python main.py -c custom.yaml     Use custom config file
        """
    )
    
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run in live trading mode"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Run in dry-run mode (default)"
    )
    
    parser.add_argument(
        "--backtest",
        action="store_true",
        help="Run backtest simulation"
    )
    
    parser.add_argument(
        "--backtest-duration",
        type=float,
        default=300.0,
        help="Backtest duration in simulated seconds (default: 300)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Run the async main
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nShutdown complete.")


if __name__ == "__main__":
    main()

