"""
Shared bootstrap for trading-bot entrypoints.

Both ``main.py`` and ``run_with_dashboard.py`` need to wire up the same set of
core components (cache, API client, portfolio, risk, execution, arb engine,
data feed). Doing that wiring twice in two slightly different ways is a recipe
for behavioural drift between the headless bot and the dashboard bot.

This module centralizes the construction so:

* ``RiskConfig`` and ``ArbConfig`` are built from the YAML config in one place.
* The risk manager's auto-unwind callback is wired to the execution engine the
  same way regardless of entrypoint.
* The data feed forwards market freshness / volume into the risk manager via
  ``register_market_state``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from polymarket_client import PolymarketClient
from core.arb_engine import ArbConfig, ArbEngine
from core.data_feed import DataFeed
from core.execution import ExecutionConfig, ExecutionEngine
from core.portfolio import Portfolio
from core.risk_manager import RiskConfig, RiskManager
from polymarket_client.models import TokenType
from utils.config_loader import BotConfig
from utils.redis_cache import RedisCacheConfig, create_cache_store


logger = logging.getLogger(__name__)


@dataclass
class TradingComponents:
    """Container for the live components a trading entrypoint needs."""

    cache_store: object
    client: PolymarketClient
    portfolio: Portfolio
    risk_manager: RiskManager
    execution_engine: ExecutionEngine
    arb_engine: ArbEngine
    data_feed: DataFeed


def _build_risk_manager(config: BotConfig) -> RiskManager:
    return RiskManager(RiskConfig(
        max_position_per_market=config.risk.max_position_per_market,
        max_global_exposure=config.risk.max_global_exposure,
        max_daily_loss=config.risk.max_daily_loss,
        max_drawdown_pct=config.risk.max_drawdown_pct,
        min_peak_pnl_for_drawdown=config.risk.min_peak_pnl_for_drawdown,
        trade_only_high_volume=config.risk.trade_only_high_volume,
        min_24h_volume=config.risk.min_24h_volume,
        max_market_staleness_seconds=config.risk.max_market_staleness_seconds,
        whitelist=list(config.risk.whitelist),
        blacklist=list(config.risk.blacklist),
        kill_switch_enabled=config.risk.kill_switch_enabled,
        auto_unwind_on_breach=config.risk.auto_unwind_on_breach,
    ))


def _build_arb_engine(config: BotConfig) -> ArbEngine:
    return ArbEngine(ArbConfig(
        min_edge=config.trading.min_edge,
        bundle_arb_enabled=config.trading.bundle_arb_enabled,
        event_bundle_arb_enabled=config.trading.event_bundle_arb_enabled,
        min_spread=config.trading.min_spread,
        mm_max_spread=config.trading.mm_max_spread,
        mm_min_price=config.trading.mm_min_price,
        mm_max_price=config.trading.mm_max_price,
        mm_cooldown_seconds=config.trading.mm_cooldown_seconds,
        mm_invalidation_grace_seconds=config.trading.mm_invalidation_grace_seconds,
        mm_invalidation_min_updates=config.trading.mm_invalidation_min_updates,
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


async def bootstrap_components(
    config: BotConfig,
    on_market_update,
    *,
    position_refresh_interval: Optional[float] = None,
) -> TradingComponents:
    """Construct, wire, and start the shared trading components.

    The caller is expected to start the data feed and run the monitoring loop
    after this returns. The execution engine is started here so the kill-switch
    callback has somewhere to land before any orders flow.
    """
    cache_config = RedisCacheConfig(
        enabled=config.cache.enabled,
        backend=config.cache.backend,
        redis_url=config.cache.redis_url,
        key_prefix=config.cache.key_prefix,
        connect_timeout_seconds=config.cache.connect_timeout_seconds,
        op_timeout_seconds=config.cache.op_timeout_seconds,
        markets_ttl_seconds=config.cache.markets_ttl_seconds,
        matches_ttl_seconds=config.cache.matches_ttl_seconds,
    )
    cache_store = await create_cache_store(cache_config)

    client = PolymarketClient(
        public_url=config.api.polymarket_public_url,
        private_url=config.api.polymarket_private_url,
        markets_ws_url=config.api.polymarket_markets_ws_url,
        private_ws_url=config.api.polymarket_private_ws_url,
        key_id=config.api.key_id,
        secret_key=config.api.secret_key,
        timeout=config.api.timeout_seconds,
        max_retries=config.api.max_retries,
        retry_delay=config.api.retry_delay_seconds,
        dry_run=config.is_dry_run,
        use_websocket=config.api.use_websocket,
        use_rest_fallback=config.api.use_rest_fallback,
        cache_store=cache_store,
        markets_cache_ttl_seconds=config.cache.markets_ttl_seconds,
    )
    await client.connect()

    initial_balance = (
        config.mode.dry_run_initial_balance if config.is_dry_run else 0.0
    )
    portfolio = Portfolio(initial_balance=initial_balance)
    risk_manager = _build_risk_manager(config)

    execution_engine = ExecutionEngine(
        client=client,
        risk_manager=risk_manager,
        portfolio=portfolio,
        config=ExecutionConfig(
            slippage_tolerance=config.trading.slippage_tolerance,
            order_timeout_seconds=config.trading.order_timeout_seconds,
            mm_order_timeout_seconds=config.trading.mm_order_timeout_seconds,
            unplaceable_market_skip_seconds=config.trading.unplaceable_market_skip_seconds,
            dry_run=config.is_dry_run,
        ),
    )
    await execution_engine.start()

    # Wire auto-unwind: when the risk manager trips its kill switch it asks
    # the execution engine to cancel every resting order. We always set the
    # callback; ``RiskManager`` only invokes it when ``auto_unwind_on_breach``.
    async def _on_kill_switch(reason: str) -> None:
        logger.critical("Auto-unwind triggered: cancelling all orders (%s)", reason)
        try:
            await execution_engine.cancel_all_orders()
        except Exception as exc:
            logger.error("Auto-unwind cancel_all_orders failed: %s", exc)
    risk_manager.set_kill_switch_callback(_on_kill_switch)

    # Seed risk manager AND portfolio with positions already on the exchange.
    # Without this, `max_position_per_market` is enforced only WITHIN a single
    # bot session — every restart silently doubles the headroom (this let
    # real losses accumulate to ~7x the configured cap). Skip in dry-run
    # because `get_positions()` returns the simulator's state which is
    # already empty for a fresh process.
    if config.is_live:
        try:
            existing = await client.get_positions()
            seeded = 0
            for market_id, by_token in (existing or {}).items():
                for token_type, position in (by_token or {}).items():
                    size = float(getattr(position, "size", 0.0) or 0.0)
                    if size == 0:
                        continue
                    avg_price = float(getattr(position, "avg_entry_price", 0.0) or 0.0)
                    risk_manager.update_position(market_id, token_type, size, avg_price)
                    portfolio.seed_position(market_id, token_type, size, avg_price)
                    seeded += 1
            logger.info(
                "Seeded %d existing exchange position(s) into risk manager + "
                "portfolio; per-market cap and inventory-aware MM are now "
                "enforced across restarts",
                seeded,
            )
        except Exception as exc:
            logger.warning(
                "Could not load existing positions for seeding: %s. "
                "Per-market cap and inventory check will only see fills "
                "observed in this session.",
                exc,
            )

    arb_engine = _build_arb_engine(config)

    # Inventory-aware MM: route current per-leg contract count from the
    # portfolio into the arb engine so we don't keep adding to a side that
    # already has fills against us.
    def _position_probe(market_id, token_type) -> float:
        position = portfolio.get_position(market_id, token_type)
        if position is None:
            return 0.0
        return abs(float(getattr(position, "size", 0.0) or 0.0))
    arb_engine.set_position_probe(_position_probe)

    def _on_opportunity_expired(market_id: str, opportunity_type: str, opportunity_id: str) -> None:
        if opportunity_type not in {
            "mm_bid",
            "mm_ask",
        }:
            return
        token_type = TokenType.YES if opportunity_type == "mm_bid" else TokenType.NO
        signal = execution_engine.build_cancel_signal(
            market_id=market_id,
            strategy_tag="market_making",
            token_type=token_type,
            quote_group_id=opportunity_id,
            priority=10,
        )
        if signal is not None:
            execution_engine.submit_signal_nowait(signal)

    arb_engine.set_expiry_callback(_on_opportunity_expired)

    if position_refresh_interval is None:
        position_refresh_interval = 30.0 if config.is_live else 5.0

    data_feed = DataFeed(
        client=client,
        market_ids=list(config.trading.markets),
        position_refresh_interval=position_refresh_interval,
        on_update=on_market_update,
        config=config,
        risk_manager=risk_manager,
    )

    return TradingComponents(
        cache_store=cache_store,
        client=client,
        portfolio=portfolio,
        risk_manager=risk_manager,
        execution_engine=execution_engine,
        arb_engine=arb_engine,
        data_feed=data_feed,
    )
