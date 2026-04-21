# Operations Guide

This doc describes how to run, validate, and graduate the bot from local testing
to live trading. It is the canonical operational reference. The README points
here for live readiness.

## Supported Entrypoints

| Entrypoint | Mode | Status |
| --- | --- | --- |
| `python main.py` | Headless bot | Supported for dry-run and live. |
| `python run_with_dashboard.py` | Bot + FastAPI dashboard | Supported for dry-run and live. **Recommended for live runs** because the dashboard surfaces health, staleness, and risk state. |
| `python utils/backtest.py` (via `main.py --backtest`) | Synthetic backtest | Supported. Useful for sanity-checking strategy parameters; not a substitute for paper trading. |

Both bot entrypoints share the same `bootstrap_components` helper
(`utils/bootstrap.py`), so risk / portfolio / execution / data-feed wiring is
identical. Differences are limited to dashboard glue and Kalshi monitoring.

## Required Environment for Live Trading

Live trading requires a Polymarket US API key pair signed with Ed25519. Provide
them via env or `config.yaml`:

```bash
export POLYMARKET_KEY_ID=...
export POLYMARKET_SECRET_KEY=...   # base64-encoded 32-byte private key seed
```

The `cryptography` Python package must be installed (it is in
`requirements.txt`). The bot will fail fast at startup if signing material or
the dependency is missing in live mode.

## Mode Matrix

The combination of `mode.trading_mode` and `mode.data_mode` controls behavior:

| trading_mode | data_mode | Behavior |
| --- | --- | --- |
| `dry_run` | `simulation` | Synthetic books and simulated fills. Best for UI demos and unit-style validation. |
| `dry_run` | `real` | Real Polymarket data, no real orders. The recommended **paper-trading** mode. |
| `live`    | `real` | Real data and real orders. Requires API credentials and explicit operator action. |
| `live`    | `simulation` | Disallowed by `_validate_config`; would imply trading against fake books. |

`mode.simulate_fills=true` is force-disabled in live mode by both
`load_config` and `run_with_dashboard.main_async` to prevent accidental
fake-fill processing while real orders are also live.

## Live-Readiness Gate

Before flipping `trading_mode` to `live`, confirm the following:

1. **Credentials.** `POLYMARKET_KEY_ID` and `POLYMARKET_SECRET_KEY` are set and
   the bot can sign a request (run `python test_connection.py`).
2. **Dependencies.** `cryptography` and `polymarket_us` (the SDK) import cleanly
   in your environment.
3. **Connectivity.** `python test_real_data.py` returns at least one live order
   book and at least one discovered market.
4. **Risk config sanity check.** In `config.yaml`:
   - `risk.max_position_per_market` and `risk.max_global_exposure` reflect
     amounts you are willing to lose.
   - `risk.max_daily_loss` and `risk.max_drawdown_pct` are set, and
     `risk.kill_switch_enabled=true`.
   - `risk.auto_unwind_on_breach` is `true` if you want the bot to flatten
     resting orders on kill-switch trigger.
5. **Volume gating.** If `risk.trade_only_high_volume=true`, confirm that
   `min_24h_volume` matches the markets you are targeting; the data feed will
   forward live `volume_24h` into the risk manager automatically.
6. **Strategy economics.** Configured `min_edge` exceeds the worst-case sum of
   maker/taker fees, expected slippage, and a margin of safety.
7. **Telemetry baseline.** A paper-trading run (see below) shows positive
   post-fee expectancy for the strategy you intend to run.

## Recommended Validation Progression

Treat profitability as a *measurement*, not an assumption.

### Stage 1 — `dry_run` + `simulation`

```bash
python main.py
```

Use to verify changes do not crash, queues do not stall, and unit tests pass.

### Stage 2 — `dry_run` + `real` (paper trading)

```bash
python run_with_dashboard.py -c config.observation.yaml
```

This is the canonical "paper trading" configuration:

- `trading_mode=dry_run`, `data_mode=real`.
- Full market discovery, conservative risk limits.
- Dashboard exposes signal counts, opportunity edge histograms, fill simulation,
  and operational health.

Run it for a multi-day period. The dashboard exposes profit telemetry at
`/api/profit`, and the bot logs a per-strategy summary at shutdown showing:

- Per-strategy detected opportunity count and average edge.
- Average **post-fee** edge per opportunity.
- Simulated fill count, notional, fees, and fill rate.

Use `ProfitTelemetry.evaluate_gate(min_opportunities=..., min_avg_post_fee_edge=...)`
to assert these thresholds in scripts before promoting to the next stage.
Sample default for paper -> tiny live: at least 50 opportunities per strategy
with average post-fee edge ≥ 0.005 (0.5 cents per share). Only proceed if the
gate passes and the result is stable across multiple runs.

### Stage 3 — `live` with strict caps

Lower `default_order_size`, `max_order_size`, `max_position_per_market`, and
`max_global_exposure` to small dollar amounts. Run `run_with_dashboard.py
--live`. Watch the dashboard:

- Operational health indicators (staleness, reconnects, errors).
- Risk panel (kill switch, daily PnL, drawdown).
- Live trades feed.

Compare realized fills against the local opportunity that triggered them. If
realized post-fee edge is much worse than detected edge, do not scale up.

### Stage 4 — Scale only after evidence

Increase risk limits gradually. Repeat the Stage 2 telemetry comparison weekly
in production to confirm assumptions still hold.

## Operational Health Signals

The dashboard surfaces these health indicators (also exported via `/api/state`):

| Signal | Source | Healthy range |
| --- | --- | --- |
| Order book staleness (avg / max / p95 sec) | `DataFeed.get_staleness_summary` | Sub-second to a few seconds for active markets. |
| Stream reconnects | `DataFeed.get_runtime_stats.stream_reconnects` | Increases occasionally; rapid growth is bad. |
| Signal queue depth | `ExecutionEngine.signal_queue_size` | Should stay small; growing depth implies blocked execution. |
| Open orders | `ExecutionEngine.open_order_count` | Bounded by risk limits; stale opens are a smell. |
| Risk kill switch | `RiskManager.state.kill_switch_triggered` | Should be `false` in normal operation. |
| Daily PnL vs limit | `RiskManager.get_summary` | Above `-max_daily_loss`. |
| Live fill bootstrap done | `TradingBotWithDashboard._live_fill_bootstrapped` | `true` shortly after live startup. |
| Stale-market rejections | `RiskManager.get_summary.stale_market_rejections` | Should remain low; rapid growth means feed is lagging behind risk's `max_market_staleness_seconds`. |
| Volume rejections | `RiskManager.get_summary.volume_rejections` | Expected when targeting low-volume markets; should be 0 once `min_24h_volume` is calibrated. |
| Profit telemetry | `/api/profit`, `ProfitTelemetry.summary()` | `avg_post_fee_edge` per strategy should match (or beat) configured `min_edge`. |
| Health snapshot | `/api/health` | Single-call summary for paging / external monitoring. |

## Degraded-Mode Behavior

The risk manager and data feed cooperate to degrade safely:

- `RiskManager.register_market_state(MarketState)` records the latest book
  freshness for that market. `check_order` rejects orders whose underlying
  market has gone stale beyond `max_market_staleness_seconds`.
- When the kill switch trips, `RiskManager` invokes the registered
  `on_kill_switch` callback. `run_with_dashboard.py` and `main.py` register that
  callback to call `ExecutionEngine.cancel_all_orders` whenever
  `risk.auto_unwind_on_breach=true`.
- The execution engine will not place new orders while
  `RiskManager.within_global_limits()` is `false`.

## Shutdown Behavior

Both entrypoints handle SIGINT / SIGTERM by:

1. Cancelling background tasks (fill polling, portfolio sync, matching).
2. Stopping `DashboardIntegration`, `DataFeed`, and `ExecutionEngine`.
3. `ExecutionEngine.stop` cancels every tracked open order before returning.
4. Disconnecting the API client and (if running) the uvicorn server.

If you want a hard "do not place any new orders" stop without a full process
exit, trigger the kill switch via the dashboard or by calling
`risk_manager._trigger_kill_switch("manual")`.

## Known Unsupported / Limited Areas

These are documented in the relevant feature docs but worth listing in one
place:

- **Cross-platform execution.** Kalshi integration is read-only (market data
  + matching). The bot does not place Kalshi orders or maintain hedged
  cross-exchange positions.
- **Continuous market discovery.** The Polymarket markets cache is refreshed
  periodically (`markets_cache_ttl_seconds`) and on warm start, but bespoke
  add/remove events outside that cadence are not subscribed.
- **Maker rebate optimisation.** Fee config is honoured for net-edge math, but
  there is no specialised maker-only execution mode.

## Cheat Sheet

```bash
pytest tests -v                                  # unit tests
python test_connection.py -c config.yaml         # auth + Redis sanity
python test_real_data.py                         # one live market + one book
python main.py                                   # headless dry run
python run_with_dashboard.py -c config.observation.yaml   # paper trading
python run_with_dashboard.py --live --host 127.0.0.1      # live, local dashboard
```
