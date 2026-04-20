# Trading Bot

## Purpose
This feature is the runtime orchestration layer that wires together config, market data, strategy logic, execution, risk, portfolio tracking, and shutdown behavior.

## Primary Files
- `main.py`
- `run_with_dashboard.py`

## Main Classes
### `TradingBot` in `main.py`
Handles the standard bot lifecycle:

- initialize components
- start the feed
- react to market updates
- run monitoring
- optionally simulate fills
- stop everything gracefully

### `TradingBotWithDashboard` in `run_with_dashboard.py`
Builds on the same lifecycle with:

- dashboard integration
- web server startup
- optional Kalshi loading and matching

## Bot Startup Sequence
Both entrypoints follow the same broad structure:

1. load config with `load_config()`
2. apply CLI mode overrides
3. create `PolymarketClient`
4. create `Portfolio`
5. create `RiskManager`
6. create `ExecutionEngine`
7. create `ArbEngine`
8. create `DataFeed`
9. start background work

## Market Update Callback
### `TradingBot._on_market_update()`
For each market update:

1. increment update counters
2. check `risk_manager.within_global_limits()`
3. call `arb_engine.analyze(market_state)`
4. submit each returned signal asynchronously to execution

### `TradingBotWithDashboard._on_market_update()`
Does the same, and also:

- pushes opportunity summaries to the dashboard
- pushes signal summaries to the dashboard

## Monitoring Loop
Only `main.py` includes `_monitoring_loop()`, which periodically:

- reads portfolio PnL and exposure
- updates risk PnL state
- logs arb, execution, and risk stats
- logs when the kill switch is active

The interval comes from `monitoring.snapshot_interval`.

## Dry-Run Fill Simulation
Both entrypoints can start `_simulate_fills()` when:

- `config.is_dry_run`
- `config.mode.simulate_fills` is true

This loop periodically:

1. reads open orders from `ExecutionEngine`
2. randomly decides fills using `fill_probability`
3. calls `client.simulate_fill()`
4. forwards fills into execution and dashboard state

## Backtest Mode
`main.py` supports:

```bash
python main.py --backtest
python main.py --backtest --backtest-duration 300
```

Backtest mode does not start the normal `TradingBot`; it calls `run_backtest()` instead.

## CLI Interfaces
### `main.py`
Supports:

- `-c`, `--config`
- `--live`
- `--dry-run`
- `--backtest`
- `--backtest-duration`
- `-v`, `--verbose`

### `run_with_dashboard.py`
Supports:

- `-c`, `--config`
- `--port`
- `--host`
- `--live`
- `--dry-run`
- `-v`, `--verbose`

Dashboard defaults:

- host: `0.0.0.0`
- port: `8888`

## Shutdown Behavior
Both entrypoints register signal handlers for:

- `SIGINT`
- `SIGTERM`

During shutdown they stop:

- dashboard integration, if applicable
- data feed
- execution engine
- Polymarket client
- server, if applicable

They also log final portfolio stats, and the dashboard path logs cross-platform stats.

## Known Limitations
- `main.py` and `run_with_dashboard.py` do not forward all fee-related config values into `ArbEngine`.
- `run_with_dashboard.py` starts Kalshi matching, but not cross-platform trade generation/execution.
- The dashboard and non-dashboard entrypoints have similar setup code and could drift over time.

## Related Docs
- `polymarket-data-feed.md`
- `strategies.md`
- `execution-risk-portfolio.md`
- `dashboard.md`
- `cross-platform.md`

