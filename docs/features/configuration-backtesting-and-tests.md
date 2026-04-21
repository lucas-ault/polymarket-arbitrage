# Configuration, Backtesting, and Tests

## Purpose
This document covers how runtime settings enter the system, how backtesting works, and what automated or manual validation exists today.

## Primary Files
- `config.yaml`
- `utils/config_loader.py`
- `utils/backtest.py`
- `utils/logging_utils.py`
- `tests/test_arb_engine.py`
- `tests/test_risk_manager.py`
- `tests/test_portfolio.py`
- `test_connection.py`
- `test_real_data.py`

## Configuration Loading
### Main loader
`utils/config_loader.py` defines:

- `ApiConfig`
- `TradingConfig`
- `RiskConfig`
- `ModeConfig`
- `LoggingConfig`
- `MonitoringConfig`
- `CacheConfig`
- `BotConfig`

`load_config()`:

1. reads YAML
2. separates sections
3. applies environment overrides for secrets
4. builds dataclass instances
5. validates core settings

## Environment Variable Overrides
Supported env vars:

- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_PASSPHRASE`
- `POLYMARKET_PRIVATE_KEY`
- `CACHE_ENABLED`
- `CACHE_BACKEND`
- `REDIS_URL`
- `CACHE_KEY_PREFIX`
- `CACHE_CONNECT_TIMEOUT_SECONDS`
- `CACHE_OP_TIMEOUT_SECONDS`
- `CACHE_MARKETS_TTL_SECONDS`
- `CACHE_MATCHES_TTL_SECONDS`

These override the YAML values for the matching keys.

## Validation Behavior
The loader validates:

- min edge and spread ranges
- positive tick size and order size
- positive exposure limits
- non-negative daily loss
- drawdown range
- valid trading mode
- valid cache backend and positive cache timeout/TTL values

In live mode it also checks that:

- `api.api_key` is present
- `api.private_key` is present

## Config Sections
### `api`
Contains endpoint URLs, auth material, and request settings.

### `trading`
Contains:

- markets to monitor
- bundle arb toggle
- market-making toggle
- order sizing
- slippage and timeout settings
- fee assumptions

### `risk`
Contains:

- per-market and global exposure limits
- loss and drawdown limits
- optional volume filter
- whitelist/blacklist
- kill switch settings

### `mode`
Contains:

- live vs dry run
- real vs simulation data
- cross-platform toggles
- dry-run balance and fill simulation settings

### `logging`
Contains intended log file and rotation settings.

### `monitoring`
Contains snapshot/heartbeat/performance flags.

### `cache`
Contains optional Redis warm-cache controls for:

- Polymarket active market discovery snapshots
- Cross-platform matched pair snapshots

## Config Keys With Incomplete Runtime Wiring
These config values exist but are not fully consumed by the runtime entrypoints:

- `trading.maker_fee_bps`
- `trading.taker_fee_bps`
- `trading.estimated_gas_per_order`
- most of the `logging` section
- `monitoring.heartbeat_interval`

## Logging
`utils/logging_utils.py` supports:

- colored console logging
- rotating main log file
- rotating trade log file
- rotating opportunity log file
- custom `TRADE` and `OPPORTUNITY` levels

The entrypoints currently call:

```python
setup_logging(console_level=...)
```

which means the full YAML logging block is not forwarded into setup at startup time.

## Backtesting
### Entry point
`main.py --backtest` calls `run_backtest()`.

### Backtest engine
`utils/backtest.py` creates simulated markets and synthetic books using:

- price volatility
- spread ranges
- configurable mispricing probability
- configurable liquidity

### Result output
Backtests report:

- realized and unrealized PnL
- trade counts and win rate
- bundle and market-making opportunity counts
- max drawdown
- max exposure

## Automated Tests
### Current unit coverage
- `tests/test_arb_engine.py`
- `tests/test_risk_manager.py`
- `tests/test_portfolio.py`

These tests focus on deterministic local logic and do not cover the full async bot lifecycle.

## Manual Smoke Scripts
### `test_real_data.py`
Checks:

- market fetch from Gamma
- one live order book fetch from the CLOB API

### `test_connection.py`
Checks:

- config loading
- market fetch
- optional authenticated position retrieval
- optional Redis cache availability check when cache is enabled

It now defaults to `config.yaml`.

### Observation profile
`config.observation.yaml` is provided for:

- `dry_run` execution
- `real` data mode
- full market auto-discovery (`trading.markets: []`)
- dashboard-first monitoring and tuned orderbook polling controls

## Recommended Validation Strategy
For most changes:

1. run `pytest tests -v`
2. run in `dry_run` + `simulation`
3. run `test_real_data.py` if you changed Polymarket data fetching
4. run `run_with_dashboard.py -c config.observation.yaml` if you changed dashboard, matching, or real-data polling behavior

## Related Docs
- `../architecture.md`
- `trading-bot.md`
- `polymarket-data-feed.md`
- `execution-risk-portfolio.md`

