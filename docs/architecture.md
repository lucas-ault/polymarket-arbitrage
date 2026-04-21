# Architecture

## Overview
The repository is organized around a small set of runtime components:

- an entrypoint (`main.py` or `run_with_dashboard.py`)
- a Polymarket client
- a data feed
- strategy logic
- execution
- risk and portfolio state
- an optional dashboard
- an optional Kalshi-side market discovery and matching path

## Core Component Diagram
```mermaid
flowchart LR
    A[Entry point] --> B[load_config]
    B --> C[PolymarketClient]
    B --> D[DataFeed]
    B --> E[ArbEngine]
    B --> F[ExecutionEngine]
    B --> G[RiskManager]
    B --> H[Portfolio]

    C --> D
    D --> I[MarketState]
    I --> E
    E --> J[Signal]
    J --> F
    F --> C
    F --> G
    F --> H

    D --> K[DashboardIntegration]
    E --> K
    F --> K
    G --> K
    H --> K
    K --> L[FastAPI dashboard]

    M[KalshiClient] --> N[MarketMatcher]
    N --> L
```

## Entrypoints
Both entrypoints share `utils/bootstrap.bootstrap_components()`, which builds
the cache, Polymarket client, portfolio, risk manager, execution engine, arb
engine, and data feed in a single place. This guarantees the headless and
dashboard runtimes have the same risk wiring (kill-switch callback,
auto-unwind, market-staleness gating, profit telemetry, etc.).

### `main.py`
`TradingBot` is the simplest production path. It:

1. loads configuration
2. calls `bootstrap_components()` to assemble the trading stack
3. starts the execution engine and data feed
4. reacts to feed updates with `_on_market_update()`
5. in live mode, polls fills and reconciles portfolio metrics with the
   exchange (`_poll_live_fills`, `_sync_live_portfolio_metrics`)
6. in dry-run, simulates fills and records them in `ProfitTelemetry`
7. continuously marks the portfolio to mid (`_mark_portfolio_to_market`) so
   PnL reflects current prices rather than only realized fills
8. logs periodic portfolio, risk, and profit-telemetry snapshots

### `run_with_dashboard.py`
`TradingBotWithDashboard` reuses the same bootstrap and adds:

- `DashboardIntegration`
- `uvicorn` server startup
- optional Kalshi market loading
- background market matching between Polymarket and Kalshi
- live fill polling, portfolio reconciliation, and profit telemetry mirroring
  `main.py` so the two runtimes behave identically on the trading path

## Data Flow
### Polymarket-only path
```mermaid
sequenceDiagram
    participant CFG as config.yaml
    participant BOT as TradingBot
    participant POLY as PolymarketClient
    participant FEED as DataFeed
    participant ARB as ArbEngine
    participant EXEC as ExecutionEngine
    participant RISK as RiskManager
    participant PORT as Portfolio

    CFG->>BOT: load_config()
    BOT->>POLY: connect()
    BOT->>EXEC: start()
    BOT->>FEED: start()
    FEED->>POLY: list_markets()/get_market()
    loop live updates
        FEED->>POLY: stream_orderbook()
        FEED->>POLY: get_positions()
        FEED->>ARB: MarketState
        ARB-->>BOT: Signal list
        BOT->>RISK: within_global_limits()
        BOT->>EXEC: submit_signal()
        EXEC->>RISK: check_order()
        EXEC->>POLY: place_order()
        POLY-->>EXEC: Order / Trade
        EXEC->>PORT: update_from_fill()
        EXEC->>RISK: update_from_fill()
    end
```

### Dashboard and Kalshi path
The dashboard bot starts the same Polymarket loop, then also:

1. creates `DashboardIntegration`
2. starts the FastAPI app
3. loads open Kalshi markets
4. waits for some Polymarket markets to exist
5. runs `MarketMatcher.find_matches()` in a thread-backed background task
6. publishes matching progress and matched pairs into `dashboard_state.cross_platform`

At present, this path does not turn matched pairs into live cross-platform trades.

## Directory Responsibilities
| Directory | Responsibility |
| --- | --- |
| `core/` | trading logic, execution logic, risk, portfolio, cross-platform helpers |
| `polymarket_client/` | market data, order books, simulated/live order interfaces, shared models |
| `kalshi_client/` | Kalshi market data and order book conversion |
| `dashboard/` | server, dashboard state, UI bridge |
| `utils/` | config parsing, logging, backtest support, shared bootstrap, profit telemetry |
| `tests/` | unit tests for core engine pieces |

## Main State Objects
### `MarketState`
Built in `core/data_feed.py`, consumed in `core/arb_engine.py`.

Contains:

- `market`
- `order_book`
- `positions`
- `open_orders`
- `timestamp`

### `dashboard_state`
Defined in `dashboard/server.py`.

Holds:

- market snapshots
- opportunities
- signals
- open orders
- recent trades
- portfolio and risk summaries
- operational stats
- timing stats
- cross-platform matching state

## Execution Model
`ExecutionEngine` uses an async queue:

1. `submit_signal()` adds signals to `_signal_queue`
2. `_process_signals()` dequeues and routes to placement/cancellation handlers
3. orders are tracked in `_open_orders`
4. `_monitor_order_timeouts()` cancels stale orders
5. `handle_fill()` updates both `Portfolio` and `RiskManager`

This keeps strategy generation and order execution loosely coupled.

## Simulation Model
Two different simulation paths exist:

### Data simulation
`PolymarketClient.stream_orderbook(..., use_simulation=True)` generates synthetic books with occasional inefficiencies.

### Fill simulation
`main.py` and `run_with_dashboard.py` optionally call `client.simulate_fill(order_id)` for dry-run open orders.

### Backtesting
`utils/backtest.py` contains a separate simulated market engine used by `main.py --backtest`.

## Architectural Caveats
- The Polymarket live path uses REST polling for order books and positions; the
  WebSocket interface is documented but not yet the active feed.
- The dashboard path fully supports market matching progress, but does not turn
  matched Polymarket/Kalshi pairs into live cross-platform execution.
- Live authenticated trading is functional, but order sizes are rounded to
  whole contracts on send; size loss greater than 5 % is logged as a warning.
- `RiskManager.auto_unwind_on_breach` triggers `ExecutionEngine.cancel_all_orders`
  via a callback registered during bootstrap; it does not flatten existing
  positions automatically.

