# Polymarket Arbitrage Bot

An async Python trading bot for Polymarket binary markets with:

- Polymarket market discovery and order book polling
- Bundle arbitrage detection (`YES + NO` mispricing)
- Optional market-making signals
- Risk management, execution, and portfolio tracking
- A FastAPI + WebSocket dashboard
- Optional Kalshi market loading and text-based market matching
- Dry-run simulation and backtesting modes

Important: the repository contains cross-platform matching and cross-platform arbitrage logic, but it does **not** currently wire automated Polymarket <-> Kalshi execution into the live bot loop.

## Current Scope
This repo currently supports these capabilities:

| Capability | Status | Notes |
| --- | --- | --- |
| Polymarket market discovery from Gamma | Implemented | Scans active markets and caches token IDs |
| Polymarket order book ingestion | Implemented | Main live path uses REST polling, not WebSocket |
| Bundle arbitrage detection | Implemented | `core/arb_engine.py` |
| Market-making signal generation | Implemented | `core/arb_engine.py` |
| Risk checks and kill switch | Implemented | `core/risk_manager.py` |
| Execution queue and order timeout handling | Implemented | `core/execution.py` |
| Dry-run order simulation and fill simulation | Implemented | `polymarket_client/api.py`, `main.py` |
| Dashboard API and WebSocket UI | Implemented | `dashboard/server.py` |
| Kalshi market loading | Implemented | `kalshi_client/api.py` |
| Polymarket/Kalshi market matching | Implemented | `core/cross_platform_arb.py` |
| Cross-platform opportunity engine | Implemented in isolation | Logic exists but is not called from the live trading loop |
| Automated Kalshi execution | Not implemented | Kalshi client is read-only market data in this repo |
| Polymarket WebSocket order book path | Stubbed | `_connect_websocket()` exists but is not used by the main stream path |

## Architecture
```mermaid
flowchart TD
    A[config.yaml + env] --> B[load_config]
    B --> C[TradingBot or TradingBotWithDashboard]
    C --> D[PolymarketClient]
    C --> E[Portfolio]
    C --> F[RiskManager]
    C --> G[ExecutionEngine]
    C --> H[ArbEngine]
    C --> I[DataFeed]

    I --> J[list_markets / get_market]
    I --> K[stream_orderbook]
    I --> L[get_positions]
    K --> M[MarketState]
    L --> M
    J --> M

    M --> H
    H --> N[Signal]
    N --> G
    G --> D
    G --> E
    G --> F

    C --> O[DashboardIntegration]
    O --> P[FastAPI + WebSocket dashboard]

    C --> Q[KalshiClient]
    Q --> R[MarketMatcher]
    R --> P
```

## Runtime Flow
```mermaid
sequenceDiagram
    participant User
    participant Entry as main.py / run_with_dashboard.py
    participant Feed as DataFeed
    participant Poly as PolymarketClient
    participant Arb as ArbEngine
    participant Exec as ExecutionEngine
    participant Risk as RiskManager
    participant Port as Portfolio

    User->>Entry: start bot
    Entry->>Poly: connect()
    Entry->>Feed: start()
    Feed->>Poly: list_markets() / get_market()
    loop updates
        Feed->>Poly: stream_orderbook(...)
        Feed->>Poly: get_positions()
        Feed->>Arb: MarketState
        Arb-->>Entry: signals
        Entry->>Risk: within_global_limits()
        Entry->>Exec: submit_signal()
        Exec->>Risk: check_order()
        Exec->>Poly: place_order()
        Poly-->>Exec: order / simulated fill
        Exec->>Port: update_from_fill()
        Exec->>Risk: update_from_fill()
    end
```

## Repository Layout
```text
polymarket-arbitrage/
├── main.py
├── run_with_dashboard.py
├── config.yaml
├── requirements.txt
├── core/
├── polymarket_client/
├── kalshi_client/
├── dashboard/
├── utils/
├── tests/
├── test_connection.py
└── test_real_data.py
```

## Quick Start
### 1. Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Review `config.yaml`
Recommended safe starting point:

```yaml
mode:
  trading_mode: "dry_run"
  data_mode: "simulation"

trading:
  bundle_arb_enabled: true
  mm_enabled: false

risk:
  max_position_per_market: 15
  max_global_exposure: 50
  max_daily_loss: 10
```

### 3. Run
Bot only:

```bash
python main.py
```

Bot plus dashboard:

```bash
python run_with_dashboard.py
```

Dashboard default URL:

```text
http://localhost:8888
```

Useful flags:

```bash
python main.py --dry-run
python main.py --live
python main.py --backtest --backtest-duration 300
python run_with_dashboard.py --port 8888 --host 127.0.0.1
python main.py -c config.yaml -v
```

## Modes
### Trading mode
- `dry_run`: orders and fills are simulated in memory
- `live`: requires credentials and uses the repo's current live API implementation

### Data mode
- `real`: polls live Polymarket market/order book endpoints
- `simulation`: generates synthetic books with deliberate inefficiencies

## Configuration Notes
Credentials can be supplied in `config.yaml` or via environment variables:

```bash
export POLYMARKET_API_KEY="..."
export POLYMARKET_API_SECRET="..."
export POLYMARKET_PASSPHRASE="..."
export POLYMARKET_PRIVATE_KEY="..."
```

Important implementation notes:

- `mode.min_match_similarity` exists in config but is not currently passed into `MarketMatcher`
- `trading.maker_fee_bps`, `trading.taker_fee_bps`, and `trading.estimated_gas_per_order` are defined in config, but the entrypoints currently instantiate `ArbEngine` without forwarding those values
- `logging.*` exists in config, but the entrypoints currently call `setup_logging(console_level=...)` with defaults rather than the full config block
- `monitoring.heartbeat_interval` is defined but not used in the runtime loop

## Dashboard
The dashboard exposes:

- `/`
- `/api/state`
- `/api/markets`
- `/api/opportunities`
- `/api/portfolio`
- `/api/risk`
- `/api/timing`
- `/ws`

The dashboard shows market state, opportunities, signals, trades, risk, portfolio summaries, and cross-platform matching progress.

## Cross-Platform Status
This repo includes:

- a Kalshi market data client
- a market matcher between Polymarket and Kalshi
- a cross-platform arbitrage calculator
- dashboard state for matching progress and matched pairs

What is **not** currently wired end-to-end:

- live cross-platform opportunity generation inside `_on_market_update`
- Kalshi order execution
- automated Polymarket <-> Kalshi hedged execution

## Testing
Unit tests:

```bash
pytest tests -v
```

Manual smoke checks:

```bash
python test_real_data.py
python test_connection.py -c config.yaml
```

Note: `test_connection.py` defaults to `config.live.yaml`, but that file is not present in this repository.

## Documentation
Detailed docs live in `docs/`:

- `docs/README.md`
- `docs/ai-repo-map.md`
- `docs/architecture.md`
- `docs/features/trading-bot.md`
- `docs/features/polymarket-data-feed.md`
- `docs/features/strategies.md`
- `docs/features/execution-risk-portfolio.md`
- `docs/features/dashboard.md`
- `docs/features/cross-platform.md`
- `docs/features/configuration-backtesting-and-tests.md`

## Known Limitations
- The main "live" Polymarket order book stream is polling-based and rotates through market batches, so updates are not truly real-time across all discovered markets.
- The Polymarket client contains TODO/placeholder comments around full authenticated CLOB integration.
- Cross-platform matching is operational for discovery and dashboard display, but cross-platform trading is not wired into the execution loop.
- Some config keys are documented in code/config but not fully consumed by the entrypoints.

## Safety
- Start with `dry_run`
- Prefer `simulation` when testing the full loop
- Review the risk limits before enabling `live`
- Treat this repository as experimental trading software

