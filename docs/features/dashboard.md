# Dashboard

## Purpose
The dashboard is a FastAPI application plus a shared in-memory state object used to inspect the bot while it runs.

## Primary Files
- `run_with_dashboard.py`
- `dashboard/server.py`
- `dashboard/integration.py`

## Main Pieces
### `DashboardState`
Defined in `dashboard/server.py`.

Stores:

- market snapshots
- opportunities
- signals
- open orders
- recent trades
- portfolio summary
- risk summary
- timing stats
- operational stats
- cross-platform matching state
- active WebSocket connections

### `DashboardIntegration`
Defined in `dashboard/integration.py`.

Its job is to copy bot state into `dashboard_state` and broadcast updates.

### FastAPI app
Created in `dashboard/server.py` via `create_app()`.

## Routes
The server exposes:

- `GET /`
- `GET /api/state`
- `GET /api/markets`
- `GET /api/opportunities`
- `GET /api/portfolio`
- `GET /api/risk`
- `GET /api/timing`
- `WS /ws`

## Dashboard Runtime Flow
### Startup
`run_with_dashboard.py`:

1. builds `TradingBotWithDashboard`
2. starts core trading components
3. creates `DashboardIntegration`
4. starts `uvicorn`

### Ongoing updates
`DashboardIntegration._update_loop()` runs every second by default and:

1. copies market snapshots from `DataFeed`
2. copies portfolio summary
3. copies risk summary
4. copies open orders and execution stats
5. copies arb stats and timing stats
6. copies operational counters
7. broadcasts the resulting state via WebSocket

### Immediate event pushes
When signals, opportunities, or trades occur, integration methods also push event-style WebSocket updates immediately.

## Dashboard Data Shape
### Market entries
Each market contains:

- `market_id`
- truncated `question`
- best YES and NO bids/asks
- bundle total ask and total bid
- YES and NO spread values

### Cross-platform section
The `cross_platform` object stores:

- whether it is enabled
- Kalshi market count
- Polymarket market count
- matched pair count
- matching progress
- matched pair preview data
- cross-platform opportunities list

## UI Source
If `dashboard/templates/index.html` exists, it is served.

If not, `dashboard/server.py` falls back to embedded HTML returned by `get_embedded_html()`.

In the current repository snapshot, the embedded HTML path is the relevant one.

## Network Behavior
- default host: `0.0.0.0`
- default port: `8888`
- logs include localhost and best-effort LAN URLs

## Known Limitations
- The dashboard has cross-platform state fields, but the live bot does not currently populate live cross-platform opportunities.
- The UI is embedded inside a large Python string, so front-end changes are less modular than a separate static app.
- Dashboard data is in-memory only; there is no persistence layer.

## Related Docs
- `trading-bot.md`
- `cross-platform.md`
- `architecture.md`

