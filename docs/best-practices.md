# Best Practices Reference

This document consolidates two things into one operating reference:

1. Official Polymarket US guidance that is most relevant to a programmatic trader.
2. General prediction-market and exchange-bot best practices that apply regardless of venue.

Anything labelled **Official** comes from public Polymarket US documentation. Anything
labelled **Inferred** is general bot-operating practice that this repository chooses
to follow.

## Source Material

- Polymarket US documentation portal: <https://docs.polymarket.us>
- Polymarket US authentication: <https://docs.polymarket.us/api-reference/authentication>
- Polymarket US rate limits: <https://docs.polymarket.us/api-reference/rate-limits>
- Polymarket US markets WebSocket: <https://docs.polymarket.us/api-reference/websocket/markets>
- Polymarket US trader/positions/risk guide: <https://docs.polymarket.us/trader-guide/positions-risk>

The repo also bundles MCP descriptors for the `user-Polymarket US Documentation` server.
Use those tools when you need to refresh this reference against the live docs.

## Authentication

**Official.** Polymarket US uses API keys (`X-PM-Access-Key`, `X-PM-Timestamp`,
`X-PM-Signature`) signed with an Ed25519 private key. The signature payload is
`{timestamp}{METHOD}{PATH}` and the timestamp is in milliseconds since epoch.

Repository surface:

- `polymarket_client/api.py::PolymarketClient._auth_headers` implements the signing
  scheme and explicitly fails when `cryptography` is missing.
- `utils/config_loader.py::_validate_config` requires both `api.key_id` and
  `api.secret_key` for live mode.

**Inferred.** Treat any "live" run that lacks signing material or a working
`cryptography` install as a configuration error and fail fast at startup. Do not
silently fall back to dry-run, since operators may believe they are live.

## Rate Limits

**Official.** Polymarket US REST endpoints have published rate limits per key. Public
market endpoints have laxer limits than authenticated trading endpoints.

**Inferred best practices for this repo:**

- Prefer the markets WebSocket for top-of-book updates instead of polling
  `/v1/markets/.../book`.
- When polling is required, batch markets, cap concurrency, and add a small delay
  between rotations. The relevant knobs live in `monitoring.orderbook_*` config.
- Treat HTTP 429 / 503 as backpressure: back off, do not retry tighter.
- Reconciliation queries (positions, trades, portfolio) should run on a slower
  cadence than market data and skip if the previous request is still in flight.

## Markets WebSocket

**Official.** `wss://api.polymarket.us/v1/ws/markets` exposes order book / trade
streams. Subscribe to specific market slugs or token IDs and handle reconnects.

Repository surface:

- `PolymarketClient._connect_websocket` and `_stream_orderbooks_ws` provide the WS
  path. They are wired behind `api.use_websocket=true` and currently coexist with
  REST polling.

**Inferred.** Always pair WebSocket usage with:

- An exponential-backoff reconnect loop with jitter.
- A heartbeat / "last message" timer that triggers reconnect when the stream goes
  silent past a threshold.
- A REST fallback for the markets the WS does not cover.

## Private (Authenticated) WebSocket

**Official.** `wss://api.polymarket.us/v1/ws/private` exposes order, fill, and
position events for the authenticated key. It requires the same Ed25519 signing
material as the REST API.

**Inferred.** Even if the bot prefers periodic REST reconciliation, the private WS is
the lowest-latency way to learn about external fills (manual cancels, partial
fills, etc.) and should be the preferred source of truth where available.

## Order Placement

**Official.** Orders to `/v1/orders` use a structured payload with intent
(`ORDER_INTENT_BUY_LONG` / `BUY_SHORT` / `SELL_LONG` / `SELL_SHORT`), price as a
USD-denominated `{value, currency}` object, integer `quantity`, time in force, and
the manual-order indicator. Price values reference the YES side; SHORT intents are
priced as `1 - p_no` on the wire.

Repository surface:

- `PolymarketClient.place_order` constructs this payload, normalizes `NO`-side
  prices to `1 - p`, clamps the price to `[0.01, 0.99]`, and returns an `Order`.

**Inferred safeguards:**

- Always round to the venue tick before submission and reject sub-tick prices.
- Round size to the venue minimum increment instead of silently truncating.
- Snapshot the book at signal generation and reject the order if the inside has
  moved beyond the configured slippage tolerance by the time we send it.
- Tag every order with the strategy that generated it for cancel / unwind logic.

## Cancels and Order Hygiene

**Official.** Cancels target a specific order id, with the market slug typically
required as part of the body or path.

Repository surface:

- `PolymarketClient.cancel_order` tries both SDK and REST shapes and falls back
  through several body variants to handle SDK changes.
- `ExecutionEngine._monitor_order_timeouts` cancels stale orders.

**Inferred.** Always cancel resting orders when:

- Market data goes stale beyond a configurable threshold.
- The risk kill switch trips.
- Connectivity to the exchange degrades meaningfully.
- The bot shuts down.

## Positions and Risk

**Official.** `/v1/portfolio/positions` paginates with `cursor` / `eof`. Net
position can be negative, indicating short. Activity, balances, and PnL share a
common envelope.

Repository surface:

- `PolymarketClient.get_positions` paginates the cursor and converts net positions
  into `Position` objects.
- `PolymarketClient.get_portfolio_metrics` aggregates balances, PnL, and counts
  for dashboard display.
- `Portfolio.apply_exchange_metrics` mirrors the exchange-reported summary into the
  shared portfolio view.

**Inferred.** Reconciliation rules:

- Periodically compare local position deltas with `/v1/portfolio/positions`. Treat
  any sustained mismatch as a critical alert.
- Use exchange-reported PnL as the authoritative source for risk gating (daily
  loss, drawdown). Local PnL can drift due to fee or rounding differences.

## Slippage, Fees, and Edge

**Official.** Polymarket US uses tiered taker fees with maker rebates in some
configurations. Refer to the trader guide for the latest schedule.

Repository surface:

- `utils/polymarket_fees.polymarket_fee` and `core/arb_engine.ArbEngine` consume
  `taker_fee_bps`, `maker_fee_bps`, `fee_theta_taker`, and `fee_theta_maker`.

**Inferred.** Strategy correctness rules:

- A signal is profitable only after expected fees, gas / write costs (zero on
  Polymarket US but kept as a parameter for cross-venue use), and a model of
  expected slippage.
- Track *expected edge at signal* vs *realized edge at fill* and alert when these
  diverge persistently.

## Operational Hygiene Checklist

These apply to any prediction-market bot, not just Polymarket.

1. **Stale-data rejection.** Refuse to act on an order book that has not updated
   within `N` seconds.
2. **Fail-closed on degradation.** When the API is unhealthy, prefer to cancel
   open orders and stop sending new ones rather than firing into a fog.
3. **Kill switch surfaces in UI.** The dashboard / log must clearly show kill
   switch state, daily PnL versus limit, and current drawdown.
4. **Bounded queues.** Both signal and broadcast queues should be bounded and
   drop-or-block intentionally instead of silently growing.
5. **Reconciliation is non-optional.** Compare local order, position, and PnL
   state against the exchange on a slower cadence than the trading loop.
6. **Paper before live.** Run the same strategy in dry-run with simulated fills,
   then in dry-run with real market data and simulated fills, then live with a
   tiny size cap, before scaling.
7. **Telemetry is part of the strategy.** Without per-strategy fill rate, slippage,
   markout, and post-fee PnL, "is this profitable?" is unanswerable.
8. **Bound dashboard exposure.** Bind to localhost by default; expose to LAN /
   internet only when intentional and authenticated.

## Mapping Best Practices to This Repo

| Concern | Implementation surface |
| --- | --- |
| Auth + signing | `polymarket_client/api.py::_auth_headers` |
| Markets WS | `polymarket_client/api.py::_stream_orderbooks_ws` |
| Order payload | `polymarket_client/api.py::place_order` |
| Order cancels | `polymarket_client/api.py::cancel_order`, `core/execution.py::cancel_all_orders` |
| Position reconciliation | `polymarket_client/api.py::get_positions`, `run_with_dashboard.py::_sync_live_portfolio_metrics` |
| Stale-data gating | `core/data_feed.py::get_staleness`, `core/risk_manager.py::register_market_state` |
| Auto-unwind | `core/risk_manager.py::auto_unwind_on_breach`, `core/execution.py::cancel_all_orders` |
| Dashboard health surface | `dashboard/integration.py::_update_loop`, `dashboard/server.py` |
| Profit telemetry | `core/arb_engine.py::get_stats`, `core/portfolio.py::get_pnl`, `utils/profit_telemetry.py` |
| Paper / live progression | `docs/operations.md` |
