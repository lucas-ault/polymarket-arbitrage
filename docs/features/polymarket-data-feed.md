# Polymarket Data Feed

## Purpose
This feature discovers markets, fetches Polymarket order books, refreshes positions, and assembles `MarketState` objects for strategy evaluation.

## Primary Files
- `core/data_feed.py`
- `polymarket_client/api.py`
- `polymarket_client/models.py`

## Main Components
### `DataFeed`
Responsible for:

- loading market metadata
- keeping in-memory `Market`, `OrderBook`, and `Position` state
- assembling `MarketState`
- calling the bot callback whenever state changes

### `PolymarketClient`
Provides:

- Gamma API market discovery
- CLOB order book fetching
- dry-run simulated books
- dry-run positions and fills

## Market Discovery
If `trading.markets` is empty, `DataFeed._fetch_markets()` calls:

```python
client.list_markets({"active": True})
```

`PolymarketClient.list_markets()` then:

- pages through Gamma `/markets`
- keeps only markets with valid YES and NO token IDs
- caches those markets in `_markets_cache`
- returns up to about 5000 markets

If specific market IDs are supplied, `DataFeed` instead calls `client.get_market()` one-by-one.

## Order Book Ingestion
### Real mode
When `mode.data_mode: "real"`, the main stream path is:

```python
PolymarketClient.stream_orderbook(..., use_simulation=False)
```

Behavior:

- uses cached token IDs from discovered markets
- rotates through markets in batches
- fetches YES and NO token books separately using `/book?token_id=...`
- builds unified `OrderBook` objects
- yields `(market_id, orderbook)` tuples

This is polling, not the WebSocket implementation.

### Simulation mode
When `mode.data_mode: "simulation"`, the client:

- generates synthetic books
- keeps YES and NO near complementary
- occasionally introduces larger mispricings
- updates a smaller active subset for faster visible activity

This is the easiest mode for demos and dashboard screenshots.

## Position Refresh
`DataFeed._position_refresh_loop()` periodically calls:

```python
client.get_positions()
```

Behavior differs by mode:

- dry run: returns in-memory simulated positions
- live: attempts a `/positions` call, but the live client implementation is still partial

## `MarketState` Assembly
`DataFeed._update_market_state()` combines:

- market metadata
- latest order book
- latest positions
- timestamp

and stores the result in `_market_states`.

If an `on_update` callback was provided, it calls:

```python
self.on_update(market_id, state)
```

That callback is what drives strategy evaluation.

## Order Book Data Shape
The strategy layer works on unified `OrderBook` objects containing:

- `yes.bids`
- `yes.asks`
- `no.bids`
- `no.asks`
- convenience properties like `best_ask_yes`, `best_bid_no`, `total_ask`, and `total_bid`

## WebSocket Status
`polymarket_client/api.py` contains `_connect_websocket()`, but the main bot does not use it in the active live data path.

If you are changing "real-time" behavior, do not assume WebSockets are currently active.

## Performance Characteristics
- Large market discovery is optimized around Gamma pagination and token caching.
- Live updates are eventually consistent and batch-rotated, not uniformly real-time across thousands of markets.
- Simulation mode is much faster and denser than real mode.

## Known Limitations
- `DataFeed` docstrings mention WebSocket streaming even though the implemented live path is polling.
- `wait_for_data()` checks whether all markets have order books; with large discovered sets, initial warmup can be slow.
- Live positions rely on client endpoints that may not match real Polymarket auth requirements in all environments.

## Related Docs
- `trading-bot.md`
- `strategies.md`
- `configuration-backtesting-and-tests.md`

