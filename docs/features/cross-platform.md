# Cross-Platform

## Purpose
This feature family brings Kalshi market data into the repository, matches Kalshi markets to Polymarket markets, and contains logic for evaluating cross-platform arbitrage opportunities.

## Primary Files
- `run_with_dashboard.py`
- `core/cross_platform_arb.py`
- `kalshi_client/api.py`
- `kalshi_client/models.py`

## What Is Implemented
### Kalshi market data client
`KalshiClient` can:

- fetch series
- fetch events
- list markets with pagination
- fetch all open markets
- fetch order books
- convert Kalshi books into the unified `OrderBook` shape

### Market matching
`MarketMatcher` can compare Polymarket and Kalshi questions using:

- sports-specific team/date logic
- person/event heuristics
- fuzzy text similarity
- entity overlap
- category-based bucketing

### Cross-platform arbitrage calculator
`CrossPlatformArbEngine.check_arbitrage()` can compare a matched market pair plus two order books and return a `CrossPlatformOpportunity`.

It considers:

- YES on Polymarket vs YES on Kalshi
- YES on Kalshi vs YES on Polymarket
- NO on Polymarket vs NO on Kalshi
- NO on Kalshi vs NO on Polymarket

and subtracts taker fee and gas estimates.

## What the Live App Actually Does
When `run_with_dashboard.py` starts with cross-platform enabled:

1. it creates `KalshiClient`
2. it creates `CrossPlatformArbEngine`
3. it loads open Kalshi markets
4. it waits for some Polymarket markets to exist
5. it calls `MarketMatcher.find_matches()` in a background thread flow
6. it writes progress and matched pairs into dashboard state
7. it can warm-load/save matched pairs through optional Redis cache (`pmarb:xplat:matches:v1` by default)

This is real and implemented.

## What Is Not Wired End-to-End
The current live path does **not**:

- stream Kalshi order books into the dashboard loop
- call `CrossPlatformArbEngine.check_arbitrage()` from normal market updates
- create execution signals from cross-platform opportunities
- place Kalshi trades
- maintain a hedged cross-platform position book

That means cross-platform support is currently best described as:

```text
market discovery + matching + standalone arbitrage logic
```

not:

```text
fully automated cross-exchange trading
```

## Matching Logic Details
### Category bucketing
Before comparing everything-to-everything, the matcher categorizes markets into buckets such as:

- sports
- politics
- crypto
- finance
- entertainment
- tech

It skips broad `other` matching to reduce noise.

### Sports handling
Sports matching is the strongest specialized logic in the file. It uses:

- canonical team mappings
- date extraction
- same-team comparison

### Person/event handling
It also has heuristics for:

- politics
- public figures
- crypto-related entities

## Config Interaction
Relevant config keys include:

- `mode.cross_platform_enabled`
- `mode.kalshi_enabled`
- `mode.min_match_similarity`
- `api.kalshi_api_url`

`mode.min_match_similarity` is forwarded into matcher initialization and also stored in cache metadata to avoid reusing mismatched-threshold snapshots.

## Unified Order Book Conversion
Kalshi only returns bid ladders. `KalshiOrderBook.to_unified_orderbook()` derives asks using the binary-market complement relationship:

- `ask_yes = 1 - best_bid_no`
- `ask_no = 1 - best_bid_yes`

This lets the rest of the code reuse the same `OrderBook` model shape as Polymarket.

## Known Limitations
- Matching is heuristic and can produce false positives or miss valid pairs.
- Cross-platform opportunities are not fed into the execution engine.
- The dashboard includes cross-platform fields for opportunities, but the current runtime mainly fills matching progress and matched pair previews.
- Kalshi support in this repo is market-data-oriented, not trading-oriented.

## Related Docs
- `dashboard.md`
- `architecture.md`
- `ai-repo-map.md`

