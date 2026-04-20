# Strategies

## Purpose
This feature detects trading opportunities from each `MarketState` and turns them into execution-ready `Signal` objects.

## Primary Files
- `core/arb_engine.py`
- `polymarket_client/models.py`
- `tests/test_arb_engine.py`

## Main Strategy Engine
### `ArbEngine`
`ArbEngine.analyze(market_state)` is the main strategy entrypoint.

For each update it can produce:

- one bundle arbitrage signal
- zero, one, or two market-making signals

depending on configuration and the current order book.

## Bundle Arbitrage
### Idea
Binary markets should roughly satisfy:

```text
YES + NO ~= 1.0
```

The engine looks for mispricings around that relationship.

### Bundle long
Condition:

```text
best_ask_yes + best_ask_no < 1.0 - fees - gas - min_edge
```

Action:

- buy YES
- buy NO

### Bundle short
Condition:

```text
best_bid_yes + best_bid_no > 1.0 + fees + gas + min_edge
```

Action:

- sell YES
- sell NO

### Fee model
The engine supports:

- `taker_fee_bps`
- `gas_cost_per_order`

and computes net edge after estimated fees.

Important runtime note: the config dataclass supports these values, but the current entrypoints do not forward the values from `config.yaml` into `ArbEngine`.

## Market Making
### Idea
If a token spread is wide enough, the bot places both a bid and an ask just inside the spread.

For each token:

1. check that spread is at least `min_spread`
2. set `our_bid = best_bid + tick_size`
3. set `our_ask = best_ask - tick_size`
4. ensure the remaining inside spread is still positive
5. size orders from `default_order_size`

This produces paired buy/sell orders on the same token.

## Signal Shape
Signals are instances of `Signal` with:

- `action="place_orders"`
- `market_id`
- attached `Opportunity`
- a list of order specs containing token, side, price, size, and strategy tag

Bundle arbitrage uses priority `10`.

Market making uses priority `5`.

## Opportunity Tracking
`ArbEngine` also tracks:

- recent opportunities
- per-opportunity cooldowns
- active opportunities for duration measurement
- aggregate timing statistics for the dashboard

Timing stats include:

- average opportunity duration
- min/max duration
- bucket counts under 100ms, 500ms, 1s, and over 1s

## Cooldowns
To reduce repeated signal spam:

- bundle opportunity cooldown is 2 seconds
- market-making cooldown is 5 seconds per market/token pair

## Supported Opportunity Types
Defined in `polymarket_client/models.py`:

- `BUNDLE_LONG`
- `BUNDLE_SHORT`
- `MM_BID`
- `MM_ASK`

## Test Coverage
`tests/test_arb_engine.py` covers:

- bundle long detection
- bundle short detection
- no-signal fair pricing
- threshold behavior
- market-making detection
- basic signal structure
- stats tracking
- empty/missing price edge cases

## Known Limitations
- Only single-platform Polymarket strategies are wired into the live bot callback.
- The market-making opportunity type labeling uses `MM_BID` for YES and `MM_ASK` for NO, even though each MM signal actually places both sides.
- Strategy sizing is liquidity-aware at the best level, but it does not model deeper book impact.

## Related Docs
- `trading-bot.md`
- `execution-risk-portfolio.md`
- `cross-platform.md`

