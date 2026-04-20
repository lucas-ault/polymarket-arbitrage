# Execution, Risk, and Portfolio

## Purpose
These three subsystems turn strategy signals into orders, enforce trading constraints, and maintain positions and PnL.

## Primary Files
- `core/execution.py`
- `core/risk_manager.py`
- `core/portfolio.py`
- `tests/test_risk_manager.py`
- `tests/test_portfolio.py`

## Execution Engine
### Responsibilities
`ExecutionEngine` handles:

- queued signal processing
- order placement
- order cancellation
- slippage checks
- timeout-based cancellation
- fill processing

### Signal flow
1. `submit_signal(signal)` pushes onto `_signal_queue`
2. `_process_signals()` dequeues and executes
3. `_handle_place_orders()` validates each proposed order
4. `_place_order()` delegates to `PolymarketClient`
5. successful orders are stored in `_open_orders`

### Slippage checks
If a signal includes an `Opportunity`, execution compares intended order prices against the captured opportunity snapshot and rejects orders outside `slippage_tolerance`.

### Timeout handling
`_monitor_order_timeouts()` periodically cancels orders older than `order_timeout_seconds`.

### Fill handling
`handle_fill(trade)`:

- updates tracked open order state
- forwards the fill to `Portfolio`
- forwards the fill to `RiskManager`

## Risk Manager
### Responsibilities
`RiskManager` validates orders against:

- kill switch state
- blacklist/whitelist rules
- optional volume filter
- per-market exposure limits
- global exposure limits
- daily loss limits
- drawdown limits

### Kill switch
The kill switch can be triggered by:

- daily loss exceeding `max_daily_loss`
- drawdown exceeding `max_drawdown_pct`

When active, `check_order()` rejects new orders and `within_global_limits()` returns false.

### Exposure model
The risk manager tracks:

- `_market_exposure`
- `state.global_exposure`
- session trades
- daily PnL and drawdown state

### Volume filter note
If `trade_only_high_volume` is enabled, `RiskManager` expects market volume to be populated through:

- `update_market_volume()`
- `set_market_volumes()`

The main runtime path does not currently populate this cache from discovered markets.

## Portfolio
### Responsibilities
`Portfolio` tracks:

- positions by market and token
- average entry prices
- realized PnL
- unrealized PnL
- fees
- cash balance
- total volume
- win/loss counts

### Fill processing
`Portfolio.update_from_fill(trade)` handles:

- long position increases
- long reductions
- short creation
- short covering
- cost basis updates
- cash balance updates

### Price marking
Unrealized PnL is only recalculated when `update_prices()` is called. The main runtime flow does not currently wire live market prices back into the portfolio for continuous mark-to-market updates, so unrealized PnL may stay limited depending on the execution path.

## Dry-Run Interaction
In dry run:

- `PolymarketClient.place_order()` stores orders in `_simulated_orders`
- `simulate_fill()` creates `Trade` objects with a hardcoded 1.5% fee assumption
- execution passes these fills into both portfolio and risk

This makes dry run the most coherent end-to-end execution path in the repository.

## Test Coverage
### `tests/test_risk_manager.py`
Covers:

- valid and invalid orders
- blacklist behavior
- low-volume rejection
- per-market and global exposure checks
- kill switch behavior
- summary fields

### `tests/test_portfolio.py`
Covers:

- position creation and reduction
- average price calculations
- realized and unrealized PnL
- fee tracking
- market and total exposure
- win rate and summary behavior

## Known Limitations
- Risk exposures are not netted with complex hedging semantics; they are tracked using notional deltas in a simplified way.
- Portfolio marking depends on explicit `update_prices()` calls, which are not continuously driven by the main bot loop.
- Live order placement and position retrieval rely on client code that still contains partial/in-progress integration behavior.

## Related Docs
- `trading-bot.md`
- `strategies.md`
- `configuration-backtesting-and-tests.md`

