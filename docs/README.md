# Documentation Index

This directory documents the repository as it exists in code today.

## Start Here
- `../README.md`: top-level project overview and quick start
- `ai-repo-map.md`: AI-oriented map of entrypoints, modules, symbols, and change surfaces
- `architecture.md`: runtime architecture and data flow

## Feature Docs
- `features/trading-bot.md`: entrypoints, orchestration, lifecycle, shutdown, run modes
- `features/polymarket-data-feed.md`: market discovery, Polymarket polling, market state assembly
- `features/strategies.md`: bundle arbitrage and market-making logic
- `features/execution-risk-portfolio.md`: execution flow, risk checks, positions, PnL
- `features/dashboard.md`: FastAPI dashboard, API routes, WebSocket state, UI integration
- `features/cross-platform.md`: Kalshi integration, matching, cross-platform arb logic, current gaps
- `features/configuration-backtesting-and-tests.md`: config loading, env overrides, backtesting, tests, smoke scripts

## Reading Order
For a new human reader:

1. `../README.md`
2. `architecture.md`
3. one or more feature docs

For an AI agent:

1. `ai-repo-map.md`
2. `architecture.md`
3. the relevant feature doc for the subsystem being changed

## Documentation Rules Used Here
- File paths are written explicitly so both humans and AI tools can jump directly to source.
- Each feature doc includes implemented behavior, primary files, config dependencies, runtime flow, and known limitations.
- When the code and previous docs disagreed, these docs were aligned to the code.

