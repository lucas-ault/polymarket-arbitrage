"""
Tests for the execution-engine "unplaceable market" skip list.

When the API returns a permanent error (e.g. "market not found", 404), we
should:

- Stop retrying the same order in the inner retry loop.
- Suppress further place_orders signals for that market for a TTL.

Without this, a single closed market discovered by the public catalog spams
the log and burns rate-limit budget across many cycles.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.execution import ExecutionConfig, ExecutionEngine
from polymarket_client.models import OrderSide, Signal, TokenType


def _build_engine(
    *,
    place_side_effect,
    skip_seconds: float = 60.0,
    max_retries: int = 3,
) -> ExecutionEngine:
    client = MagicMock()
    client.place_order = AsyncMock(side_effect=place_side_effect)
    client.cancel_order = AsyncMock()

    risk_manager = MagicMock()
    risk_manager.check_order = MagicMock(return_value=True)

    portfolio = MagicMock()

    cfg = ExecutionConfig(
        slippage_tolerance=0.05,
        max_retries=max_retries,
        retry_delay=0.0,
        enable_slippage_check=False,
        dry_run=False,
        unplaceable_market_skip_seconds=skip_seconds,
    )
    return ExecutionEngine(client=client, risk_manager=risk_manager, portfolio=portfolio, config=cfg)


def _signal(market_id: str = "m_dead") -> Signal:
    return Signal(
        signal_id="sig_test",
        action="place_orders",
        market_id=market_id,
        orders=[
            {
                "token_type": TokenType.YES,
                "side": OrderSide.BUY,
                "price": 0.5,
                "size": 1,
                "strategy_tag": "market_making",
            }
        ],
    )


@pytest.mark.asyncio
async def test_permanent_error_skips_retries_and_blacklists_market():
    engine = _build_engine(
        place_side_effect=Exception("market not found"),
        max_retries=3,
    )

    await engine._handle_place_orders(_signal("m_dead"))

    # Only one client call despite max_retries=3 — permanent errors short-circuit.
    assert engine.client.place_order.await_count == 1
    assert engine._is_market_unplaceable("m_dead") is True


@pytest.mark.asyncio
async def test_blacklisted_market_drops_subsequent_signals():
    engine = _build_engine(
        place_side_effect=Exception("market not found"),
    )
    await engine._handle_place_orders(_signal("m_dead"))
    engine.client.place_order.reset_mock()

    # Re-emit twice; both should be silently dropped (no API calls).
    await engine._handle_place_orders(_signal("m_dead"))
    await engine._handle_place_orders(_signal("m_dead"))
    assert engine.client.place_order.await_count == 0


@pytest.mark.asyncio
async def test_transient_error_still_retries_full_count():
    engine = _build_engine(
        place_side_effect=Exception("connection reset"),
        max_retries=3,
    )

    await engine._handle_place_orders(_signal("m_alive"))

    # Transient errors retry the full max_retries; market is NOT blacklisted.
    assert engine.client.place_order.await_count == 3
    assert engine._is_market_unplaceable("m_alive") is False


@pytest.mark.asyncio
async def test_blacklist_expires_after_ttl():
    engine = _build_engine(
        place_side_effect=Exception("market not found"),
        skip_seconds=0.05,
    )
    await engine._handle_place_orders(_signal("m_dead"))
    assert engine._is_market_unplaceable("m_dead") is True

    await asyncio.sleep(0.1)
    assert engine._is_market_unplaceable("m_dead") is False


def test_permanent_error_classifier_matches_known_phrases():
    cases = [
        ("Market not found", True),
        ("HTTP 404 Not Found", True),
        ("market is closed", True),
        ("Market is resolved", True),
        ("connection reset by peer", False),
        ("timeout", False),
        ("429 Too Many Requests", False),
    ]
    for msg, expected in cases:
        assert ExecutionEngine._is_permanent_order_error(Exception(msg)) is expected, msg
