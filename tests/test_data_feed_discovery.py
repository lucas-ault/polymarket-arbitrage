"""Tests for DataFeed market discovery, including the top-up fallback.

Polymarket US currently surfaces only ~20 markets that pass the strict
liquidity/volume bar. Earlier behavior fell back to a looser query *only* when
the strict result was empty, so a strict result of 20 left the bot with 20
markets and zero opportunity surface. The top-up logic below merges the two
result sets when strict returns < 50, giving bundle-arb something to chew on.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Optional

import pytest

from core.data_feed import DataFeed
from polymarket_client.models import Market


def _market(
    market_id: str,
    *,
    question: Optional[str] = None,
    liquidity: float = 0.0,
    volume_24h: float = 0.0,
    best_bid: float = 0.45,
    best_ask: float = 0.55,
    yes_price: float = 0.50,
    no_price: float = 0.50,
) -> Market:
    return Market(
        market_id=market_id,
        condition_id=market_id,
        question=question or f"Question {market_id}?",
        market_slug=market_id,
        liquidity=liquidity,
        volume_24h=volume_24h,
        best_bid=best_bid,
        best_ask=best_ask,
        yes_price=yes_price,
        no_price=no_price,
    )


class _StubClient:
    """Minimal stand-in for PolymarketClient.list_markets calls."""

    def __init__(
        self,
        strict: list[Market],
        loose: Optional[list[Market]] = None,
    ) -> None:
        self._strict = strict
        self._loose = loose or []
        self.calls: list[dict] = []

    async def list_markets(self, filters=None, *, force_refresh: bool = False):
        params = filters.copy() if filters else {}
        self.calls.append({"params": params, "force_refresh": force_refresh})
        # Distinguish queries by presence of the strict liquidity bar.
        if params.get("liquidityNumMin") is not None:
            return list(self._strict)
        return list(self._loose)


@pytest.mark.asyncio
async def test_discovery_tops_up_when_strict_returns_few_markets():
    strict = [_market(f"strict-{i}", liquidity=500.0, volume_24h=1000.0) for i in range(20)]
    loose = [_market(f"loose-{i}", liquidity=10.0) for i in range(120)]
    # Overlap one market to confirm dedup.
    loose.append(strict[0])

    client = _StubClient(strict=strict, loose=loose)
    feed = DataFeed(client=client, market_ids=[])

    await feed._fetch_markets()

    assert len(client.calls) == 2, "should call strict then loose top-up"
    # All 20 strict markets are kept.
    assert all(f"strict-{i}" in feed._markets for i in range(20))
    # Loose markets are merged in (capped at 200 selected).
    loose_kept = [mid for mid in feed._markets if mid.startswith("loose-")]
    assert loose_kept, "expected loose-filter markets to be merged"
    # Strict overlap is not double-counted.
    assert len(feed.market_ids) <= 200


@pytest.mark.asyncio
async def test_discovery_skips_topup_when_strict_returns_enough():
    strict = [_market(f"strict-{i}", liquidity=500.0, volume_24h=1000.0) for i in range(60)]

    client = _StubClient(strict=strict, loose=[_market("loose-1")])
    feed = DataFeed(client=client, market_ids=[])

    await feed._fetch_markets()

    assert len(client.calls) == 1, "no top-up needed when strict returns >= 50"
    assert "loose-1" not in feed._markets


@pytest.mark.asyncio
async def test_discovery_falls_back_when_strict_returns_zero():
    client = _StubClient(strict=[], loose=[_market("loose-1", liquidity=5.0)])
    feed = DataFeed(client=client, market_ids=[])

    await feed._fetch_markets()

    # Two calls: strict (empty) then loose top-up that supplies everything.
    assert len(client.calls) == 2
    assert "loose-1" in feed._markets


@pytest.mark.asyncio
async def test_discovery_diversifies_across_question_buckets():
    repeated = [
        _market(
            f"mvp-{i}",
            question="NBA MVP",
            liquidity=1000.0,
            volume_24h=1000.0,
            best_bid=0.95,
            best_ask=0.96,
            yes_price=0.955,
            no_price=0.045,
        )
        for i in range(205)
    ]
    uniques = [
        _market(
            f"game-{i}",
            question=f"Game {i}",
            liquidity=500.0,
            volume_24h=500.0,
            best_bid=0.49,
            best_ask=0.53,
            yes_price=0.51,
            no_price=0.49,
        )
        for i in range(20)
    ]
    client = _StubClient(strict=repeated + uniques, loose=[])
    feed = DataFeed(client=client, market_ids=[])

    await feed._fetch_markets()

    selected_questions = {market.question for market in feed._markets.values()}
    assert "NBA MVP" in selected_questions
    assert any(question.startswith("Game ") for question in selected_questions)
    assert len(selected_questions) > 1, "selection should not collapse to one repeated event"
    assert len(feed.market_ids) == 200


@pytest.mark.asyncio
async def test_discovery_prefers_mm_friendly_markets_when_trimming_to_200():
    extreme = [
        _market(
            f"extreme-{i}",
            question=f"Extreme {i}",
            liquidity=500.0,
            volume_24h=500.0,
            best_bid=0.97,
            best_ask=0.98,
            yes_price=0.975,
            no_price=0.025,
        )
        for i in range(220)
    ]
    balanced = [
        _market(
            f"balanced-{i}",
            question=f"Balanced {i}",
            liquidity=400.0,
            volume_24h=400.0,
            best_bid=0.48,
            best_ask=0.52,
            yes_price=0.50,
            no_price=0.50,
        )
        for i in range(5)
    ]

    client = _StubClient(strict=extreme + balanced, loose=[])
    feed = DataFeed(
        client=client,
        market_ids=[],
        config=SimpleNamespace(
            trading=SimpleNamespace(
                mm_enabled=True,
                mm_min_price=0.12,
                mm_max_price=0.88,
                min_spread=0.01,
                mm_max_spread=0.18,
            )
        ),
    )

    await feed._fetch_markets()

    assert all(f"balanced-{i}" in feed._markets for i in range(5))
    assert len(feed.market_ids) == 200


@pytest.mark.asyncio
async def test_discovery_respects_zero_max_monitored_markets_as_unlimited():
    strict = [_market(f"strict-{i}", liquidity=500.0, volume_24h=1000.0) for i in range(60)]

    client = _StubClient(strict=strict, loose=[])
    feed = DataFeed(
        client=client,
        market_ids=[],
        config=SimpleNamespace(
            trading=SimpleNamespace(mm_enabled=False),
            monitoring=SimpleNamespace(max_monitored_markets=0),
        ),
    )

    await feed._fetch_markets()

    assert len(feed.market_ids) == 60
