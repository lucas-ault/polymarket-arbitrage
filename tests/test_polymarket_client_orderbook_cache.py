"""Tests for polymarket client market cache behavior."""

import pytest

from polymarket_client.api import PolymarketClient


@pytest.mark.asyncio
async def test_list_markets_uses_cache_before_api():
    client = PolymarketClient(dry_run=True)
    calls = {"count": 0}

    async def _fake_request(*args, **kwargs):
        calls["count"] += 1
        return {
            "markets": [
                {
                    "id": "market-1",
                    "slug": "market-1",
                    "question": "Will test pass?",
                    "active": True,
                }
            ]
        }

    client._request = _fake_request  # type: ignore[method-assign]

    first = await client.list_markets({"active": True})
    second = await client.list_markets({"active": True})

    assert len(first) == 1
    assert len(second) == 1
    assert calls["count"] == 1
    assert first[0].market_slug == "market-1"
