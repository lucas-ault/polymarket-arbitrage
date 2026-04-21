"""
Tests for orderbook token negative-cache behavior.
"""

import httpx
import pytest

from polymarket_client.api import PolymarketClient
from polymarket_client.models import TokenType


@pytest.mark.asyncio
async def test_404_token_is_cached_and_skipped():
    client = PolymarketClient(dry_run=True)
    calls = {"count": 0}
    request = httpx.Request("GET", "https://clob.polymarket.com/book")
    response = httpx.Response(404, request=request)

    async def _fake_request(*args, **kwargs):
        calls["count"] += 1
        raise httpx.HTTPStatusError("not found", request=request, response=response)

    client._request = _fake_request  # type: ignore[method-assign]

    first = await client._fetch_token_orderbook("bad-token", TokenType.YES)
    second = await client._fetch_token_orderbook("bad-token", TokenType.YES)

    assert first.bids.levels == []
    assert second.asks.levels == []
    assert calls["count"] == 1
    stats = client.get_runtime_stats()
    assert stats["orderbook_invalid_token_marks"] >= 1
    assert stats["orderbook_invalid_token_skips"] >= 1
