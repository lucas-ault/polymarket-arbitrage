"""Tests for NO-side synthesis in PolymarketClient.get_orderbook.

Polymarket binary markets typically return only the YES book; the NO book
must be synthesized from YES (NO_price = 1 - YES_price). Earlier behavior
only ran NO synthesis as a last-ditch fallback after every YES path failed,
which left ~100% of orderbooks one-sided in the field. These tests pin the
expected behavior across the three populate paths.
"""

from __future__ import annotations

import pytest

from polymarket_client.api import PolymarketClient
from polymarket_client.models import Market, TokenType


def _make_client_with_market() -> PolymarketClient:
    client = PolymarketClient(dry_run=True)
    market = Market(
        market_id="m1",
        condition_id="m1",
        question="Will test pass?",
        market_slug="m1",
        best_bid=0.40,
        best_ask=0.45,
        yes_price=0.42,
    )
    client._markets_cache["m1"] = market
    client._markets_by_slug["m1"] = market
    return client


@pytest.mark.asyncio
async def test_no_synthesized_when_only_yes_explicit_keys_returned():
    """When the API returns yes_bids/yes_asks but no no_*, NO must be mirrored."""
    client = _make_client_with_market()

    async def _fake_request(method, path, **kwargs):
        return {
            "yes_bids": [{"price": 0.40, "size": 100}],
            "yes_asks": [{"price": 0.45, "size": 100}],
        }

    client._request = _fake_request  # type: ignore[method-assign]

    book = await client.get_orderbook("m1")

    assert book.yes.best_bid == pytest.approx(0.40)
    assert book.yes.best_ask == pytest.approx(0.45)
    # NO bid mirrors YES ask: 1 - 0.45 = 0.55
    assert book.no.best_bid == pytest.approx(0.55)
    # NO ask mirrors YES bid: 1 - 0.40 = 0.60
    assert book.no.best_ask == pytest.approx(0.60)
    assert book.no.synthetic is True
    assert book.yes.synthetic is False


@pytest.mark.asyncio
async def test_no_synthesized_when_only_top_level_bids_asks_returned():
    """Polymarket sometimes uses flat bids/asks keys; NO must still mirror."""
    client = _make_client_with_market()

    async def _fake_request(method, path, **kwargs):
        return {
            "bids": [{"price": 0.30, "size": 50}],
            "asks": [{"price": 0.35, "size": 50}],
        }

    client._request = _fake_request  # type: ignore[method-assign]

    book = await client.get_orderbook("m1")

    assert book.yes.best_bid == pytest.approx(0.30)
    assert book.yes.best_ask == pytest.approx(0.35)
    assert book.no.best_bid == pytest.approx(0.65)  # 1 - 0.35
    assert book.no.best_ask == pytest.approx(0.70)  # 1 - 0.30
    assert book.no.synthetic is True


@pytest.mark.asyncio
async def test_real_no_book_is_not_marked_synthetic():
    """If the API returns a genuine NO book, we must NOT overwrite it."""
    client = _make_client_with_market()

    async def _fake_request(method, path, **kwargs):
        return {
            "yes_bids": [{"price": 0.40, "size": 100}],
            "yes_asks": [{"price": 0.45, "size": 100}],
            "no_bids": [{"price": 0.52, "size": 80}],
            "no_asks": [{"price": 0.58, "size": 80}],
        }

    client._request = _fake_request  # type: ignore[method-assign]

    book = await client.get_orderbook("m1")

    # NO came from the API directly, not synthesis. Note this gives us a
    # genuine 2-sided market where bundle arb is mathematically possible:
    # total_ask = 0.45 + 0.58 = 1.03, total_bid = 0.40 + 0.52 = 0.92.
    assert book.no.best_bid == pytest.approx(0.52)
    assert book.no.best_ask == pytest.approx(0.58)
    assert book.no.synthetic is False
