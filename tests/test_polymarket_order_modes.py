from __future__ import annotations

import pytest

from polymarket_client.api import PolymarketClient
from polymarket_client.models import Market, OrderSide, TokenType


def _market() -> Market:
    return Market(
        market_id="m-1",
        condition_id="c-1",
        question="Will this test pass?",
        market_slug="will-this-test-pass",
    )


@pytest.mark.asyncio
async def test_place_order_passes_ioc_limit_payload():
    client = PolymarketClient(dry_run=False, use_websocket=False, use_rest_fallback=False)
    market = _market()
    payloads = []

    async def fake_get_market(_market_id: str):
        return market

    async def fake_request(method: str, path: str, **kwargs):
        payloads.append((method, path, kwargs.get("json_data")))
        return {"id": "ord-1"}

    client.get_market = fake_get_market  # type: ignore[method-assign]
    client._request = fake_request  # type: ignore[method-assign]

    await client.place_order(
        market_id=market.market_id,
        token_type=TokenType.YES,
        side=OrderSide.BUY,
        price=0.42,
        size=2.0,
        strategy_tag="taker_entry",
        order_type="limit",
        time_in_force="ioc",
    )

    assert payloads
    _, _, payload = payloads[0]
    assert payload["type"] == "ORDER_TYPE_LIMIT"
    assert payload["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
    assert payload["price"]["value"] == "0.4200"


@pytest.mark.asyncio
async def test_close_position_uses_rest_endpoint_and_slippage_payload():
    client = PolymarketClient(dry_run=False, use_websocket=False, use_rest_fallback=False)
    market = _market()
    payloads = []

    async def fake_get_market(_market_id: str):
        return market

    async def fake_request(method: str, path: str, **kwargs):
        payloads.append((method, path, kwargs.get("json_data")))
        return {"ok": True}

    client.get_market = fake_get_market  # type: ignore[method-assign]
    client._request = fake_request  # type: ignore[method-assign]

    await client.close_position(
        market_id=market.market_id,
        slippage_ticks=4,
        current_price=0.51,
    )

    assert payloads
    method, path, payload = payloads[0]
    assert method == "POST"
    assert path == "/v1/order/close-position"
    assert payload["marketSlug"] == market.market_slug
    assert payload["slippageTolerance"]["ticks"] == 4
