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


@pytest.mark.asyncio
async def test_cancel_order_uses_sdk_signature_with_required_params():
    class FakeOrders:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict]] = []

        async def cancel(self, order_id: str, params: dict) -> None:
            self.calls.append((order_id, params))

    class FakeSdk:
        def __init__(self) -> None:
            self.orders = FakeOrders()

    client = PolymarketClient(dry_run=False)
    client._sdk_client = FakeSdk()  # type: ignore[assignment]

    async def _unexpected_request(*args, **kwargs):
        raise AssertionError("REST fallback should not be called when SDK cancel succeeds")

    client._request = _unexpected_request  # type: ignore[method-assign]

    await client.cancel_order("abc123", market_slug="election-2026")

    assert client._sdk_client.orders.calls == [
        ("abc123", {"symbol": "election-2026"})
    ]


@pytest.mark.asyncio
async def test_cancel_order_falls_back_to_rest_when_sdk_cancel_fails():
    class FakeOrders:
        async def cancel(self, *args, **kwargs) -> None:
            raise RuntimeError("sdk cancel failed")

    class FakeSdk:
        def __init__(self) -> None:
            self.orders = FakeOrders()

    client = PolymarketClient(dry_run=False)
    client._sdk_client = FakeSdk()  # type: ignore[assignment]
    captured: dict[str, object] = {}

    async def _fake_request(method, endpoint, params=None, json_data=None, use_private=False):
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["json_data"] = json_data
        captured["use_private"] = use_private
        return {}

    client._request = _fake_request  # type: ignore[method-assign]

    await client.cancel_order("xyz789", market_slug="sports-foo")

    assert captured == {
        "method": "POST",
        "endpoint": "/v1/order/xyz789/cancel",
        "json_data": {"marketSlug": "sports-foo"},
        "use_private": True,
    }
