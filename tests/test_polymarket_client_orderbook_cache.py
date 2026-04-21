"""Tests for polymarket client market cache behavior."""

from datetime import datetime

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


@pytest.mark.asyncio
async def test_get_trades_parses_timestamp_and_stable_fallback_id():
    client = PolymarketClient(dry_run=False)

    async def _fake_request(*args, **kwargs):
        return {
            "activities": [
                {
                    "type": "ACTIVITY_TYPE_TRADE",
                    "createdAt": "2026-04-20T12:34:56Z",
                    "trade": {
                        "orderId": "ord-1",
                        "marketSlug": "event-1",
                        "price": {"value": "0.61"},
                        "quantity": "3",
                        "side": "buy",
                    },
                }
            ]
        }

    client._request = _fake_request  # type: ignore[method-assign]
    trades = await client.get_trades(limit=10)

    assert len(trades) == 1
    assert trades[0].trade_id
    assert trades[0].market_slug == "event-1"
    assert trades[0].timestamp == datetime(2026, 4, 20, 12, 34, 56)


@pytest.mark.asyncio
async def test_get_account_balances_returns_balances_list():
    client = PolymarketClient(dry_run=False)

    async def _fake_request(method, endpoint, params=None, json_data=None, use_private=False):
        assert method == "GET"
        assert endpoint == "/v1/account/balances"
        assert use_private is True
        return {
            "balances": [
                {"currency": "USD", "currentBalance": "123.45", "buyingPower": "100.00"}
            ]
        }

    client._request = _fake_request  # type: ignore[method-assign]
    balances = await client.get_account_balances()

    assert len(balances) == 1
    assert balances[0]["currency"] == "USD"


@pytest.mark.asyncio
async def test_get_portfolio_metrics_combines_positions_activities_and_balances():
    client = PolymarketClient(dry_run=False)

    async def _fake_request(method, endpoint, params=None, json_data=None, use_private=False):
        assert method == "GET"
        assert use_private is True
        if endpoint == "/v1/portfolio/positions":
            return {
                "positions": {
                    "event-1": {
                        "netPosition": "10",
                        "realized": {"value": "2.00", "currency": "USD"},
                        "cashValue": {"value": "1.25", "currency": "USD"},
                    }
                },
                "eof": True,
            }
        if endpoint == "/v1/account/balances":
            return {
                "balances": [
                    {
                        "currency": "USD",
                        "currentBalance": 250.0,
                        "buyingPower": 180.0,
                        "assetNotional": 80.0,
                        "assetAvailable": 70.0,
                        "openOrders": 5.0,
                    }
                ]
            }
        if endpoint == "/v1/portfolio/activities":
            return {
                "activities": [
                    {
                        "type": "ACTIVITY_TYPE_TRADE",
                        "trade": {"realizedPnl": {"value": "1.50", "currency": "USD"}},
                    },
                    {
                        "type": "ACTIVITY_TYPE_TRADE",
                        "trade": {"realizedPnl": {"value": "-0.50", "currency": "USD"}},
                    },
                ]
            }
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    client._request = _fake_request  # type: ignore[method-assign]
    metrics = await client.get_portfolio_metrics(activity_limit=50)

    assert metrics["pnl"]["realized_pnl"] == 2.0
    assert metrics["pnl"]["unrealized_pnl"] == 1.25
    assert metrics["pnl"]["total_pnl"] == 3.25
    assert metrics["balances"]["current_balance"] == 250.0
    assert metrics["total_exposure"] == 80.0
    assert metrics["total_trades"] == 2
    assert metrics["win_rate"] == 0.5


@pytest.mark.asyncio
async def test_get_portfolio_metrics_returns_partial_data_when_positions_fail():
    client = PolymarketClient(dry_run=False)

    async def _fake_request(method, endpoint, params=None, json_data=None, use_private=False):
        if endpoint == "/v1/portfolio/positions":
            raise RuntimeError("429 Too Many Requests")
        if endpoint == "/v1/account/balances":
            return {
                "balances": [
                    {
                        "currency": "USD",
                        "currentBalance": 99.0,
                        "buyingPower": 88.0,
                        "assetNotional": 11.0,
                    }
                ]
            }
        if endpoint == "/v1/portfolio/activities":
            return {
                "activities": [
                    {
                        "type": "ACTIVITY_TYPE_TRADE",
                        "trade": {"realizedPnl": {"value": "0.75", "currency": "USD"}},
                    }
                ]
            }
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    client._request = _fake_request  # type: ignore[method-assign]
    metrics = await client.get_portfolio_metrics(activity_limit=50)

    assert metrics["balances"]["current_balance"] == 99.0
    assert metrics["total_exposure"] == 11.0
    assert metrics["total_trades"] == 1
    assert metrics["pnl"]["realized_pnl"] == 0.0  # realized from positions unavailable
