"""
Tests for dashboard state serialization and broadcast behavior.
"""

import pytest

from dashboard.server import DashboardState


class DummyWebSocket:
    def __init__(self):
        self.messages = []

    async def send_text(self, message: str) -> None:
        self.messages.append(message)


def test_to_dict_respects_market_limit():
    state = DashboardState()
    state.markets = {
        "m1": {"market_id": "m1"},
        "m2": {"market_id": "m2"},
        "m3": {"market_id": "m3"},
    }

    payload = state.to_dict(include_markets=True, market_limit=2)
    assert len(payload["markets"]) == 2
    assert payload["operational"]["dashboard_clients"] == 0


def test_to_dict_zero_market_limit_returns_all_markets():
    state = DashboardState()
    state.markets = {
        "m1": {"market_id": "m1"},
        "m2": {"market_id": "m2"},
        "m3": {"market_id": "m3"},
    }

    payload = state.to_dict(include_markets=True, market_limit=0)
    assert len(payload["markets"]) == 3


@pytest.mark.asyncio
async def test_broadcast_updates_metrics():
    state = DashboardState()
    ws1 = DummyWebSocket()
    ws2 = DummyWebSocket()
    state._connections.extend([ws1, ws2])

    await state.broadcast({"type": "update", "data": {"ok": True}})

    assert len(ws1.messages) == 1
    assert len(ws2.messages) == 1
    payload = state.to_dict(include_markets=False)
    assert payload["operational"]["last_ws_payload_bytes"] > 0
    assert payload["operational"]["last_ws_broadcast_ms"] >= 0
