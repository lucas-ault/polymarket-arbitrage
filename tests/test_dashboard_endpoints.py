"""
Tests for the dashboard FastAPI endpoints, including the new /api/health and
/api/profit surfaces added during the live-readiness pass.
"""

from datetime import datetime

from fastapi.testclient import TestClient

from dashboard.server import create_app, dashboard_state


def _reset_state():
    dashboard_state.markets = {}
    dashboard_state.opportunities = []
    dashboard_state.signals = []
    dashboard_state.orders = []
    dashboard_state.trades = []
    dashboard_state.portfolio = {}
    dashboard_state.risk = {}
    dashboard_state.stats = {}
    dashboard_state.timing = {}
    dashboard_state.operational = {}
    dashboard_state.profit_telemetry = {}
    dashboard_state.is_running = False
    dashboard_state.mode = "dry_run"
    dashboard_state.last_update = datetime.utcnow()


def test_health_endpoint_reports_risk_and_stream_state():
    _reset_state()
    dashboard_state.is_running = True
    dashboard_state.mode = "live"
    dashboard_state.risk = {
        "kill_switch_triggered": True,
        "auto_unwind_on_breach": True,
        "stale_market_rejections": 3,
        "exchange_health_rejections": 2,
        "exchange_health_degraded": True,
        "volume_rejections": 1,
        "max_market_staleness_seconds": 5.0,
    }
    dashboard_state.operational = {
        "is_streaming": True,
        "stream_errors": 2,
        "stream_reconnects": 4,
        "markets_rest_fallback_active": True,
        "private_ws_connected": False,
        "avg_staleness_seconds": 0.4,
        "p95_staleness_seconds": 1.2,
        "max_staleness_seconds": 2.5,
    }

    client = TestClient(create_app())
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_running"] is True
    assert body["mode"] == "live"
    assert body["kill_switch_triggered"] is True
    assert body["auto_unwind_on_breach"] is True
    assert body["stale_market_rejections"] == 3
    assert body["exchange_health_rejections"] == 2
    assert body["exchange_health_degraded"] is True
    assert body["volume_rejections"] == 1
    assert body["max_market_staleness_seconds"] == 5.0
    assert body["is_streaming"] is True
    assert body["stream_errors"] == 2
    assert body["stream_reconnects"] == 4
    assert body["markets_rest_fallback_active"] is True
    assert body["private_ws_connected"] is False


def test_profit_endpoint_returns_telemetry_summary():
    _reset_state()
    dashboard_state.profit_telemetry = {
        "total_opportunities": 5,
        "per_strategy": {"bundle_long": {"avg_edge": 0.02}},
    }
    client = TestClient(create_app())
    resp = client.get("/api/profit")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_opportunities"] == 5
    assert body["per_strategy"]["bundle_long"]["avg_edge"] == 0.02


def test_state_endpoint_includes_profit_telemetry_field():
    _reset_state()
    dashboard_state.profit_telemetry = {"total_opportunities": 0}
    client = TestClient(create_app())
    resp = client.get("/api/state")
    assert resp.status_code == 200
    body = resp.json()
    assert "profit_telemetry" in body


def test_state_endpoint_market_limit_zero_returns_all_markets():
    _reset_state()
    dashboard_state.markets = {
        "m1": {"market_id": "m1"},
        "m2": {"market_id": "m2"},
    }
    client = TestClient(create_app())
    resp = client.get("/api/state?market_limit=0")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["markets"]) == 2
