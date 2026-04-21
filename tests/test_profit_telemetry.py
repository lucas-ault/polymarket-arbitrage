"""Tests for the post-fee ProfitTelemetry aggregator."""

from datetime import datetime, timedelta

from utils.profit_telemetry import ProfitTelemetry


def test_record_opportunity_aggregates():
    pt = ProfitTelemetry()
    pt.record_opportunity("m1", "bundle_long", edge=0.02, fee_bps=10)
    pt.record_opportunity("m2", "bundle_long", edge=0.04, fee_bps=10)
    pt.record_opportunity("m3", "mm_bid", edge=0.01)

    summary = pt.summary()
    assert summary["total_opportunities"] == 3
    bundle = summary["per_strategy"]["bundle_long"]
    assert bundle["opportunities"] == 2
    assert bundle["avg_edge"] == 0.03
    # fee haircut = 10bps = 0.001 in price space
    assert abs(bundle["avg_post_fee_edge"] - (0.03 - 0.001)) < 1e-9
    assert bundle["min_edge"] == 0.02
    assert bundle["max_edge"] == 0.04


def test_record_fill_and_fill_rate():
    pt = ProfitTelemetry()
    pt.record_opportunity("m1", "bundle_long", edge=0.02)
    pt.record_opportunity("m2", "bundle_long", edge=0.02)
    pt.record_fill("m1", price=0.5, size=10, fee=0.05)

    summary = pt.summary()
    assert summary["total_fills"] == 1
    assert summary["total_fill_notional"] == 5.0
    assert summary["total_fees_paid"] == 0.05
    assert summary["fill_rate"] == 0.5


def test_evaluate_gate_passes_with_sample_and_edge():
    pt = ProfitTelemetry()
    for _ in range(60):
        pt.record_opportunity("m", "bundle_long", edge=0.02, fee_bps=20)
    verdict = pt.evaluate_gate(min_opportunities=50, min_avg_post_fee_edge=0.005)
    assert verdict["passed"] is True
    assert verdict["per_strategy"]["bundle_long"]["passed"] is True


def test_evaluate_gate_fails_on_low_sample():
    pt = ProfitTelemetry()
    for _ in range(5):
        pt.record_opportunity("m", "bundle_long", edge=0.05)
    verdict = pt.evaluate_gate(min_opportunities=50)
    assert verdict["passed"] is False
    assert "insufficient sample" in verdict["reason"]


def test_evaluate_gate_fails_on_thin_post_fee_edge():
    pt = ProfitTelemetry()
    for _ in range(60):
        pt.record_opportunity("m", "bundle_long", edge=0.001, fee_bps=20)
    verdict = pt.evaluate_gate(min_opportunities=10, min_avg_post_fee_edge=0.01)
    assert verdict["passed"] is False
    assert "below threshold" in verdict["reason"]


def test_invalid_inputs_are_ignored():
    pt = ProfitTelemetry()
    pt.record_opportunity("m1", "bundle_long", edge="bad")  # type: ignore[arg-type]
    pt.record_fill("m1", price="x", size=1, fee=0)  # type: ignore[arg-type]
    summary = pt.summary()
    assert summary["total_opportunities"] == 0
    assert summary["total_fills"] == 0


def test_per_strategy_fill_and_cancel_metrics():
    pt = ProfitTelemetry()
    pt.record_opportunity("m1", "taker_entry", edge=0.02)
    pt.record_fill(
        "m1",
        price=0.5,
        size=4,
        fee=0.02,
        strategy="taker_entry",
        expected_edge=0.02,
        realized_edge=0.015,
        quote_freshness="fresh",
    )
    pt.record_cancel("taker_entry")

    row = pt.summary()["per_strategy"]["taker_entry"]
    assert row["fills"] == 1.0
    assert row["cancels"] == 1.0
    assert row["cancel_to_fill_ratio"] == 1.0
    assert row["avg_expected_edge_at_fill"] == 0.02
    assert row["avg_realized_edge"] == 0.015


def test_markout_aggregation_by_horizon():
    pt = ProfitTelemetry()
    fill_ts = datetime.utcnow() - timedelta(seconds=40)
    pt.record_fill(
        "m1",
        price=0.5,
        size=1,
        fee=0.0,
        strategy="bundle_arb",
        token_type="yes",
        side="buy",
        timestamp=fill_ts,
    )
    pt.record_market_snapshot(
        market_id="m1",
        yes_price=0.55,
        no_price=0.45,
        timestamp=datetime.utcnow(),
    )

    row = pt.summary()["per_strategy"]["bundle_arb"]
    assert row["markout_count_1s"] == 1.0
    assert row["markout_count_5s"] == 1.0
    assert row["markout_count_30s"] == 1.0
    assert abs(row["markout_avg_1s"] - 0.05) < 1e-9
