"""Tests for the post-fee ProfitTelemetry aggregator."""

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
