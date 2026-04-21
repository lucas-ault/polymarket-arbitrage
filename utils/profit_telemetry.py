"""
Profit Telemetry
================

Lightweight in-memory aggregator that lets paper / live runs answer
"is this strategy profitable?" with data, instead of relying on the local
Portfolio's per-trade PnL alone.

The numbers it exports are designed to be consumed by:

* The dashboard (so operators can see expected vs realized edge in real time).
* The post-run summary log (so a paper-trading run produces a single
  human-readable verdict).
* Tests (so the structure of the export is stable).

This module is intentionally synchronous and side-effect free outside of its
own state. It does not write to disk; persistence belongs to the runtime that
owns it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class _StrategyAccumulator:
    opportunities: int = 0
    total_edge: float = 0.0
    total_post_fee_edge: float = 0.0
    edge_min: Optional[float] = None
    edge_max: Optional[float] = None
    last_seen: Optional[datetime] = None


@dataclass
class ProfitTelemetry:
    """Track per-strategy expected edge and aggregate fill statistics."""

    _per_strategy: dict[str, _StrategyAccumulator] = field(default_factory=dict)
    _total_opportunities: int = 0
    _total_fills: int = 0
    _total_fill_notional: float = 0.0
    _total_fees_paid: float = 0.0
    _started_at: datetime = field(default_factory=datetime.utcnow)

    def record_opportunity(
        self,
        market_id: str,
        strategy: str,
        edge: float,
        fee_bps: float = 0.0,
    ) -> None:
        """Record a detected opportunity and the per-unit edge it implied."""
        try:
            edge_value = float(edge)
        except (TypeError, ValueError):
            return
        # Express fees as a price-space haircut on edge. fee_bps is on notional
        # so a single round-trip taker order roughly costs fee_bps / 10000.
        fee_haircut = max(0.0, float(fee_bps) / 10_000.0)
        post_fee = edge_value - fee_haircut

        accum = self._per_strategy.setdefault(strategy, _StrategyAccumulator())
        accum.opportunities += 1
        accum.total_edge += edge_value
        accum.total_post_fee_edge += post_fee
        accum.edge_min = edge_value if accum.edge_min is None else min(accum.edge_min, edge_value)
        accum.edge_max = edge_value if accum.edge_max is None else max(accum.edge_max, edge_value)
        accum.last_seen = datetime.utcnow()
        self._total_opportunities += 1

    def record_fill(self, market_id: str, price: float, size: float, fee: float) -> None:
        """Record an executed (or simulated) fill for fill-rate / fee math."""
        try:
            notional = abs(float(price) * float(size))
            fee_value = float(fee)
        except (TypeError, ValueError):
            return
        self._total_fills += 1
        self._total_fill_notional += notional
        self._total_fees_paid += fee_value

    def fill_rate(self) -> float:
        """Fills per detected opportunity. Useful as a rough quality signal."""
        if self._total_opportunities == 0:
            return 0.0
        return self._total_fills / self._total_opportunities

    def evaluate_gate(
        self,
        *,
        min_opportunities: int = 50,
        min_avg_post_fee_edge: float = 0.005,
        min_fill_rate: float = 0.0,
    ) -> dict:
        """
        Decide whether the current sample looks profitable enough to advance
        to the next validation tier (e.g. paper -> tiny live -> scale).
        
        Returns a dict with ``passed`` plus per-strategy verdicts and the
        triggering reason when a gate fails. The thresholds are intentionally
        conservative defaults; operators should override them for the tier
        they are validating.
        """
        per_strategy: dict[str, dict] = {}
        any_strategy_passed = False
        worst_reason: str | None = None
        for strategy, accum in self._per_strategy.items():
            avg_post_fee_edge = (
                accum.total_post_fee_edge / accum.opportunities if accum.opportunities else 0.0
            )
            reason: str | None = None
            if accum.opportunities < min_opportunities:
                reason = (
                    f"insufficient sample ({accum.opportunities} < {min_opportunities})"
                )
            elif avg_post_fee_edge < min_avg_post_fee_edge:
                reason = (
                    f"avg post-fee edge {avg_post_fee_edge:.4f} below "
                    f"threshold {min_avg_post_fee_edge:.4f}"
                )
            per_strategy[strategy] = {
                "opportunities": accum.opportunities,
                "avg_post_fee_edge": avg_post_fee_edge,
                "passed": reason is None,
                "reason": reason,
            }
            if reason is None:
                any_strategy_passed = True
            elif worst_reason is None:
                worst_reason = f"{strategy}: {reason}"
        
        fill_rate = self.fill_rate()
        fill_rate_ok = fill_rate >= min_fill_rate
        passed = any_strategy_passed and fill_rate_ok
        if not fill_rate_ok and worst_reason is None:
            worst_reason = (
                f"fill rate {fill_rate:.2f} below threshold {min_fill_rate:.2f}"
            )
        return {
            "passed": passed,
            "fill_rate": fill_rate,
            "fill_rate_threshold": min_fill_rate,
            "min_opportunities_required": min_opportunities,
            "min_avg_post_fee_edge_required": min_avg_post_fee_edge,
            "per_strategy": per_strategy,
            "reason": None if passed else (worst_reason or "no strategy met thresholds"),
        }
    
    def summary(self) -> dict:
        """Return a JSON-friendly snapshot for dashboards / logs / tests."""
        per_strategy: dict[str, dict[str, float]] = {}
        for strategy, accum in self._per_strategy.items():
            avg_edge = accum.total_edge / accum.opportunities if accum.opportunities else 0.0
            avg_post_fee_edge = (
                accum.total_post_fee_edge / accum.opportunities if accum.opportunities else 0.0
            )
            per_strategy[strategy] = {
                "opportunities": float(accum.opportunities),
                "avg_edge": avg_edge,
                "avg_post_fee_edge": avg_post_fee_edge,
                "min_edge": float(accum.edge_min) if accum.edge_min is not None else 0.0,
                "max_edge": float(accum.edge_max) if accum.edge_max is not None else 0.0,
                "last_seen": accum.last_seen.isoformat() if accum.last_seen else "",
            }
        return {
            "started_at": self._started_at.isoformat(),
            "total_opportunities": self._total_opportunities,
            "total_fills": self._total_fills,
            "fill_rate": self.fill_rate(),
            "total_fill_notional": self._total_fill_notional,
            "total_fees_paid": self._total_fees_paid,
            "per_strategy": per_strategy,
        }
