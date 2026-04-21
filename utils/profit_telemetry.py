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
from datetime import datetime, timedelta
from typing import Optional


@dataclass
class _StrategyAccumulator:
    opportunities: int = 0
    total_edge: float = 0.0
    total_post_fee_edge: float = 0.0
    edge_min: Optional[float] = None
    edge_max: Optional[float] = None
    last_seen: Optional[datetime] = None
    fills: int = 0
    cancels: int = 0
    total_realized_edge: float = 0.0
    realized_edge_count: int = 0
    total_expected_edge_at_fill: float = 0.0
    expected_edge_at_fill_count: int = 0
    fill_freshness_counts: dict[str, int] = field(default_factory=dict)
    markout_sums: dict[int, float] = field(default_factory=dict)
    markout_counts: dict[int, int] = field(default_factory=dict)


@dataclass
class _PendingMarkout:
    due_at: datetime
    strategy: str
    market_id: str
    token_type: str
    side: str
    fill_price: float
    horizon_seconds: int


@dataclass
class ProfitTelemetry:
    """Track per-strategy expected edge and aggregate fill statistics."""

    _per_strategy: dict[str, _StrategyAccumulator] = field(default_factory=dict)
    _total_opportunities: int = 0
    _total_fills: int = 0
    _total_fill_notional: float = 0.0
    _total_fees_paid: float = 0.0
    _total_cancels: int = 0
    _latest_opportunity_by_market: dict[str, dict] = field(default_factory=dict)
    _latest_mark_price_by_market: dict[str, dict[str, float]] = field(default_factory=dict)
    _pending_markouts: list[_PendingMarkout] = field(default_factory=list)
    _started_at: datetime = field(default_factory=datetime.utcnow)
    _markout_horizons: tuple[int, ...] = (1, 5, 30)

    def record_opportunity(
        self,
        market_id: str,
        strategy: str,
        edge: float,
        fee_bps: float = 0.0,
        quote_freshness: str = "unknown",
    ) -> None:
        """Record a detected opportunity and the per-unit edge it implied."""
        try:
            edge_value = float(edge)
        except (TypeError, ValueError):
            return
        strategy_name = str(strategy or "unknown")
        # Express fees as a price-space haircut on edge. fee_bps is on notional
        # so a single round-trip taker order roughly costs fee_bps / 10000.
        fee_haircut = max(0.0, float(fee_bps) / 10_000.0)
        post_fee = edge_value - fee_haircut

        accum = self._per_strategy.setdefault(strategy_name, _StrategyAccumulator())
        accum.opportunities += 1
        accum.total_edge += edge_value
        accum.total_post_fee_edge += post_fee
        accum.edge_min = edge_value if accum.edge_min is None else min(accum.edge_min, edge_value)
        accum.edge_max = edge_value if accum.edge_max is None else max(accum.edge_max, edge_value)
        accum.last_seen = datetime.utcnow()
        self._total_opportunities += 1
        self._latest_opportunity_by_market[market_id] = {
            "strategy": strategy_name,
            "edge": edge_value,
            "post_fee_edge": post_fee,
            "quote_freshness": str(quote_freshness or "unknown"),
            "timestamp": datetime.utcnow().timestamp(),
        }

    def _infer_latest_context(self, market_id: str) -> dict:
        return dict(self._latest_opportunity_by_market.get(market_id, {}))

    @staticmethod
    def _normalize_enum_value(value: object) -> str:
        if hasattr(value, "value"):
            return str(getattr(value, "value", "") or "").strip().lower()
        return str(value or "").strip().lower()

    def record_fill(
        self,
        market_id: str,
        price: float,
        size: float,
        fee: float,
        *,
        strategy: Optional[str] = None,
        expected_edge: Optional[float] = None,
        realized_edge: Optional[float] = None,
        quote_freshness: str = "unknown",
        token_type: Optional[object] = None,
        side: Optional[object] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Record an executed (or simulated) fill for fill-rate / fee math."""
        try:
            notional = abs(float(price) * float(size))
            fee_value = float(fee)
            fill_price = float(price)
        except (TypeError, ValueError):
            return
        inferred = self._infer_latest_context(market_id)
        strategy_name = str(strategy or inferred.get("strategy") or "unknown")
        expected_edge_value: Optional[float]
        try:
            expected_edge_value = (
                float(expected_edge)
                if expected_edge is not None
                else (
                    float(inferred.get("edge"))
                    if inferred.get("edge") is not None
                    else None
                )
            )
        except (TypeError, ValueError):
            expected_edge_value = None

        try:
            realized_edge_value = float(realized_edge) if realized_edge is not None else None
        except (TypeError, ValueError):
            realized_edge_value = None

        self._total_fills += 1
        self._total_fill_notional += notional
        self._total_fees_paid += fee_value
        accum = self._per_strategy.setdefault(strategy_name, _StrategyAccumulator())
        accum.fills += 1
        bucket = str(quote_freshness or inferred.get("quote_freshness") or "unknown")
        accum.fill_freshness_counts[bucket] = accum.fill_freshness_counts.get(bucket, 0) + 1
        if expected_edge_value is not None:
            accum.total_expected_edge_at_fill += expected_edge_value
            accum.expected_edge_at_fill_count += 1
        if realized_edge_value is not None:
            accum.total_realized_edge += realized_edge_value
            accum.realized_edge_count += 1

        side_name = self._normalize_enum_value(side)
        token_name = self._normalize_enum_value(token_type)
        if side_name in {"buy", "sell"} and token_name in {"yes", "no"}:
            fill_ts = timestamp or datetime.utcnow()
            for horizon in self._markout_horizons:
                self._pending_markouts.append(
                    _PendingMarkout(
                        due_at=fill_ts + timedelta(seconds=horizon),
                        strategy=strategy_name,
                        market_id=market_id,
                        token_type=token_name,
                        side=side_name,
                        fill_price=fill_price,
                        horizon_seconds=int(horizon),
                    )
                )
            self._flush_pending_markouts(as_of=fill_ts)

    def record_cancel(self, strategy: str) -> None:
        strategy_name = str(strategy or "unknown")
        accum = self._per_strategy.setdefault(strategy_name, _StrategyAccumulator())
        accum.cancels += 1
        self._total_cancels += 1

    def record_market_snapshot(
        self,
        *,
        market_id: str,
        yes_price: Optional[float],
        no_price: Optional[float],
        timestamp: Optional[datetime] = None,
    ) -> None:
        prices: dict[str, float] = {}
        try:
            if yes_price is not None:
                prices["yes"] = float(yes_price)
            if no_price is not None:
                prices["no"] = float(no_price)
        except (TypeError, ValueError):
            return
        if not prices:
            return
        self._latest_mark_price_by_market[market_id] = prices
        self._flush_pending_markouts(as_of=timestamp or datetime.utcnow())

    def _flush_pending_markouts(self, *, as_of: datetime) -> None:
        if not self._pending_markouts:
            return
        remaining: list[_PendingMarkout] = []
        for pending in self._pending_markouts:
            if pending.due_at > as_of:
                remaining.append(pending)
                continue
            latest = self._latest_mark_price_by_market.get(pending.market_id, {})
            mark_price = latest.get(pending.token_type)
            if mark_price is None:
                remaining.append(pending)
                continue
            direction = 1.0 if pending.side == "buy" else -1.0
            markout = (float(mark_price) - float(pending.fill_price)) * direction
            accum = self._per_strategy.setdefault(pending.strategy, _StrategyAccumulator())
            horizon = int(pending.horizon_seconds)
            accum.markout_sums[horizon] = accum.markout_sums.get(horizon, 0.0) + markout
            accum.markout_counts[horizon] = accum.markout_counts.get(horizon, 0) + 1
        self._pending_markouts = remaining

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
                "fills": accum.fills,
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
            fill_rate = (accum.fills / accum.opportunities) if accum.opportunities else 0.0
            avg_expected_edge_at_fill = (
                accum.total_expected_edge_at_fill / accum.expected_edge_at_fill_count
                if accum.expected_edge_at_fill_count
                else 0.0
            )
            avg_realized_edge = (
                accum.total_realized_edge / accum.realized_edge_count
                if accum.realized_edge_count
                else 0.0
            )
            markout_1s = (
                accum.markout_sums.get(1, 0.0) / accum.markout_counts.get(1, 1)
                if accum.markout_counts.get(1, 0) > 0
                else 0.0
            )
            markout_5s = (
                accum.markout_sums.get(5, 0.0) / accum.markout_counts.get(5, 1)
                if accum.markout_counts.get(5, 0) > 0
                else 0.0
            )
            markout_30s = (
                accum.markout_sums.get(30, 0.0) / accum.markout_counts.get(30, 1)
                if accum.markout_counts.get(30, 0) > 0
                else 0.0
            )
            per_strategy[strategy] = {
                "opportunities": float(accum.opportunities),
                "avg_edge": avg_edge,
                "avg_post_fee_edge": avg_post_fee_edge,
                "min_edge": float(accum.edge_min) if accum.edge_min is not None else 0.0,
                "max_edge": float(accum.edge_max) if accum.edge_max is not None else 0.0,
                "fills": float(accum.fills),
                "fill_rate": fill_rate,
                "cancels": float(accum.cancels),
                "cancel_to_fill_ratio": (
                    float(accum.cancels) / float(accum.fills)
                    if accum.fills > 0
                    else 0.0
                ),
                "avg_expected_edge_at_fill": avg_expected_edge_at_fill,
                "avg_realized_edge": avg_realized_edge,
                "avg_realized_minus_expected_edge": avg_realized_edge - avg_expected_edge_at_fill,
                "markout_avg_1s": markout_1s,
                "markout_avg_5s": markout_5s,
                "markout_avg_30s": markout_30s,
                "markout_count_1s": float(accum.markout_counts.get(1, 0)),
                "markout_count_5s": float(accum.markout_counts.get(5, 0)),
                "markout_count_30s": float(accum.markout_counts.get(30, 0)),
                "fill_freshness": dict(accum.fill_freshness_counts),
                "last_seen": accum.last_seen.isoformat() if accum.last_seen else "",
            }
        return {
            "started_at": self._started_at.isoformat(),
            "total_opportunities": self._total_opportunities,
            "total_fills": self._total_fills,
            "fill_rate": self.fill_rate(),
            "total_fill_notional": self._total_fill_notional,
            "total_fees_paid": self._total_fees_paid,
            "total_cancels": self._total_cancels,
            "pending_markouts": len(self._pending_markouts),
            "per_strategy": per_strategy,
        }
