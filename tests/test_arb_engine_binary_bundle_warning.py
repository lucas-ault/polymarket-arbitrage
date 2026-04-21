"""Tests for the polymarket.us-incompatibility warning on binary bundle arb.

The upstream repo (ImMike/polymarket-arbitrage) was designed for
polymarket.com (international), which exposes separate YES and NO
orderbooks. polymarket.us only has a YES book per binary market — NO is just
(1 - YES). Live diagnostics confirmed this empirically: 100% of bundle
scans returned synthesized NO books and zero opportunities.

These tests pin the operator-facing warning so any future change that
silently re-enables bundle arb against polymarket.us trips CI.
"""

from __future__ import annotations

import logging

import pytest

from core.arb_engine import ArbConfig, ArbEngine


def test_binary_bundle_arb_emits_warning_when_enabled(caplog):
    """If bundle_arb_enabled=True, ArbEngine must warn on initialization."""
    with caplog.at_level(logging.WARNING, logger="core.arb_engine"):
        ArbEngine(ArbConfig(bundle_arb_enabled=True))

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(
        "structurally impossible on polymarket.us" in r.message for r in warnings
    ), f"expected polymarket.us warning, got: {[r.message for r in warnings]}"


def test_binary_bundle_arb_silent_when_disabled(caplog):
    """The warning must NOT fire when the operator has correctly disabled it."""
    with caplog.at_level(logging.WARNING, logger="core.arb_engine"):
        ArbEngine(ArbConfig(bundle_arb_enabled=False))

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any(
        "structurally impossible" in r.message for r in warnings
    ), "warning should be suppressed when bundle arb is disabled"


def test_event_bundle_arb_flag_threads_through_config():
    """The event_bundle_arb_enabled config knob must reach ArbConfig."""
    cfg = ArbConfig(bundle_arb_enabled=False, event_bundle_arb_enabled=True)
    engine = ArbEngine(cfg)
    assert engine.config.event_bundle_arb_enabled is True
