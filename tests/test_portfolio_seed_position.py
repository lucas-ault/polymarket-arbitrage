"""
Tests for Portfolio.seed_position, used at startup to load existing exchange
positions so the per-market exposure cap and inventory-aware MM survive
restarts.
"""

from core.portfolio import Portfolio
from polymarket_client.models import TokenType


def test_seed_creates_position_with_size_and_avg_price():
    p = Portfolio(initial_balance=0.0)
    p.seed_position("m1", TokenType.YES, size=12.0, avg_entry_price=0.18)
    pos = p.get_position("m1", TokenType.YES)
    assert pos is not None
    assert pos.size == 12.0
    assert pos.avg_entry_price == 0.18
    assert pos.cost_basis == 12.0 * 0.18


def test_seed_overwrites_existing_seeded_position():
    p = Portfolio(initial_balance=0.0)
    p.seed_position("m1", TokenType.YES, size=5.0, avg_entry_price=0.10)
    p.seed_position("m1", TokenType.YES, size=20.0, avg_entry_price=0.25)
    pos = p.get_position("m1", TokenType.YES)
    assert pos.size == 20.0
    assert pos.avg_entry_price == 0.25


def test_seed_zero_size_is_a_no_op():
    p = Portfolio(initial_balance=0.0)
    p.seed_position("m1", TokenType.YES, size=0.0, avg_entry_price=0.5)
    assert p.get_position("m1", TokenType.YES) is None


def test_seed_does_not_count_as_a_trade():
    """Seeded positions came from a previous session; they must not double-
    count fees, win-rate, or trade count in this session's stats."""
    p = Portfolio(initial_balance=0.0)
    p.seed_position("m1", TokenType.YES, size=10.0, avg_entry_price=0.20)
    assert p.stats.total_trades == 0
    assert p.stats.total_volume == 0.0
    assert p.stats.total_fees_paid == 0.0
