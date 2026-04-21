"""
Tests for market matcher cache serialization.
"""

from core.cross_platform_arb import MarketMatcher


def test_market_matcher_export_import_round_trip():
    matcher = MarketMatcher(min_similarity=0.6)
    raw_pairs = [
        {
            "polymarket_id": "1",
            "kalshi_ticker": "KXTEST-1",
            "polymarket_question": "Will Team A win?",
            "kalshi_title": "Team A to win",
            "similarity_score": 0.91,
            "category": "sports",
            "matched_at": "2026-01-01T00:00:00",
        },
        {
            "polymarket_id": "2",
            "kalshi_ticker": "KXTEST-2",
            "polymarket_question": "Will inflation exceed 4%?",
            "kalshi_title": "Inflation above four percent",
            "similarity_score": 0.77,
            "category": "finance",
        },
    ]

    loaded = matcher.import_cached_pairs(raw_pairs)
    assert loaded == 2

    exported = matcher.export_cached_pairs()
    assert len(exported) == 2
    assert exported[0]["polymarket_id"] in {"1", "2"}
    assert exported[1]["kalshi_ticker"].startswith("KXTEST")
