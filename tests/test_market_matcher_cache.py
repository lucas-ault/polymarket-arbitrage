"""
Tests for market matcher cache serialization.
"""

from core.cross_platform_arb import MarketMatcher
from kalshi_client.models import KalshiMarket
from polymarket_client.models import Market


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


def test_market_matcher_parallel_matching():
    matcher = MarketMatcher(min_similarity=0.4)
    polymarkets = [
        Market(market_id="p1", condition_id="c1", question="Will Bitcoin exceed 100k this year?", active=True),
        Market(market_id="p2", condition_id="c2", question="Will Team A beat Team B tonight?", active=True),
    ]
    kalshi_markets = [
        KalshiMarket(
            ticker="KXBTC-1",
            event_ticker="KXBTC",
            series_ticker="KX",
            title="Bitcoin above 100k by year end",
            status="open",
        ),
        KalshiMarket(
            ticker="KXSPRT-1",
            event_ticker="KXSPRT",
            series_ticker="KX",
            title="Team A vs Team B winner",
            status="open",
        ),
    ]

    matches = matcher.find_matches_parallel(
        polymarket_markets=polymarkets,
        kalshi_markets=kalshi_markets,
        process_workers=1,
        candidate_limit=50,
    )

    assert len(matches) >= 1
