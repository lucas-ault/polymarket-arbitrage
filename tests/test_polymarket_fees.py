from polymarket_client.api import PolymarketClient
from utils.polymarket_fees import bankers_round_cents, polymarket_fee


def test_bankers_rounding_examples():
    assert bankers_round_cents(0.025) == 0.02
    assert bankers_round_cents(0.035) == 0.04


def test_fee_schedule_examples_for_100_lot():
    # Taker at p=0.50: 0.05 * 100 * 0.5 * 0.5 = 1.25
    assert polymarket_fee(theta=0.05, contracts=100, price=0.50, round_to_cents=True) == 1.25
    # Maker at p=0.50: -0.0125 * 100 * 0.5 * 0.5 = -0.3125 -> -0.31
    assert polymarket_fee(theta=-0.0125, contracts=100, price=0.50, round_to_cents=True) == -0.31


def test_client_fee_estimator_defaults_to_exchange_rounding():
    client = PolymarketClient(dry_run=True)
    assert client.estimate_polymarket_us_fee(price=0.65, contracts=1000, is_maker=False) == 11.38
    assert client.estimate_polymarket_us_fee(price=0.65, contracts=1000, is_maker=True) == -2.84
