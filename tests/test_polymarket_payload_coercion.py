"""Tests for `_coerce_to_dict` — the SDK pydantic-object normalizer.

The polymarket_us SDK can return either plain dicts (per docs) or typed
pydantic-style objects depending on version. The downstream parsers all rely
on dict.get() and isinstance(payload, dict) checks, so a typed object
silently produces an empty orderbook (the symptom we hit in production: 100%
one-sided books, zero opportunities).

These tests pin the normalizer so any regression that re-introduces the
SDK-typed-object problem fails CI.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List

from polymarket_client.api import PolymarketClient


def _coerce(payload: Any) -> Any:
    return PolymarketClient._coerce_to_dict(payload)


def test_plain_dict_passes_through_unchanged():
    payload = {"marketData": {"bids": [{"px": {"value": "0.04"}, "qty": "5"}]}}
    assert _coerce(payload) == payload


def test_pydantic_v2_model_dump_is_used():
    class FakeAmount:
        def __init__(self, value: str):
            self.value = value

        def model_dump(self):
            return {"value": self.value, "currency": "USD"}

    class FakeBookEntry:
        def __init__(self, px: FakeAmount, qty: str):
            self.px = px
            self.qty = qty

        def model_dump(self):
            return {"px": self.px.model_dump(), "qty": self.qty}

    class FakeMarketData:
        def __init__(self):
            self.marketSlug = "btc-100k-2026"
            self.bids = [FakeBookEntry(FakeAmount("0.04"), "5")]
            self.offers = [FakeBookEntry(FakeAmount("0.05"), "3")]

        def model_dump(self):
            return {
                "marketSlug": self.marketSlug,
                "bids": [b.model_dump() for b in self.bids],
                "offers": [o.model_dump() for o in self.offers],
            }

    class FakeBookResponse:
        def __init__(self):
            self.marketData = FakeMarketData()

        def model_dump(self):
            return {"marketData": self.marketData.model_dump()}

    coerced = _coerce(FakeBookResponse())

    assert isinstance(coerced, dict)
    assert "marketData" in coerced
    assert coerced["marketData"]["bids"][0]["px"]["value"] == "0.04"
    assert coerced["marketData"]["offers"][0]["qty"] == "3"


def test_pydantic_v1_dict_is_used():
    """Older pydantic models expose .dict() instead of .model_dump()."""

    class V1Model:
        def __init__(self):
            self.bids = [{"price": "0.10", "size": "1"}]

        def dict(self):
            return {"bids": self.bids}

    coerced = _coerce(V1Model())
    assert coerced == {"bids": [{"price": "0.10", "size": "1"}]}


def test_dataclass_falls_back_to_vars():
    """Plain dataclasses without serialization hooks fall through __dict__."""

    @dataclass
    class Entry:
        price: str
        size: str

    @dataclass
    class Book:
        bids: List[Entry]
        offers: List[Entry]

    book = Book(
        bids=[Entry("0.10", "5")],
        offers=[Entry("0.12", "3")],
    )
    coerced = _coerce(book)
    assert coerced == {
        "bids": [{"price": "0.10", "size": "5"}],
        "offers": [{"price": "0.12", "size": "3"}],
    }


def test_nested_lists_and_dicts_recurse():
    """Coercion must recurse into lists and dicts of typed objects."""

    class Inner:
        def __init__(self, x: int):
            self.x = x

        def model_dump(self):
            return {"x": self.x}

    payload = {"items": [Inner(1), Inner(2)], "meta": {"count": 2}}
    coerced = _coerce(payload)
    assert coerced == {"items": [{"x": 1}, {"x": 2}], "meta": {"count": 2}}


def test_primitives_pass_through():
    assert _coerce(None) is None
    assert _coerce(42) == 42
    assert _coerce(3.14) == 3.14
    assert _coerce("hello") == "hello"
    assert _coerce(True) is True
