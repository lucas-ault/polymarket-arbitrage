"""Diagnostic script: fetch one Polymarket orderbook and dump its raw shape.

When the bot reports 100% one-sided books and zero opportunities, run this on
the host that has POLYMARKET_KEY_ID/SECRET_KEY to see what the SDK or REST
endpoint is actually returning. Compare against the expected docs shape:
    {"marketData": {"bids": [{"px": {"value": "0.04"}, "qty": "5"}, ...],
                     "offers": [...]}}

If you see typed objects, our parser was silently dropping all data.
If you see empty bids/offers arrays, the underlying market has no resting
liquidity (and bundle arb is structurally impossible on it).

Usage:
    python scripts/debug_orderbook.py                       # uses default config + first liquid market
    python scripts/debug_orderbook.py SLUG                  # specific market slug
    python scripts/debug_orderbook.py SLUG -c config.live.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from polymarket_client.api import PolymarketClient  # noqa: E402
from utils.config_loader import load_config  # noqa: E402


def _summarize(payload, depth: int = 0, max_depth: int = 4) -> str:
    if depth >= max_depth:
        return f"<{type(payload).__name__}>"
    if payload is None or isinstance(payload, (bool, int, float)):
        return repr(payload)
    if isinstance(payload, str):
        return f"<str len={len(payload)}>: {payload[:60]!r}"
    if isinstance(payload, dict):
        return "{\n" + ",\n".join(
            f"{'  ' * (depth + 1)}{k!r}: {_summarize(v, depth + 1, max_depth)}"
            for k, v in payload.items()
        ) + f"\n{'  ' * depth}}}"
    if isinstance(payload, (list, tuple)):
        if not payload:
            return "[]"
        return (
            f"[len={len(payload)}, item0=" + _summarize(payload[0], depth + 1, max_depth) + "]"
        )
    attrs = [a for a in dir(payload) if not a.startswith("_")][:15]
    return f"<{type(payload).__name__} attrs={attrs}>"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("slug", nargs="?", help="Market slug (defaults to first discovered)")
    parser.add_argument("-c", "--config", default="config.live.yaml")
    parser.add_argument(
        "--no-sdk",
        action="store_true",
        help="Disable SDK path and use raw HTTP only (for comparison).",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    client = PolymarketClient(
        public_url=cfg.api.polymarket_public_url,
        private_url=cfg.api.polymarket_private_url,
        key_id=cfg.api.key_id,
        secret_key=cfg.api.secret_key,
        timeout=cfg.api.timeout_seconds,
        dry_run=False,
    )
    await client.connect()
    if args.no_sdk:
        # Force the REST path for an apples-to-apples comparison.
        client._sdk_client = None
        print("[debug] SDK path DISABLED; using raw HTTP\n")

    try:
        slug = args.slug
        if not slug:
            print("[debug] no slug provided — discovering first liquid market...")
            markets = await client.list_markets(
                {"active": True, "closed": False, "limit": 5}
            )
            if not markets:
                print("[debug] no markets returned!")
                return
            slug = markets[0].market_slug
            print(f"[debug] using slug={slug}: {markets[0].question[:80]}\n")

        # Raw fetch first (mimics what get_orderbook does internally).
        print("=" * 80)
        print(f"RAW SDK/HTTP RESPONSE for /v1/markets/{slug}/book")
        print("=" * 80)
        if client._sdk_client:
            raw = await client._sdk_client.markets.book(slug)
        else:
            raw = await client._request(
                "GET", f"/v1/markets/{slug}/book", use_private=False
            )
        print(f"Python type: {type(raw).__name__}")
        print(f"Shape:\n{_summarize(raw)}\n")

        # Now dump the parsed OrderBook the bot would actually use.
        print("=" * 80)
        print("PARSED OrderBook (what ArbEngine sees)")
        print("=" * 80)
        book = await client.get_orderbook(slug)
        print(f"market_id    : {book.market_id}")
        print(f"yes.bids     : {[(p.price, p.size) for p in book.yes.bids.levels]}")
        print(f"yes.asks     : {[(p.price, p.size) for p in book.yes.asks.levels]}")
        print(f"yes.synthetic: {book.yes.synthetic}")
        print(f"no.bids      : {[(p.price, p.size) for p in book.no.bids.levels]}")
        print(f"no.asks      : {[(p.price, p.size) for p in book.no.asks.levels]}")
        print(f"no.synthetic : {book.no.synthetic}")
        print(f"best_bid_yes : {book.best_bid_yes}")
        print(f"best_ask_yes : {book.best_ask_yes}")
        print(f"best_bid_no  : {book.best_bid_no}")
        print(f"best_ask_no  : {book.best_ask_no}")

        all_four = (
            book.best_bid_yes is not None
            and book.best_ask_yes is not None
            and book.best_bid_no is not None
            and book.best_ask_no is not None
        )
        print()
        if all_four and not book.no.synthetic:
            total_ask = book.best_ask_yes + book.best_ask_no
            total_bid = book.best_bid_yes + book.best_bid_no
            print(f"VERDICT: real 2-sided book")
            print(f"  total_ask = {total_ask:.4f} (gross_long_edge = {1 - total_ask:+.4f})")
            print(f"  total_bid = {total_bid:.4f} (gross_short_edge = {total_bid - 1:+.4f})")
        elif book.no.synthetic:
            print("VERDICT: NO side synthesized from YES (bundle arb impossible)")
        else:
            print("VERDICT: missing leg (parser couldn't get all 4 prices)")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
