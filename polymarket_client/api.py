"""
Polymarket US API Client
========================

Adapter that preserves the bot's internal interface while targeting
polymarket.us APIs and SDK semantics.
"""

import asyncio
import base64
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except Exception:  # pragma: no cover - optional for pure dry-run usage
    ed25519 = None

try:
    from polymarket_us import AsyncPolymarketUS
except Exception:  # pragma: no cover - optional import until dependency installed
    AsyncPolymarketUS = None

from polymarket_client.models import (
    Market,
    Order,
    OrderBook,
    OrderBookSide,
    OrderSide,
    OrderStatus,
    Position,
    PriceLevel,
    TokenOrderBook,
    TokenType,
    Trade,
)
from utils.cache_store import CacheStore, NoopCacheStore
from utils.polymarket_fees import polymarket_fee


logger = logging.getLogger(__name__)


MARKETS_CACHE_KEY = "pm:active_markets:v2"
MARKETS_CACHE_SCHEMA_VERSION = 2


class BasePolymarketClient(ABC):
    """Abstract base class for Polymarket client implementations."""

    @abstractmethod
    async def list_markets(self, filters: Optional[dict] = None) -> list[Market]:
        pass

    @abstractmethod
    async def get_market(self, market_id: str) -> Market:
        pass

    @abstractmethod
    async def get_orderbook(self, market_id: str) -> OrderBook:
        pass

    @abstractmethod
    async def stream_orderbook(self, market_ids: list[str]) -> AsyncIterator[tuple[str, OrderBook]]:
        pass

    @abstractmethod
    async def get_positions(self) -> dict[str, dict[TokenType, Position]]:
        pass

    @abstractmethod
    async def place_order(
        self,
        market_id: str,
        token_type: TokenType,
        side: OrderSide,
        price: float,
        size: float,
        strategy_tag: str = "",
        order_type: str = "ORDER_TYPE_LIMIT",
        time_in_force: str = "TIME_IN_FORCE_GOOD_TILL_CANCEL",
    ) -> Order:
        pass

    @abstractmethod
    async def close_position(
        self,
        market_id: str,
        *,
        slippage_ticks: Optional[int] = None,
        current_price: Optional[float] = None,
    ) -> dict[str, Any]:
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, market_slug: Optional[str] = None) -> None:
        pass

    @abstractmethod
    async def get_open_orders(self, market_id: Optional[str] = None) -> list[Order]:
        pass

    @abstractmethod
    async def get_trades(self, market_id: Optional[str] = None, limit: int = 100) -> list[Trade]:
        pass

    @abstractmethod
    async def get_account_balances(self) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_portfolio_metrics(self, activity_limit: int = 200) -> dict[str, Any]:
        pass


class PolymarketClient(BasePolymarketClient):
    """Polymarket US adapter with SDK-first, HTTP/WS fallback behavior."""

    def __init__(
        self,
        public_url: str = "https://gateway.polymarket.us",
        private_url: str = "https://api.polymarket.us",
        markets_ws_url: str = "wss://api.polymarket.us/v1/ws/markets",
        private_ws_url: str = "wss://api.polymarket.us/v1/ws/private",
        key_id: Optional[str] = None,
        secret_key: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        dry_run: bool = True,
        use_websocket: bool = True,
        use_rest_fallback: bool = True,
        cache_store: Optional[CacheStore] = None,
        markets_cache_ttl_seconds: int = 900,
        # Backward-compatible legacy kwargs.
        rest_url: Optional[str] = None,
        ws_url: Optional[str] = None,
        gamma_url: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        passphrase: Optional[str] = None,
        private_key: Optional[str] = None,
        **_: Any,
    ):
        self.public_url = (rest_url or gamma_url or public_url).rstrip("/")
        self.private_url = (private_url or self.public_url).rstrip("/")
        self.markets_ws_url = ws_url or markets_ws_url
        self.private_ws_url = private_ws_url
        self.key_id = key_id or api_key
        self.secret_key = secret_key or api_secret
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.dry_run = dry_run
        self.use_websocket = use_websocket
        self.use_rest_fallback = use_rest_fallback
        self.cache_store = cache_store or NoopCacheStore()
        self.markets_cache_ttl_seconds = max(1, markets_cache_ttl_seconds)

        self._http_client: Optional[httpx.AsyncClient] = None
        self._sdk_client: Optional[Any] = None

        self._simulated_orders: dict[str, Order] = {}
        self._simulated_positions: dict[str, dict[TokenType, Position]] = {}
        self._simulated_trades: list[Trade] = []
        self._markets_cache: dict[str, Market] = {}
        self._markets_by_slug: dict[str, Market] = {}

        self._runtime_stats: dict[str, float] = {
            "markets_discovered_valid": 0.0,
            "orderbook_updates_emitted": 0.0,
            "orderbook_rotations": 0.0,
            "ws_reconnects": 0.0,
            "cache_markets_hit": 0.0,
            "cache_markets_miss": 0.0,
            "cache_markets_writes": 0.0,
        }

    async def __aenter__(self) -> "PolymarketClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        await self.cache_store.connect()
        self._http_client = httpx.AsyncClient(
            timeout=self.timeout,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        if AsyncPolymarketUS:
            if self.key_id and self.secret_key:
                self._sdk_client = AsyncPolymarketUS(
                    key_id=self.key_id,
                    secret_key=self.secret_key,
                    timeout=self.timeout,
                )
            else:
                self._sdk_client = AsyncPolymarketUS(timeout=self.timeout)
        logger.info("Polymarket US client connected (dry_run=%s)", self.dry_run)

    async def disconnect(self) -> None:
        if self._sdk_client:
            try:
                await self._sdk_client.close()
            except Exception:
                pass
            self._sdk_client = None
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        await self.cache_store.close()

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        use_private: bool = False,
    ) -> Any:
        if not self._http_client:
            await self.connect()
        assert self._http_client is not None
        base = self.private_url if use_private else self.public_url
        url = f"{base}{endpoint}"
        headers = self._auth_headers(method, endpoint) if use_private else None

        for attempt in range(self.max_retries):
            try:
                response = await self._http_client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    headers=headers,
                )
                response.raise_for_status()
                if not response.content:
                    return {}
                return response.json()
            except (httpx.HTTPStatusError, httpx.RequestError):
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(self.retry_delay * (attempt + 1))

    def _auth_headers(self, method: str, path: str) -> dict[str, str]:
        if not self.key_id or not self.secret_key:
            raise RuntimeError("Live authenticated requests require api.key_id and api.secret_key")
        if ed25519 is None:
            raise RuntimeError("cryptography package is required for signed API requests")
        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method.upper()}{path}"
        secret_bytes = base64.b64decode(self.secret_key)[:32]
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_bytes)
        signature = base64.b64encode(private_key.sign(message.encode())).decode()
        return {
            "X-PM-Access-Key": self.key_id,
            "X-PM-Timestamp": timestamp,
            "X-PM-Signature": signature,
            "Content-Type": "application/json",
        }

    def _extract_items(self, payload: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in keys:
                value = payload.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def _to_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "on"}:
                return True
            if lowered in {"false", "0", "no", "off"}:
                return False
            return default
        if isinstance(value, (int, float)):
            return bool(value)
        return default

    @staticmethod
    def _parse_outcome_prices(raw: Any) -> tuple[float, float]:
        """Parse outcomePrices from string/list formats into YES/NO floats."""
        values: list[Any]
        if isinstance(raw, list):
            values = raw
        elif isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                values = parsed if isinstance(parsed, list) else []
            except (TypeError, ValueError, json.JSONDecodeError):
                values = []
        else:
            values = []
        yes_price = 0.0
        no_price = 0.0
        if len(values) >= 1:
            try:
                yes_price = float(values[0] or 0.0)
            except (TypeError, ValueError):
                yes_price = 0.0
        if len(values) >= 2:
            try:
                no_price = float(values[1] or 0.0)
            except (TypeError, ValueError):
                no_price = 0.0
        return yes_price, no_price

    def _parse_market(self, data: dict[str, Any]) -> Optional[Market]:
        slug = str(
            data.get("slug")
            or data.get("marketSlug")
            or data.get("market_slug")
            or data.get("id")
            or ""
        ).strip()
        if not slug:
            return None
        market_id = str(data.get("id") or slug)
        question = str(data.get("question") or data.get("title") or slug)
        yes_price, no_price = self._parse_outcome_prices(
            data.get("outcomePrices") or data.get("outcome_prices")
        )
        best_bid = self._amount_value(data.get("bestBid") or data.get("best_bid"))
        best_ask = self._amount_value(data.get("bestAsk") or data.get("best_ask"))
        market = Market(
            market_id=market_id,
            condition_id=str(data.get("conditionId") or data.get("condition_id") or slug),
            question=question,
            market_slug=slug,
            description=str(data.get("description") or ""),
            yes_token_id=str(data.get("yes_token_id") or ""),
            no_token_id=str(data.get("no_token_id") or ""),
            active=self._to_bool(data.get("active"), default=True),
            closed=self._to_bool(data.get("closed"), default=False),
            resolved=self._to_bool(data.get("resolved"), default=False),
            volume_24h=float(
                data.get("volume24hr")
                or data.get("volume24h")
                or data.get("volume_24h")
                or data.get("volumeNum")
                or data.get("volume")
                or 0.0
            ),
            liquidity=float(
                data.get("liquidityNum")
                or data.get("liquidity")
                or data.get("liquidityUsd")
                or 0.0
            ),
            best_bid=best_bid,
            best_ask=best_ask,
            yes_price=yes_price,
            no_price=no_price,
            category=str(data.get("category") or ""),
        )
        return market

    async def _hydrate_markets_cache(self) -> bool:
        payload = await self.cache_store.get_json(MARKETS_CACHE_KEY)
        if not payload or payload.get("schema_version") != MARKETS_CACHE_SCHEMA_VERSION:
            self._runtime_stats["cache_markets_miss"] += 1
            return False
        loaded = 0
        for item in payload.get("markets", []):
            market = self._parse_market(item)
            if not market:
                continue
            loaded += 1
            self._markets_cache[market.market_id] = market
            self._markets_by_slug[market.market_slug] = market
        if loaded:
            self._runtime_stats["cache_markets_hit"] += 1
            return True
        self._runtime_stats["cache_markets_miss"] += 1
        return False

    async def _persist_markets_cache(self, raw_markets: list[dict[str, Any]]) -> None:
        payload = {
            "schema_version": MARKETS_CACHE_SCHEMA_VERSION,
            "cached_at": datetime.utcnow().isoformat(),
            "market_count": len(raw_markets),
            "markets": raw_markets,
        }
        stored = await self.cache_store.set_json(
            MARKETS_CACHE_KEY,
            payload,
            ttl_seconds=self.markets_cache_ttl_seconds,
        )
        if stored:
            self._runtime_stats["cache_markets_writes"] += 1

    async def list_markets(
        self,
        filters: Optional[dict] = None,
        *,
        force_refresh: bool = False,
    ) -> list[Market]:
        params = filters.copy() if filters else {}
        params.setdefault("active", True)

        if force_refresh:
            # Drop in-memory and warm-cache snapshots so we definitely re-hit
            # the API. Used by background discovery refresh in the data feed.
            self._markets_cache.clear()
            self._markets_by_slug.clear()
        else:
            if self._markets_cache:
                return list(self._markets_cache.values())

            if await self._hydrate_markets_cache():
                return list(self._markets_cache.values())

        payload: Any
        if self._sdk_client:
            payload = await self._sdk_client.markets.list(params)
        else:
            payload = await self._request("GET", "/v1/markets", params=params, use_private=False)
        payload = self._coerce_to_dict(payload)
        items = self._extract_items(payload, ("markets", "data", "results"))
        if not items and isinstance(payload, dict):
            maybe_single = self._parse_market(payload)
            if maybe_single:
                items = [payload]

        markets: list[Market] = []
        for item in items:
            market = self._parse_market(item)
            if not market:
                continue
            markets.append(market)
            self._markets_cache[market.market_id] = market
            self._markets_by_slug[market.market_slug] = market

        self._runtime_stats["markets_discovered_valid"] = float(len(markets))
        if items:
            await self._persist_markets_cache(items)
        return markets

    async def get_market(self, market_id: str) -> Market:
        if market_id in self._markets_cache:
            return self._markets_cache[market_id]
        if market_id in self._markets_by_slug:
            return self._markets_by_slug[market_id]

        payload: Any
        if self._sdk_client:
            payload = await self._sdk_client.markets.retrieve_by_slug(market_id)
        else:
            payload = await self._request("GET", f"/v1/markets/{market_id}", use_private=False)
        payload = self._coerce_to_dict(payload)
        market = self._parse_market(payload)
        if not market:
            logger.warning(
                "Market lookup returned no metadata for target=%s; using synthetic market fallback",
                market_id,
            )
            market = Market(
                market_id=market_id,
                market_slug=market_id,
                condition_id=market_id,
                question=market_id,
            )
        self._markets_cache[market.market_id] = market
        self._markets_by_slug[market.market_slug] = market
        return market

    def _parse_price_levels(self, raw_levels: Any) -> list[PriceLevel]:
        levels: list[PriceLevel] = []
        if not isinstance(raw_levels, list):
            return levels
        for item in raw_levels[:15]:
            if isinstance(item, dict):
                px = item.get("price") or item.get("p") or item.get("px") or 0
                if isinstance(px, dict):
                    px = px.get("value") or px.get("amount") or 0
                qty = item.get("size") or item.get("quantity") or item.get("q") or item.get("qty") or 0
                if isinstance(qty, dict):
                    qty = qty.get("value") or qty.get("amount") or 0
                price = float(px or 0)
                size = float(qty or 0)
            elif isinstance(item, list) and len(item) >= 2:
                price = float(item[0])
                size = float(item[1])
            else:
                continue
            if price <= 0:
                continue
            levels.append(PriceLevel(price=price, size=max(size, 0.0)))
        return levels

    @staticmethod
    def _amount_value(raw: Any) -> float:
        """Parse USD amount fields that may be primitives or {value, currency} objects."""
        if isinstance(raw, dict):
            raw = raw.get("value") or raw.get("amount") or 0
        try:
            return float(raw or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _coerce_to_dict(cls, payload: Any) -> Any:
        """Convert SDK pydantic-like response objects to plain dicts/lists.

        The polymarket_us SDK has historically returned both plain dicts and
        typed pydantic models depending on version. The downstream parsers
        rely on dict.get() and isinstance(dict) checks, so a typed object
        silently produces empty orderbooks. This helper recursively unwraps
        anything that exposes pydantic-style serialization hooks.
        """
        if payload is None or isinstance(payload, (str, int, float, bool)):
            return payload
        if isinstance(payload, dict):
            return {key: cls._coerce_to_dict(value) for key, value in payload.items()}
        if isinstance(payload, (list, tuple)):
            return [cls._coerce_to_dict(item) for item in payload]
        # pydantic v2
        for attr in ("model_dump", "dict", "to_dict"):
            method = getattr(payload, attr, None)
            if callable(method):
                try:
                    return cls._coerce_to_dict(method())
                except Exception:
                    continue
        # last-resort: drop into __dict__ for typed objects without serializers
        if hasattr(payload, "__dict__"):
            try:
                return cls._coerce_to_dict(
                    {
                        k: v
                        for k, v in vars(payload).items()
                        if not k.startswith("_")
                    }
                )
            except Exception:
                return payload
        return payload

    @staticmethod
    def _summarize_payload_shape(payload: Any, depth: int = 0, max_depth: int = 3) -> str:
        """Produce a compact, log-safe summary of a response payload's shape.

        Used as a one-shot diagnostic so the operator can see what keys the
        SDK or REST endpoint actually returned without dumping potentially
        huge / sensitive payloads to the log.
        """
        if depth >= max_depth:
            return f"<{type(payload).__name__}>"
        if payload is None or isinstance(payload, (bool, int, float)):
            return f"<{type(payload).__name__}>"
        if isinstance(payload, str):
            return f"<str len={len(payload)}>"
        if isinstance(payload, dict):
            keys = list(payload.keys())[:8]
            inner = ", ".join(
                f"{k}={PolymarketClient._summarize_payload_shape(payload[k], depth + 1, max_depth)}"
                for k in keys
            )
            extra = "" if len(payload) <= 8 else f" (+{len(payload) - 8} more)"
            return "{" + inner + extra + "}"
        if isinstance(payload, (list, tuple)):
            head = (
                PolymarketClient._summarize_payload_shape(payload[0], depth + 1, max_depth)
                if payload
                else "empty"
            )
            return f"[len={len(payload)}, item0={head}]"
        # typed object: list non-private attrs
        attrs = [a for a in dir(payload) if not a.startswith("_")][:8]
        return f"<{type(payload).__name__} attrs={attrs}>"

    @staticmethod
    def _parse_timestamp(*raw_values: Any) -> datetime:
        """Parse API timestamps across ISO, epoch-ms, and epoch-sec shapes."""
        for raw in raw_values:
            if raw is None:
                continue
            if isinstance(raw, datetime):
                ts = raw
            elif isinstance(raw, (int, float)):
                value = float(raw)
                if value > 1_000_000_000_000:
                    value /= 1000.0
                try:
                    ts = datetime.utcfromtimestamp(value)
                except (OverflowError, OSError, ValueError):
                    continue
            elif isinstance(raw, str):
                text = raw.strip()
                if not text:
                    continue
                if text.isdigit():
                    value = float(text)
                    if value > 1_000_000_000_000:
                        value /= 1000.0
                    try:
                        ts = datetime.utcfromtimestamp(value)
                    except (OverflowError, OSError, ValueError):
                        continue
                else:
                    try:
                        ts = datetime.fromisoformat(text.replace("Z", "+00:00"))
                    except ValueError:
                        continue
            else:
                continue
            if ts.tzinfo is not None:
                ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
            return ts
        return datetime.utcnow()

    def _book_side(self, payload: dict[str, Any], prefix: str) -> TokenOrderBook:
        return TokenOrderBook(
            token_type=TokenType.YES if prefix == "yes" else TokenType.NO,
            bids=OrderBookSide(levels=self._parse_price_levels(payload.get(f"{prefix}_bids") or payload.get("bids"))),
            asks=OrderBookSide(levels=self._parse_price_levels(payload.get(f"{prefix}_asks") or payload.get("asks"))),
        )

    async def get_orderbook(self, market_id: str) -> OrderBook:
        market = await self.get_market(market_id)
        slug = market.market_slug or market.market_id

        payload: Any
        if self._sdk_client:
            payload = await self._sdk_client.markets.book(slug)
        else:
            payload = await self._request(
                "GET",
                f"/v1/markets/{slug}/book",
                use_private=False,
            )

        # One-shot diagnostic: dump the raw payload shape for the first few
        # orderbooks fetched. Without this we can't tell if the SDK is
        # returning typed objects (which would silently fail isinstance(dict)
        # checks below and produce empty books) or unexpected key names.
        # Auto-disables after 2 logs to avoid spam.
        if self._runtime_stats.get("orderbook_payload_samples_logged", 0.0) < 2.0:
            self._runtime_stats["orderbook_payload_samples_logged"] = (
                self._runtime_stats.get("orderbook_payload_samples_logged", 0.0) + 1.0
            )
            sample_repr = self._summarize_payload_shape(payload)
            logger.info(
                "Orderbook raw payload sample [slug=%s]: type=%s | shape=%s",
                slug,
                type(payload).__name__,
                sample_repr,
            )

        # Normalize SDK-returned typed objects (pydantic-like) to dicts so the
        # downstream key-based parsing works regardless of whether we hit the
        # SDK or raw HTTP. SDK responses look like dicts in the docs but some
        # versions return wrapped pydantic models — without this normalization
        # the entire orderbook data is silently discarded.
        payload = self._coerce_to_dict(payload)

        if isinstance(payload, dict):
            if "book" in payload and isinstance(payload["book"], dict):
                payload = payload["book"]
            elif "marketData" in payload and isinstance(payload["marketData"], dict):
                payload = payload["marketData"]
            elif "market_data" in payload and isinstance(payload["market_data"], dict):
                payload = payload["market_data"]
        payload = payload if isinstance(payload, dict) else {}

        # Try explicit YES/NO sides first.
        yes_book = TokenOrderBook(
            token_type=TokenType.YES,
            bids=OrderBookSide(levels=self._parse_price_levels(payload.get("yes_bids") or payload.get("long_bids"))),
            asks=OrderBookSide(levels=self._parse_price_levels(payload.get("yes_asks") or payload.get("long_asks"))),
        )
        no_book = TokenOrderBook(
            token_type=TokenType.NO,
            bids=OrderBookSide(levels=self._parse_price_levels(payload.get("no_bids") or payload.get("short_bids"))),
            asks=OrderBookSide(levels=self._parse_price_levels(payload.get("no_asks") or payload.get("short_asks"))),
        )

        # If the YES side is completely empty, the API likely returned a flat
        # one-sided shape (top-level "bids"/"asks") — populate YES from that.
        if not yes_book.bids.levels and not yes_book.asks.levels:
            bids = self._parse_price_levels(payload.get("bids"))
            asks = self._parse_price_levels(payload.get("asks") or payload.get("offers"))
            yes_book = TokenOrderBook(TokenType.YES, OrderBookSide(bids), OrderBookSide(asks))

        # If full book is unavailable, fall back to BBO for top-of-book prices.
        if not yes_book.bids.levels and not yes_book.asks.levels:
            try:
                bbo_payload = await self._request("GET", f"/v1/markets/{slug}/bbo", use_private=False)
                if isinstance(bbo_payload, dict):
                    bbo_data = (
                        bbo_payload.get("marketData")
                        or bbo_payload.get("market_data")
                        or bbo_payload
                    )
                else:
                    bbo_data = {}
                if isinstance(bbo_data, dict):
                    best_bid = self._amount_value(bbo_data.get("bestBid") or bbo_data.get("best_bid"))
                    best_ask = self._amount_value(bbo_data.get("bestAsk") or bbo_data.get("best_ask"))
                    bid_depth = float(bbo_data.get("bidDepth") or bbo_data.get("bid_depth") or 1)
                    ask_depth = float(bbo_data.get("askDepth") or bbo_data.get("ask_depth") or 1)
                    if best_bid > 0:
                        yes_book.bids.levels = [PriceLevel(price=best_bid, size=bid_depth)]
                    if best_ask > 0:
                        yes_book.asks.levels = [PriceLevel(price=best_ask, size=ask_depth)]
            except Exception as exc:
                logger.debug("BBO fallback failed for %s: %s", slug, exc)

        # Last resort: seed top-of-book from /v1/markets snapshot fields.
        if not yes_book.bids.levels and not yes_book.asks.levels:
            if market.best_bid > 0:
                yes_book.bids.levels = [PriceLevel(price=market.best_bid, size=1.0)]
            if market.best_ask > 0:
                yes_book.asks.levels = [PriceLevel(price=market.best_ask, size=1.0)]
            if not yes_book.bids.levels and not yes_book.asks.levels and market.yes_price > 0:
                bid = max(0.01, market.yes_price - 0.01)
                ask = min(0.99, market.yes_price + 0.01)
                yes_book.bids.levels = [PriceLevel(price=bid, size=1.0)]
                yes_book.asks.levels = [PriceLevel(price=ask, size=1.0)]

        # Synthesize NO from YES whenever the API didn't return a real NO book.
        # Polymarket binary markets share liquidity between the YES and NO
        # complement (price_NO = 1 - price_YES) and the API frequently returns
        # only one side. Without this synthesis, bundle arbitrage saw 100% of
        # markets as one-sided and skipped every scan. Mirroring is bid<->ask:
        # NO bids at (1 - YES asks), NO asks at (1 - YES bids).
        # We mark the synthesized side so bundle arbitrage knows to skip it
        # (synthesized YES+NO is forced to sum to ~1.0 and can't host an arb).
        if not no_book.bids.levels and not no_book.asks.levels:
            no_book = TokenOrderBook(
                TokenType.NO,
                OrderBookSide(
                    [
                        PriceLevel(price=max(0.01, 1.0 - level.price), size=level.size)
                        for level in yes_book.asks.levels
                    ]
                ),
                OrderBookSide(
                    [
                        PriceLevel(price=min(0.99, 1.0 - level.price), size=level.size)
                        for level in yes_book.bids.levels
                    ]
                ),
                synthetic=True,
            )

        return OrderBook(market_id=market.market_id, yes=yes_book, no=no_book, timestamp=datetime.utcnow())

    async def _stream_orderbooks_polling(
        self,
        market_ids: list[str],
        active_batch_size: int,
        markets_per_request_batch: int,
        batch_delay: float,
        rotation_delay: float,
    ) -> AsyncIterator[tuple[str, OrderBook]]:
        active_batch_size = max(1, active_batch_size)
        markets_per_request_batch = max(1, markets_per_request_batch)
        rotation_delay = max(0.2, rotation_delay)
        batch_delay = max(0.0, batch_delay)

        targets = market_ids.copy()
        if not targets:
            discovered = await self.list_markets({"active": True, "limit": 200})
            targets = [m.market_slug or m.market_id for m in discovered]

        offset = 0
        while True:
            batch = targets[offset : offset + active_batch_size]
            if not batch:
                offset = 0
                continue
            for idx in range(0, len(batch), markets_per_request_batch):
                group = batch[idx : idx + markets_per_request_batch]
                books = await asyncio.gather(*(self.get_orderbook(market_id) for market_id in group), return_exceptions=True)
                for market_id, book in zip(group, books):
                    if isinstance(book, Exception):
                        logger.warning("Orderbook polling error for %s: %s", market_id, book)
                        continue
                    self._runtime_stats["orderbook_updates_emitted"] += 1
                    yield book.market_id, book
                if batch_delay:
                    await asyncio.sleep(batch_delay)
            offset += active_batch_size
            if offset >= len(targets):
                offset = 0
                self._runtime_stats["orderbook_rotations"] += 1
            await asyncio.sleep(rotation_delay)

    @staticmethod
    def _chunked(items: list[str], chunk_size: int) -> list[list[str]]:
        """Split a list into fixed-size chunks."""
        chunk_size = max(1, int(chunk_size or 1))
        return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

    def _resolve_market_identifier(self, market_slug: str, market_id: Optional[str] = None) -> str:
        """Resolve a websocket market slug back to the canonical market id."""
        explicit_market_id = str(market_id or "").strip()
        if explicit_market_id:
            return explicit_market_id
        cached = self._markets_by_slug.get(market_slug)
        if cached is not None:
            return cached.market_id
        return market_slug

    def _synthesize_no_book(self, yes_book: TokenOrderBook, no_book: TokenOrderBook) -> TokenOrderBook:
        """Mirror YES quotes into the NO book when the exchange omits it."""
        if no_book.bids.levels or no_book.asks.levels:
            return no_book
        return TokenOrderBook(
            TokenType.NO,
            OrderBookSide(
                [
                    PriceLevel(price=max(0.01, 1.0 - level.price), size=level.size)
                    for level in yes_book.asks.levels
                ]
            ),
            OrderBookSide(
                [
                    PriceLevel(price=min(0.99, 1.0 - level.price), size=level.size)
                    for level in yes_book.bids.levels
                ]
            ),
            synthetic=True,
        )

    def _orderbook_from_ws_payload(self, data: dict[str, Any]) -> Optional[tuple[str, OrderBook]]:
        """Convert a markets websocket message into an OrderBook snapshot."""
        market_slug = str(data.get("marketSlug") or data.get("market_slug") or data.get("slug") or "").strip()
        if not market_slug:
            return None
        market_id = self._resolve_market_identifier(
            market_slug,
            data.get("marketId") or data.get("market_id"),
        )

        bids = self._parse_price_levels(data.get("bids"))
        asks = self._parse_price_levels(data.get("offers") or data.get("asks"))
        best_bid = data.get("bestBid") or data.get("best_bid")
        best_ask = data.get("bestAsk") or data.get("best_ask")
        if not bids and best_bid is not None:
            bids = self._parse_price_levels([{"px": best_bid, "qty": data.get("bidDepth") or data.get("bid_depth") or 1}])
        if not asks and best_ask is not None:
            asks = self._parse_price_levels([{"px": best_ask, "qty": data.get("askDepth") or data.get("ask_depth") or 1}])

        yes_book = TokenOrderBook(
            token_type=TokenType.YES,
            bids=OrderBookSide(levels=self._parse_price_levels(data.get("yes_bids")) or bids),
            asks=OrderBookSide(levels=self._parse_price_levels(data.get("yes_asks")) or asks),
        )
        no_book = TokenOrderBook(
            token_type=TokenType.NO,
            bids=OrderBookSide(levels=self._parse_price_levels(data.get("no_bids"))),
            asks=OrderBookSide(levels=self._parse_price_levels(data.get("no_asks"))),
        )
        no_book = self._synthesize_no_book(yes_book, no_book)
        return market_id, OrderBook(
            market_id=market_id,
            yes=yes_book,
            no=no_book,
            timestamp=datetime.utcnow(),
        )

    def _build_market_ws_subscriptions(self, market_slugs: list[str]) -> list[dict[str, Any]]:
        """Build documented camelCase subscription payloads for the markets websocket."""
        messages: list[dict[str, Any]] = []
        for index, chunk in enumerate(self._chunked(market_slugs, 100), start=1):
            messages.append(
                {
                    "subscribe": {
                        "requestId": f"mdl-{index}-{uuid.uuid4().hex[:8]}",
                        "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA_LITE",
                        "marketSlugs": chunk,
                    }
                }
            )
            messages.append(
                {
                    "subscribe": {
                        "requestId": f"trade-{index}-{uuid.uuid4().hex[:8]}",
                        "subscriptionType": "SUBSCRIPTION_TYPE_TRADE",
                        "marketSlugs": chunk,
                    }
                }
            )
        return messages

    def _build_market_ws_shards(
        self,
        market_slugs: list[str],
        *,
        max_markets_per_connection: int = 100,
    ) -> list[list[str]]:
        """Shard market subscriptions into doc-compliant websocket groups."""
        return self._chunked(market_slugs, max_markets_per_connection)

    def _build_private_ws_subscriptions(self) -> list[dict[str, Any]]:
        """Build private websocket subscriptions for order, position, and balance updates."""
        return [
            {
                "subscribe": {
                    "requestId": f"order-{uuid.uuid4().hex[:8]}",
                    "subscriptionType": "SUBSCRIPTION_TYPE_ORDER",
                    "marketSlugs": [],
                }
            },
            {
                "subscribe": {
                    "requestId": f"position-{uuid.uuid4().hex[:8]}",
                    "subscriptionType": "SUBSCRIPTION_TYPE_POSITION",
                    "marketSlugs": [],
                }
            },
            {
                "subscribe": {
                    "requestId": f"balance-{uuid.uuid4().hex[:8]}",
                    "subscriptionType": "SUBSCRIPTION_TYPE_ACCOUNT_BALANCE",
                }
            },
        ]

    def _trade_from_private_execution(self, update: dict[str, Any]) -> Optional[Trade]:
        """Convert a private websocket execution update into a Trade."""
        execution = update.get("execution") if isinstance(update, dict) else None
        if not isinstance(execution, dict):
            return None
        execution_type = str(execution.get("type") or "").upper()
        if "FILL" not in execution_type:
            return None

        order = execution.get("order") if isinstance(execution.get("order"), dict) else {}
        market_slug = str(order.get("marketSlug") or order.get("market_slug") or "").strip()
        market_id = self._resolve_market_identifier(
            market_slug,
            order.get("marketId") or order.get("market_id"),
        )
        intent = str(order.get("intent") or execution.get("intent") or "")
        token_type = TokenType.NO if "SHORT" in intent else TokenType.YES
        side = self._parse_side(order.get("side") or execution.get("side") or execution.get("direction"))
        price = self._amount_value(execution.get("lastPx") or order.get("price"))
        size = float(
            execution.get("lastShares")
            or execution.get("lastQty")
            or execution.get("lastQuantity")
            or execution.get("quantity")
            or 0.0
        )
        trade_id = str(execution.get("tradeId") or execution.get("trade_id") or execution.get("id") or "").strip()
        order_id = str(
            order.get("id")
            or order.get("orderId")
            or order.get("order_id")
            or execution.get("orderId")
            or execution.get("order_id")
            or ""
        ).strip()
        if not trade_id or not market_id or size <= 0 or price <= 0:
            return None
        timestamp = self._parse_timestamp(
            execution.get("tradeTime"),
            execution.get("updateTime"),
            execution.get("transactTime"),
            order.get("updateTime"),
            order.get("createTime"),
        )
        return Trade(
            trade_id=trade_id,
            order_id=order_id,
            market_id=market_id,
            market_slug=market_slug,
            token_type=token_type,
            side=side,
            price=price,
            size=size,
            fee=self._amount_value(execution.get("fee")),
            timestamp=timestamp,
        )

    def _parse_private_ws_event(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Normalize private websocket payloads into a small event envelope."""
        if not isinstance(payload, dict):
            return None
        if payload.get("heartbeat") is not None:
            return None

        snapshot = payload.get("orderSubscriptionSnapshot") or payload.get("order_subscription_snapshot")
        if isinstance(snapshot, dict):
            return {"type": "order_snapshot", "data": snapshot}

        update = payload.get("orderSubscriptionUpdate") or payload.get("order_subscription_update")
        if isinstance(update, dict):
            return {
                "type": "order_update",
                "data": update,
                "trade": self._trade_from_private_execution(update),
            }

        position = payload.get("positionSubscription") or payload.get("position_subscription")
        if isinstance(position, dict):
            return {"type": "position_update", "data": position}

        balances_snapshot = payload.get("accountBalancesSnapshot") or payload.get("account_balances_snapshot")
        if isinstance(balances_snapshot, dict):
            return {"type": "account_balance_snapshot", "data": balances_snapshot}

        balances_update = payload.get("accountBalancesUpdate") or payload.get("account_balances_update")
        if isinstance(balances_update, dict):
            return {"type": "account_balance_update", "data": balances_update}

        return None

    async def _stream_orderbooks_ws_shard(
        self,
        market_slugs: list[str],
        slug_to_market_id: dict[str, str],
        rotation_delay: float,
        shard_index: int,
        out_queue: asyncio.Queue[tuple[str, OrderBook]],
    ) -> None:
        reconnect_attempt = 0
        while True:
            try:
                headers = self._auth_headers("GET", "/v1/ws/markets")
                async with websockets.connect(
                    self.markets_ws_url,
                    additional_headers=list(headers.items()),
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    reconnect_attempt = 0
                    for message in self._build_market_ws_subscriptions(market_slugs):
                        await ws.send(json.dumps(message))
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(45.0, rotation_delay * 24))
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")
                        payload = json.loads(raw)
                        if payload.get("heartbeat") is not None:
                            continue
                        data = payload.get("marketData") or payload.get("marketDataLite")
                        if not isinstance(data, dict):
                            continue
                        parsed = self._orderbook_from_ws_payload(data)
                        if parsed is None:
                            continue
                        market_id, orderbook = parsed
                        orderbook.market_id = slug_to_market_id.get(market_id, market_id)
                        self._runtime_stats["orderbook_updates_emitted"] += 1
                        await out_queue.put((orderbook.market_id, orderbook))
            except asyncio.CancelledError:
                raise
            except ConnectionClosed:
                self._runtime_stats["ws_reconnects"] += 1
                reconnect_attempt += 1
                await asyncio.sleep(min(15.0, max(0.5, rotation_delay) * (2 ** min(reconnect_attempt, 4))))
            except asyncio.TimeoutError:
                logger.warning(
                    "Markets websocket shard %d idle for %.0fs; reconnecting",
                    shard_index,
                    max(45.0, rotation_delay * 24),
                )
                self._runtime_stats["ws_reconnects"] += 1
                reconnect_attempt += 1
                await asyncio.sleep(min(15.0, max(0.5, rotation_delay) * (2 ** min(reconnect_attempt, 4))))
            except Exception as exc:
                logger.warning("Markets websocket shard %d failure: %s", shard_index, exc)
                self._runtime_stats["ws_reconnects"] += 1
                reconnect_attempt += 1
                await asyncio.sleep(min(15.0, max(0.5, rotation_delay) * (2 ** min(reconnect_attempt, 4))))

    async def _stream_orderbooks_ws(
        self,
        market_slugs: list[str],
        slug_to_market_id: dict[str, str],
        rotation_delay: float,
    ) -> AsyncIterator[tuple[str, OrderBook]]:
        shards = self._build_market_ws_shards(market_slugs)
        if not shards:
            return
        logger.info("Starting %d markets websocket shard(s)", len(shards))
        queue: asyncio.Queue[tuple[str, OrderBook]] = asyncio.Queue(maxsize=max(1000, len(shards) * 200))
        tasks = [
            asyncio.create_task(
                self._stream_orderbooks_ws_shard(
                    shard,
                    slug_to_market_id,
                    rotation_delay,
                    index,
                    queue,
                ),
                name=f"pm_markets_ws_shard_{index}",
            )
            for index, shard in enumerate(shards, start=1)
        ]
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=max(90.0, rotation_delay * 48))
                except asyncio.TimeoutError:
                    logger.warning(
                        "All markets websocket shards were idle for %.0fs; falling back to REST polling",
                        max(90.0, rotation_delay * 48),
                    )
                    raise
                yield item
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _resolve_market_targets(self, targets: list[str]) -> tuple[list[str], dict[str, str]]:
        """Resolve requested targets to market slugs and slug->market_id mapping."""
        if not targets:
            markets = await self.list_markets({"active": True, "limit": 200})
            targets = [market.market_id for market in markets]

        slugs: list[str] = []
        slug_to_market_id: dict[str, str] = {}
        for target in targets:
            market = self._markets_cache.get(target) or self._markets_by_slug.get(target)
            if not market:
                try:
                    market = await self.get_market(target)
                except Exception:
                    continue
            slug = (market.market_slug or market.market_id or target).strip()
            if not slug:
                continue
            slug_to_market_id[slug] = market.market_id
            slugs.append(slug)
        # De-duplicate while preserving order.
        slugs = list(dict.fromkeys(slugs))
        return slugs, slug_to_market_id

    async def stream_private_updates(self) -> AsyncIterator[dict[str, Any]]:
        """Stream private order, position, and balance websocket updates."""
        if self.dry_run or not self.key_id or not self.secret_key:
            return
        while True:
            try:
                headers = self._auth_headers("GET", "/v1/ws/private")
                async with websockets.connect(
                    self.private_ws_url,
                    additional_headers=list(headers.items()),
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    for message in self._build_private_ws_subscriptions():
                        await ws.send(json.dumps(message))
                    while True:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")
                        payload = json.loads(raw)
                        event = self._parse_private_ws_event(payload)
                        if event is not None:
                            yield event
            except asyncio.CancelledError:
                raise
            except ConnectionClosed:
                await asyncio.sleep(1.0)
            except Exception as exc:
                logger.warning("Private websocket stream failure: %s", exc)
                await asyncio.sleep(1.0)

    async def stream_orderbook(
        self,
        market_ids: list[str],
        use_simulation: bool = False,
        active_batch_size: int = 500,
        markets_per_request_batch: int = 20,
        request_concurrency: int = 12,
        request_delay: float = 0.0,
        batch_delay: float = 0.15,
        rotation_delay: float = 1.25,
    ) -> AsyncIterator[tuple[str, OrderBook]]:
        del request_concurrency, request_delay  # Not used in US adapter (kept for compatibility).
        if use_simulation:
            async for item in self._stream_simulated_orderbooks(market_ids):
                yield item
            return

        targets = market_ids.copy()
        resolved_slugs, slug_to_market_id = await self._resolve_market_targets(targets)
        if not resolved_slugs:
            logger.warning("No valid market targets resolved for orderbook stream")
            return

        if self.use_websocket and self.key_id and self.secret_key:
            ws_failed = False
            ws_emitted = False
            try:
                async for item in self._stream_orderbooks_ws(
                    resolved_slugs,
                    slug_to_market_id,
                    rotation_delay,
                ):
                    ws_emitted = True
                    yield item
            except Exception:
                ws_failed = True
            # If websocket loop ended without exceptions and without emitting,
            # treat it as a soft failure and fall back to REST polling.
            if not ws_failed and ws_emitted:
                return
            logger.warning(
                "Websocket stream unavailable (failed=%s emitted=%s); switching to REST polling",
                ws_failed,
                ws_emitted,
            )

        async for item in self._stream_orderbooks_polling(
            resolved_slugs,
            active_batch_size=active_batch_size,
            markets_per_request_batch=markets_per_request_batch,
            batch_delay=batch_delay,
            rotation_delay=rotation_delay,
        ):
            yield item

    async def _stream_simulated_orderbooks(self, market_ids: list[str]) -> AsyncIterator[tuple[str, OrderBook]]:
        import random

        targets = market_ids or ["sim-market-1"]
        while True:
            batch = random.sample(targets, min(len(targets), 10))
            for market_id in batch:
                yes_mid = 0.50 + random.uniform(-0.20, 0.20)
                no_mid = max(0.01, min(0.99, 1.0 - yes_mid + random.uniform(-0.03, 0.03)))

                def _levels(mid: float, is_bid: bool) -> list[PriceLevel]:
                    levels: list[PriceLevel] = []
                    for i in range(5):
                        delta = 0.01 * (i + 1)
                        price = (mid - delta) if is_bid else (mid + delta)
                        price = max(0.01, min(0.99, round(price, 2)))
                        levels.append(PriceLevel(price=price, size=round(random.uniform(10, 150), 2)))
                    return levels

                yield (
                    market_id,
                    OrderBook(
                        market_id=market_id,
                        yes=TokenOrderBook(TokenType.YES, OrderBookSide(_levels(yes_mid, True)), OrderBookSide(_levels(yes_mid, False))),
                        no=TokenOrderBook(TokenType.NO, OrderBookSide(_levels(no_mid, True)), OrderBookSide(_levels(no_mid, False))),
                        timestamp=datetime.utcnow(),
                    ),
                )
            await asyncio.sleep(0.6)

    def _intent_from_order(self, token_type: TokenType, side: OrderSide) -> str:
        if side == OrderSide.BUY and token_type == TokenType.YES:
            return "ORDER_INTENT_BUY_LONG"
        if side == OrderSide.BUY and token_type == TokenType.NO:
            return "ORDER_INTENT_BUY_SHORT"
        if side == OrderSide.SELL and token_type == TokenType.YES:
            return "ORDER_INTENT_SELL_LONG"
        return "ORDER_INTENT_SELL_SHORT"

    @staticmethod
    def _normalize_order_type(value: str) -> str:
        key = str(value or "").strip().lower()
        aliases = {
            "limit": "ORDER_TYPE_LIMIT",
            "order_type_limit": "ORDER_TYPE_LIMIT",
            "market": "ORDER_TYPE_MARKET",
            "order_type_market": "ORDER_TYPE_MARKET",
        }
        return aliases.get(key, "ORDER_TYPE_LIMIT")

    @staticmethod
    def _normalize_time_in_force(value: str) -> str:
        key = str(value or "").strip().lower()
        aliases = {
            "gtc": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "good_till_cancel": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "time_in_force_good_till_cancel": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "ioc": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
            "immediate_or_cancel": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
            "time_in_force_immediate_or_cancel": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
            "fok": "TIME_IN_FORCE_FILL_OR_KILL",
            "fill_or_kill": "TIME_IN_FORCE_FILL_OR_KILL",
            "time_in_force_fill_or_kill": "TIME_IN_FORCE_FILL_OR_KILL",
            "gtd": "TIME_IN_FORCE_GOOD_TILL_DATE",
            "good_till_date": "TIME_IN_FORCE_GOOD_TILL_DATE",
            "time_in_force_good_till_date": "TIME_IN_FORCE_GOOD_TILL_DATE",
        }
        return aliases.get(key, "TIME_IN_FORCE_GOOD_TILL_CANCEL")

    async def get_positions(self) -> dict[str, dict[TokenType, Position]]:
        if self.dry_run:
            return self._simulated_positions.copy()
        try:
            positions: dict[str, dict[TokenType, Position]] = {}
            cursor: Optional[str] = None
            for _ in range(20):
                params: dict[str, Any] = {"limit": 200}
                if cursor:
                    params["cursor"] = cursor
                payload = await self._request("GET", "/v1/portfolio/positions", params=params, use_private=True)
                positions_map = payload.get("positions") if isinstance(payload, dict) else None
                if isinstance(positions_map, dict):
                    iterable = positions_map.items()
                else:
                    iterable = []
                for market_slug, item in iterable:
                    if not isinstance(item, dict):
                        continue
                    market_slug = str(market_slug or item.get("marketSlug") or item.get("slug") or "")
                    market_id = str(item.get("marketId") or item.get("market_id") or market_slug)
                    net = float(item.get("netPosition") or item.get("net_position") or 0.0)
                    if net == 0:
                        continue
                    token_type = TokenType.YES if net > 0 else TokenType.NO
                    positions.setdefault(market_id, {})[token_type] = Position(
                        market_id=market_id,
                        market_slug=market_slug,
                        token_type=token_type,
                        size=abs(net),
                        avg_entry_price=self._amount_value(item.get("cost")) / abs(net) if abs(net) > 0 else 0.0,
                        realized_pnl=self._amount_value(item.get("realized") or item.get("realizedPnl")),
                    )
                if not isinstance(payload, dict):
                    break
                if bool(payload.get("eof", True)):
                    break
                next_cursor = payload.get("nextCursor")
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = str(next_cursor)
            return positions
        except Exception as exc:
            logger.warning("Failed to fetch positions: %s", exc)
            return {}

    async def place_order(
        self,
        market_id: str,
        token_type: TokenType,
        side: OrderSide,
        price: float,
        size: float,
        strategy_tag: str = "",
        order_type: str = "ORDER_TYPE_LIMIT",
        time_in_force: str = "TIME_IN_FORCE_GOOD_TILL_CANCEL",
    ) -> Order:
        order_id = f"order_{uuid.uuid4().hex[:12]}"
        market = await self.get_market(market_id)
        order = Order(
            order_id=order_id,
            market_id=market.market_id,
            market_slug=market.market_slug,
            token_type=token_type,
            side=side,
            price=price,
            size=size,
            status=OrderStatus.OPEN,
            strategy_tag=strategy_tag,
            order_type=self._normalize_order_type(order_type),
            time_in_force=self._normalize_time_in_force(time_in_force),
        )
        if self.dry_run:
            self._simulated_orders[order_id] = order
            return order

        intent = self._intent_from_order(token_type, side)
        # API price.value always references YES-side price, even for SHORT intents.
        api_price = price
        if token_type == TokenType.NO:
            api_price = 1.0 - price
        api_price = max(0.01, min(0.99, api_price))
        # Polymarket US orders are denominated in whole contracts. The strategy
        # layer sometimes emits fractional sizes; round to nearest integer and
        # warn loudly when we drop more than 5% of the requested size so
        # operators are not silently trading less than they expected.
        rounded_quantity = int(max(1, round(size)))
        size_loss = abs(rounded_quantity - float(size))
        if float(size) > 0 and size_loss / float(size) > 0.05:
            logger.warning(
                "Order size %.4f rounded to %d contracts on %s (delta=%.4f)",
                float(size),
                rounded_quantity,
                market.market_slug or market.market_id,
                size_loss,
            )
        normalized_order_type = order.order_type
        normalized_tif = order.time_in_force
        payload = {
            "marketSlug": market.market_slug or market.market_id,
            "intent": intent,
            "type": normalized_order_type,
            "quantity": rounded_quantity,
            "tif": normalized_tif,
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
        }
        if normalized_order_type == "ORDER_TYPE_LIMIT":
            payload["price"] = {"value": f"{api_price:.4f}", "currency": "USD"}
        try:
            if self._sdk_client:
                response = await self._sdk_client.orders.create(payload)
            else:
                response = await self._request("POST", "/v1/orders", json_data=payload, use_private=True)
            returned_id = (
                response.get("id")
                or response.get("orderId")
                or response.get("order_id")
                or response.get("orderID")
                or order_id
            )
            order.order_id = str(returned_id)
            return order
        except Exception as exc:
            logger.error(
                "Failed to place order: %s | market_id=%s | market_slug=%s | intent=%s | "
                "type=%s | tif=%s | price=%.4f | size=%d",
                exc,
                market.market_id,
                market.market_slug or market.market_id,
                intent,
                normalized_order_type,
                normalized_tif,
                api_price,
                rounded_quantity,
            )
            order.status = OrderStatus.REJECTED
            raise

    async def cancel_order(self, order_id: str, market_slug: Optional[str] = None) -> None:
        if self.dry_run:
            if order_id in self._simulated_orders:
                self._simulated_orders[order_id].status = OrderStatus.CANCELLED
            return
        resolved_slug = (market_slug or "").strip()
        if not resolved_slug:
            maybe_order = await self._lookup_open_order(order_id)
            if maybe_order:
                resolved_slug = (maybe_order.market_slug or maybe_order.market_id or "").strip()
        if self._sdk_client:
            sdk_cancel = self._sdk_client.orders.cancel
            sdk_errors: list[Exception] = []
            # Support SDK variants where cancel expects either:
            #   cancel(order_id) OR cancel(order_id, params)
            cancel_arg_variants: list[tuple[Any, ...]] = []
            if resolved_slug:
                cancel_arg_variants.extend(
                    [
                        (order_id, {"symbol": resolved_slug}),
                        (order_id, {"marketSlug": resolved_slug}),
                        (order_id, {"symbol": resolved_slug, "marketSlug": resolved_slug}),
                    ]
                )
            cancel_arg_variants.extend(
                [
                    (order_id,),
                    (order_id, {}),
                ]
            )
            for args in cancel_arg_variants:
                try:
                    await sdk_cancel(*args)
                    return
                except Exception as exc:
                    sdk_errors.append(exc)
            if sdk_errors:
                logger.warning(
                    "SDK order cancel failed for %s (slug=%s); falling back to REST cancel: %s",
                    order_id,
                    resolved_slug or "<missing>",
                    sdk_errors[-1],
                )
        rest_bodies: list[dict[str, Any]] = []
        if resolved_slug:
            rest_bodies.extend(
                [
                    {"marketSlug": resolved_slug},
                    {"symbol": resolved_slug},
                    {"marketSlug": resolved_slug, "symbol": resolved_slug},
                ]
            )
        else:
            rest_bodies.append({})
        last_rest_error: Optional[Exception] = None
        for body in rest_bodies:
            try:
                await self._request(
                    "POST",
                    f"/v1/order/{order_id}/cancel",
                    json_data=body,
                    use_private=True,
                )
                return
            except Exception as exc:
                last_rest_error = exc
        if last_rest_error:
            raise last_rest_error

    async def close_position(
        self,
        market_id: str,
        *,
        slippage_ticks: Optional[int] = None,
        current_price: Optional[float] = None,
    ) -> dict[str, Any]:
        """Close an entire market position using the exchange close-position endpoint."""
        market = await self.get_market(market_id)
        market_slug = market.market_slug or market.market_id
        payload: dict[str, Any] = {"marketSlug": market_slug}
        if slippage_ticks is not None and slippage_ticks >= 0 and current_price is not None:
            bounded_price = max(0.01, min(0.99, float(current_price)))
            payload["slippageTolerance"] = {
                "currentPrice": {"value": f"{bounded_price:.4f}", "currency": "USD"},
                "ticks": int(slippage_ticks),
            }

        if self.dry_run:
            return {"status": "simulated", "marketSlug": market_slug}

        if self._sdk_client:
            close_fn = getattr(self._sdk_client.orders, "close_position", None)
            if close_fn is None:
                close_fn = getattr(self._sdk_client.orders, "closePosition", None)
            if close_fn is not None:
                return await close_fn(payload)

        response = await self._request(
            "POST",
            "/v1/order/close-position",
            json_data=payload,
            use_private=True,
        )
        return response if isinstance(response, dict) else {"response": response}

    async def cancel_all_orders(self, market_id: Optional[str] = None) -> int:
        orders = await self.get_open_orders(market_id)
        cancelled = 0
        for order in orders:
            try:
                await self.cancel_order(order.order_id, market_slug=order.market_slug)
                cancelled += 1
            except Exception as exc:
                logger.warning("Failed to cancel order %s: %s", order.order_id, exc)
        return cancelled

    async def _lookup_open_order(self, order_id: str) -> Optional[Order]:
        """Best-effort lookup for market slug when only order ID is known."""
        try:
            open_orders = await self.get_open_orders()
        except Exception:
            return None
        for order in open_orders:
            if order.order_id == order_id:
                return order
        return None

    def _parse_order_status(self, raw_status: Optional[str]) -> OrderStatus:
        status = (raw_status or "").lower().replace("order_state_", "")
        mapping = {
            "pending": OrderStatus.PENDING,
            "pending_new": OrderStatus.PENDING,
            "pending_risk": OrderStatus.PENDING,
            "open": OrderStatus.OPEN,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELLED,
            "cancelled": OrderStatus.CANCELLED,
            "replaced": OrderStatus.OPEN,
            "expired": OrderStatus.EXPIRED,
            "rejected": OrderStatus.REJECTED,
        }
        return mapping.get(status, OrderStatus.OPEN)

    def _parse_side(self, raw_side: Optional[str]) -> OrderSide:
        side = (raw_side or "").lower().replace("order_side_", "")
        if side in {"sell", "ask"}:
            return OrderSide.SELL
        return OrderSide.BUY

    async def get_open_orders(self, market_id: Optional[str] = None) -> list[Order]:
        if self.dry_run:
            return [
                order
                for order in self._simulated_orders.values()
                if order.is_open and (market_id is None or order.market_id == market_id)
            ]
        try:
            if self._sdk_client:
                payload = await self._sdk_client.orders.list()
            else:
                payload = await self._request("GET", "/v1/orders/open", use_private=True)
            items = self._extract_items(payload, ("orders", "data", "results"))
            parsed: list[Order] = []
            for item in items:
                market_slug = str(item.get("marketSlug") or item.get("market_slug") or "")
                resolved_market_id = str(item.get("marketId") or item.get("market_id") or market_slug)
                if market_id and resolved_market_id != market_id and market_slug != market_id:
                    continue
                intent = str(item.get("intent") or "")
                token_type = TokenType.NO if "SHORT" in intent else TokenType.YES
                parsed.append(
                    Order(
                        order_id=str(item.get("id") or item.get("orderId") or item.get("order_id") or ""),
                        market_id=resolved_market_id,
                        market_slug=market_slug,
                        token_type=token_type,
                        side=self._parse_side(item.get("side") or item.get("direction")),
                        price=float(item.get("price", {}).get("value") if isinstance(item.get("price"), dict) else item.get("price") or 0.0),
                        size=float(item.get("quantity") or item.get("size") or 0.0),
                        filled_size=float(item.get("filledQuantity") or item.get("filled_size") or 0.0),
                        status=self._parse_order_status(item.get("state") or item.get("status")),
                        order_type=str(item.get("type") or "ORDER_TYPE_LIMIT"),
                        time_in_force=str(item.get("tif") or "TIME_IN_FORCE_GOOD_TILL_CANCEL"),
                    )
                )
            return parsed
        except Exception as exc:
            logger.warning("Failed to fetch open orders: %s", exc)
            return []

    async def get_trades(self, market_id: Optional[str] = None, limit: int = 100) -> list[Trade]:
        if self.dry_run:
            trades = self._simulated_trades[-max(1, limit) :]
            if market_id:
                trades = [trade for trade in trades if trade.market_id == market_id]
            return trades
        try:
            trades: list[Trade] = []
            remaining = max(1, limit)
            cursor: Optional[str] = None
            page_limit = min(200, remaining)
            for _ in range(20):
                params: dict[str, Any] = {"limit": page_limit}
                if cursor:
                    params["cursor"] = cursor
                payload = await self._request("GET", "/v1/portfolio/activities", params=params, use_private=True)
                items = self._extract_items(payload, ("activities", "data", "results"))
                for item in items:
                    raw_type = str(item.get("type") or "").upper()
                    if raw_type not in {"ACTIVITY_TYPE_TRADE", "TRADE", "FILL", "ORDER_FILL"}:
                        continue
                    trade_obj = item.get("trade") if isinstance(item.get("trade"), dict) else item
                    market_slug = str(trade_obj.get("marketSlug") or trade_obj.get("market_slug") or "")
                    resolved_market_id = str(trade_obj.get("marketId") or trade_obj.get("market_id") or market_slug)
                    if market_id and resolved_market_id != market_id and market_slug != market_id:
                        continue
                    intent = str(trade_obj.get("intent") or "")
                    token_type = TokenType.NO if "SHORT" in intent else TokenType.YES
                    side = self._parse_side(trade_obj.get("side") or trade_obj.get("direction"))
                    price = float(
                        trade_obj.get("price", {}).get("value")
                        if isinstance(trade_obj.get("price"), dict)
                        else (trade_obj.get("price") or 0.0)
                    )
                    size = float(trade_obj.get("qty") or trade_obj.get("quantity") or trade_obj.get("size") or 0.0)
                    timestamp = self._parse_timestamp(
                        trade_obj.get("createTime"),
                        trade_obj.get("updateTime"),
                        trade_obj.get("createdAt"),
                        trade_obj.get("created_at"),
                        trade_obj.get("timestamp"),
                        trade_obj.get("time"),
                        item.get("createTime"),
                        item.get("updateTime"),
                        item.get("createdAt"),
                        item.get("created_at"),
                        item.get("timestamp"),
                        item.get("time"),
                    )
                    trade_id = str(
                        trade_obj.get("id")
                        or trade_obj.get("tradeId")
                        or trade_obj.get("trade_id")
                        or item.get("id")
                        or item.get("activityId")
                        or item.get("activity_id")
                        or ""
                    )
                    if not trade_id:
                        order_for_id = str(trade_obj.get("orderId") or trade_obj.get("order_id") or "unknown-order")
                        ts_ms = int(timestamp.timestamp() * 1000)
                        trade_id = (
                            f"{resolved_market_id}:{order_for_id}:{ts_ms}:"
                            f"{side.value}:{price:.6f}:{size:.6f}"
                        )
                    trades.append(
                        Trade(
                            trade_id=trade_id,
                            order_id=str(trade_obj.get("orderId") or trade_obj.get("order_id") or ""),
                            market_id=resolved_market_id,
                            market_slug=market_slug,
                            token_type=token_type,
                            side=side,
                            price=price,
                            size=size,
                            fee=self._amount_value(trade_obj.get("fee")),
                            timestamp=timestamp,
                        )
                    )
                    if len(trades) >= limit:
                        return trades[:limit]
                if not isinstance(payload, dict):
                    break
                if bool(payload.get("eof", True)):
                    break
                next_cursor = payload.get("nextCursor")
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = str(next_cursor)
                remaining = max(1, limit - len(trades))
                page_limit = min(200, remaining)
            return trades[:limit]
        except Exception as exc:
            logger.warning("Failed to fetch trades: %s", exc)
            return []

    async def get_account_balances(self) -> list[dict[str, Any]]:
        if self.dry_run:
            return []
        try:
            payload = await self._request("GET", "/v1/account/balances", use_private=True)
            items = self._extract_items(payload, ("balances", "data", "results"))
            return items
        except Exception as exc:
            logger.warning("Failed to fetch account balances: %s", exc)
            return []

    async def get_portfolio_metrics(self, activity_limit: int = 200) -> dict[str, Any]:
        """Return exchange-reported portfolio metrics for dashboard/risk sync."""
        if self.dry_run:
            return {}
        positions_payload: dict[str, Any] = {}
        balances: list[dict[str, Any]] = []
        activities_payload: dict[str, Any] = {}

        try:
            positions_payload = await self._request(
                "GET",
                "/v1/portfolio/positions",
                params={"limit": 200},
                use_private=True,
            )
        except Exception as exc:
            logger.warning("Failed to fetch portfolio positions for metrics: %s", exc)

        try:
            balances = await self.get_account_balances()
        except Exception:
            balances = []

        try:
            activities_payload = await self._request(
                "GET",
                "/v1/portfolio/activities",
                params={"limit": max(1, min(200, activity_limit))},
                use_private=True,
            )
        except Exception as exc:
            logger.warning("Failed to fetch portfolio activities for metrics: %s", exc)

        try:
            positions_map = (
                positions_payload.get("positions")
                if isinstance(positions_payload, dict)
                else {}
            )
            if not isinstance(positions_map, dict):
                positions_map = {}
            total_realized = 0.0
            total_unrealized = 0.0
            positions_cash_value_total = 0.0
            positions_count = 0
            markets_traded = 0
            for item in positions_map.values():
                if not isinstance(item, dict):
                    continue
                net = float(item.get("netPosition") or item.get("net_position") or 0.0)
                if net == 0:
                    continue
                markets_traded += 1
                positions_count += 1
                total_realized += self._amount_value(item.get("realized"))
                cash_value = self._amount_value(item.get("cashValue"))
                positions_cash_value_total += cash_value
                total_unrealized += cash_value

            activities = self._extract_items(activities_payload, ("activities", "data", "results"))
            total_trades = 0
            winning_trades = 0
            losing_trades = 0
            for activity in activities:
                activity_type = str(activity.get("type") or "")
                if activity_type != "ACTIVITY_TYPE_TRADE":
                    continue
                total_trades += 1
                trade_data = activity.get("trade") if isinstance(activity.get("trade"), dict) else {}
                realized = self._amount_value(trade_data.get("realizedPnl"))
                if realized > 0:
                    winning_trades += 1
                elif realized < 0:
                    losing_trades += 1

            primary_balance = balances[0] if balances else {}
            total_exposure = self._amount_value(primary_balance.get("assetNotional"))
            win_rate_den = winning_trades + losing_trades
            win_rate = (winning_trades / win_rate_den) if win_rate_den > 0 else 0.0

            if not positions_map and not activities and not balances:
                return {}

            return {
                "source": "exchange_portfolio_api",
                "pnl": {
                    "realized_pnl": total_realized,
                    # cashValue semantics are ambiguous across docs/surfaces; keep
                    # unrealized as local model responsibility to avoid false peaks.
                    "unrealized_pnl": total_unrealized,
                    "total_pnl": total_realized + total_unrealized,
                    "fees_paid": 0.0,
                    "net_pnl": total_realized + total_unrealized,
                },
                "positions_cash_value": positions_cash_value_total,
                "balances": {
                    "current_balance": self._amount_value(primary_balance.get("currentBalance")),
                    "buying_power": self._amount_value(primary_balance.get("buyingPower")),
                    "asset_notional": total_exposure,
                    "asset_available": self._amount_value(primary_balance.get("assetAvailable")),
                    "open_orders": self._amount_value(primary_balance.get("openOrders")),
                    "unsettled_funds": self._amount_value(primary_balance.get("unsettledFunds")),
                    "pending_credit": self._amount_value(primary_balance.get("pendingCredit")),
                    "margin_requirement": self._amount_value(primary_balance.get("marginRequirement")),
                    "currency": str(primary_balance.get("currency") or "USD"),
                    "last_updated": str(primary_balance.get("lastUpdated") or ""),
                },
                "total_exposure": total_exposure,
                "total_trades": total_trades,
                "win_rate": win_rate,
                "positions_count": positions_count,
                "markets_traded": markets_traded,
            }
        except Exception as exc:
            logger.warning("Failed to fetch portfolio metrics: %s", exc)
            return {}

    def estimate_polymarket_us_fee(
        self,
        price: float,
        contracts: float,
        is_maker: bool = False,
        round_to_cents: bool = True,
    ) -> float:
        theta = -0.0125 if is_maker else 0.05
        return polymarket_fee(theta=theta, contracts=contracts, price=price, round_to_cents=round_to_cents)

    def simulate_fill(self, order_id: str, fill_size: Optional[float] = None) -> Optional[Trade]:
        if order_id not in self._simulated_orders:
            return None
        order = self._simulated_orders[order_id]
        if not order.is_open:
            return None

        fill_size = fill_size or order.remaining_size
        fill_size = min(fill_size, order.remaining_size)
        fee = self.estimate_polymarket_us_fee(price=order.price, contracts=fill_size, is_maker=False)
        trade = Trade(
            trade_id=f"trade_{uuid.uuid4().hex[:12]}",
            order_id=order_id,
            market_id=order.market_id,
            market_slug=order.market_slug,
            token_type=order.token_type,
            side=order.side,
            price=order.price,
            size=fill_size,
            fee=fee,
        )
        order.filled_size += fill_size
        order.updated_at = datetime.utcnow()
        if order.remaining_size <= 0:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIALLY_FILLED

        self._update_simulated_position(trade)
        self._simulated_trades.append(trade)
        return trade

    def _update_simulated_position(self, trade: Trade) -> None:
        if trade.market_id not in self._simulated_positions:
            self._simulated_positions[trade.market_id] = {}
        if trade.token_type not in self._simulated_positions[trade.market_id]:
            self._simulated_positions[trade.market_id][trade.token_type] = Position(
                market_id=trade.market_id,
                market_slug=trade.market_slug,
                token_type=trade.token_type,
                size=0.0,
                avg_entry_price=0.0,
            )
        position = self._simulated_positions[trade.market_id][trade.token_type]
        if trade.side == OrderSide.BUY:
            new_size = position.size + trade.size
            if new_size > 0:
                position.avg_entry_price = ((position.avg_entry_price * position.size) + (trade.price * trade.size)) / new_size
            position.size = new_size
        else:
            if position.size > 0:
                position.realized_pnl += (trade.price - position.avg_entry_price) * trade.size
            position.size -= trade.size

    def get_runtime_stats(self) -> dict[str, float]:
        stats = self._runtime_stats.copy()
        cache_stats = self.cache_store.stats
        stats.update(
            {
                "cache_reads": float(cache_stats.reads),
                "cache_writes": float(cache_stats.writes),
                "cache_hits": float(cache_stats.hits),
                "cache_misses": float(cache_stats.misses),
                "cache_errors": float(cache_stats.errors),
                "cache_connected": 1.0 if cache_stats.connected else 0.0,
            }
        )
        return stats

