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
from datetime import datetime
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
    ) -> Order:
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        pass

    @abstractmethod
    async def get_open_orders(self, market_id: Optional[str] = None) -> list[Order]:
        pass

    @abstractmethod
    async def get_trades(self, market_id: Optional[str] = None, limit: int = 100) -> list[Trade]:
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
        market = Market(
            market_id=market_id,
            condition_id=str(data.get("conditionId") or data.get("condition_id") or slug),
            question=question,
            market_slug=slug,
            description=str(data.get("description") or ""),
            yes_token_id=str(data.get("yes_token_id") or ""),
            no_token_id=str(data.get("no_token_id") or ""),
            active=bool(data.get("active", True)),
            closed=bool(data.get("closed", False)),
            resolved=bool(data.get("resolved", False)),
            volume_24h=float(data.get("volume24h") or data.get("volume_24h") or 0.0),
            liquidity=float(data.get("liquidity") or data.get("liquidityUsd") or 0.0),
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

    async def list_markets(self, filters: Optional[dict] = None) -> list[Market]:
        params = filters.copy() if filters else {}
        params.setdefault("active", True)

        if self._markets_cache:
            return list(self._markets_cache.values())

        if await self._hydrate_markets_cache():
            return list(self._markets_cache.values())

        payload: Any
        if self._sdk_client:
            payload = await self._sdk_client.markets.list(params)
        else:
            payload = await self._request("GET", "/v1/markets", params=params, use_private=False)
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
        market = self._parse_market(payload)
        if not market:
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

        # If API returns only one side (best bid/ask), synthesize complements.
        if not yes_book.bids.levels and not yes_book.asks.levels:
            bids = self._parse_price_levels(payload.get("bids"))
            asks = self._parse_price_levels(payload.get("asks") or payload.get("offers"))
            yes_book = TokenOrderBook(TokenType.YES, OrderBookSide(bids), OrderBookSide(asks))
            no_book = TokenOrderBook(
                TokenType.NO,
                OrderBookSide([PriceLevel(price=max(0.01, 1.0 - level.price), size=level.size) for level in asks]),
                OrderBookSide([PriceLevel(price=min(0.99, 1.0 - level.price), size=level.size) for level in bids]),
            )

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
                    if yes_book.bids.levels or yes_book.asks.levels:
                        no_book = TokenOrderBook(
                            TokenType.NO,
                            OrderBookSide(
                                [PriceLevel(price=max(0.01, 1.0 - level.price), size=level.size) for level in yes_book.asks.levels]
                            ),
                            OrderBookSide(
                                [PriceLevel(price=min(0.99, 1.0 - level.price), size=level.size) for level in yes_book.bids.levels]
                            ),
                        )
            except Exception as exc:
                logger.debug("BBO fallback failed for %s: %s", slug, exc)

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

    async def _stream_orderbooks_ws(
        self,
        market_slugs: list[str],
        slug_to_market_id: dict[str, str],
        rotation_delay: float,
    ) -> AsyncIterator[tuple[str, OrderBook]]:
        while True:
            try:
                headers = self._auth_headers("GET", "/v1/ws/markets")
                async with websockets.connect(
                    self.markets_ws_url,
                    additional_headers=list(headers.items()),
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    request_id = f"markets-{uuid.uuid4().hex[:10]}"
                    subscribe_msg = {
                        "subscribe": {
                            # Use snake_case + numeric enum for broad compatibility.
                            "request_id": request_id,
                            "subscription_type": 1,
                            "market_slugs": market_slugs,
                        }
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(8.0, rotation_delay * 6))
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")
                        payload = json.loads(raw)
                        if payload.get("heartbeat") is not None:
                            continue
                        data = (
                            payload.get("marketData")
                            or payload.get("marketDataLite")
                            or payload.get("market_data_lite")
                            or payload.get("market_data")
                            or payload.get("market_subscription_snapshot")
                        )
                        if not isinstance(data, dict):
                            continue
                        market_slug = str(data.get("marketSlug") or data.get("market_slug") or data.get("slug") or "")
                        if not market_slug:
                            continue
                        market_id = slug_to_market_id.get(market_slug, market_slug)

                        bids = self._parse_price_levels(data.get("bids"))
                        asks = self._parse_price_levels(data.get("asks") or data.get("offers"))
                        best_bid = data.get("bestBid")
                        best_ask = data.get("bestAsk")
                        if not bids and isinstance(best_bid, dict):
                            bids = self._parse_price_levels([{"px": best_bid, "qty": data.get("bidDepth") or 1}])
                        if not asks and isinstance(best_ask, dict):
                            asks = self._parse_price_levels([{"px": best_ask, "qty": data.get("askDepth") or 1}])

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
                        orderbook = OrderBook(
                            market_id=market_id,
                            yes=yes_book,
                            no=no_book,
                            timestamp=datetime.utcnow(),
                        )
                        self._runtime_stats["orderbook_updates_emitted"] += 1
                        yield market_id, orderbook
            except asyncio.CancelledError:
                raise
            except ConnectionClosed:
                self._runtime_stats["ws_reconnects"] += 1
                await asyncio.sleep(max(0.5, rotation_delay))
            except asyncio.TimeoutError:
                logger.warning("Markets websocket connected but received no data; falling back to REST polling")
                self._runtime_stats["ws_reconnects"] += 1
                if self.use_rest_fallback:
                    break
                await asyncio.sleep(max(0.5, rotation_delay))
            except Exception as exc:
                logger.warning("Markets websocket stream failure: %s", exc)
                self._runtime_stats["ws_reconnects"] += 1
                if not self.use_rest_fallback:
                    await asyncio.sleep(max(0.5, rotation_delay))
                else:
                    break

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

    async def get_positions(self) -> dict[str, dict[TokenType, Position]]:
        if self.dry_run:
            return self._simulated_positions.copy()
        try:
            if self._sdk_client:
                payload = await self._sdk_client.portfolio.positions()
            else:
                payload = await self._request("GET", "/v1/portfolio/positions", use_private=True)
            positions: dict[str, dict[TokenType, Position]] = {}
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
        payload = {
            "marketSlug": market.market_slug or market.market_id,
            "intent": intent,
            "type": "ORDER_TYPE_LIMIT",
            "price": {"value": f"{api_price:.4f}", "currency": "USD"},
            "quantity": int(max(1, round(size))),
            "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
        }
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
            logger.error("Failed to place order: %s", exc)
            order.status = OrderStatus.REJECTED
            raise

    async def cancel_order(self, order_id: str) -> None:
        if self.dry_run:
            if order_id in self._simulated_orders:
                self._simulated_orders[order_id].status = OrderStatus.CANCELLED
            return
        if self._sdk_client:
            await self._sdk_client.orders.cancel(order_id)
            return
        await self._request("POST", f"/v1/order/{order_id}/cancel", use_private=True)

    async def cancel_all_orders(self, market_id: Optional[str] = None) -> int:
        orders = await self.get_open_orders(market_id)
        cancelled = 0
        for order in orders:
            try:
                await self.cancel_order(order.order_id)
                cancelled += 1
            except Exception as exc:
                logger.warning("Failed to cancel order %s: %s", order.order_id, exc)
        return cancelled

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
            if self._sdk_client:
                payload = await self._sdk_client.portfolio.activities({"limit": limit})
            else:
                payload = await self._request("GET", "/v1/portfolio/activities", params={"limit": limit}, use_private=True)
            items = self._extract_items(payload, ("activities", "data", "results"))
            trades: list[Trade] = []
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
                trades.append(
                    Trade(
                        trade_id=str(trade_obj.get("id") or trade_obj.get("tradeId") or trade_obj.get("trade_id") or ""),
                        order_id=str(trade_obj.get("orderId") or trade_obj.get("order_id") or ""),
                        market_id=resolved_market_id,
                        market_slug=market_slug,
                        token_type=token_type,
                        side=self._parse_side(trade_obj.get("side") or trade_obj.get("direction")),
                        price=float(
                            trade_obj.get("price", {}).get("value")
                            if isinstance(trade_obj.get("price"), dict)
                            else (trade_obj.get("price") or 0.0)
                        ),
                        size=float(trade_obj.get("qty") or trade_obj.get("quantity") or trade_obj.get("size") or 0.0),
                        fee=self._amount_value(trade_obj.get("fee")),
                    )
                )
            return trades[-max(1, limit) :]
        except Exception as exc:
            logger.warning("Failed to fetch trades: %s", exc)
            return []

    def estimate_polymarket_us_fee(self, price: float, contracts: float, is_maker: bool = False) -> float:
        theta = -0.0125 if is_maker else 0.05
        return theta * max(0.0, contracts) * max(0.0, min(1.0, price)) * (1.0 - max(0.0, min(1.0, price)))

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

