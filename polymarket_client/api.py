"""
Polymarket API Client
======================

Abstracted client for Polymarket REST and WebSocket APIs.
Designed to be easily pluggable with real API implementations.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, AsyncIterator, Optional

import httpx
import websockets
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OpenOrderParams, OrderArgs, TradeParams
from websockets.exceptions import ConnectionClosed

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

MARKETS_CACHE_KEY = "pm:active_markets:v1"
MARKETS_CACHE_SCHEMA_VERSION = 1
INVALID_TOKEN_TTL_SECONDS = 1800


class BasePolymarketClient(ABC):
    """Abstract base class for Polymarket client implementations."""
    
    @abstractmethod
    async def list_markets(self, filters: Optional[dict] = None) -> list[Market]:
        """Fetch list of available markets."""
        pass
    
    @abstractmethod
    async def get_market(self, market_id: str) -> Market:
        """Get details for a specific market."""
        pass
    
    @abstractmethod
    async def get_orderbook(self, market_id: str) -> OrderBook:
        """Fetch current order book for a market."""
        pass
    
    @abstractmethod
    async def stream_orderbook(self, market_ids: list[str]) -> AsyncIterator[tuple[str, OrderBook]]:
        """Stream order book updates for multiple markets."""
        pass
    
    @abstractmethod
    async def get_positions(self) -> dict[str, dict[TokenType, Position]]:
        """Get all current positions."""
        pass
    
    @abstractmethod
    async def place_order(
        self,
        market_id: str,
        token_type: TokenType,
        side: OrderSide,
        price: float,
        size: float,
        strategy_tag: str = ""
    ) -> Order:
        """Place a limit order."""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        """Cancel an open order."""
        pass
    
    @abstractmethod
    async def get_open_orders(self, market_id: Optional[str] = None) -> list[Order]:
        """Get all open orders, optionally filtered by market."""
        pass
    
    @abstractmethod
    async def get_trades(self, market_id: Optional[str] = None, limit: int = 100) -> list[Trade]:
        """Get recent trades."""
        pass


class PolymarketClient(BasePolymarketClient):
    """
    Polymarket API client implementation.
    
    This implementation provides the structure for real API integration.
    Currently uses placeholder implementations that can be replaced with
    actual Polymarket CLOB API calls.
    """
    
    def __init__(
        self,
        rest_url: str = "https://clob.polymarket.com",
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        gamma_url: str = "https://gamma-api.polymarket.com",
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        passphrase: Optional[str] = None,
        private_key: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        dry_run: bool = True,
        clob_chain_id: int = 137,
        clob_signature_type: int = 0,
        clob_funder_address: Optional[str] = None,
        clob_api_key_nonce: Optional[int] = None,
        cache_store: Optional[CacheStore] = None,
        markets_cache_ttl_seconds: int = 900,
    ):
        self.rest_url = rest_url.rstrip("/")
        self.ws_url = ws_url
        self.gamma_url = gamma_url.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.private_key = private_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.dry_run = dry_run
        self.clob_chain_id = clob_chain_id
        self.clob_signature_type = clob_signature_type
        self.clob_funder_address = clob_funder_address or ""
        self.clob_api_key_nonce = clob_api_key_nonce
        self.cache_store = cache_store or NoopCacheStore()
        self.markets_cache_ttl_seconds = max(1, markets_cache_ttl_seconds)
        
        # HTTP client
        self._http_client: Optional[httpx.AsyncClient] = None
        
        # WebSocket connection
        self._ws_connection = None
        self._ws_subscriptions: set[str] = set()
        self._clob_client: Optional[ClobClient] = None
        
        # Simulated state for dry run
        self._simulated_orders: dict[str, Order] = {}
        self._simulated_positions: dict[str, dict[TokenType, Position]] = {}
        self._simulated_trades: list[Trade] = []
        
        # Cache for market data (avoids re-fetching)
        self._markets_cache: dict[str, Market] = {}
        
        # Runtime metrics for observability/perf tuning
        self._runtime_stats: dict[str, float] = {
            "market_discovery_batches": 0.0,
            "market_discovery_duration_ms": 0.0,
            "markets_discovered_valid": 0.0,
            "orderbook_updates_emitted": 0.0,
            "orderbook_rotations": 0.0,
            "last_rotation_duration_ms": 0.0,
            "avg_rotation_duration_ms": 0.0,
            "max_rotation_duration_ms": 0.0,
            "active_batch_size": 0.0,
            "markets_per_request_batch": 0.0,
            "request_concurrency": 0.0,
            "cache_markets_hit": 0.0,
            "cache_markets_miss": 0.0,
            "cache_markets_writes": 0.0,
            "orderbook_invalid_token_skips": 0.0,
            "orderbook_invalid_token_marks": 0.0,
        }
        self._markets_refresh_task: Optional[asyncio.Task] = None
        self._invalid_token_ids: dict[str, float] = {}
        self._priority_market_ids: list[str] = []
        
    async def __aenter__(self) -> "PolymarketClient":
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()
    
    async def connect(self) -> None:
        """Initialize HTTP client."""
        await self.cache_store.connect()
        self._http_client = httpx.AsyncClient(
            timeout=self.timeout,
            headers=self._get_headers(),
        )
        logger.info(f"Polymarket client connected (dry_run={self.dry_run})")
    
    async def disconnect(self) -> None:
        """Close connections."""
        if self._markets_refresh_task:
            self._markets_refresh_task.cancel()
            try:
                await self._markets_refresh_task
            except asyncio.CancelledError:
                pass
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        if self._ws_connection:
            await self._ws_connection.close()
            self._ws_connection = None
        self._clob_client = None
        await self.cache_store.close()
        logger.info("Polymarket client disconnected")

    async def _hydrate_markets_cache(self) -> bool:
        """Load market snapshot from external cache, if available."""
        payload = await self.cache_store.get_json(MARKETS_CACHE_KEY)
        if not payload:
            self._runtime_stats["cache_markets_miss"] += 1
            return False
        if payload.get("schema_version") != MARKETS_CACHE_SCHEMA_VERSION:
            self._runtime_stats["cache_markets_miss"] += 1
            return False
        markets = payload.get("markets", [])
        self._priority_market_ids = [
            str(market_id) for market_id in payload.get("top_market_ids", []) if market_id
        ]
        loaded = 0
        for item in markets:
            market = self._parse_market(item)
            if market and market.yes_token_id and market.no_token_id:
                self._markets_cache[market.market_id] = market
                loaded += 1
        if loaded == 0:
            self._runtime_stats["cache_markets_miss"] += 1
            return False
        self._runtime_stats["cache_markets_hit"] += 1
        logger.info("Hydrated %s markets from cache", loaded)
        return True

    async def _persist_markets_cache(self, raw_markets: list[dict[str, Any]]) -> None:
        """Persist raw market payload for warm startup."""
        payload = {
            "schema_version": MARKETS_CACHE_SCHEMA_VERSION,
            "cached_at": datetime.utcnow().isoformat(),
            "market_count": len(raw_markets),
            "top_market_ids": self._priority_market_ids[:1500],
            "markets": raw_markets,
        }
        stored = await self.cache_store.set_json(
            MARKETS_CACHE_KEY,
            payload,
            ttl_seconds=self.markets_cache_ttl_seconds,
        )
        if stored:
            self._runtime_stats["cache_markets_writes"] += 1

    def _can_serve_cached_markets(self, params: dict[str, Any]) -> bool:
        """Return True when filters are compatible with cached snapshot usage."""
        if "limit" in params or "offset" in params:
            return False
        if params.get("order", "volume24hr") != "volume24hr":
            return False
        if str(params.get("ascending", "false")).lower() != "false":
            return False
        if params.get("closed", "false") != "false":
            return False
        return True

    async def _fetch_markets_from_api(self, params: dict[str, Any]) -> tuple[list[Market], list[dict[str, Any]], int]:
        """Fetch markets via paginated Gamma API calls."""
        all_markets: list[Market] = []
        all_raw_markets: list[dict[str, Any]] = []
        offset = 0
        limit = 100
        max_markets = 5000
        discovery_batches = 0

        while True:
            params["limit"] = limit
            params["offset"] = offset

            data = await self._request(
                "GET",
                "/markets",
                params=params,
                base_url=self.gamma_url,
            )
            if not data:
                break

            discovery_batches += 1
            all_raw_markets.extend(data)
            batch_valid = 0
            for item in data:
                market = self._parse_market(item)
                if market and market.yes_token_id and market.no_token_id:
                    all_markets.append(market)
                    self._markets_cache[market.market_id] = market
                    batch_valid += 1

            logger.info(
                "Fetched batch: offset=%s, got %s markets (%s valid)",
                offset,
                len(data),
                batch_valid,
            )

            if len(data) < limit:
                break
            offset += limit
            await asyncio.sleep(0.15)
            if len(all_markets) >= max_markets:
                logger.info("Reached %s market cap", max_markets)
                break

        return all_markets, all_raw_markets, discovery_batches

    def _refresh_priority_market_ids(self, markets: list[Market]) -> None:
        """Track highest-liquidity market IDs for faster startup rotation."""
        ranked = sorted(markets, key=lambda market: market.volume_24h, reverse=True)
        self._priority_market_ids = [market.market_id for market in ranked if market.market_id][:1500]

    def _schedule_markets_refresh(self, params: dict[str, Any]) -> None:
        """Refresh cached market list asynchronously without blocking startup."""
        if self._markets_refresh_task and not self._markets_refresh_task.done():
            return

        async def _refresh() -> None:
            try:
                fresh_markets, raw_markets, _ = await self._fetch_markets_from_api(params.copy())
                if raw_markets:
                    await self._persist_markets_cache(raw_markets)
                logger.info("Background market refresh complete (%s markets)", len(fresh_markets))
            except Exception as exc:
                logger.warning("Background market refresh failed: %s", exc)

        self._markets_refresh_task = asyncio.create_task(_refresh(), name="markets_refresh")
    
    def _get_headers(self) -> dict[str, str]:
        """Get default HTTP headers for public REST endpoints."""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get_clob_funder_address(self) -> str:
        """Resolve funder address for CLOB authenticated client."""
        env_funder = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip()
        funder = env_funder or self.clob_funder_address.strip()
        if funder:
            return funder

        # EOA mode defaults to signer address.
        if not self.private_key:
            raise RuntimeError("Private key required to derive funder address")
        return Account.from_key(self.private_key).address

    def _build_clob_client(self) -> ClobClient:
        """Create configured py_clob_client instance."""
        if not self.private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required for live trading")
        return ClobClient(
            host=self.rest_url,
            chain_id=self.clob_chain_id,
            key=self.private_key,
            signature_type=self.clob_signature_type,
            funder=self._get_clob_funder_address(),
        )

    async def _ensure_live_trading_client(self) -> ClobClient:
        """Ensure a properly authenticated CLOB client for live endpoints."""
        if self.dry_run:
            raise RuntimeError("Live CLOB client requested while in dry_run mode")

        if not self._clob_client:
            self._clob_client = self._build_clob_client()

        has_explicit_l2_creds = (
            bool(self.api_key)
            and bool(self.api_secret)
            and bool(self.passphrase)
            and self.api_key != "YOUR_API_KEY_HERE"
            and self.api_secret != "YOUR_API_SECRET_HERE"
            and self.passphrase != "YOUR_PASSPHRASE_HERE"
        )
        if has_explicit_l2_creds:
            creds = ApiCreds(
                api_key=self.api_key,
                api_secret=self.api_secret,
                api_passphrase=self.passphrase,
            )
            self._clob_client.set_api_creds(creds)
            return self._clob_client

        # Fall back to L1 create-or-derive when creds are not fully configured.
        creds = await asyncio.to_thread(
            self._clob_client.create_or_derive_api_creds,
            self.clob_api_key_nonce,
        )
        self.api_key = creds.api_key
        self.api_secret = creds.api_secret
        self.passphrase = creds.api_passphrase
        logger.info("Derived Polymarket API credentials via L1 auth")
        return self._clob_client

    async def _ensure_market_for_token(self, market_id: str) -> Market:
        """Ensure market metadata (token IDs) exists for order operations."""
        market = self._markets_cache.get(market_id)
        if market and market.yes_token_id and market.no_token_id:
            return market
        market = await self.get_market(market_id)
        self._markets_cache[market_id] = market
        return market

    async def _token_lookup(self) -> dict[str, tuple[str, TokenType]]:
        """Build token_id -> (market_id, token_type) lookup."""
        if not self._markets_cache:
            try:
                await self.list_markets({"active": True})
            except Exception:
                return {}
        lookup: dict[str, tuple[str, TokenType]] = {}
        for market in self._markets_cache.values():
            if market.yes_token_id:
                lookup[str(market.yes_token_id)] = (market.market_id, TokenType.YES)
            if market.no_token_id:
                lookup[str(market.no_token_id)] = (market.market_id, TokenType.NO)
        return lookup

    @staticmethod
    def _parse_order_status(raw_status: Optional[str]) -> OrderStatus:
        """Map arbitrary CLOB status strings to local enum."""
        status = (raw_status or "").lower()
        mapping = {
            "open": OrderStatus.OPEN,
            "live": OrderStatus.OPEN,
            "matched": OrderStatus.PARTIALLY_FILLED,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELLED,
            "cancelled": OrderStatus.CANCELLED,
            "expired": OrderStatus.EXPIRED,
            "rejected": OrderStatus.REJECTED,
        }
        return mapping.get(status, OrderStatus.OPEN)

    @staticmethod
    def _parse_side(raw_side: Optional[str]) -> OrderSide:
        side = (raw_side or "").lower()
        if side in {"buy", "bid"}:
            return OrderSide.BUY
        if side in {"sell", "ask"}:
            return OrderSide.SELL
        return OrderSide.BUY
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        base_url: Optional[str] = None,
    ) -> Any:
        """Make an HTTP request with retry logic."""
        if not self._http_client:
            await self.connect()
        
        url = f"{base_url or self.rest_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                response = await self._http_client.request(
                    method,
                    url,
                    params=params,
                    json=json_data,
                )
                response.raise_for_status()
                if response.status_code == 204 or not response.content:
                    return {}

                content_type = response.headers.get("content-type", "").lower()
                if "application/json" in content_type:
                    return response.json()

                # Some endpoints may return an empty/non-JSON body on success.
                try:
                    return response.json()
                except ValueError:
                    return {}
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP error {e.response.status_code} on {url}: {e}")
                if e.response.status_code >= 500:
                    # Retry on server errors
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay * (attempt + 1))
                        continue
                raise
            except httpx.RequestError as e:
                logger.warning(f"Request error on {url}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                    continue
                raise
    
    async def list_markets(self, filters: Optional[dict] = None) -> list[Market]:
        """
        Fetch list of available markets from Gamma API.
        
        Endpoint: GET https://gamma-api.polymarket.com/markets
        
        Uses pagination to get ALL active markets across all categories!
        """
        try:
            discovery_started_at = time.perf_counter()
            params = filters.copy() if filters else {}
            params.setdefault("closed", "false")
            params.setdefault("order", "volume24hr")
            params.setdefault("ascending", "false")
            
            cache_loaded = await self._hydrate_markets_cache()
            if cache_loaded and self._can_serve_cached_markets(params):
                cached_markets = [
                    market
                    for market in self._markets_cache.values()
                    if market.active and not market.closed and market.yes_token_id and market.no_token_id
                ]
                cached_markets.sort(key=lambda m: m.volume_24h, reverse=True)
                self._schedule_markets_refresh(params)
                logger.info("Serving %s markets from cache (refreshing in background)", len(cached_markets))
                self._runtime_stats["markets_discovered_valid"] = float(len(cached_markets))
                self._runtime_stats["market_discovery_duration_ms"] = (
                    time.perf_counter() - discovery_started_at
                ) * 1000
                return cached_markets[:5000]
            
            logger.info("Fetching ALL available markets from Polymarket...")
            all_markets, all_raw_markets, discovery_batches = await self._fetch_markets_from_api(params)
            self._refresh_priority_market_ids(all_markets)
            
            self._runtime_stats["market_discovery_batches"] = float(discovery_batches)
            self._runtime_stats["markets_discovered_valid"] = float(len(all_markets))
            self._runtime_stats["market_discovery_duration_ms"] = (
                time.perf_counter() - discovery_started_at
            ) * 1000
            if all_raw_markets:
                await self._persist_markets_cache(all_raw_markets)
            logger.info(f"=== TOTAL: {len(all_markets)} active markets with valid tokens ===")
            return all_markets
            
        except Exception as e:
            logger.error(f"Failed to fetch markets from API: {e}")
            raise
    
    async def list_events(self, filters: Optional[dict] = None) -> list[dict]:
        """
        Fetch events (which contain markets) from Gamma API.
        
        Endpoint: GET https://gamma-api.polymarket.com/events
        
        Events are useful for getting grouped markets.
        """
        try:
            params = filters.copy() if filters else {}
            params.setdefault("closed", "false")
            params.setdefault("limit", 50)
            params.setdefault("order", "id")
            params.setdefault("ascending", "false")
            
            data = await self._request(
                "GET",
                "/events",
                params=params,
                base_url=self.gamma_url,
            )
            
            return data
            
        except Exception as e:
            logger.warning(f"Failed to fetch events: {e}")
            return []
    
    def _parse_market(self, data: dict) -> Optional[Market]:
        """Parse market data from Gamma API response."""
        try:
            market_id = str(data.get("id", ""))
            condition_id = data.get("conditionId", "")
            
            if not market_id:
                return None
            
            # Parse clobTokenIds - JSON string like '["tokenId1","tokenId2"]'
            clob_token_ids_raw = data.get("clobTokenIds", "")
            yes_token_id = ""
            no_token_id = ""
            
            if clob_token_ids_raw:
                token_ids = []
                if isinstance(clob_token_ids_raw, list):
                    token_ids = clob_token_ids_raw
                elif isinstance(clob_token_ids_raw, str):
                    try:
                        parsed = json.loads(clob_token_ids_raw)
                        if isinstance(parsed, list):
                            token_ids = parsed
                    except (json.JSONDecodeError, TypeError):
                        # Fallback: try comma-separated
                        token_ids = clob_token_ids_raw.split(",")

                yes_token_id = str(token_ids[0]).strip() if len(token_ids) > 0 else ""
                no_token_id = str(token_ids[1]).strip() if len(token_ids) > 1 else ""
            
            # Parse outcomes - JSON string like '["Yes", "No"]'
            outcomes_str = data.get("outcomes", "")
            # Parse outcome prices - JSON string like '[0.65, 0.35]'
            outcome_prices_str = data.get("outcomePrices", "")
            
            return Market(
                market_id=market_id,
                condition_id=condition_id,
                question=data.get("question", "") or "",
                description=data.get("description", "") or "",
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                active=bool(data.get("active", True)),
                closed=bool(data.get("closed", False)),
                resolved=data.get("umaResolutionStatus") == "resolved",
                volume_24h=float(data.get("volume24hr") or data.get("volume24hrClob") or 0),
                liquidity=float(data.get("liquidityNum") or data.get("liquidityClob") or 0),
                category=data.get("category", "") or "",
            )
        except Exception as e:
            logger.warning(f"Failed to parse market: {e}")
            return None
    
    def _get_placeholder_markets(self) -> list[Market]:
        """Get placeholder markets for testing."""
        return [
            Market(
                market_id="placeholder_market_1",
                condition_id="0x1234",
                question="Will BTC be above $100k by end of 2025?",
                description="Resolves YES if Bitcoin price exceeds $100,000",
                yes_token_id="yes_token_1",
                no_token_id="no_token_1",
                active=True,
                volume_24h=50000.0,
                liquidity=100000.0,
            ),
            Market(
                market_id="placeholder_market_2",
                condition_id="0x5678",
                question="Will ETH 2.0 be fully deployed by Q2 2025?",
                description="Resolves YES if Ethereum completes all upgrades",
                yes_token_id="yes_token_2",
                no_token_id="no_token_2",
                active=True,
                volume_24h=25000.0,
                liquidity=50000.0,
            ),
        ]
    
    async def get_market(self, market_id: str) -> Market:
        """
        Get details for a specific market.
        
        Can fetch by ID or by slug:
        - GET /markets/{id} - by numeric ID
        - GET /markets/slug/{slug} - by slug
        """
        try:
            # Try fetching by ID first
            data = await self._request(
                "GET",
                f"/markets/{market_id}",
                base_url=self.gamma_url,
            )
            market = self._parse_market(data)
            if market:
                return market
            raise ValueError("Failed to parse market")
        except Exception as e:
            logger.warning(f"Failed to fetch market {market_id}: {e}")
            if self.dry_run:
                return Market(
                    market_id=market_id,
                    condition_id=market_id,
                    question=f"Market {market_id}",
                    active=True,
                )
            raise
    
    async def get_market_by_slug(self, slug: str) -> Market:
        """
        Get market by its slug.
        
        Endpoint: GET /markets/slug/{slug}
        
        The slug can be extracted from Polymarket URLs:
        https://polymarket.com/event/some-event-slug
        """
        try:
            data = await self._request(
                "GET",
                f"/markets/slug/{slug}",
                base_url=self.gamma_url,
            )
            market = self._parse_market(data)
            if market:
                return market
            raise ValueError("Failed to parse market")
        except Exception as e:
            logger.error(f"Failed to fetch market by slug {slug}: {e}")
            raise
    
    async def get_event_by_slug(self, slug: str) -> dict:
        """
        Get event by its slug.
        
        Endpoint: GET /events/slug/{slug}
        
        Events contain multiple related markets.
        """
        try:
            data = await self._request(
                "GET",
                f"/events/slug/{slug}",
                base_url=self.gamma_url,
            )
            return data
        except Exception as e:
            logger.error(f"Failed to fetch event by slug {slug}: {e}")
            raise
    
    async def get_orderbook(self, market_id: str) -> OrderBook:
        """
        Fetch current order book for a market.
        
        Uses Polymarket CLOB API:
        GET https://clob.polymarket.com/book?token_id={token_id}
        """
        # Get market to find token IDs
        market = await self.get_market(market_id)
        
        if not market.yes_token_id or not market.no_token_id:
            logger.warning(f"No token IDs for market {market_id}")
            return OrderBook(market_id=market_id, timestamp=datetime.utcnow())
        
        # Fetch REAL order books from CLOB API
        yes_book = await self._fetch_token_orderbook(market.yes_token_id, TokenType.YES)
        no_book = await self._fetch_token_orderbook(market.no_token_id, TokenType.NO)
        
        return OrderBook(
            market_id=market_id,
            yes=yes_book,
            no=no_book,
            timestamp=datetime.utcnow(),
        )
    
    async def _fetch_token_orderbook(self, token_id: str, token_type: TokenType) -> TokenOrderBook:
        """Fetch order book for a single token from CLOB API."""
        invalid_until = self._invalid_token_ids.get(token_id)
        if invalid_until and invalid_until > time.time():
            self._runtime_stats["orderbook_invalid_token_skips"] += 1
            return TokenOrderBook(
                token_type=token_type,
                bids=OrderBookSide(levels=[]),
                asks=OrderBookSide(levels=[]),
            )
        try:
            data = await self._request(
                "GET",
                "/book",
                params={"token_id": token_id},
                base_url=self.rest_url,
            )
            
            # Parse bids and asks
            bids = []
            asks = []
            
            for bid in data.get("bids", [])[:10]:
                bids.append(PriceLevel(
                    price=float(bid.get("price", 0)),
                    size=float(bid.get("size", 0)),
                ))
            
            for ask in data.get("asks", [])[:10]:
                asks.append(PriceLevel(
                    price=float(ask.get("price", 0)),
                    size=float(ask.get("size", 0)),
                ))
            
            return TokenOrderBook(
                token_type=token_type,
                bids=OrderBookSide(levels=bids),
                asks=OrderBookSide(levels=asks),
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                self._invalid_token_ids[token_id] = time.time() + INVALID_TOKEN_TTL_SECONDS
                self._runtime_stats["orderbook_invalid_token_marks"] += 1
                logger.debug("Caching invalid token %s after 404", token_id)
            logger.warning(f"Failed to fetch orderbook for token {token_id}: {exc}")
            return TokenOrderBook(
                token_type=token_type,
                bids=OrderBookSide(levels=[]),
                asks=OrderBookSide(levels=[]),
            )
        except Exception as e:
            logger.warning(f"Failed to fetch orderbook for token {token_id}: {e}")
            # Return empty book
            return TokenOrderBook(
                token_type=token_type,
                bids=OrderBookSide(levels=[]),
                asks=OrderBookSide(levels=[]),
            )
    
    def _generate_simulated_orderbook(self, market_id: str) -> OrderBook:
        """Generate a simulated order book for testing."""
        import random
        
        # Simulate realistic prices with occasional mispricings
        yes_mid = 0.50 + random.uniform(-0.30, 0.30)
        
        # 20% chance of significant mispricing (arb opportunity!)
        if random.random() < 0.20:
            inefficiency = random.uniform(-0.08, 0.08)  # Bigger mispricing
        else:
            inefficiency = random.uniform(-0.02, 0.02)  # Normal slight inefficiency
        
        no_mid = 1.0 - yes_mid + inefficiency
        
        spread = random.uniform(0.02, 0.06)
        
        def generate_levels(mid: float, is_bid: bool, count: int = 5) -> list[PriceLevel]:
            levels = []
            for i in range(count):
                offset = (i + 1) * 0.01
                if is_bid:
                    price = max(0.01, mid - spread/2 - offset)
                else:
                    price = min(0.99, mid + spread/2 + offset)
                size = random.uniform(100, 1000)
                levels.append(PriceLevel(price=round(price, 2), size=round(size, 2)))
            return levels
        
        yes_book = TokenOrderBook(
            token_type=TokenType.YES,
            bids=OrderBookSide(levels=generate_levels(yes_mid, is_bid=True)),
            asks=OrderBookSide(levels=generate_levels(yes_mid, is_bid=False)),
        )
        
        no_book = TokenOrderBook(
            token_type=TokenType.NO,
            bids=OrderBookSide(levels=generate_levels(no_mid, is_bid=True)),
            asks=OrderBookSide(levels=generate_levels(no_mid, is_bid=False)),
        )
        
        return OrderBook(
            market_id=market_id,
            yes=yes_book,
            no=no_book,
            timestamp=datetime.utcnow(),
        )
    
    async def _fetch_market_orderbook(
        self,
        market_id: str,
        yes_token: str,
        no_token: str,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, OrderBook]:
        """Fetch YES/NO books for one market with bounded concurrency."""
        async with semaphore:
            yes_task = self._fetch_token_orderbook(yes_token, TokenType.YES)
            no_task = self._fetch_token_orderbook(no_token, TokenType.NO)
            yes_book, no_book = await asyncio.gather(yes_task, no_task)
            return (
                market_id,
                OrderBook(
                    market_id=market_id,
                    yes=yes_book,
                    no=no_book,
                    timestamp=datetime.utcnow(),
                ),
            )
    
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
        """
        Stream order book updates.
        
        If use_simulation=True, generates simulated data with opportunities.
        Otherwise fetches REAL data from Polymarket CLOB API.
        """
        if use_simulation:
            async for item in self._stream_simulated_orderbooks(market_ids):
                yield item
            return
        
        logger.info(f"Starting REAL orderbook stream for {len(market_ids)} markets")
        
        # We already have token IDs in the cached markets - use them directly!
        # Build token map from cached market data (no extra API calls needed)
        market_tokens: dict[str, tuple[str, str]] = {}
        
        for market_id in market_ids:
            if market_id in self._markets_cache:
                market = self._markets_cache[market_id]
                if market.yes_token_id and market.no_token_id:
                    market_tokens[market_id] = (market.yes_token_id, market.no_token_id)
        
        logger.info(f"Have token IDs for {len(market_tokens)} markets (from cache)")
        
        if not market_tokens:
            logger.warning("No markets with valid token IDs found!")
            return
        
        # Sanitize settings for large market counts
        active_batch_size = max(1, active_batch_size)
        markets_per_request_batch = max(1, markets_per_request_batch)
        request_concurrency = max(1, request_concurrency)
        request_delay = max(0.0, request_delay)
        batch_delay = max(0.0, batch_delay)
        rotation_delay = max(0.0, rotation_delay)
        
        self._runtime_stats["active_batch_size"] = float(active_batch_size)
        self._runtime_stats["markets_per_request_batch"] = float(markets_per_request_batch)
        self._runtime_stats["request_concurrency"] = float(request_concurrency)
        
        if self._priority_market_ids:
            prioritized = [market_id for market_id in self._priority_market_ids if market_id in market_tokens]
            prioritized_set = set(prioritized)
            remaining = [market_id for market_id in market_tokens.keys() if market_id not in prioritized_set]
            market_list = prioritized + remaining
        else:
            market_list = list(market_tokens.keys())
        total_markets = len(market_list)
        current_offset = 0
        
        logger.info(f"Will rotate through {total_markets} markets, {active_batch_size} at a time")
        
        try:
            while True:
                rotation_started_at = time.perf_counter()
                # Get current batch of 500 markets
                end_offset = min(current_offset + active_batch_size, total_markets)
                active_markets = market_list[current_offset:end_offset]
                
                logger.info(f"Processing markets {current_offset+1}-{end_offset} of {total_markets}")
                
                # Process this batch
                for i in range(0, len(active_markets), markets_per_request_batch):
                    request_batch = active_markets[i:i + markets_per_request_batch]
                    semaphore = asyncio.Semaphore(request_concurrency)
                    tasks = [
                        self._fetch_market_orderbook(
                            market_id=market_id,
                            yes_token=market_tokens[market_id][0],
                            no_token=market_tokens[market_id][1],
                            semaphore=semaphore,
                        )
                        for market_id in request_batch
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception):
                            continue
                        yield result
                        self._runtime_stats["orderbook_updates_emitted"] += 1
                        if request_delay > 0:
                            await asyncio.sleep(request_delay)
                    
                    await asyncio.sleep(batch_delay)
                
                # Move to next batch of 500
                current_offset = end_offset
                if current_offset >= total_markets:
                    current_offset = 0  # Start over from beginning
                    logger.info("Completed full market cycle, starting over...")
                
                rotation_duration_ms = (time.perf_counter() - rotation_started_at) * 1000
                total_rotations = self._runtime_stats["orderbook_rotations"] + 1
                previous_avg = self._runtime_stats["avg_rotation_duration_ms"]
                self._runtime_stats["orderbook_rotations"] = total_rotations
                self._runtime_stats["last_rotation_duration_ms"] = rotation_duration_ms
                self._runtime_stats["avg_rotation_duration_ms"] = (
                    (previous_avg * (total_rotations - 1)) + rotation_duration_ms
                ) / total_rotations
                self._runtime_stats["max_rotation_duration_ms"] = max(
                    self._runtime_stats["max_rotation_duration_ms"],
                    rotation_duration_ms,
                )
                
                await asyncio.sleep(rotation_delay)
                
        except asyncio.CancelledError:
            logger.info("Orderbook stream cancelled")
            raise
        except Exception as e:
            logger.error(f"Orderbook stream error: {e}")
            raise
    
    async def _stream_simulated_orderbooks(self, market_ids: list[str]) -> AsyncIterator[tuple[str, OrderBook]]:
        """Generate simulated order books with occasional arbitrage opportunities."""
        import random
        
        logger.info(f"Starting SIMULATED orderbook stream for {len(market_ids)} markets")
        
        # Use subset for faster updates
        active_markets = market_ids[:100] if len(market_ids) > 100 else market_ids
        
        try:
            while True:
                # Update 10-20 random markets per cycle
                batch = random.sample(active_markets, min(15, len(active_markets)))
                
                for market_id in batch:
                    orderbook = self._generate_simulated_orderbook(market_id)
                    yield (market_id, orderbook)
                    await asyncio.sleep(0.02)  # Fast updates
                
                await asyncio.sleep(0.5)  # Brief pause between cycles
                
        except asyncio.CancelledError:
            logger.info("Simulated orderbook stream cancelled")
            raise

    async def _connect_websocket(self, market_ids: list[str]) -> None:
        """
        Connect to Polymarket WebSocket.
        
        TODO: Implement actual WebSocket connection and subscription.
        """
        try:
            self._ws_connection = await websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=10,
            )
            
            # Subscribe to markets
            for market_id in market_ids:
                subscribe_msg = json.dumps({
                    "type": "subscribe",
                    "market": market_id,
                    "channel": "book",
                })
                await self._ws_connection.send(subscribe_msg)
                self._ws_subscriptions.add(market_id)
            
            logger.info(f"WebSocket connected, subscribed to {len(market_ids)} markets")
            
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            raise
    
    async def get_positions(self) -> dict[str, dict[TokenType, Position]]:
        """
        Get all current positions.
        
        TODO: Implement with actual Polymarket API.
        """
        if self.dry_run:
            return self._simulated_positions.copy()
        
        try:
            # Keep existing endpoint fallback for now; many accounts will have
            # no positions and this endpoint may be unavailable per environment.
            data = await self._request("GET", "/positions")
            positions = {}
            for item in data:
                market_id = item["market_id"]
                token_type = TokenType.YES if item["outcome"] == "Yes" else TokenType.NO
                positions.setdefault(market_id, {})[token_type] = Position(
                    market_id=market_id,
                    token_type=token_type,
                    size=float(item["size"]),
                    avg_entry_price=float(item.get("avg_price", 0)),
                    realized_pnl=float(item.get("realized_pnl", 0)),
                )
            return positions
        except Exception as e:
            logger.warning(f"Failed to fetch positions: {e}")
            return {}
    
    async def place_order(
        self,
        market_id: str,
        token_type: TokenType,
        side: OrderSide,
        price: float,
        size: float,
        strategy_tag: str = ""
    ) -> Order:
        """
        Place a limit order.
        
        TODO: Implement with actual Polymarket CLOB API:
        POST https://clob.polymarket.com/order
        """
        order_id = f"order_{uuid.uuid4().hex[:12]}"
        order = Order(
            order_id=order_id,
            market_id=market_id,
            token_type=token_type,
            side=side,
            price=price,
            size=size,
            status=OrderStatus.OPEN,
            strategy_tag=strategy_tag,
        )
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Placing order: {order}")
            self._simulated_orders[order_id] = order
            return order
        
        try:
            market = await self._ensure_market_for_token(market_id)
            token_id = market.yes_token_id if token_type == TokenType.YES else market.no_token_id
            if not token_id:
                raise ValueError(f"Missing {token_type.value} token_id for market {market_id}")

            clob = await self._ensure_live_trading_client()
            signed_order = await asyncio.to_thread(
                clob.create_order,
                OrderArgs(
                    token_id=str(token_id),
                    price=float(price),
                    size=float(size),
                    side=side.value.upper(),
                ),
            )
            response = await asyncio.to_thread(clob.post_order, signed_order)
            # py_clob_client normalizes this key as "orderID" on success.
            returned_order_id = (
                response.get("orderID")
                or response.get("orderId")
                or response.get("order_id")
                or order_id
            )
            order.order_id = str(returned_order_id)
            order.status = OrderStatus.OPEN
            
            logger.info(f"Order placed: {order.order_id}")
            return order
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            order.status = OrderStatus.REJECTED
            raise
    
    async def cancel_order(self, order_id: str) -> None:
        """
        Cancel an open order.
        
        TODO: Implement with actual Polymarket CLOB API:
        DELETE https://clob.polymarket.com/order/{order_id}
        """
        if self.dry_run:
            if order_id in self._simulated_orders:
                self._simulated_orders[order_id].status = OrderStatus.CANCELLED
                logger.info(f"[DRY RUN] Cancelled order: {order_id}")
            return
        
        try:
            clob = await self._ensure_live_trading_client()
            await asyncio.to_thread(clob.cancel, order_id)
            logger.info(f"Order cancelled: {order_id}")
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            raise
    
    async def cancel_all_orders(self, market_id: Optional[str] = None) -> int:
        """Cancel all open orders, optionally for a specific market."""
        orders = await self.get_open_orders(market_id)
        cancelled = 0
        
        for order in orders:
            try:
                await self.cancel_order(order.order_id)
                cancelled += 1
            except Exception as e:
                logger.warning(f"Failed to cancel order {order.order_id}: {e}")
        
        return cancelled
    
    async def get_open_orders(self, market_id: Optional[str] = None) -> list[Order]:
        """Get all open orders."""
        if self.dry_run:
            orders = [
                o for o in self._simulated_orders.values()
                if o.is_open and (market_id is None or o.market_id == market_id)
            ]
            return orders
        
        try:
            clob = await self._ensure_live_trading_client()
            token_lookup = await self._token_lookup()
            raw_orders = await asyncio.to_thread(clob.get_orders, OpenOrderParams())

            parsed_orders: list[Order] = []
            for item in raw_orders:
                asset_id = str(item.get("asset_id") or item.get("assetId") or "")
                market_and_token = token_lookup.get(asset_id)
                if market_and_token:
                    resolved_market_id, token_type = market_and_token
                else:
                    resolved_market_id = str(item.get("market") or item.get("market_id") or "")
                    token_type = TokenType.YES

                if market_id and resolved_market_id != market_id:
                    continue

                parsed_orders.append(
                    Order(
                        order_id=str(item.get("id") or item.get("orderID") or item.get("order_id") or ""),
                        market_id=resolved_market_id,
                        token_type=token_type,
                        side=self._parse_side(item.get("side")),
                        price=float(item.get("price", 0.0) or 0.0),
                        size=float(item.get("original_size") or item.get("size") or 0.0),
                        filled_size=float(item.get("size_matched") or item.get("filled_size") or 0.0),
                        status=self._parse_order_status(item.get("status")),
                    )
                )
            return parsed_orders
        except Exception as e:
            logger.warning(f"Failed to fetch open orders: {e}")
            return []
    
    async def get_trades(self, market_id: Optional[str] = None, limit: int = 100) -> list[Trade]:
        """Get recent trades."""
        if self.dry_run:
            trades = self._simulated_trades[-limit:]
            if market_id:
                trades = [t for t in trades if t.market_id == market_id]
            return trades
        
        try:
            clob = await self._ensure_live_trading_client()
            token_lookup = await self._token_lookup()
            raw_trades = await asyncio.to_thread(clob.get_trades, TradeParams())

            parsed_trades: list[Trade] = []
            for item in raw_trades[-max(1, limit):]:
                asset_id = str(item.get("asset_id") or item.get("assetId") or "")
                market_and_token = token_lookup.get(asset_id)
                if market_and_token:
                    resolved_market_id, token_type = market_and_token
                else:
                    resolved_market_id = str(item.get("market") or item.get("market_id") or "")
                    token_type = TokenType.YES

                if market_id and resolved_market_id != market_id:
                    continue

                parsed_trades.append(
                    Trade(
                        trade_id=str(item.get("id") or item.get("tradeID") or item.get("trade_id") or ""),
                        order_id=str(item.get("order_id") or item.get("taker_order_id") or ""),
                        market_id=resolved_market_id,
                        token_type=token_type,
                        side=self._parse_side(item.get("side")),
                        price=float(item.get("price", 0.0) or 0.0),
                        size=float(item.get("size", 0.0) or 0.0),
                        fee=float(item.get("fee", 0.0) or 0.0),
                    )
                )
            return parsed_trades
        except Exception as e:
            logger.warning(f"Failed to fetch trades: {e}")
            return []
    
    def get_runtime_stats(self) -> dict[str, float]:
        """Return client-level runtime metrics for observability."""
        stats = self._runtime_stats.copy()
        cache_stats = self.cache_store.stats
        stats.update({
            "cache_reads": float(cache_stats.reads),
            "cache_writes": float(cache_stats.writes),
            "cache_hits": float(cache_stats.hits),
            "cache_misses": float(cache_stats.misses),
            "cache_errors": float(cache_stats.errors),
            "cache_last_read_ms": cache_stats.last_read_ms,
            "cache_last_write_ms": cache_stats.last_write_ms,
            "cache_connected": 1.0 if cache_stats.connected else 0.0,
        })
        return stats
    
    def simulate_fill(self, order_id: str, fill_size: Optional[float] = None) -> Optional[Trade]:
        """
        Simulate an order fill (for dry run mode).
        Returns the generated trade if successful.
        """
        if order_id not in self._simulated_orders:
            return None
        
        order = self._simulated_orders[order_id]
        if not order.is_open:
            return None
        
        fill_size = fill_size or order.remaining_size
        fill_size = min(fill_size, order.remaining_size)
        
        # Create trade with realistic Polymarket fees
        # Taker fee is ~1.5% (150 bps), maker is 0%
        # Assume taker for simulation (conservative)
        notional = fill_size * order.price
        fee_rate = 0.015  # 1.5% taker fee
        fee = notional * fee_rate
        
        trade = Trade(
            trade_id=f"trade_{uuid.uuid4().hex[:12]}",
            order_id=order_id,
            market_id=order.market_id,
            token_type=order.token_type,
            side=order.side,
            price=order.price,
            size=fill_size,
            fee=fee,  # Realistic 1.5% fee
        )
        
        # Update order
        order.filled_size += fill_size
        order.updated_at = datetime.utcnow()
        if order.remaining_size <= 0:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIALLY_FILLED
        
        # Update position
        self._update_simulated_position(trade)
        self._simulated_trades.append(trade)
        
        logger.info(f"[DRY RUN] Simulated fill: {trade}")
        return trade
    
    def _update_simulated_position(self, trade: Trade) -> None:
        """Update simulated position after a trade."""
        market_id = trade.market_id
        token_type = trade.token_type
        
        if market_id not in self._simulated_positions:
            self._simulated_positions[market_id] = {}
        
        if token_type not in self._simulated_positions[market_id]:
            self._simulated_positions[market_id][token_type] = Position(
                market_id=market_id,
                token_type=token_type,
                size=0,
                avg_entry_price=0,
            )
        
        pos = self._simulated_positions[market_id][token_type]
        
        # Update position
        if trade.side == OrderSide.BUY:
            new_size = pos.size + trade.size
            if new_size > 0:
                pos.avg_entry_price = (
                    (pos.avg_entry_price * pos.size + trade.price * trade.size) / new_size
                )
            pos.size = new_size
        else:
            # SELL reduces position
            if pos.size > 0:
                realized = (trade.price - pos.avg_entry_price) * trade.size
                pos.realized_pnl += realized
            pos.size -= trade.size

