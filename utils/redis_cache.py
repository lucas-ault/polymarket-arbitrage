"""
Redis-backed cache with safe fallback behavior.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

from utils.cache_store import CacheStats, CacheStore, NoopCacheStore

logger = logging.getLogger(__name__)


@dataclass
class RedisCacheConfig:
    """Redis cache configuration."""

    enabled: bool = False
    backend: str = "redis"
    redis_url: str = "redis://localhost:6379/0"
    key_prefix: str = "pmarb"
    connect_timeout_seconds: float = 1.0
    op_timeout_seconds: float = 0.5
    markets_ttl_seconds: int = 900
    matches_ttl_seconds: int = 3600


class RedisCacheStore(CacheStore):
    """Best-effort Redis cache implementation."""

    def __init__(self, config: RedisCacheConfig):
        self.config = config
        self._stats = CacheStats(enabled=config.enabled, connected=False)
        self._client = None

    @property
    def stats(self) -> CacheStats:
        return self._stats

    async def connect(self) -> None:
        if not self.config.enabled:
            return
        try:
            import redis.asyncio as redis
        except Exception as exc:
            logger.warning("Redis package unavailable; falling back to no cache: %s", exc)
            self._stats.errors += 1
            self._stats.connected = False
            return

        try:
            self._client = redis.from_url(
                self.config.redis_url,
                decode_responses=True,
                socket_connect_timeout=self.config.connect_timeout_seconds,
                socket_timeout=self.config.op_timeout_seconds,
            )
            await asyncio.wait_for(
                self._client.ping(),
                timeout=self.config.connect_timeout_seconds,
            )
            self._stats.connected = True
            logger.info("Redis cache connected")
        except Exception as exc:
            self._stats.errors += 1
            self._stats.connected = False
            self._client = None
            logger.warning("Redis unavailable, continuing without cache: %s", exc)

    def is_available(self) -> bool:
        return bool(self._stats.connected and self._client is not None)

    def _prefixed(self, key: str) -> str:
        return f"{self.config.key_prefix}:{key}"

    async def get_json(self, key: str) -> Optional[dict[str, Any]]:
        self._stats.reads += 1
        if not self.is_available():
            self._stats.misses += 1
            return None

        started = time.perf_counter()
        try:
            raw = await asyncio.wait_for(
                self._client.get(self._prefixed(key)),
                timeout=self.config.op_timeout_seconds,
            )
            self._stats.last_read_ms = (time.perf_counter() - started) * 1000
            if not raw:
                self._stats.misses += 1
                return None
            self._stats.hits += 1
            return json.loads(raw)
        except Exception as exc:
            self._stats.errors += 1
            self._stats.misses += 1
            logger.warning("Redis read failed for %s: %s", key, exc)
            return None

    async def set_json(self, key: str, payload: dict[str, Any], ttl_seconds: int) -> bool:
        self._stats.writes += 1
        if not self.is_available():
            return False

        started = time.perf_counter()
        try:
            serialized = json.dumps(payload)
            await asyncio.wait_for(
                self._client.set(self._prefixed(key), serialized, ex=max(1, ttl_seconds)),
                timeout=self.config.op_timeout_seconds,
            )
            self._stats.last_write_ms = (time.perf_counter() - started) * 1000
            return True
        except Exception as exc:
            self._stats.errors += 1
            logger.warning("Redis write failed for %s: %s", key, exc)
            return False

    async def close(self) -> None:
        if self._client is None:
            return
        try:
            await self._client.aclose()
        except Exception:
            pass
        finally:
            self._client = None
            self._stats.connected = False


async def create_cache_store(config: RedisCacheConfig) -> CacheStore:
    """
    Create cache store using config and availability checks.
    
    Always returns a usable store (Redis-backed or no-op fallback).
    """
    if not config.enabled or config.backend.lower() != "redis":
        return NoopCacheStore(enabled=config.enabled)

    store = RedisCacheStore(config)
    await store.connect()
    if store.is_available():
        return store
    return NoopCacheStore(enabled=config.enabled)
