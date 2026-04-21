"""
Cache store abstractions used for optional warm caches.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class CacheStats:
    """Simple cache counters for dashboard observability."""

    enabled: bool = False
    connected: bool = False
    reads: int = 0
    writes: int = 0
    hits: int = 0
    misses: int = 0
    errors: int = 0
    last_read_ms: float = 0.0
    last_write_ms: float = 0.0


class CacheStore(ABC):
    """Abstract cache interface for JSON payloads."""

    @property
    @abstractmethod
    def stats(self) -> CacheStats:
        """Return current cache stats."""

    @abstractmethod
    async def connect(self) -> None:
        """Initialize/cache connection if needed."""

    @abstractmethod
    def is_available(self) -> bool:
        """Whether cache backend is currently usable."""

    @abstractmethod
    async def get_json(self, key: str) -> Optional[dict[str, Any]]:
        """Fetch JSON payload by key."""

    @abstractmethod
    async def set_json(self, key: str, payload: dict[str, Any], ttl_seconds: int) -> bool:
        """Store JSON payload by key with TTL."""

    @abstractmethod
    async def close(self) -> None:
        """Close connections and cleanup."""


class NoopCacheStore(CacheStore):
    """Fallback cache implementation that never stores data."""

    def __init__(self, enabled: bool = False) -> None:
        self._stats = CacheStats(enabled=enabled, connected=False)

    @property
    def stats(self) -> CacheStats:
        return self._stats

    async def connect(self) -> None:
        return None

    def is_available(self) -> bool:
        return False

    async def get_json(self, key: str) -> Optional[dict[str, Any]]:
        self._stats.reads += 1
        self._stats.misses += 1
        return None

    async def set_json(self, key: str, payload: dict[str, Any], ttl_seconds: int) -> bool:
        self._stats.writes += 1
        return False

    async def close(self) -> None:
        return None
