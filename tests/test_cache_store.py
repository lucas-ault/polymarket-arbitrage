"""
Tests for cache store behavior and fallback.
"""

import pytest

from utils.cache_store import NoopCacheStore
from utils.redis_cache import RedisCacheConfig, create_cache_store


@pytest.mark.asyncio
async def test_noop_cache_store_behaves_as_fallback():
    store = NoopCacheStore(enabled=True)
    await store.connect()
    assert store.is_available() is False
    assert await store.get_json("missing") is None
    assert await store.set_json("k", {"a": 1}, 10) is False
    assert store.stats.reads == 1
    assert store.stats.misses == 1
    await store.close()


@pytest.mark.asyncio
async def test_create_cache_store_returns_noop_when_disabled():
    config = RedisCacheConfig(enabled=False)
    store = await create_cache_store(config)
    assert isinstance(store, NoopCacheStore)
    assert store.stats.enabled is False
