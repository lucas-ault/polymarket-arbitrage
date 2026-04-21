# Redis Warm Cache (Optional)

## Purpose
This feature adds an optional Redis-backed warm cache that reduces restart warmup time for:

- Polymarket active market discovery snapshots
- Cross-platform Polymarket/Kalshi matched pairs

If Redis is disabled or unavailable, runtime continues with the existing in-memory and API flow.

## Primary Files
- `utils/cache_store.py`
- `utils/redis_cache.py`
- `utils/config_loader.py`
- `polymarket_client/api.py`
- `run_with_dashboard.py`
- `core/cross_platform_arb.py`

## Behavior Contract
- Redis is **never required** for startup.
- Redis cache is a warm-start accelerator, not a source of truth.
- Cache read/write failures are logged and treated as non-fatal.
- API refresh still runs after warm-starting market discovery from cache.

## Config
`config.yaml` and `config.observation.yaml` include:

```yaml
cache:
  enabled: false
  backend: "redis"
  redis_url: "redis://localhost:6379/0"
  key_prefix: "pmarb"
  connect_timeout_seconds: 1.0
  op_timeout_seconds: 0.5
  markets_ttl_seconds: 900
  matches_ttl_seconds: 3600
```

### Environment overrides
- `CACHE_ENABLED`
- `CACHE_BACKEND`
- `REDIS_URL`
- `CACHE_KEY_PREFIX`
- `CACHE_CONNECT_TIMEOUT_SECONDS`
- `CACHE_OP_TIMEOUT_SECONDS`
- `CACHE_MARKETS_TTL_SECONDS`
- `CACHE_MATCHES_TTL_SECONDS`

## Redis Keys and Schema
Default keys use `cache.key_prefix`:

- `<prefix>:pm:active_markets:v1`
  - `schema_version`
  - `cached_at`
  - `market_count`
  - `markets` (raw Gamma market payloads)

- `<prefix>:xplat:matches:v1`
  - `schema_version`
  - `cached_at`
  - `pair_count`
  - `min_similarity`
  - `pairs` (serialized `MarketPair` list)

## Runtime Sequence
### Startup with cache disabled or unavailable
1. Bot uses `NoopCacheStore`.
2. Discovery and matching run normally from APIs/computation.

### Startup with cache enabled and healthy Redis
1. Market discovery attempts to hydrate from Redis.
2. If cache snapshot is compatible, startup serves cached markets quickly.
3. Discovery refreshes in background and rewrites cache.
4. Dashboard cross-platform matching attempts to pre-load cached pairs.
5. Full matching recomputation still runs and overwrites cache with latest pairs.

## Observability
Cache metrics are exposed through runtime operational stats:

- `cache_connected`
- `cache_reads`
- `cache_writes`
- `cache_hits`
- `cache_misses`
- `cache_errors`
- `cache_last_read_ms`
- `cache_last_write_ms`

## Validation Checklist
1. Run `pytest tests -v`.
2. Run `python test_connection.py -c config.yaml` with cache disabled.
3. Enable cache and run `python test_connection.py -c config.yaml` to confirm Redis connectivity check.
4. Run `python run_with_dashboard.py -c config.observation.yaml`.
5. Restart and compare time-to-first markets / match preview before and after cache enablement.
