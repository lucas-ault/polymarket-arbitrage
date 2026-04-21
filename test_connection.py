#!/usr/bin/env python3
"""
Test Polymarket API Connection
===============================

Run this BEFORE going live to verify your credentials work.

Usage:
    python3 test_connection.py
    python3 test_connection.py --config config.yaml
"""

import asyncio
import argparse
import sys

from utils.config_loader import load_config
from utils.logging_utils import setup_logging
from polymarket_client import PolymarketClient
from utils.redis_cache import RedisCacheConfig, create_cache_store


async def test_connection(config_path: str = "config.yaml"):
    """Test the API connection and credentials."""
    print("=" * 60)
    print("🔌 Polymarket API Connection Test")
    print("=" * 60)
    
    # Load config
    try:
        config = load_config(config_path)
        print(f"✅ Config loaded from {config_path}")
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        return False
    
    print(f"   Mode: {config.mode.trading_mode.upper()}")
    print(f"   Key ID: {config.api.key_id[:8]}..." if config.api.key_id else "   Key ID: NOT SET")
    
    # Check credentials
    if config.is_live:
        if not config.api.key_id or config.api.key_id == "YOUR_POLYMARKET_KEY_ID_HERE":
            print("❌ key_id not configured!")
            print(f"   Edit {config_path} and set api.key_id from polymarket.us/developer")
            return False
        if not config.api.secret_key or config.api.secret_key == "YOUR_POLYMARKET_SECRET_KEY_HERE":
            print("❌ secret_key not configured!")
            print(f"   Edit {config_path} and set api.secret_key from polymarket.us/developer")
            return False
    
    print()
    print("📡 Testing API connection...")
    
    # Optional cache connectivity check
    if getattr(config, "cache", None) and config.cache.enabled:
        print()
        print("🧠 Testing optional Redis cache...")
        redis_config = RedisCacheConfig(
            enabled=config.cache.enabled,
            backend=config.cache.backend,
            redis_url=config.cache.redis_url,
            key_prefix=config.cache.key_prefix,
            connect_timeout_seconds=config.cache.connect_timeout_seconds,
            op_timeout_seconds=config.cache.op_timeout_seconds,
            markets_ttl_seconds=config.cache.markets_ttl_seconds,
            matches_ttl_seconds=config.cache.matches_ttl_seconds,
        )
        cache_store = await create_cache_store(redis_config)
        if cache_store.is_available():
            print("✅ Redis cache connected")
        else:
            print("⚠️  Redis unavailable - continuing with fallback behavior")
        await cache_store.close()
    
    # Create client
    client = PolymarketClient(
        public_url=config.api.polymarket_public_url,
        private_url=config.api.polymarket_private_url,
        markets_ws_url=config.api.polymarket_markets_ws_url,
        private_ws_url=config.api.polymarket_private_ws_url,
        key_id=config.api.key_id,
        secret_key=config.api.secret_key,
        timeout=config.api.timeout_seconds,
        dry_run=config.is_dry_run,
        use_websocket=config.api.use_websocket,
        use_rest_fallback=config.api.use_rest_fallback,
        markets_cache_ttl_seconds=getattr(config.cache, "markets_ttl_seconds", 900),
    )
    
    try:
        await client.connect()
        print("✅ HTTP client connected")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    # Test public markets API
    print()
    print("📊 Testing public market data...")
    try:
        markets = await client.list_markets({"limit": 5, "active": True})
        print(f"✅ Public API working - found {len(markets)} markets")
        
        if markets:
            print("   Sample markets:")
            for m in markets[:3]:
                print(f"   - {m.question[:50]}...")
                print(f"     Volume 24h: ${m.volume_24h:,.0f} | Liquidity: ${m.liquidity:,.0f}")
    except Exception as e:
        print(f"❌ Public API error: {e}")
        await client.disconnect()
        return False
    
    # Test authenticated endpoints
    if config.is_live:
        print()
        print("💼 Testing authenticated endpoints...")
        try:
            positions = await client.get_positions()
            open_orders = await client.get_open_orders()
            print(f"✅ Auth working - {sum(len(v) for v in positions.values())} positions, {len(open_orders)} open orders")
        except Exception as e:
            print(f"❌ Auth test failed: {e}")
            await client.disconnect()
            return False
    
    await client.disconnect()
    
    print()
    print("=" * 60)
    print("✅ Connection test PASSED!")
    print("=" * 60)
    print()
    print("Next steps:")
    print(f"1. Review {config_path} settings")
    print(f"2. Start with: python3 run_with_dashboard.py -c {config_path}")
    print("3. Monitor closely on the dashboard")
    print()
    
    return True


def main():
    parser = argparse.ArgumentParser(description="Test Polymarket API connection")
    parser.add_argument("-c", "--config", default="config.yaml", help="Config file")
    args = parser.parse_args()
    
    setup_logging(console_level="WARNING")
    
    success = asyncio.run(test_connection(args.config))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

