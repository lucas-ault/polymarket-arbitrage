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
    print(f"   API Key: {config.api.api_key[:8]}..." if config.api.api_key else "   API Key: NOT SET")
    
    # Check credentials
    if config.is_live:
        if not config.api.private_key or config.api.private_key in {"YOUR_WALLET_PRIVATE_KEY_HERE", "YOUR_PRIVATE_KEY_HERE"}:
            print("❌ Private key not configured!")
            print(f"   Edit {config_path} and add your wallet private key")
            return False
        
        api_fields = [
            bool(config.api.api_key and config.api.api_key != "YOUR_API_KEY_HERE"),
            bool(config.api.api_secret and config.api.api_secret != "YOUR_API_SECRET_HERE"),
            bool(config.api.passphrase and config.api.passphrase != "YOUR_PASSPHRASE_HERE"),
        ]
        if sum(1 for ok in api_fields if ok) not in (0, 3):
            print("❌ Incomplete API credentials.")
            print("   Set api_key + api_secret + passphrase together, or leave all empty for auto-derive.")
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
        rest_url=config.api.polymarket_rest_url,
        ws_url=config.api.polymarket_ws_url,
        gamma_url=config.api.gamma_api_url,
        api_key=config.api.api_key,
        api_secret=config.api.api_secret,
        passphrase=config.api.passphrase,
        private_key=config.api.private_key,
        clob_chain_id=int(config.api.clob_chain_id),
        clob_signature_type=int(config.api.clob_signature_type),
        clob_funder_address=config.api.clob_funder_address or None,
        clob_api_key_nonce=config.api.clob_api_key_nonce,
        timeout=config.api.timeout_seconds,
        dry_run=config.is_dry_run,
        markets_cache_ttl_seconds=getattr(config.cache, "markets_ttl_seconds", 900),
    )
    
    try:
        await client.connect()
        print("✅ HTTP client connected")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    # Test Gamma API (market data)
    print()
    print("📊 Testing Gamma API (market data)...")
    try:
        markets = await client.list_markets({"limit": 5, "closed": "false"})
        print(f"✅ Gamma API working - found {len(markets)} markets")
        
        if markets:
            print("   Sample markets:")
            for m in markets[:3]:
                print(f"   - {m.question[:50]}...")
                print(f"     Volume 24h: ${m.volume_24h:,.0f} | Liquidity: ${m.liquidity:,.0f}")
    except Exception as e:
        print(f"❌ Gamma API error: {e}")
        await client.disconnect()
        return False
    
    # Test authenticated CLOB endpoints (L2 auth)
    if config.is_live:
        print()
        print("💼 Testing authenticated endpoints...")
        try:
            open_orders = await client.get_open_orders()
            print(f"✅ L2 auth working - fetched {len(open_orders)} open orders")
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

