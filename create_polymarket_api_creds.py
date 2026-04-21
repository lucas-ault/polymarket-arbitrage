#!/usr/bin/env python3
"""
Print setup instructions for polymarket.us API credentials.

The legacy CLOB wallet-derived credential flow is not used for polymarket.us.
Generate keys in https://polymarket.us/developer and export them as env vars.
"""

import sys


def main() -> int:
    print("polymarket.us credentials are created in the developer portal:")
    print("  https://polymarket.us/developer")
    print("")
    print("Export these values before running in live mode:")
    print('  export POLYMARKET_KEY_ID="your-key-id"')
    print('  export POLYMARKET_SECRET_KEY="your-secret-key"')
    print("")
    print("Then run:")
    print("  python3 test_connection.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
