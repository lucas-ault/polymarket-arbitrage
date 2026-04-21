#!/usr/bin/env python3
"""
Create or derive Polymarket CLOB API credentials (L2) from L1 key.

Usage:
  export POLYMARKET_PRIVATE_KEY=0x...
  python3 create_polymarket_api_creds.py

Optional env vars:
  POLYMARKET_HOST=https://clob.polymarket.com
  POLYMARKET_CHAIN_ID=137
  POLYMARKET_SIGNATURE_TYPE=0
  POLYMARKET_FUNDER_ADDRESS=0x...
  POLYMARKET_API_KEY_NONCE=0
"""

import os
import sys

from eth_account import Account
from py_clob_client.client import ClobClient


def main() -> int:
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    if not private_key:
        print("Missing POLYMARKET_PRIVATE_KEY")
        return 1

    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com").strip()
    chain_id = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
    signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
    funder = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip()
    if not funder:
        funder = Account.from_key(private_key).address

    nonce_env = os.getenv("POLYMARKET_API_KEY_NONCE", "").strip()
    nonce = int(nonce_env) if nonce_env else None

    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
    )
    creds = client.create_or_derive_api_creds(nonce)

    print("# Save these in your shell profile or deployment secret store")
    print(f'export POLYMARKET_API_KEY="{creds.api_key}"')
    print(f'export POLYMARKET_API_SECRET="{creds.api_secret}"')
    print(f'export POLYMARKET_PASSPHRASE="{creds.api_passphrase}"')
    print(f'export POLYMARKET_FUNDER_ADDRESS="{funder}"')
    return 0


if __name__ == "__main__":
    sys.exit(main())
