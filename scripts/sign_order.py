#!/usr/bin/env python3
"""
Standalone script to generate Extended DEX order signatures using the Python SDK.
Called from Rust to ensure 100% compatibility with Extended's signature format.
"""

import sys
import json
import math
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timedelta

# Add the vendored Python SDK to the path, anchored to THIS file rather than the
# working directory. Rust invokes this script as a subprocess, so the CWD is
# whatever the caller happened to be in -- the previous relative "../python_sdk-starknet"
# only resolved when launched from scripts/. .parent.parent is this project's
# root (this file lives in scripts/), so it stays inside the project.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python_sdk-starknet"))

from fast_stark_crypto import get_order_msg_hash, sign
from x10.perpetual.configuration import StarknetDomain


def calculate_settlement_expiration(expiry_epoch_millis: int) -> int:
    """Calculate settlement expiration with 14-day buffer"""
    expiry_datetime = datetime.fromtimestamp(expiry_epoch_millis / 1000.0)
    expire_time_with_buffer = expiry_datetime + timedelta(days=14)
    expire_time_as_seconds = math.ceil(expire_time_with_buffer.timestamp())
    return expire_time_as_seconds


def sign_order(
    # Asset IDs (hex strings)
    base_asset_id: str,      # Synthetic asset ID
    quote_asset_id: str,     # Collateral asset ID
    fee_asset_id: str,       # Same as quote_asset_id

    # Scaled amounts (integers)
    base_amount: int,         # Negative for SELL, positive for BUY
    quote_amount: int,        # Negative for BUY, positive for SELL
    fee_amount: int,          # Always positive

    # Order metadata
    position_id: int,         # Vault ID
    nonce: int,
    expiration_epoch_millis: int,  # Milliseconds

    # Keys
    public_key: str,          # Stark public key (hex with 0x prefix)
    private_key: str,         # Stark private key (hex with 0x prefix)

    # Domain separator
    domain_name: str = "Perpetuals",
    domain_version: str = "v0",
    domain_chain_id: str = "SN_MAIN",
    domain_revision: str = "1",
) -> dict:
    """Generate SNIP12 order signature using Extended SDK"""

    # Convert expiration to settlement format (seconds with buffer)
    expiration_seconds = calculate_settlement_expiration(expiration_epoch_millis)

    # Convert hex strings to integers
    base_asset_id_int = int(base_asset_id, 16)
    quote_asset_id_int = int(quote_asset_id, 16)
    fee_asset_id_int = int(fee_asset_id, 16)
    public_key_int = int(public_key, 16)
    private_key_int = int(private_key, 16)

    # Create domain
    domain = StarknetDomain(
        name=domain_name,
        version=domain_version,
        chain_id=domain_chain_id,
        revision=domain_revision,
    )

    # Compute message hash using SDK
    message_hash = get_order_msg_hash(
        position_id=position_id,
        base_asset_id=base_asset_id_int,
        base_amount=base_amount,
        quote_asset_id=quote_asset_id_int,
        quote_amount=quote_amount,
        fee_amount=fee_amount,
        fee_asset_id=fee_asset_id_int,
        expiration=expiration_seconds,
        salt=nonce,
        user_public_key=public_key_int,
        domain_name=domain.name,
        domain_version=domain.version,
        domain_chain_id=domain.chain_id,
        domain_revision=domain.revision,
    )

    # Sign the message hash
    r, s = sign(msg_hash=message_hash, private_key=private_key_int)

    return {
        "r": hex(r),
        "s": hex(s),
        "message_hash": hex(message_hash),
        "expiration_seconds": expiration_seconds,
    }


def main():
    """Read JSON from stdin, compute signature, write JSON to stdout"""
    try:
        # Read input from stdin (do not echo sensitive content)
        input_str = sys.stdin.read()
        input_data = json.loads(input_str)

        # Extract parameters
        result = sign_order(
            base_asset_id=input_data["base_asset_id"],
            quote_asset_id=input_data["quote_asset_id"],
            fee_asset_id=input_data["fee_asset_id"],
            base_amount=int(input_data["base_amount"]),
            quote_amount=int(input_data["quote_amount"]),
            fee_amount=int(input_data["fee_amount"]),
            position_id=int(input_data["position_id"]),
            nonce=int(input_data["nonce"]),
            expiration_epoch_millis=int(input_data["expiration_epoch_millis"]),
            public_key=input_data["public_key"],
            private_key=input_data["private_key"],
            domain_name=input_data.get("domain_name", "Perpetuals"),
            domain_version=input_data.get("domain_version", "v0"),
            domain_chain_id=input_data.get("domain_chain_id", "SN_MAIN"),
            domain_revision=input_data.get("domain_revision", "1"),
        )

        # Write minimal result to stdout (only what the Rust caller requires)
        # Note: message_hash and expiration are not printed to stderr to avoid leaks
        print(json.dumps({"r": result["r"], "s": result["s"]}))
        sys.exit(0)

    except Exception as e:
        # Write error to stderr and exit with error code
        sys.stderr.write(f"Error: {str(e)}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
