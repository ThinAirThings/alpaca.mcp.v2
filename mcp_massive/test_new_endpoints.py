#!/usr/bin/env python3
"""
Test script for the three new options endpoints.
"""
import asyncio
from src.mcp_massive.server import list_options_contracts, get_options_contract, list_snapshot_options_chain


async def test_list_options_contracts():
    """Test listing options contracts for AAPL."""
    print("\n=== Testing list_options_contracts ===")
    try:
        result = await list_options_contracts(
            underlying_ticker="AAPL",
            contract_type="call",
            expiration_date_gte="2025-11-01",
            limit=5
        )
        print("Success! First 500 chars of result:")
        print(result[:500])
        print("...")
    except Exception as e:
        print(f"Error: {e}")


async def test_get_options_contract():
    """Test getting a specific options contract."""
    print("\n=== Testing get_options_contract ===")
    try:
        # First, get some contracts to find a valid ticker
        contracts_result = await list_options_contracts(
            underlying_ticker="AAPL",
            contract_type="call",
            limit=1
        )
        print("Listing contracts to find a ticker...")
        print(contracts_result[:300])

        # Try a known format ticker (you may need to adjust this)
        print("\nTesting with a sample ticker...")
        result = await get_options_contract(
            ticker="O:AAPL251121C00230000"
        )
        print("Success! Result:")
        print(result[:500])
    except Exception as e:
        print(f"Error: {e}")


async def test_list_snapshot_options_chain():
    """Test getting options chain snapshot for AAPL."""
    print("\n=== Testing list_snapshot_options_chain ===")
    try:
        result = await list_snapshot_options_chain(
            underlying_ticker="AAPL",
            strike_price_gte=220.0,
            strike_price_lte=240.0,
            expiration_date_gte="2025-11-01",
            contract_type="call",
            limit=3
        )
        print("Success! First 500 chars of result:")
        print(result[:500])
        print("...")
    except Exception as e:
        print(f"Error: {e}")


async def main():
    """Run all tests."""
    print("Testing new options endpoints...")

    await test_list_options_contracts()
    await test_get_options_contract()
    await test_list_snapshot_options_chain()

    print("\n=== All tests complete! ===")


if __name__ == "__main__":
    asyncio.run(main())
