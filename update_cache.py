#!/usr/bin/env python3
"""
Update cached data for Streamlit dashboard.

This script fetches all data from various sources and saves to data/cache.json.
Run via GitHub Actions every 15 minutes.
"""

import json
import os
from datetime import datetime

from solana_perps_dashboard import (
    fetch_defillama_volume,
    fetch_global_derivatives,
    fetch_drift_markets_from_api,
    fetch_drift_accurate_traders,
    fetch_drift_liquidations,
    fetch_jupiter_accurate_traders,
    fetch_jupiter_market_breakdown,
    fetch_signature_count,
    distribute_volume_by_trades,
    PROTOCOLS,
)


def update_cache():
    """Fetch all data and save to cache file."""
    print(f"Updating cache at {datetime.utcnow().isoformat()}Z")
    print("=" * 60)

    cache = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "protocols": [],
        "drift_markets": {},
        "jupiter_markets": {},
        "drift_traders_1h": 0,
        "jupiter_traders_6h": 0,
        "total_open_interest": 0,
        "global_derivatives": [],
        "liquidations_1h": {"count": 0, "txns": 0},
    }

    # Fetch volumes from DeFiLlama (fast)
    defillama_volumes = fetch_defillama_volume()

    # Fetch global derivatives for cross-chain comparison
    try:
        cache["global_derivatives"] = fetch_global_derivatives()
    except Exception as e:
        print(f"Global derivatives failed: {e}")
        cache["global_derivatives"] = []

    # Fetch accurate trader counts
    # Drift: use 1h window (complex query, times out with 6h)
    # Jupiter: use 6h window (simple query, fast)
    try:
        cache["drift_traders_1h"] = fetch_drift_accurate_traders(hours=1)
    except Exception as e:
        print(f"Drift traders failed: {e}")
        cache["drift_traders_1h"] = 0

    try:
        cache["jupiter_traders_6h"] = fetch_jupiter_accurate_traders(hours=6)
    except Exception as e:
        print(f"Jupiter traders failed: {e}")
        cache["jupiter_traders_6h"] = 0

    # Fetch liquidation data (1h window to avoid Dune timeout)
    try:
        cache["liquidations_1h"] = fetch_drift_liquidations(hours=1)
    except Exception as e:
        print(f"Liquidations failed: {e}")
        cache["liquidations_1h"] = {"count": 0, "txns": 0}

    # Build protocol metrics
    for protocol_name, config in PROTOCOLS.items():
        print(f"\nProcessing {protocol_name}...")
        volume_data = defillama_volumes.get(config["defillama_name"], {})
        volume_24h = volume_data.get("volume_24h", 0)

        # Get tx count from RPC
        program_id = config["program_id"]
        try:
            tx_count = fetch_signature_count(program_id, 24) if program_id else 0
        except Exception as e:
            print(f"  Tx count failed: {e}")
            tx_count = 0

        # Scale traders to 24h estimate
        if protocol_name == "Drift":
            traders = int(cache["drift_traders_1h"] * 6)  # 1h -> 24h (not linear due to overlap)
        elif protocol_name == "Jupiter Perps":
            traders = int(cache["jupiter_traders_6h"] * 2)  # 6h -> 24h
        else:
            traders = 0

        fees = volume_24h * config["fee_rate"]

        cache["protocols"].append({
            "protocol": protocol_name,
            "volume_24h": volume_24h,
            "volume_7d": volume_data.get("volume_7d", 0),
            "change_1d": volume_data.get("change_1d", 0),
            "change_7d": volume_data.get("change_7d", 0),
            "transactions": tx_count,
            "traders": traders,
            "fees": fees,
        })

    # Fetch Drift market breakdown from API (fast)
    try:
        cache["drift_markets"] = fetch_drift_markets_from_api()
        # Calculate total open interest from Drift markets
        cache["total_open_interest"] = sum(
            m.get("open_interest", 0) * m.get("last_price", 0)
            for m in cache["drift_markets"].values()
        )
    except Exception as e:
        print(f"Drift markets failed: {e}")
        cache["drift_markets"] = {}

    # Fetch Jupiter market breakdown from Dune
    try:
        jupiter_trades = fetch_jupiter_market_breakdown(hours=1)
        jupiter_volume = next(
            (p["volume_24h"] for p in cache["protocols"] if p["protocol"] == "Jupiter Perps"), 0
        )
        jupiter_volumes = distribute_volume_by_trades(jupiter_volume, jupiter_trades)
        cache["jupiter_markets"] = {
            "trades": jupiter_trades,
            "volumes": jupiter_volumes,
        }
    except Exception as e:
        print(f"Jupiter markets failed: {e}")
        cache["jupiter_markets"] = {}

    # Save to file
    os.makedirs("data", exist_ok=True)
    cache_path = "data/cache.json"
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"Cache saved to {cache_path}")
    print(f"Protocols: {len(cache['protocols'])}")
    print(f"Drift markets: {len(cache['drift_markets'])}")
    print(f"Jupiter markets: {len(cache['jupiter_markets'].get('trades', {}))}")
    print(f"Global derivatives: {len(cache['global_derivatives'])}")
    print(f"Total Open Interest: ${cache['total_open_interest']:,.0f}")
    print(f"Drift traders (1h): {cache['drift_traders_1h']}")
    print(f"Jupiter traders (6h): {cache['jupiter_traders_6h']}")


if __name__ == "__main__":
    update_cache()
