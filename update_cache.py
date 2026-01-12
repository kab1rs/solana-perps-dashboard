#!/usr/bin/env python3
"""
Update cached data for Streamlit dashboard.

This script fetches all data from various sources and saves to data/cache.json.
Run via GitHub Actions every 15 minutes.
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Time windows to cache (in hours)
TIME_WINDOWS = [1, 4, 8, 24]

from solana_perps_dashboard import (
    fetch_defillama_volume,
    fetch_global_derivatives,
    fetch_drift_markets_from_api,
    fetch_drift_accurate_traders,
    fetch_drift_liquidations,
    fetch_cross_platform_wallets,
    fetch_jupiter_accurate_traders,
    fetch_jupiter_market_breakdown,
    fetch_signature_count,
    distribute_volume_by_trades,
    PROTOCOLS,
)


def update_cache():
    """Fetch all data and save to cache file."""
    logger.info(f"Updating cache at {datetime.utcnow().isoformat()}Z")
    logger.info("=" * 60)

    cache = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "protocols": [],
        "drift_markets": {},
        "jupiter_markets": {},
        "total_open_interest": 0,
        "global_derivatives": [],
        "time_windows": {},
        # Legacy keys for backward compatibility
        "drift_traders_1h": 0,
        "jupiter_traders_1h": 0,
        "liquidations_1h": {"count": 0, "txns": 0},
        "wallet_overlap": {"multi_platform": 0, "drift_only": 0, "jupiter_only": 0},
    }

    # Fetch fast APIs in parallel
    logger.info("Fetching fast APIs in parallel...")
    defillama_volumes = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_name = {
            executor.submit(fetch_defillama_volume): "defillama",
            executor.submit(fetch_global_derivatives): "global",
            executor.submit(fetch_drift_markets_from_api): "drift_markets",
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                if name == "defillama":
                    defillama_volumes = result
                elif name == "global":
                    cache["global_derivatives"] = result
                elif name == "drift_markets":
                    cache["drift_markets"] = result
                    cache["total_open_interest"] = sum(
                        m.get("open_interest", 0) * m.get("last_price", 0)
                        for m in result.values()
                    )
            except Exception as e:
                logger.error(f"{name} failed: {e}")
                if name == "global":
                    cache["global_derivatives"] = []
                elif name == "drift_markets":
                    cache["drift_markets"] = {}

    # Fetch time-windowed Dune queries in parallel per window
    for hours in TIME_WINDOWS:
        window_key = f"{hours}h"
        logger.info(f"Fetching {window_key} window data (parallel)...")
        cache["time_windows"][window_key] = {}

        # Build list of queries to run for this window
        queries = {
            "drift_traders": lambda h=hours: fetch_drift_accurate_traders(hours=h),
            "jupiter_traders": lambda h=hours: fetch_jupiter_accurate_traders(hours=h),
        }
        if hours <= 8:
            queries["liquidations"] = lambda h=hours: fetch_drift_liquidations(hours=h)
        if hours <= 4:
            queries["wallet_overlap"] = lambda h=hours: fetch_cross_platform_wallets(hours=h)

        # Run queries in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_name = {executor.submit(fn): name for name, fn in queries.items()}
            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    cache["time_windows"][window_key][name] = future.result()
                except Exception as e:
                    logger.error(f"{name} ({window_key}) failed: {e}")
                    if name == "drift_traders":
                        cache["time_windows"][window_key][name] = 0
                        cache["time_windows"][window_key][f"{name}_error"] = str(e)
                    elif name == "jupiter_traders":
                        cache["time_windows"][window_key][name] = 0
                        cache["time_windows"][window_key][f"{name}_error"] = str(e)
                    elif name == "liquidations":
                        cache["time_windows"][window_key][name] = {"count": 0, "txns": 0, "error": str(e)}
                    elif name == "wallet_overlap":
                        cache["time_windows"][window_key][name] = {"multi_platform": 0, "drift_only": 0, "jupiter_only": 0, "error": str(e)}

        # Set defaults for skipped queries
        if hours > 8:
            cache["time_windows"][window_key]["liquidations"] = {"count": 0, "txns": 0, "error": "Skipped (query timeout)"}
        if hours > 4:
            cache["time_windows"][window_key]["wallet_overlap"] = {"multi_platform": 0, "drift_only": 0, "jupiter_only": 0, "error": "Skipped (query timeout)"}

    # Set legacy keys from 1h window for backward compatibility
    if "1h" in cache["time_windows"]:
        cache["drift_traders_1h"] = cache["time_windows"]["1h"].get("drift_traders", 0)
        cache["jupiter_traders_1h"] = cache["time_windows"]["1h"].get("jupiter_traders", 0)
        cache["liquidations_1h"] = cache["time_windows"]["1h"].get("liquidations", {"count": 0, "txns": 0})
        cache["wallet_overlap"] = cache["time_windows"]["1h"].get("wallet_overlap", {})

    # Fetch signature counts in parallel
    logger.info("Fetching signature counts in parallel...")
    tx_counts = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_protocol = {
            executor.submit(fetch_signature_count, config["program_id"], 24): name
            for name, config in PROTOCOLS.items()
            if config["program_id"]
        }
        for future in as_completed(future_to_protocol):
            protocol_name = future_to_protocol[future]
            try:
                tx_counts[protocol_name] = future.result()
            except Exception as e:
                logger.error(f"Tx count for {protocol_name} failed: {e}")
                tx_counts[protocol_name] = 0

    # Build protocol metrics
    for protocol_name, config in PROTOCOLS.items():
        logger.info(f"Processing {protocol_name}...")
        volume_data = defillama_volumes.get(config["defillama_name"], {})
        volume_24h = volume_data.get("volume_24h", 0)
        tx_count = tx_counts.get(protocol_name, 0)

        # Use actual 24h trader counts from Dune
        if protocol_name == "Drift":
            traders = cache["time_windows"].get("24h", {}).get("drift_traders", 0)
        elif protocol_name == "Jupiter Perps":
            traders = cache["time_windows"].get("24h", {}).get("jupiter_traders", 0)
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
        logger.error(f"Jupiter markets failed: {e}")
        cache["jupiter_markets"] = {}

    # Save to file
    os.makedirs("data", exist_ok=True)
    cache_path = "data/cache.json"
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)

    logger.info("=" * 60)
    logger.info(f"Cache saved to {cache_path}")
    logger.info(f"Protocols: {len(cache['protocols'])}")
    logger.info(f"Drift markets: {len(cache['drift_markets'])}")
    logger.info(f"Jupiter markets: {len(cache['jupiter_markets'].get('trades', {}))}")
    logger.info(f"Global derivatives: {len(cache['global_derivatives'])}")
    logger.info(f"Total Open Interest: ${cache['total_open_interest']:,.0f}")
    logger.info("Time window data:")
    for window_key, data in cache["time_windows"].items():
        drift_t = data.get("drift_traders", 0)
        jup_t = data.get("jupiter_traders", 0)
        liq = data.get("liquidations", {}).get("count", 0)
        multi = data.get("wallet_overlap", {}).get("multi_platform", 0)
        logger.info(f"  {window_key}: Drift={drift_t}, Jupiter={jup_t}, Liqs={liq}, MultiPlatform={multi}")


if __name__ == "__main__":
    update_cache()
