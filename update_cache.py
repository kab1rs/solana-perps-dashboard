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
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Time windows to cache (in hours)
TIME_WINDOWS = [1, 4, 8, 24]

# Cache file path
CACHE_PATH = Path("data/cache.json")

# Required keys for valid cache
REQUIRED_CACHE_KEYS = ["protocols", "drift_markets", "time_windows", "updated_at"]


def load_existing_cache() -> Optional[dict]:
    """Load existing cache file as fallback."""
    if not CACHE_PATH.exists():
        return None
    try:
        with open(CACHE_PATH) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load existing cache: {e}")
        return None


def validate_cache(cache: dict) -> bool:
    """Validate cache has required data."""
    # Check required keys exist
    for key in REQUIRED_CACHE_KEYS:
        if key not in cache:
            logger.error(f"Cache missing required key: {key}")
            return False

    # Must have at least some protocol data
    if not cache.get("protocols"):
        logger.error("Cache has no protocol data")
        return False

    # Must have Drift markets (our primary data source)
    if not cache.get("drift_markets"):
        logger.error("Cache has no Drift market data")
        return False

    return True


def save_cache(cache: dict, old_cache: Optional[dict]) -> bool:
    """Save cache if valid, otherwise keep old cache."""
    if validate_cache(cache):
        os.makedirs("data", exist_ok=True)
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
        logger.info(f"Cache saved to {CACHE_PATH}")
        return True
    else:
        logger.error("New cache is invalid!")
        if old_cache and validate_cache(old_cache):
            logger.info("Keeping previous cache")
            # Update timestamp to indicate we tried
            old_cache["last_update_attempt"] = datetime.utcnow().isoformat() + "Z"
            with open(CACHE_PATH, "w") as f:
                json.dump(old_cache, f, indent=2)
        return False

from solana_perps_dashboard import (
    fetch_defillama_volume,
    fetch_global_derivatives,
    fetch_drift_markets_from_api,
    fetch_drift_accurate_traders,
    fetch_drift_liquidations,
    fetch_cross_platform_wallets,
    fetch_jupiter_accurate_traders,
    fetch_pacifica_traders,
    fetch_flashtrade_traders,
    fetch_adrena_traders,
    fetch_jupiter_market_breakdown,
    fetch_signature_count,
    distribute_volume_by_trades,
    PROTOCOL_METADATA,
)


def update_cache():
    """Fetch all data and save to cache file."""
    logger.info(f"Updating cache at {datetime.utcnow().isoformat()}Z")
    logger.info("=" * 60)

    # Load existing cache as fallback
    old_cache = load_existing_cache()
    if old_cache:
        logger.info(f"Loaded existing cache from {old_cache.get('updated_at', 'unknown')}")

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
            "pacifica_traders": lambda h=hours: fetch_pacifica_traders(hours=h),
            "flashtrade_traders": lambda h=hours: fetch_flashtrade_traders(hours=h),
            "adrena_traders": lambda h=hours: fetch_adrena_traders(hours=h),
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
                    if name in ("drift_traders", "jupiter_traders", "pacifica_traders", "flashtrade_traders", "adrena_traders"):
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

    # Fetch signature counts in parallel for protocols with program IDs
    logger.info("Fetching signature counts in parallel...")
    tx_counts = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_protocol = {
            executor.submit(fetch_signature_count, metadata["program_id"], 24): name
            for name, metadata in PROTOCOL_METADATA.items()
            if metadata.get("program_id")
        }
        for future in as_completed(future_to_protocol):
            protocol_name = future_to_protocol[future]
            try:
                tx_counts[protocol_name] = future.result()
            except Exception as e:
                logger.error(f"Tx count for {protocol_name} failed: {e}")
                tx_counts[protocol_name] = 0

    # Build protocol metrics dynamically from DeFiLlama data
    for protocol_name, volume_data in defillama_volumes.items():
        volume_24h = volume_data.get("volume_24h", 0)
        if volume_24h < 1000:  # Skip tiny protocols
            continue

        logger.info(f"Processing {protocol_name}...")

        # Get extra metadata if available
        metadata = PROTOCOL_METADATA.get(protocol_name, {})
        fee_rate = metadata.get("fee_rate", 0.0005)  # Default 0.05%
        tx_count = tx_counts.get(protocol_name, 0)

        # Use actual 24h trader counts from Dune for known protocols
        if protocol_name == "Drift Trade":
            traders = cache["time_windows"].get("24h", {}).get("drift_traders", 0)
        elif protocol_name == "Jupiter Perpetual Exchange":
            traders = cache["time_windows"].get("24h", {}).get("jupiter_traders", 0)
        elif protocol_name == "Pacifica":
            traders = cache["time_windows"].get("24h", {}).get("pacifica_traders", 0)
        elif protocol_name == "FlashTrade":
            traders = cache["time_windows"].get("24h", {}).get("flashtrade_traders", 0)
        elif protocol_name == "Adrena Protocol":
            traders = cache["time_windows"].get("24h", {}).get("adrena_traders", 0)
        else:
            traders = 0  # No Dune query for this protocol

        fees = volume_24h * fee_rate

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
            (p["volume_24h"] for p in cache["protocols"] if p["protocol"] == "Jupiter Perpetual Exchange"), 0
        )
        jupiter_volumes = distribute_volume_by_trades(jupiter_volume, jupiter_trades)
        cache["jupiter_markets"] = {
            "trades": jupiter_trades,
            "volumes": jupiter_volumes,
        }
    except Exception as e:
        logger.error(f"Jupiter markets failed: {e}")
        cache["jupiter_markets"] = {}

    # Save to file (with validation and fallback)
    logger.info("=" * 60)
    save_cache(cache, old_cache)
    logger.info(f"Protocols: {len(cache['protocols'])}")
    logger.info(f"Drift markets: {len(cache['drift_markets'])}")
    logger.info(f"Jupiter markets: {len(cache['jupiter_markets'].get('trades', {}))}")
    logger.info(f"Global derivatives: {len(cache['global_derivatives'])}")
    logger.info(f"Total Open Interest: ${cache['total_open_interest']:,.0f}")
    logger.info("Time window data:")
    for window_key, data in cache["time_windows"].items():
        drift_t = data.get("drift_traders", 0)
        jup_t = data.get("jupiter_traders", 0)
        paci_t = data.get("pacifica_traders", 0)
        flash_t = data.get("flashtrade_traders", 0)
        adrena_t = data.get("adrena_traders", 0)
        liq = data.get("liquidations", {}).get("count", 0)
        multi = data.get("wallet_overlap", {}).get("multi_platform", 0)
        logger.info(f"  {window_key}: Drift={drift_t}, Jupiter={jup_t}, Pacifica={paci_t}, Flash={flash_t}, Adrena={adrena_t}, Liqs={liq}, Multi={multi}")


if __name__ == "__main__":
    update_cache()
