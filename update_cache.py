#!/usr/bin/env python3
"""
Update cached data for Streamlit dashboard.

This script fetches all data from various sources and saves to data/cache.json.
Run via GitHub Actions every 15 minutes.

Also maintains historical data in data/history.json for trend analysis.
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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

# Cache file paths
CACHE_PATH = Path("data/cache.json")
HISTORY_PATH = Path("data/history.json")

# History retention: keep 7 days of hourly snapshots
HISTORY_RETENTION_HOURS = 24 * 7  # 168 hours
HISTORY_SNAPSHOT_INTERVAL_MINUTES = 60  # Save snapshot every hour

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


def load_history() -> dict:
    """Load existing history file."""
    if not HISTORY_PATH.exists():
        return {"snapshots": [], "last_snapshot_at": None}
    try:
        with open(HISTORY_PATH) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load history: {e}")
        return {"snapshots": [], "last_snapshot_at": None}


def should_save_snapshot(history: dict) -> bool:
    """Check if enough time has passed to save a new snapshot."""
    last_snapshot = history.get("last_snapshot_at")
    if not last_snapshot:
        return True

    try:
        last_time = datetime.fromisoformat(last_snapshot.replace("Z", "+00:00"))
        elapsed_minutes = (datetime.now(tz=last_time.tzinfo) - last_time).total_seconds() / 60
        return elapsed_minutes >= HISTORY_SNAPSHOT_INTERVAL_MINUTES
    except (ValueError, TypeError):
        return True


def extract_snapshot(cache: dict) -> dict:
    """Extract key metrics for historical snapshot."""
    now = datetime.utcnow().isoformat() + "Z"

    # Protocol-level metrics
    protocols = {}
    for p in cache.get("protocols", []):
        name = p.get("protocol", "")
        if name:
            protocols[name] = {
                "volume_24h": p.get("volume_24h", 0),
                "traders": p.get("traders", 0),
                "fees": p.get("fees", 0),
            }

    # Aggregate metrics
    total_volume = sum(p.get("volume_24h", 0) for p in cache.get("protocols", []))
    total_traders = sum(p.get("traders", 0) for p in cache.get("protocols", []))
    total_oi = cache.get("total_open_interest", 0)

    # Top market funding rates
    drift_markets = cache.get("drift_markets", {})
    funding_rates = {}
    for market, data in drift_markets.items():
        if data.get("volume", 0) > 10000:  # Only significant markets
            funding_rates[market] = data.get("funding_rate", 0)

    # Store 1h liquidation count for aggregation
    liquidations_1h = cache.get("time_windows", {}).get("1h", {}).get("liquidations", {})

    return {
        "timestamp": now,
        "total_volume_24h": total_volume,
        "total_traders_24h": total_traders,
        "total_open_interest": total_oi,
        "protocols": protocols,
        "funding_rates": funding_rates,
        "liquidations_1h": liquidations_1h,  # Store hourly liquidation count for 24h aggregation
    }


def prune_old_snapshots(history: dict) -> dict:
    """Remove snapshots older than retention period."""
    cutoff = datetime.utcnow() - timedelta(hours=HISTORY_RETENTION_HOURS)
    cutoff_str = cutoff.isoformat() + "Z"

    original_count = len(history.get("snapshots", []))
    history["snapshots"] = [
        s for s in history.get("snapshots", [])
        if s.get("timestamp", "") >= cutoff_str
    ]
    pruned_count = original_count - len(history["snapshots"])

    if pruned_count > 0:
        logger.info(f"Pruned {pruned_count} old snapshots (>{HISTORY_RETENTION_HOURS}h)")

    return history


def aggregate_liquidations_from_history(hours: int = 24) -> dict:
    """Aggregate liquidation counts from historical snapshots.

    Instead of running a slow 24h Dune query, sum up hourly liquidation counts
    from stored snapshots. This avoids timeouts for longer time windows.
    """
    history = load_history()
    snapshots = history.get("snapshots", [])

    if not snapshots:
        logger.warning("No historical snapshots available for liquidation aggregation")
        return {"count": 0, "txns": 0, "error": "No historical data"}

    # Get snapshots from the last N hours
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    cutoff_str = cutoff.isoformat() + "Z"

    recent_snapshots = [
        s for s in snapshots
        if s.get("timestamp", "") >= cutoff_str
    ]

    if len(recent_snapshots) < hours * 0.5:  # Need at least half the expected snapshots
        logger.warning(f"Insufficient snapshots for {hours}h aggregation: {len(recent_snapshots)} found")
        return {
            "count": 0,
            "txns": 0,
            "error": f"Insufficient data ({len(recent_snapshots)} snapshots for {hours}h)"
        }

    # Sum up liquidation counts (note: each snapshot is ~1h of data)
    total_count = 0
    total_txns = 0
    valid_snapshots = 0

    for s in recent_snapshots:
        liq_data = s.get("liquidations_1h", {})
        if liq_data and not liq_data.get("error"):
            total_count += liq_data.get("count", 0)
            total_txns += liq_data.get("txns", 0)
            valid_snapshots += 1

    logger.info(f"Aggregated {hours}h liquidations from {valid_snapshots} snapshots: {total_count} count, {total_txns} txns")

    return {
        "count": total_count,
        "txns": total_txns,
        "aggregated_from": valid_snapshots,
    }


def save_history_snapshot(cache: dict) -> bool:
    """Save a historical snapshot if enough time has passed."""
    history = load_history()

    if not should_save_snapshot(history):
        logger.info("Skipping history snapshot (interval not reached)")
        return False

    # Extract and append new snapshot
    snapshot = extract_snapshot(cache)
    history["snapshots"].append(snapshot)
    history["last_snapshot_at"] = snapshot["timestamp"]

    # Prune old data
    history = prune_old_snapshots(history)

    # Save
    try:
        os.makedirs("data", exist_ok=True)
        with open(HISTORY_PATH, "w") as f:
            json.dump(history, f, indent=2)
        logger.info(f"History snapshot saved ({len(history['snapshots'])} total snapshots)")
        return True
    except Exception as e:
        logger.error(f"Failed to save history: {e}")
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
    fetch_pacifica_markets,
    fetch_flashtrade_traders,
    fetch_adrena_traders,
    fetch_jupiter_market_breakdown,
    fetch_signature_count,
    distribute_volume_by_trades,
    fetch_pacifica_pnl_leaderboard,
    fetch_jupiter_pnl_leaderboard,
    PROTOCOL_METADATA,
)


def fetch_time_window_data(hours: int) -> dict:
    """Fetch all data for a single time window.

    Runs all queries for the window in parallel and returns the results.
    This function is designed to be called from a ThreadPoolExecutor.
    """
    window_key = f"{hours}h"
    logger.info(f"Fetching {window_key} window data...")
    result = {}

    # Build list of queries to run for this window
    queries = {
        "drift_traders": lambda h=hours: fetch_drift_accurate_traders(hours=h),
        "jupiter_traders": lambda h=hours: fetch_jupiter_accurate_traders(hours=h),
        "pacifica_traders": lambda h=hours: fetch_pacifica_traders(hours=h),
        "flashtrade_traders": lambda h=hours: fetch_flashtrade_traders(hours=h),
        "adrena_traders": lambda h=hours: fetch_adrena_traders(hours=h),
    }
    if hours <= 8:
        # For shorter windows, query Dune directly (fast)
        queries["liquidations"] = lambda h=hours: fetch_drift_liquidations(hours=h)
    if hours <= 24:
        queries["wallet_overlap"] = lambda h=hours: fetch_cross_platform_wallets(hours=h)

    # Run queries in parallel within this window
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_name = {executor.submit(fn): name for name, fn in queries.items()}
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result[name] = future.result()
            except Exception as e:
                logger.error(f"{name} ({window_key}) failed: {e}")
                if name in ("drift_traders", "jupiter_traders", "pacifica_traders", "flashtrade_traders", "adrena_traders"):
                    result[name] = 0
                    result[f"{name}_error"] = str(e)
                elif name == "liquidations":
                    result[name] = {"count": 0, "txns": 0, "error": str(e)}
                elif name == "wallet_overlap":
                    result[name] = {"multi_platform": 0, "drift_only": 0, "jupiter_only": 0, "error": str(e)}

    # For 24h liquidations, use historical aggregation instead of slow Dune query
    if hours == 24:
        logger.info("Using historical aggregation for 24h liquidations...")
        result["liquidations"] = aggregate_liquidations_from_history(hours=24)
    elif hours > 8 and hours != 24:
        # Other long windows still skip liquidations
        result["liquidations"] = {"count": 0, "txns": 0, "error": "Skipped (query timeout)"}

    logger.info(f"Completed {window_key} window data")
    return result


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
        "pacifica_markets": {},
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
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_name = {
            executor.submit(fetch_defillama_volume): "defillama",
            executor.submit(fetch_global_derivatives): "global",
            executor.submit(fetch_drift_markets_from_api): "drift_markets",
            executor.submit(fetch_pacifica_markets): "pacifica_markets",
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
                elif name == "pacifica_markets":
                    cache["pacifica_markets"] = result
            except Exception as e:
                logger.error(f"{name} failed: {e}")
                if name == "global":
                    cache["global_derivatives"] = []
                elif name == "drift_markets":
                    cache["drift_markets"] = {}
                elif name == "pacifica_markets":
                    cache["pacifica_markets"] = {}

    # Fetch ALL time windows in parallel (major performance improvement)
    # Previously: windows ran sequentially (~10 min total)
    # Now: all windows run concurrently (~3 min total)
    logger.info(f"Fetching all {len(TIME_WINDOWS)} time windows in parallel...")
    with ThreadPoolExecutor(max_workers=len(TIME_WINDOWS)) as executor:
        future_to_hours = {
            executor.submit(fetch_time_window_data, hours): hours
            for hours in TIME_WINDOWS
        }
        for future in as_completed(future_to_hours):
            hours = future_to_hours[future]
            window_key = f"{hours}h"
            try:
                cache["time_windows"][window_key] = future.result()
            except Exception as e:
                logger.error(f"Time window {window_key} failed completely: {e}")
                cache["time_windows"][window_key] = {
                    "drift_traders": 0,
                    "jupiter_traders": 0,
                    "pacifica_traders": 0,
                    "flashtrade_traders": 0,
                    "adrena_traders": 0,
                    "liquidations": {"count": 0, "txns": 0, "error": str(e)},
                    "wallet_overlap": {"multi_platform": 0, "drift_only": 0, "jupiter_only": 0, "error": str(e)},
                }

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

    # Fetch P&L leaderboard data from Pacifica and Jupiter
    logger.info("Fetching P&L leaderboard data...")
    cache["pnl_leaderboard"] = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_protocol = {
            executor.submit(fetch_pacifica_pnl_leaderboard, 50): "pacifica",
            executor.submit(fetch_jupiter_pnl_leaderboard, 50): "jupiter",
        }
        for future in as_completed(future_to_protocol):
            protocol = future_to_protocol[future]
            try:
                cache["pnl_leaderboard"][protocol] = future.result()
                winners = len(cache["pnl_leaderboard"][protocol].get("top_winners", []))
                losers = len(cache["pnl_leaderboard"][protocol].get("top_losers", []))
                logger.info(f"P&L {protocol}: {winners} winners, {losers} losers")
            except Exception as e:
                logger.error(f"P&L leaderboard {protocol} failed: {e}")
                cache["pnl_leaderboard"][protocol] = {"top_winners": [], "top_losers": []}

    # Save to file (with validation and fallback)
    logger.info("=" * 60)
    cache_saved = save_cache(cache, old_cache)

    # Save historical snapshot (hourly) if cache was valid
    if cache_saved:
        save_history_snapshot(cache)

    logger.info(f"Protocols: {len(cache['protocols'])}")
    logger.info(f"Drift markets: {len(cache['drift_markets'])}")
    logger.info(f"Jupiter markets: {len(cache['jupiter_markets'].get('trades', {}))}")
    logger.info(f"Pacifica markets: {len(cache['pacifica_markets'])}")
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
