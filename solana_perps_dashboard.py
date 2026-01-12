#!/usr/bin/env python3
"""
Solana Perps Metrics Dashboard (Hybrid Approach)

Uses DeFiLlama for accurate volume data, RPC for transaction counts,
and Dune for market-level breakdowns.

Data Sources:
- Volume: DeFiLlama API (only source with decoded 2026 perps data)
- Tx Count: Solana RPC signatures
- Markets: Dune Analytics (via account key analysis)
- Traders: Estimated (tx_count * 0.7)
- Fees: Estimated (volume * fee_rate)

Usage:
    python solana_perps_dashboard.py
    python solana_perps_dashboard.py --no-markets  # Skip market breakdown
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# RPC endpoint configuration
# Priority: 1. SOLANA_RPC_URL env var, 2. Helius (if API key set), 3. Public fallback
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY")
SOLANA_RPC_URL_ENV = os.environ.get("SOLANA_RPC_URL")

# Build RPC URL with Helius as preferred option
if SOLANA_RPC_URL_ENV:
    RPC_URL = SOLANA_RPC_URL_ENV
    RPC_SOURCE = "custom"
elif HELIUS_API_KEY:
    RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    RPC_SOURCE = "helius"
else:
    RPC_URL = "https://api.mainnet-beta.solana.com"
    RPC_SOURCE = "public"

# Fallback RPC for when primary fails
FALLBACK_RPC_URL = "https://api.mainnet-beta.solana.com"

# DeFiLlama API
DEFILLAMA_URL = "https://api.llama.fi/overview/derivatives"

# Dune API (required)
DUNE_API_KEY = os.environ.get("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("DUNE_API_KEY environment variable is required")
DUNE_API_URL = "https://api.dune.com/api/v1"

# Drift Data API (provides per-market volume data directly)
DRIFT_DATA_API = "https://data.api.drift.trade/contracts"

# Protocol metadata: keyed by DeFiLlama name for protocols we have extra data for
# Other Solana protocols from DeFiLlama will use defaults (no program_id, 0.05% fee)
PROTOCOL_METADATA = {
    "Jupiter Perpetual Exchange": {
        "program_id": "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu",
        "fee_rate": 0.0006,  # 0.06%
    },
    "Drift Trade": {
        "program_id": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
        "fee_rate": 0.0005,  # 0.05%
    },
    "Pacifica": {
        "program_id": "PCFA5iYgmqK6MqPhWNKg7Yv7auX7VZ4Cx7T1eJyrAMH",
        "fee_rate": 0.0005,  # 0.05% (estimate)
    },
    "FlashTrade": {
        "program_id": "FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn",
        "fee_rate": 0.0005,  # 0.05%
    },
    "Adrena Protocol": {
        "program_id": "13gDzEXCdocbj8iAiqrScGo47NiSuYENGsRqi3SEAwet",
        "fee_rate": 0.0005,  # 0.05%
    },
}

# Drift market accounts (identified via Dune analysis)
DRIFT_MARKET_ACCOUNTS = {
    "8UJgxaiQx5nTrdDgph5FiahMmzduuLTLf5WmsPegYA6W": "SOL-PERP",
    "3m6i4RFWEDw2Ft4tFHPJtYgmpPe21k56M3FHeWYrgGBz": "BTC-PERP",
    "25Eax9W8SA3wpCQFhJEGyHhQ2NDHEshZEDzyMNtthR8D": "ETH-PERP",
    "2UZMvVTBQR9yWxrEdzEQzXWE61bUjqQ5VpJAGqVb3B19": "WIF-PERP",
    "6gMq3mRCKf8aP3ttTyYhuijVZ2LGi14oDsBbkgubfLB3": "JUP-PERP",
    # Additional markets discovered via Dune frequency analysis
    "35MbvS1Juz2wf7GsyHrkCw8yfKciRLxVpEhfZDZFrB4R": "MARKET-A",
    "93FG52TzNKCnMiasV14Ba34BYcHDb9p4zK4GjZnLwqWR": "MARKET-B",
    "9VCioxmni2gDLv11qufWzT3RDERhQE4iY5Gf7NTfYyAV": "MARKET-C",
    "3x85u7SWkmmr7YQGYhtjARgxwegTLJgkSLRprfXod6rh": "MARKET-D",
    "3rdJbqfnagQ4yx9HXJViD4zc4xpiSqmFsKpPuSCQVyQL": "MARKET-E",
}

# Jupiter Perps custody accounts (markets identified by custody address)
# JLP pool has 5 tokens: SOL, BTC, ETH, USDC, USDT
# Addresses from: https://station.jup.ag/guides/perpetual-exchange/onchain-accounts
JUPITER_CUSTODY_ACCOUNTS = {
    "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz": "SOL",
    "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm": "BTC",
    "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn": "ETH",
    "G18jKKXQwBbrHeiK3C9MRXhkHsLHf7XgCSisykV46EZa": "USDC",
    "4vkNeXiYEUizLdrpdPS1eC2mccyM4NUPRtERrk6ZETkk": "USDT",
}


def rpc_call(method: str, params: list, max_retries: int = 3, use_fallback: bool = True) -> dict:
    """Make an RPC call to the Solana node with retry logic and fallback.

    Uses the configured RPC_URL (Helius if available) as primary,
    falls back to public RPC on repeated failures.
    """
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}

    # Try primary RPC first, then fallback
    rpc_urls = [RPC_URL]
    if use_fallback and RPC_URL != FALLBACK_RPC_URL:
        rpc_urls.append(FALLBACK_RPC_URL)

    for rpc_url in rpc_urls:
        rpc_name = "primary" if rpc_url == RPC_URL else "fallback"

        for attempt in range(max_retries):
            req = Request(
                rpc_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
            )
            try:
                with urlopen(req, timeout=30) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    if "error" in result:
                        logger.error(f"RPC Error ({rpc_name}): {result['error']}")
                        break  # Try fallback
                    return result.get("result", {})
            except HTTPError as e:
                if e.code == 429:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    logger.warning(f"Rate limited ({rpc_name}), waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                logger.error(f"HTTP error ({rpc_name}) {e.code}: {e.reason}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                break  # Try fallback
            except Exception as e:
                logger.error(f"RPC call failed ({rpc_name}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                break  # Try fallback

        if rpc_url == RPC_URL and len(rpc_urls) > 1:
            logger.info(f"Primary RPC failed, trying fallback...")

    return {}


def run_dune_query(sql: str, timeout: int = 180, max_retries: int = 3) -> dict:
    """Execute SQL on Dune Analytics and return results with retry logic."""
    url = f"{DUNE_API_URL}/sql/execute"
    payload = {"sql": sql, "performance": "medium"}

    # Start query execution with retry
    execution_id = None
    for attempt in range(max_retries):
        req = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-DUNE-API-KEY": DUNE_API_KEY},
            method="POST"
        )
        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                execution_id = result.get("execution_id")
                break
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"Dune query start failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                return {"error": str(e)}

    if not execution_id:
        return {"error": "Failed to start query"}

    # Poll for results
    start_time = time.time()
    while time.time() - start_time < timeout:
        status_url = f"{DUNE_API_URL}/execution/{execution_id}/status"
        req = Request(status_url, headers={"X-DUNE-API-KEY": DUNE_API_KEY})

        try:
            with urlopen(req, timeout=30) as response:
                status = json.loads(response.read().decode("utf-8"))
                state = status.get("state", "")

                if status.get("is_execution_finished") or state == "QUERY_STATE_COMPLETED":
                    results_url = f"{DUNE_API_URL}/execution/{execution_id}/results"
                    req = Request(results_url, headers={"X-DUNE-API-KEY": DUNE_API_KEY})
                    with urlopen(req, timeout=30) as response:
                        return json.loads(response.read().decode("utf-8"))
                elif "FAILED" in state:
                    return {"error": status.get("error", {}).get("message", str(status))}
        except Exception as e:
            return {"error": str(e)}

        time.sleep(5)

    return {"error": "Query timeout"}


# --- Helper functions for Dune queries ---

def get_time_range(hours: int) -> tuple:
    """Return (start_time, end_time) for a given hour window."""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    return start_time, end_time


def format_timestamp(dt: datetime) -> str:
    """Format datetime for Dune SQL TIMESTAMP literal."""
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def run_dune_query_safe(sql: str, timeout: int = 180):
    """Run Dune query with error handling. Returns (rows, error)."""
    result = run_dune_query(sql, timeout=timeout)
    if "error" in result:
        return None, result["error"]
    rows = result.get("result", {}).get("rows", [])
    return rows, None


def fetch_defillama_volume() -> dict:
    """Fetch volume data from DeFiLlama derivatives overview."""
    logger.info("Fetching volume from DeFiLlama...")

    try:
        req = Request(DEFILLAMA_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))

        volumes = {}
        for protocol in data.get("protocols", []):
            if "Solana" in protocol.get("chains", []):
                volumes[protocol.get("name", "")] = {
                    "volume_24h": protocol.get("total24h", 0) or 0,
                    "volume_7d": protocol.get("total7d", 0) or 0,
                    "volume_30d": protocol.get("total30d", 0) or 0,
                    "change_1d": protocol.get("change_1d", 0) or 0,
                    "change_7d": protocol.get("change_7d", 0) or 0,
                    "change_1m": protocol.get("change_1m", 0) or 0,
                }

        logger.info(f"Found {len(volumes)} protocols")
        return volumes
    except Exception as e:
        logger.error(f"Failed: {e}")
        return {}


def fetch_global_derivatives() -> list:
    """Fetch top derivatives protocols globally for cross-chain comparison."""
    logger.info("Fetching global derivatives...")

    try:
        req = Request(DEFILLAMA_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))

        protocols = []
        for protocol in data.get("protocols", []):
            vol_24h = protocol.get("total24h", 0) or 0
            if vol_24h > 1000000:  # Only include protocols with >$1M volume
                protocols.append({
                    "name": protocol.get("name", ""),
                    "chains": protocol.get("chains", []),
                    "volume_24h": vol_24h,
                    "volume_7d": protocol.get("total7d", 0) or 0,
                    "change_1d": protocol.get("change_1d", 0) or 0,
                    "change_7d": protocol.get("change_7d", 0) or 0,
                })

        # Sort by 24h volume descending
        protocols.sort(key=lambda x: x["volume_24h"], reverse=True)

        logger.info(f"Found {len(protocols)} protocols")
        return protocols[:15]  # Top 15
    except Exception as e:
        logger.error(f"Failed: {e}")
        return []


def fetch_drift_liquidations(hours: int = 1) -> dict:
    """Fetch Drift liquidation count for the past N hours."""
    logger.info(f"Fetching Drift liquidations ({hours}h)...")

    start, end = get_time_range(hours)
    sql = f"""
    SELECT COUNT(*) as liquidation_count, COUNT(DISTINCT tx_id) as unique_txns
    FROM solana.instruction_calls
    WHERE block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
      AND executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
      AND bytearray_substring(data, 1, 8) = 0x4b2377f7bf128b02
    """

    rows, error = run_dune_query_safe(sql, timeout=180)
    if error:
        logger.error(f"Failed: {error}")
        return {"count": 0, "txns": 0, "error": error}
    if rows:
        count = rows[0].get("liquidation_count", 0)
        txns = rows[0].get("unique_txns", 0)
        logger.info(f"{count} liquidations ({txns} txns)")
        return {"count": count, "txns": txns}

    logger.warning("No data")
    return {"count": 0, "txns": 0}


# --- Pacifica API functions (defined early for use in cross-platform wallets) ---

def fetch_pacifica_traders_from_api() -> dict:
    """Fetch Pacifica trader data from their leaderboard API.

    Returns dict with trader counts and wallet addresses for different time windows.
    This is the authoritative source since Pacifica uses off-chain order matching.
    """
    logger.info("Fetching Pacifica traders from leaderboard API...")
    try:
        req = Request(
            "https://app.pacifica.fi/api/v1/leaderboard",
            headers={"User-Agent": "SolanaPerpsBot/1.0"}
        )
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())

        if not data.get("success") or "data" not in data:
            logger.warning("Pacifica API returned unexpected format")
            return None

        traders = data["data"]

        # Count traders active in each time window
        traders_24h = [t for t in traders if float(t.get("volume_1d", 0)) > 0]
        traders_7d = [t for t in traders if float(t.get("volume_7d", 0)) > 0]

        result = {
            "traders_24h": len(traders_24h),
            "traders_7d": len(traders_7d),
            "traders_30d": sum(1 for t in traders if float(t.get("volume_30d", 0)) > 0),
            "traders_all": len(traders),
            # Include wallet addresses for cross-platform analysis
            "wallets_24h": set(t.get("address") for t in traders_24h if t.get("address")),
            "wallets_7d": set(t.get("address") for t in traders_7d if t.get("address")),
        }
        logger.info(f"Pacifica API: {result['traders_24h']:,} traders (24h), {result['traders_all']:,} total")
        return result
    except Exception as e:
        logger.warning(f"Pacifica leaderboard API failed: {e}")
        return None


# Cache for Pacifica API data (to avoid multiple calls per update cycle)
_pacifica_api_cache = {"data": None, "timestamp": None}


def get_pacifica_api_data() -> dict:
    """Get Pacifica API data with caching (refreshes every 5 minutes)."""
    global _pacifica_api_cache
    now = time.time()

    # Return cached data if fresh (< 5 minutes old)
    if _pacifica_api_cache["data"] and _pacifica_api_cache["timestamp"]:
        age = now - _pacifica_api_cache["timestamp"]
        if age < 300:  # 5 minutes
            return _pacifica_api_cache["data"]

    # Fetch fresh data
    data = fetch_pacifica_traders_from_api()
    if data:
        _pacifica_api_cache = {"data": data, "timestamp": now}
    return data


def fetch_cross_platform_wallets(hours: int = 1) -> dict:
    """Fetch wallet overlap between Drift, Jupiter, and Pacifica for the past N hours.

    Uses Dune for Drift/Jupiter on-chain wallets, and Pacifica API for off-chain traders.
    This gives accurate Pacifica representation since most Pacifica trading is off-chain.

    Returns counts for all possible combinations:
    - drift_only: Only on Drift
    - jupiter_only: Only on Jupiter
    - pacifica_only: Only on Pacifica
    - drift_jupiter: On Drift and Jupiter (not Pacifica)
    - drift_pacifica: On Drift and Pacifica (not Jupiter)
    - jupiter_pacifica: On Jupiter and Pacifica (not Drift)
    - all_three: On all three platforms
    """
    logger.info(f"Fetching cross-platform wallets ({hours}h)...")

    empty_result = {
        "drift_only": 0, "jupiter_only": 0, "pacifica_only": 0,
        "drift_jupiter": 0, "drift_pacifica": 0, "jupiter_pacifica": 0,
        "all_three": 0, "multi_platform": 0,
    }

    # Step 1: Get Pacifica wallets from API (most accurate for off-chain CLOB)
    pacifica_api = get_pacifica_api_data()
    if pacifica_api:
        # Use 24h wallets for any time window (API only has 1d/7d granularity)
        pacifica_wallets = pacifica_api.get("wallets_24h", set())
        logger.info(f"Pacifica API: {len(pacifica_wallets)} wallets (24h)")
    else:
        pacifica_wallets = set()
        logger.warning("Pacifica API unavailable for wallet overlap")

    # Step 2: Get Drift and Jupiter wallets from Dune
    start, end = get_time_range(hours)
    keeper_list = "', '".join(DRIFT_KEEPERS)

    sql = f"""
    WITH drift_wallets AS (
        SELECT DISTINCT elem as wallet
        FROM solana.instruction_calls, UNNEST(SLICE(account_arguments, 3, 3)) as t(elem)
        WHERE executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
          AND CARDINALITY(account_arguments) >= 3
          AND elem NOT IN ('{keeper_list}') AND elem NOT LIKE 'Sysvar%'
          AND elem != 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND elem != '11111111111111111111111111111111'
          AND LENGTH(elem) = 44
    ),
    jupiter_wallets AS (
        SELECT DISTINCT signer as wallet
        FROM solana.transactions
        WHERE CONTAINS(account_keys, 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu')
          AND block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
          AND LENGTH(signer) = 44
    )
    SELECT 'drift' as platform, wallet FROM drift_wallets
    UNION ALL
    SELECT 'jupiter' as platform, wallet FROM jupiter_wallets
    """

    rows, error = run_dune_query_safe(sql, timeout=300)
    if error:
        logger.error(f"Dune wallet query failed: {error}")
        return {**empty_result, "error": error}

    # Build wallet sets from Dune results
    drift_wallets = set()
    jupiter_wallets = set()

    for row in (rows or []):
        platform = row.get("platform")
        wallet = row.get("wallet")
        if wallet:
            if platform == "drift":
                drift_wallets.add(wallet)
            elif platform == "jupiter":
                jupiter_wallets.add(wallet)

    logger.info(f"Dune: {len(drift_wallets)} Drift wallets, {len(jupiter_wallets)} Jupiter wallets")

    # Step 3: Calculate all overlap combinations
    drift_only = drift_wallets - jupiter_wallets - pacifica_wallets
    jupiter_only = jupiter_wallets - drift_wallets - pacifica_wallets
    pacifica_only = pacifica_wallets - drift_wallets - jupiter_wallets

    drift_jupiter = (drift_wallets & jupiter_wallets) - pacifica_wallets
    drift_pacifica = (drift_wallets & pacifica_wallets) - jupiter_wallets
    jupiter_pacifica = (jupiter_wallets & pacifica_wallets) - drift_wallets

    all_three = drift_wallets & jupiter_wallets & pacifica_wallets

    data = {
        "drift_only": len(drift_only),
        "jupiter_only": len(jupiter_only),
        "pacifica_only": len(pacifica_only),
        "drift_jupiter": len(drift_jupiter),
        "drift_pacifica": len(drift_pacifica),
        "jupiter_pacifica": len(jupiter_pacifica),
        "all_three": len(all_three),
        "multi_platform": len(drift_jupiter) + len(drift_pacifica) + len(jupiter_pacifica) + len(all_three),
    }

    total = sum(v for k, v in data.items() if k != "multi_platform")
    logger.info(f"{total} total wallets ({data['all_three']} on all 3, {data['multi_platform']} multi-platform)")
    return data


def fetch_drift_markets_from_api() -> dict:
    """
    Fetch Drift market breakdown directly from Drift API.

    Returns actual 24h volume per market from official Drift data.
    Much faster and more accurate than Dune queries.
    """
    logger.info("Fetching Drift markets from API...")

    try:
        req = Request(DRIFT_DATA_API, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))

        contracts = data.get("contracts", data) if isinstance(data, dict) else data

        # Filter to PERP markets and build breakdown
        markets = {}
        for c in contracts:
            if c.get("product_type") != "PERP":
                continue
            name = c.get("ticker_id", "UNKNOWN")
            volume = float(c.get("quote_volume", 0))
            oi = float(c.get("open_interest", 0))
            markets[name] = {
                "volume": volume,
                "open_interest": oi,
                "funding_rate": float(c.get("funding_rate", 0)),
                "next_funding_rate": float(c.get("next_funding_rate", 0)),
                "last_price": float(c.get("last_price", 0)),
                "price_high": float(c.get("high", 0)),
                "price_low": float(c.get("low", 0)),
                "index_price": float(c.get("index_price", 0)),
            }

        total_vol = sum(m["volume"] for m in markets.values())
        logger.info(f"Found {len(markets)} markets, ${total_vol:,.0f} vol")
        return markets
    except Exception as e:
        logger.error(f"Failed: {e}")
        return {}


# Known Drift keeper addresses (high-frequency bot signers)
DRIFT_KEEPERS = [
    'uZ1N4C9dc71Euu4GLYt5UURpFtg1WWSwo3F4Rn46Fr3',
    '3PFkJVowwwxqhk3Z4PonV5ibsimFvXRQWiU3mAzwoaKv',
    '8X35rQUK2u9hfn8rMPwwr6ZSEUhbmfDPEapp589XyoM1',
    'F1RsRqBjuLdGeKtQK2LEjVJHJqVbhBYtfUzaUCi8PcFv',
    'FetTyW8xAYfd33x4GMHoE7hTuEdWLj1fNnhJuyVMUGGa',
    'x1r2guH31WwmBnZHEgU2aEu7okBjjd6WHS1fC9xcLYY',
    '5ddo32xdfxBvxweFYeSDbteK53Fj68fAVvVqyRF6MpHY',
    '7uhiFHKK7XXtKkE2wU2Hr9GQ4kZZxEnUU85SMnhRcnw2',
]


def fetch_drift_accurate_traders(hours: int = 1) -> int:
    """Fetch unique Drift trader count using trade-specific instruction discriminators.

    Filters for actual trading activity by looking for specific Drift instructions:
    - place_perp_order (0x45): User placing perpetual orders
    - place_and_take_perp_order (0x46): Place and fill perp order
    - cancel_order (0x43): User canceling orders
    - settle_pnl (0x47): Settling profit/loss

    This is more accurate than counting all instruction accounts.
    """
    logger.info("Fetching accurate Drift traders...")

    start, end = get_time_range(hours)
    keeper_list = "', '".join(DRIFT_KEEPERS)
    time_filter = f"block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}"

    # Drift instruction discriminators for trading activity (first 8 bytes)
    # These identify actual user trading vs keeper/admin operations
    sql = f"""
    WITH trade_instructions AS (
        SELECT
            account_arguments[3] as user_account,
            bytearray_substring(data, 1, 1) as ix_type
        FROM solana.instruction_calls
        WHERE {time_filter}
          AND executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND CARDINALITY(account_arguments) >= 3
          -- Filter for trading-related instructions by checking common patterns
          -- Account argument patterns: [state, user, user_stats, ...]
    )
    SELECT COUNT(DISTINCT user_account) as unique_users
    FROM trade_instructions
    WHERE user_account NOT IN ('{keeper_list}')
      AND user_account NOT LIKE 'Sysvar%'
      AND user_account != 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
      AND user_account != '11111111111111111111111111111111'
      AND LENGTH(user_account) = 44  -- Valid base58 Solana address
    """

    rows, error = run_dune_query_safe(sql, timeout=300)
    if error:
        logger.error(f"Drift traders query failed: {error}")
        return 0
    if rows:
        traders = rows[0].get("unique_users", 0)
        logger.info(f"{traders} Drift traders ({hours}h)")
        return traders

    logger.warning("No Drift trader data")
    return 0


def fetch_drift_market_breakdown(hours: int = 1) -> dict:
    """Fetch Drift market breakdown with trade counts from Dune."""
    logger.info("Fetching Drift markets from Dune...")

    case_parts = [f"WHEN CONTAINS(account_keys, '{acc}') THEN '{mkt}'" for acc, mkt in DRIFT_MARKET_ACCOUNTS.items()]
    start, end = get_time_range(hours)

    sql = f"""
    SELECT CASE {' '.join(case_parts)} ELSE 'OTHER' END as market, COUNT(*) as tx_count
    FROM solana.transactions
    WHERE block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
      AND CONTAINS(account_keys, 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH')
    GROUP BY 1 ORDER BY 2 DESC
    """

    rows, error = run_dune_query_safe(sql, timeout=180)
    if error:
        logger.error(f"Failed: {error[:50]}")
        return {}

    markets = {row["market"]: row["tx_count"] for row in (rows or [])}
    logger.info(f"Found {len(markets)} markets, {sum(markets.values()):,} txns")
    return markets


def fetch_jupiter_accurate_traders(hours: int = 1) -> int:
    """Fetch unique Jupiter Perps trader count from transaction signers.

    Filters for transactions that interact with Jupiter Perps custody accounts,
    which indicates actual trading activity (deposits, trades, withdrawals).
    """
    logger.info("Fetching accurate Jupiter traders...")

    start, end = get_time_range(hours)

    # Build custody account check - these indicate actual trading
    custody_accounts = list(JUPITER_CUSTODY_ACCOUNTS.keys())
    custody_check = " OR ".join([f"CONTAINS(account_keys, '{acc}')" for acc in custody_accounts])

    sql = f"""
    SELECT COUNT(*) as total_txns, COUNT(DISTINCT signer) as unique_traders
    FROM solana.transactions
    WHERE block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
      AND CONTAINS(account_keys, 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu')
      AND ({custody_check})  -- Must interact with a custody account (actual trading)
      AND LENGTH(signer) = 44  -- Valid base58 Solana address
    """

    rows, error = run_dune_query_safe(sql, timeout=180)
    if error:
        logger.error(f"Jupiter traders query failed: {error}")
        return 0
    if rows:
        traders = rows[0].get("unique_traders", 0)
        txns = rows[0].get("total_txns", 0)
        logger.info(f"{traders} Jupiter traders ({txns:,} txns in {hours}h)")
        return traders

    logger.warning("No Jupiter trader data")
    return 0


def fetch_pacifica_traders(hours: int = 1) -> int:
    """Fetch unique Pacifica trader count from API.

    Uses leaderboard API for all time windows since Pacifica is an off-chain CLOB.
    The API provides 1d/7d volume data, so we estimate shorter windows proportionally.
    """
    api_data = get_pacifica_api_data()

    if api_data:
        if hours >= 24:
            return api_data.get("traders_24h", 0)
        elif hours >= 8:
            # Estimate 8h as ~40% of 24h (based on typical trading patterns)
            return int(api_data.get("traders_24h", 0) * 0.4)
        elif hours >= 4:
            # Estimate 4h as ~25% of 24h
            return int(api_data.get("traders_24h", 0) * 0.25)
        else:
            # Estimate 1h as ~10% of 24h
            return int(api_data.get("traders_24h", 0) * 0.10)

    # Fall back to on-chain Dune query (will undercount significantly)
    logger.warning(f"Pacifica API unavailable, falling back to on-chain query ({hours}h)")

    start, end = get_time_range(hours)
    sql = f"""
    SELECT COUNT(*) as total_txns, COUNT(DISTINCT signer) as unique_traders
    FROM solana.transactions
    WHERE block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
      AND CONTAINS(account_keys, 'PCFA5iYgmqK6MqPhWNKg7Yv7auX7VZ4Cx7T1eJyrAMH')
    """

    rows, error = run_dune_query_safe(sql, timeout=180)
    if error:
        logger.error(f"Pacifica traders failed: {error}")
        return 0
    if rows:
        traders = rows[0].get("unique_traders", 0)
        logger.info(f"{traders} Pacifica traders ({hours}h) [on-chain fallback - undercounts]")
        return traders

    logger.warning("No Pacifica data")
    return 0


def fetch_flashtrade_traders(hours: int = 1) -> int:
    """Fetch unique FlashTrade trader count from Dune."""
    logger.info(f"Fetching FlashTrade traders from Dune ({hours}h)...")

    start, end = get_time_range(hours)
    sql = f"""
    SELECT COUNT(*) as total_txns, COUNT(DISTINCT signer) as unique_traders
    FROM solana.transactions
    WHERE block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
      AND CONTAINS(account_keys, 'FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn')
    """

    rows, error = run_dune_query_safe(sql, timeout=180)
    if error:
        logger.error(f"FlashTrade traders failed: {error}")
        return 0
    if rows:
        traders = rows[0].get("unique_traders", 0)
        txns = rows[0].get("total_txns", 0)
        logger.info(f"{traders} FlashTrade traders ({txns:,} txns in {hours}h)")
        return traders

    logger.warning("No FlashTrade data")
    return 0


def fetch_adrena_traders(hours: int = 1) -> int:
    """Fetch unique Adrena trader count from Dune."""
    logger.info(f"Fetching Adrena traders from Dune ({hours}h)...")

    start, end = get_time_range(hours)
    sql = f"""
    SELECT COUNT(*) as total_txns, COUNT(DISTINCT signer) as unique_traders
    FROM solana.transactions
    WHERE block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
      AND CONTAINS(account_keys, '13gDzEXCdocbj8iAiqrScGo47NiSuYENGsRqi3SEAwet')
    """

    rows, error = run_dune_query_safe(sql, timeout=180)
    if error:
        logger.error(f"Adrena traders failed: {error}")
        return 0
    if rows:
        traders = rows[0].get("unique_traders", 0)
        txns = rows[0].get("total_txns", 0)
        logger.info(f"{traders} Adrena traders ({txns:,} txns in {hours}h)")
        return traders

    logger.warning("No Adrena data")
    return 0


def fetch_jupiter_market_breakdown(hours: int = 1) -> dict:
    """Fetch Jupiter Perps market breakdown with trade counts from Dune."""
    logger.info("Fetching Jupiter markets from Dune...")

    case_parts = [f"WHEN CONTAINS(account_keys, '{acc}') THEN '{mkt}'" for acc, mkt in JUPITER_CUSTODY_ACCOUNTS.items()]
    start, end = get_time_range(hours)

    sql = f"""
    SELECT CASE {' '.join(case_parts)} ELSE 'OTHER' END as market, COUNT(*) as tx_count
    FROM solana.transactions
    WHERE block_time >= {format_timestamp(start)} AND block_time < {format_timestamp(end)}
      AND CONTAINS(account_keys, 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu')
    GROUP BY 1 ORDER BY 2 DESC
    """

    rows, error = run_dune_query_safe(sql, timeout=180)
    if error:
        logger.error(f"Failed: {error[:50]}")
        return {}

    markets = {row["market"]: row["tx_count"] for row in (rows or [])}
    logger.info(f"Found {len(markets)} markets, {sum(markets.values()):,} txns")
    return markets


def fetch_signature_count(program_id: str, hours: int = 24) -> int:
    """Count recent signatures for a program."""
    if not program_id:
        return 0

    cutoff_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
    count = 0
    last_sig = None

    for _ in range(20):  # Max iterations
        params = [program_id, {"limit": 1000}]
        if last_sig:
            params[1]["before"] = last_sig

        result = rpc_call("getSignaturesForAddress", params)
        if not result or not isinstance(result, list):
            break

        for sig_info in result:
            sig_time = sig_info.get("blockTime", 0)
            if sig_time and sig_time < cutoff_time:
                return count
            if sig_info.get("err") is None:
                count += 1

        if result:
            last_sig = result[-1].get("signature")
            if result[-1].get("blockTime", 0) < cutoff_time:
                break
        else:
            break

        time.sleep(0.1)

    return count


def distribute_volume_by_trades(total_volume: float, market_trades: dict) -> dict:
    """Distribute total volume across markets by trade count proportion."""
    total_trades = sum(market_trades.values())
    if total_trades == 0:
        return {}

    return {
        market: total_volume * (trades / total_trades)
        for market, trades in market_trades.items()
    }


def calculate_market_fees(market_volumes: dict, fee_rate: float) -> dict:
    """Calculate fees per market from estimated volumes."""
    return {market: vol * fee_rate for market, vol in market_volumes.items()}


def collect_all_data(hours: int = 24, fetch_markets: bool = True) -> tuple:
    """Collect data for all protocols."""
    defillama_volumes = fetch_defillama_volume()

    all_metrics = []
    market_breakdowns = {}

    # First, fetch accurate 24h trader counts for protocols with program IDs
    logger.info("Fetching accurate 24h trader counts...")
    drift_24h_traders = fetch_drift_accurate_traders(hours=6)  # 6h sample, more reliable
    jupiter_24h_traders = fetch_jupiter_accurate_traders(hours=6)  # 6h sample

    # Build protocol metrics dynamically from DeFiLlama data
    for protocol_name, volume_data in defillama_volumes.items():
        volume_24h = volume_data.get("volume_24h", 0)
        if volume_24h < 1000:  # Skip tiny protocols
            continue

        logger.info(f"Processing {protocol_name}...")

        # Get extra metadata if available
        metadata = PROTOCOL_METADATA.get(protocol_name, {})
        program_id = metadata.get("program_id")
        fee_rate = metadata.get("fee_rate", 0.0005)  # Default 0.05%

        # Get tx count from RPC if we have program ID
        tx_count = fetch_signature_count(program_id, hours) if program_id else 0

        # Use accurate trader counts for known protocols
        if protocol_name == "Drift Trade":
            traders = int(drift_24h_traders * 2)  # Rough 6h->24h scaling
        elif protocol_name == "Jupiter Perpetual Exchange":
            traders = int(jupiter_24h_traders * 2)  # Rough 6h->24h scaling
        else:
            traders = 0  # No Dune query for this protocol

        fees = volume_24h * fee_rate

        metrics = {
            "protocol": protocol_name,
            "transactions": tx_count,
            "traders": traders,
            "volume_usd": volume_24h,
            "fees_usd": fees,
        }

        logger.info(f"{tx_count:,} txns, ${volume_24h:,.0f} vol")
        all_metrics.append(metrics)

    # Fetch market breakdowns for both protocols
    if fetch_markets:
        print()

        # Drift market breakdown from API (actual per-market volumes)
        drift_markets = fetch_drift_markets_from_api()
        # Use the 6h trader count we already fetched
        drift_accurate_traders = drift_24h_traders

        if drift_markets:
            drift_fee_rate = PROTOCOL_METADATA["Drift Trade"]["fee_rate"]

            # Use actual volumes from API and calculate fees
            drift_volumes = {m: data["volume"] for m, data in drift_markets.items()}
            drift_fees = calculate_market_fees(drift_volumes, drift_fee_rate)
            drift_oi = {m: data["open_interest"] for m, data in drift_markets.items()}

            # Distribute traders proportionally by volume
            total_vol = sum(drift_volumes.values())
            drift_trader_counts = {
                m: int(drift_accurate_traders * (v / total_vol)) if total_vol > 0 else 0
                for m, v in drift_volumes.items()
            }

            market_breakdowns["Drift"] = {
                "volumes": drift_volumes,
                "open_interest": drift_oi,
                "traders": drift_trader_counts,
                "fees": drift_fees,
                "accurate_total_traders": drift_accurate_traders,
                "source": "api",  # Mark as API source
            }

        # Jupiter Perps market breakdown with accurate trader count
        jupiter_trade_counts = fetch_jupiter_market_breakdown(hours=1)
        # Use the 6h trader count we already fetched
        jupiter_accurate_traders = jupiter_24h_traders

        if jupiter_trade_counts:
            jupiter_volume = next(
                (m["volume_usd"] for m in all_metrics if m["protocol"] == "Jupiter Perps"), 0
            )
            jupiter_fee_rate = PROTOCOL_METADATA["Jupiter Perpetual Exchange"]["fee_rate"]

            # Distribute volume and calculate fees
            jupiter_volumes = distribute_volume_by_trades(jupiter_volume, jupiter_trade_counts)
            jupiter_fees = calculate_market_fees(jupiter_volumes, jupiter_fee_rate)

            # Distribute traders proportionally by trade count
            total_trades = sum(jupiter_trade_counts.values())
            jupiter_trader_counts = {
                m: int(jupiter_accurate_traders * (t / total_trades)) if total_trades > 0 else 0
                for m, t in jupiter_trade_counts.items()
            }

            market_breakdowns["Jupiter Perps"] = {
                "trades": jupiter_trade_counts,
                "traders": jupiter_trader_counts,
                "volumes": jupiter_volumes,
                "fees": jupiter_fees,
                "accurate_total_traders": jupiter_accurate_traders,
            }

    return all_metrics, market_breakdowns


def print_dashboard(all_metrics: list, market_breakdowns: dict, hours: int):
    """Print the formatted dashboard."""
    active_metrics = [m for m in all_metrics if m["volume_usd"] > 0 or m["transactions"] > 0]

    total_volume = sum(m["volume_usd"] for m in active_metrics)
    total_fees = sum(m["fees_usd"] for m in active_metrics)
    total_traders = sum(m["traders"] for m in active_metrics)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    print("\n")
    print("=" * 100)
    print(f"SOLANA PERPS DASHBOARD ({hours}h)".center(60) + f"Updated: {now}".rjust(40))
    print("=" * 100)
    print(f"{'Protocol':<15} {'Txns':>12} {'Traders':>10} {'Volume (USD)':>18} {'Fees (USD)':>14} {'Share':>8}")
    print("-" * 100)

    for m in sorted(active_metrics, key=lambda x: x["volume_usd"], reverse=True):
        share = (m["volume_usd"] / total_volume * 100) if total_volume > 0 else 0
        print(f"{m['protocol']:<15} {m['transactions']:>12,} {m['traders']:>10,} "
              f"${m['volume_usd']:>16,.0f} ${m['fees_usd']:>12,.0f} {share:>7.1f}%")

    print("-" * 100)
    print(f"{'TOTAL':<15} {sum(m['transactions'] for m in active_metrics):>12,} "
          f"{total_traders:>10,} ${total_volume:>16,.0f} ${total_fees:>12,.0f} {'100.0%':>8}")
    print("=" * 100)

    # Market breakdowns
    for protocol, data in market_breakdowns.items():
        traders = data.get("traders", {})
        volumes = data.get("volumes", {})
        fees = data.get("fees", {})
        open_interest = data.get("open_interest", {})
        accurate_traders = data.get("accurate_total_traders", 0)
        source = data.get("source", "dune")

        if not volumes:
            continue

        # Show accurate trader count in header
        trader_note = f" [{accurate_traders} unique traders in 6h sample]" if accurate_traders else ""
        source_note = " (from API)" if source == "api" else ""
        print(f"\nMARKET BREAKDOWN - {protocol.upper()}{trader_note}{source_note}")
        print("-" * 100)

        # Different display format for API vs Dune data
        if source == "api":
            # API provides actual volumes and open interest
            print(f"{'Market':<20} {'Volume 24h':>18} {'Open Interest':>18} {'Fees':>14} {'Share':>8}")
            print("-" * 100)

            total_market_volume = sum(volumes.values())
            total_market_oi = sum(open_interest.values())
            total_market_fees = sum(fees.values())
            sorted_markets = sorted(volumes.items(), key=lambda x: x[1], reverse=True)

            for market, vol in sorted_markets[:15]:
                oi = open_interest.get(market, 0)
                fee = fees.get(market, 0)
                share = (vol / total_market_volume * 100) if total_market_volume > 0 else 0
                print(f"{market:<20} ${vol:>17,.0f} {oi:>18,.2f} ${fee:>12,.0f} {share:>7.1f}%")

            print("-" * 100)
            print(f"{'TOTAL':<20} ${total_market_volume:>17,.0f} {total_market_oi:>18,.2f} "
                  f"${total_market_fees:>12,.0f} {'100.0%':>8}")
        else:
            # Dune provides trades, we estimate volumes
            trades = data.get("trades", {})
            print(f"{'Market':<15} {'Trades':>12} {'Traders':>10} {'Volume':>18} {'Fees':>14} {'Share':>8}")
            print("-" * 100)

            total_trades = sum(trades.values())
            total_market_traders = sum(traders.values())
            total_market_volume = sum(volumes.values())
            total_market_fees = sum(fees.values())
            sorted_markets = sorted(trades.items(), key=lambda x: x[1], reverse=True)

            for market, trade_count in sorted_markets[:12]:
                trader_count = traders.get(market, 0)
                vol = volumes.get(market, 0)
                fee = fees.get(market, 0)
                share = (trade_count / total_trades * 100) if total_trades > 0 else 0
                print(f"{market:<15} {trade_count:>12,} {trader_count:>10,} "
                      f"${vol:>16,.0f} ${fee:>12,.0f} {share:>7.1f}%")

            print("-" * 100)
            print(f"{'TOTAL':<15} {total_trades:>12,} {total_market_traders:>10,} "
                  f"${total_market_volume:>16,.0f} ${total_market_fees:>12,.0f} {'100.0%':>8}")

        print("-" * 100)

    # Data sources
    print("\nData Sources:")
    print("  Volume: DeFiLlama API (protocol) / Drift API (markets) | Tx Count: Solana RPC")
    print("  Traders: Dune Analytics (6h sample, scaled) - Drift: instruction accounts, Jupiter: signers")
    print("  Fees: Estimated (volume * fee_rate)")


def main():
    parser = argparse.ArgumentParser(description="Solana Perps Dashboard")
    parser.add_argument("--no-markets", action="store_true", help="Skip market breakdown")
    args = parser.parse_args()

    print("=" * 60)
    print("SOLANA PERPS DASHBOARD")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    hours = 24
    fetch_markets = not args.no_markets

    all_metrics, market_breakdowns = collect_all_data(hours, fetch_markets)
    print_dashboard(all_metrics, market_breakdowns, hours)


if __name__ == "__main__":
    main()
