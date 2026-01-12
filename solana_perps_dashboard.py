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
import os
import sys
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# RPC endpoint (uses public fallback if not set)
RPC_URL = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

# DeFiLlama API
DEFILLAMA_URL = "https://api.llama.fi/overview/derivatives"

# Dune API (required)
DUNE_API_KEY = os.environ.get("DUNE_API_KEY")
if not DUNE_API_KEY:
    raise ValueError("DUNE_API_KEY environment variable is required")
DUNE_API_URL = "https://api.dune.com/api/v1"

# Drift Data API (provides per-market volume data directly)
DRIFT_DATA_API = "https://data.api.drift.trade/contracts"

# Protocols: map display name to (program_id, defillama_name)
PROTOCOLS = {
    "Jupiter Perps": {
        "program_id": "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu",
        "defillama_name": "Jupiter Perpetual Exchange",
        "fee_rate": 0.0006,  # 0.06%
    },
    "Drift": {
        "program_id": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
        "defillama_name": "Drift Trade",
        "fee_rate": 0.0005,  # 0.05%
    },
    "FlashTrade": {
        "program_id": None,
        "defillama_name": "FlashTrade",
        "fee_rate": 0.0005,
    },
    "Adrena": {
        "program_id": None,
        "defillama_name": "Adrena Protocol",
        "fee_rate": 0.0005,
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
JUPITER_CUSTODY_ACCOUNTS = {
    "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz": "SOL",
    "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm": "BTC",
    "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn": "ETH",
}


def rpc_call(method: str, params: list) -> dict:
    """Make an RPC call to the Solana node."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    req = Request(
        RPC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
    )
    try:
        with urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))
            if "error" in result:
                print(f"RPC Error: {result['error']}", file=sys.stderr)
                return {}
            return result.get("result", {})
    except HTTPError as e:
        if e.code == 429:
            time.sleep(2)
            return rpc_call(method, params)
        return {}
    except Exception as e:
        print(f"RPC call failed: {e}", file=sys.stderr)
        return {}


def run_dune_query(sql: str, timeout: int = 180) -> dict:
    """Execute SQL on Dune Analytics and return results."""
    # Start query execution
    url = f"{DUNE_API_URL}/sql/execute"
    payload = {"sql": sql, "performance": "medium"}
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
    except Exception as e:
        return {"error": str(e)}

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


def fetch_defillama_volume() -> dict:
    """Fetch volume data from DeFiLlama derivatives overview."""
    print("Fetching volume from DeFiLlama...", end=" ", flush=True)

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

        print(f"found {len(volumes)} protocols")
        return volumes
    except Exception as e:
        print(f"failed: {e}", file=sys.stderr)
        return {}


def fetch_global_derivatives() -> list:
    """Fetch top derivatives protocols globally for cross-chain comparison."""
    print("Fetching global derivatives...", end=" ", flush=True)

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

        print(f"found {len(protocols)} protocols")
        return protocols[:15]  # Top 15
    except Exception as e:
        print(f"failed: {e}", file=sys.stderr)
        return []


def fetch_drift_liquidations(hours: int = 1) -> dict:
    """
    Fetch Drift liquidation data from Dune Analytics.

    Returns liquidation count for the past N hours.
    Uses a short window (1h) to avoid Dune query timeouts.
    """
    print(f"Fetching Drift liquidations ({hours}h)...", end=" ", flush=True)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)

    # Query for liquidation events - liquidate_perp instruction
    # Discriminator is SHA256("global:liquidate_perp")[:8] = 0x4b2377f7bf128b02
    sql = f"""
    SELECT
        COUNT(*) as liquidation_count,
        COUNT(DISTINCT tx_id) as unique_txns
    FROM solana.instruction_calls
    WHERE block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
      AND bytearray_substring(data, 1, 8) = 0x4b2377f7bf128b02
    """

    result = run_dune_query(sql, timeout=180)

    if "error" in result:
        print(f"failed: {result['error']}", file=sys.stderr)
        return {"count": 0, "txns": 0, "error": result["error"]}

    rows = result.get("result", {}).get("rows", [])
    if rows:
        count = rows[0].get("liquidation_count", 0)
        txns = rows[0].get("unique_txns", 0)
        print(f"{count} liquidations ({txns} txns)")
        return {"count": count, "txns": txns}

    print("no data")
    return {"count": 0, "txns": 0}


def fetch_cross_platform_wallets(hours: int = 1) -> dict:
    """
    Fetch wallets trading on Drift and/or Jupiter in the last N hours.

    Returns counts of:
    - multi_platform: wallets on BOTH Drift AND Jupiter
    - drift_only: wallets ONLY on Drift
    - jupiter_only: wallets ONLY on Jupiter
    """
    print(f"Fetching cross-platform wallets ({hours}h)...", end=" ", flush=True)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)

    # Build keeper exclusion list
    keeper_list = "', '".join(DRIFT_KEEPERS)

    # Query uses UNNEST to extract account positions [3], [4], [5] in a single scan
    # because user wallets appear in different positions depending on instruction type
    sql = f"""
    WITH drift_wallets AS (
        SELECT DISTINCT elem as wallet
        FROM solana.instruction_calls,
             UNNEST(SLICE(account_arguments, 3, 3)) as t(elem)
        WHERE executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND CARDINALITY(account_arguments) >= 3
          AND elem NOT IN ('{keeper_list}')
          AND elem NOT LIKE 'Sysvar%'
          AND elem != 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND elem != '11111111111111111111111111111111'
    ),
    jupiter_wallets AS (
        SELECT DISTINCT signer as wallet
        FROM solana.transactions
        WHERE CONTAINS(account_keys, 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu')
          AND block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
    )
    SELECT
        COUNT(CASE WHEN d.wallet IS NOT NULL AND j.wallet IS NOT NULL THEN 1 END) as multi_platform,
        COUNT(CASE WHEN d.wallet IS NOT NULL AND j.wallet IS NULL THEN 1 END) as drift_only,
        COUNT(CASE WHEN d.wallet IS NULL AND j.wallet IS NOT NULL THEN 1 END) as jupiter_only
    FROM drift_wallets d
    FULL OUTER JOIN jupiter_wallets j ON d.wallet = j.wallet
    """

    result = run_dune_query(sql, timeout=300)

    if "error" in result:
        print(f"failed: {result['error']}", file=sys.stderr)
        return {
            "multi_platform": 0,
            "drift_only": 0,
            "jupiter_only": 0,
            "error": result["error"]
        }

    rows = result.get("result", {}).get("rows", [])
    if rows:
        data = {
            "multi_platform": rows[0].get("multi_platform", 0),
            "drift_only": rows[0].get("drift_only", 0),
            "jupiter_only": rows[0].get("jupiter_only", 0),
        }
        total = data["multi_platform"] + data["drift_only"] + data["jupiter_only"]
        print(f"{total} total ({data['multi_platform']} multi-platform)")
        return data

    print("no data")
    return {"multi_platform": 0, "drift_only": 0, "jupiter_only": 0}


def fetch_drift_markets_from_api() -> dict:
    """
    Fetch Drift market breakdown directly from Drift API.

    Returns actual 24h volume per market from official Drift data.
    Much faster and more accurate than Dune queries.
    """
    print("Fetching Drift markets from API...", end=" ", flush=True)

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
        print(f"found {len(markets)} markets, ${total_vol:,.0f} vol")
        return markets
    except Exception as e:
        print(f"failed: {e}", file=sys.stderr)
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
    """
    Fetch accurate Drift trader count by parsing ALL instruction accounts.

    Looks at account_arguments[3], [4], [5] across all instruction types
    to find unique user accounts, excluding known keepers and system accounts.
    """
    print("Fetching accurate Drift traders...", end=" ", flush=True)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)

    keeper_list = "', '".join(DRIFT_KEEPERS)

    # Count unique accounts at positions 3, 4, 5 across ALL instruction types
    sql = f"""
    WITH all_accounts AS (
        SELECT DISTINCT account_arguments[3] as user_account
        FROM solana.instruction_calls
        WHERE block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND CARDINALITY(account_arguments) >= 3
        UNION
        SELECT DISTINCT account_arguments[4] as user_account
        FROM solana.instruction_calls
        WHERE block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND CARDINALITY(account_arguments) >= 4
        UNION
        SELECT DISTINCT account_arguments[5] as user_account
        FROM solana.instruction_calls
        WHERE block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
          AND executing_account = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
          AND CARDINALITY(account_arguments) >= 5
    )
    SELECT COUNT(DISTINCT user_account) as unique_users
    FROM all_accounts
    WHERE user_account NOT IN ('{keeper_list}')
      AND user_account NOT LIKE 'Sysvar%'
      AND user_account != 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
      AND user_account != '11111111111111111111111111111111'
    """

    result = run_dune_query(sql, timeout=300)

    if "error" in result:
        print(f"failed", file=sys.stderr)
        return 0

    rows = result.get("result", {}).get("rows", [])
    if rows:
        traders = rows[0].get("unique_users", 0)
        print(f"{traders} traders ({hours}h)")
        return traders

    print("no data")
    return 0


def fetch_drift_market_breakdown(hours: int = 1) -> dict:
    """Fetch Drift market breakdown with trade counts from Dune."""
    print("Fetching Drift markets from Dune...", end=" ", flush=True)

    # Build CASE statement for market identification
    case_parts = []
    for account, market in DRIFT_MARKET_ACCOUNTS.items():
        case_parts.append(f"WHEN CONTAINS(account_keys, '{account}') THEN '{market}'")
    case_stmt = "\n        ".join(case_parts)

    # Query last N hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)

    sql = f"""
    SELECT
        CASE
            {case_stmt}
            ELSE 'OTHER'
        END as market,
        COUNT(*) as tx_count
    FROM solana.transactions
    WHERE block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND CONTAINS(account_keys, 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH')
    GROUP BY 1
    ORDER BY 2 DESC
    """

    result = run_dune_query(sql, timeout=180)

    if "error" in result:
        print(f"failed: {result.get('error', '')[:50]}", file=sys.stderr)
        return {}

    rows = result.get("result", {}).get("rows", [])
    markets = {row["market"]: row["tx_count"] for row in rows}

    total = sum(markets.values())
    print(f"found {len(markets)} markets, {total:,} txns")

    return markets


def fetch_jupiter_accurate_traders(hours: int = 1) -> int:
    """
    Fetch accurate Jupiter Perps trader count using transaction signers.

    Jupiter Perps uses a different model - users sign their own transactions
    directly (not via keepers), so signer count is accurate.
    """
    print("Fetching accurate Jupiter traders...", end=" ", flush=True)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)

    sql = f"""
    SELECT
        COUNT(*) as total_txns,
        COUNT(DISTINCT signer) as unique_traders
    FROM solana.transactions
    WHERE block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND CONTAINS(account_keys, 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu')
    """

    result = run_dune_query(sql, timeout=180)

    if "error" in result:
        print(f"failed", file=sys.stderr)
        return 0

    rows = result.get("result", {}).get("rows", [])
    if rows:
        traders = rows[0].get("unique_traders", 0)
        txns = rows[0].get("total_txns", 0)
        print(f"{traders} traders ({txns:,} txns in {hours}h)")
        return traders

    print("no data")
    return 0


def fetch_jupiter_market_breakdown(hours: int = 1) -> dict:
    """Fetch Jupiter Perps market breakdown with trade counts from Dune."""
    print("Fetching Jupiter markets from Dune...", end=" ", flush=True)

    # Build CASE statement for market identification
    case_parts = []
    for account, market in JUPITER_CUSTODY_ACCOUNTS.items():
        case_parts.append(f"WHEN CONTAINS(account_keys, '{account}') THEN '{market}'")
    case_stmt = "\n        ".join(case_parts)

    # Query last N hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)

    sql = f"""
    SELECT
        CASE
            {case_stmt}
            ELSE 'OTHER'
        END as market,
        COUNT(*) as tx_count
    FROM solana.transactions
    WHERE block_time >= TIMESTAMP '{start_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND block_time < TIMESTAMP '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
      AND CONTAINS(account_keys, 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu')
    GROUP BY 1
    ORDER BY 2 DESC
    """

    result = run_dune_query(sql, timeout=180)

    if "error" in result:
        print(f"failed: {result.get('error', '')[:50]}", file=sys.stderr)
        return {}

    rows = result.get("result", {}).get("rows", [])
    markets = {row["market"]: row["tx_count"] for row in rows}

    total = sum(markets.values())
    print(f"found {len(markets)} markets, {total:,} txns")

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
    print("\nFetching accurate 24h trader counts...")
    drift_24h_traders = fetch_drift_accurate_traders(hours=6)  # 6h sample, more reliable
    jupiter_24h_traders = fetch_jupiter_accurate_traders(hours=6)  # 6h sample

    for protocol_name, config in PROTOCOLS.items():
        print(f"\nProcessing {protocol_name}...", end=" ", flush=True)

        # Get volume from DeFiLlama
        volume_data = defillama_volumes.get(config["defillama_name"], {})
        volume_24h = volume_data.get("volume_24h", 0)

        # Get tx count from RPC
        program_id = config["program_id"]
        tx_count = fetch_signature_count(program_id, hours) if program_id else 0

        # Use accurate trader counts (scale 6h to 24h estimate)
        if protocol_name == "Drift":
            traders = int(drift_24h_traders * 2)  # Rough 6h->24h scaling (not 4x due to overlap)
        elif protocol_name == "Jupiter Perps":
            traders = int(jupiter_24h_traders * 2)  # Rough 6h->24h scaling
        else:
            traders = 0  # No data for other protocols

        fees = volume_24h * config["fee_rate"]

        metrics = {
            "protocol": protocol_name,
            "transactions": tx_count,
            "traders": traders,
            "volume_usd": volume_24h,
            "fees_usd": fees,
        }

        print(f"{tx_count:,} txns, ${volume_24h:,.0f} vol")
        all_metrics.append(metrics)

    # Fetch market breakdowns for both protocols
    if fetch_markets:
        print()

        # Drift market breakdown from API (actual per-market volumes)
        drift_markets = fetch_drift_markets_from_api()
        # Use the 6h trader count we already fetched
        drift_accurate_traders = drift_24h_traders

        if drift_markets:
            drift_fee_rate = PROTOCOLS["Drift"]["fee_rate"]

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
            jupiter_fee_rate = PROTOCOLS["Jupiter Perps"]["fee_rate"]

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
