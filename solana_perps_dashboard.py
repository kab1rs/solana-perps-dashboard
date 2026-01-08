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
import sys
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# RPC endpoint
RPC_URL = "https://ellipsis.rpcpool.com/7ba0a839-324a-417c-8b44-f37b444f43ee"

# DeFiLlama API
DEFILLAMA_URL = "https://api.llama.fi/overview/derivatives"

# Dune API
DUNE_API_KEY = "l1JAVmXJYrPw9DFGIBtcSXkszCLgTVUz"
DUNE_API_URL = "https://api.dune.com/api/v1"

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
                }

        print(f"found {len(volumes)} protocols")
        return volumes
    except Exception as e:
        print(f"failed: {e}", file=sys.stderr)
        return {}


def fetch_drift_market_breakdown(hours: int = 1) -> dict:
    """Fetch Drift market breakdown with trader counts from Dune."""
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
        COUNT(*) as tx_count,
        COUNT(DISTINCT signer) as trader_count
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
    markets = {
        row["market"]: {
            "tx_count": row["tx_count"],
            "trader_count": row.get("trader_count", 0)
        }
        for row in rows
    }

    total = sum(m["tx_count"] for m in markets.values())
    print(f"found {len(markets)} markets, {total:,} txns")

    return markets


def fetch_jupiter_market_breakdown(hours: int = 1) -> dict:
    """Fetch Jupiter Perps market breakdown with trader counts from Dune."""
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
        COUNT(*) as tx_count,
        COUNT(DISTINCT signer) as trader_count
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
    markets = {
        row["market"]: {
            "tx_count": row["tx_count"],
            "trader_count": row.get("trader_count", 0)
        }
        for row in rows
    }

    total = sum(m["tx_count"] for m in markets.values())
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

    for protocol_name, config in PROTOCOLS.items():
        print(f"\nProcessing {protocol_name}...", end=" ", flush=True)

        # Get volume from DeFiLlama
        volume_data = defillama_volumes.get(config["defillama_name"], {})
        volume_24h = volume_data.get("volume_24h", 0)

        # Get tx count from RPC
        program_id = config["program_id"]
        tx_count = fetch_signature_count(program_id, hours) if program_id else 0

        # Estimate derived metrics
        traders = int(tx_count * 0.7)
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

        # Drift market breakdown
        drift_data = fetch_drift_market_breakdown(hours=1)
        if drift_data:
            drift_volume = next(
                (m["volume_usd"] for m in all_metrics if m["protocol"] == "Drift"), 0
            )
            drift_fee_rate = PROTOCOLS["Drift"]["fee_rate"]

            # Extract trade counts for volume distribution
            drift_trade_counts = {m: d["tx_count"] for m, d in drift_data.items()}
            drift_trader_counts = {m: d["trader_count"] for m, d in drift_data.items()}
            drift_volumes = distribute_volume_by_trades(drift_volume, drift_trade_counts)
            drift_fees = calculate_market_fees(drift_volumes, drift_fee_rate)

            market_breakdowns["Drift"] = {
                "trades": drift_trade_counts,
                "traders": drift_trader_counts,
                "volumes": drift_volumes,
                "fees": drift_fees,
            }

        # Jupiter Perps market breakdown
        jupiter_data = fetch_jupiter_market_breakdown(hours=1)
        if jupiter_data:
            jupiter_volume = next(
                (m["volume_usd"] for m in all_metrics if m["protocol"] == "Jupiter Perps"), 0
            )
            jupiter_fee_rate = PROTOCOLS["Jupiter Perps"]["fee_rate"]

            # Extract trade counts for volume distribution
            jupiter_trade_counts = {m: d["tx_count"] for m, d in jupiter_data.items()}
            jupiter_trader_counts = {m: d["trader_count"] for m, d in jupiter_data.items()}
            jupiter_volumes = distribute_volume_by_trades(jupiter_volume, jupiter_trade_counts)
            jupiter_fees = calculate_market_fees(jupiter_volumes, jupiter_fee_rate)

            market_breakdowns["Jupiter Perps"] = {
                "trades": jupiter_trade_counts,
                "traders": jupiter_trader_counts,
                "volumes": jupiter_volumes,
                "fees": jupiter_fees,
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
        trades = data.get("trades", {})
        traders = data.get("traders", {})
        volumes = data.get("volumes", {})
        fees = data.get("fees", {})

        if not trades:
            continue

        print(f"\nMARKET BREAKDOWN - {protocol.upper()}")
        print("-" * 100)
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
    print("  Volume: DeFiLlama API | Tx Count: Solana RPC | Markets: Dune Analytics")
    print("  Traders: COUNT(DISTINCT signer) from Dune | Fees: Estimated (volume * fee_rate)")


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
