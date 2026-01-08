#!/usr/bin/env python3
"""
Solana Perps Metrics Dashboard (Hybrid Approach)

Uses DeFiLlama for accurate volume data and RPC for transaction counts.
Traders and fees are estimated from derived metrics.

Data Sources:
- Volume: DeFiLlama API (only source with decoded 2026 perps data)
- Tx Count: Solana RPC signatures
- Traders: Estimated (tx_count * 0.7)
- Fees: Estimated (volume * fee_rate)

Usage:
    python solana_perps_dashboard.py
"""

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

# Protocols: map display name to (program_id, defillama_name)
# DeFiLlama names found via API on 2026-01-08
PROTOCOLS = {
    "Jupiter Perps": {
        "program_id": "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu",
        "defillama_name": "Jupiter Perpetual Exchange",  # Correct name from DeFiLlama
        "fee_rate": 0.0006,  # 0.06%
    },
    "Drift": {
        "program_id": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
        "defillama_name": "Drift Trade",  # Correct name from DeFiLlama
        "fee_rate": 0.0005,  # 0.05%
    },
    "FlashTrade": {
        "program_id": None,  # TBD
        "defillama_name": "FlashTrade",
        "fee_rate": 0.0005,  # estimated
    },
    "Adrena": {
        "program_id": None,  # TBD
        "defillama_name": "Adrena Protocol",
        "fee_rate": 0.0005,  # estimated
    },
}


def rpc_call(method: str, params: list) -> dict:
    """Make an RPC call to the Solana node."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }

    req = Request(
        RPC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0"
        }
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
            print("Rate limited, waiting 2 seconds...", file=sys.stderr)
            time.sleep(2)
            return rpc_call(method, params)
        print(f"HTTP Error {e.code}: {e.reason}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"RPC call failed: {e}", file=sys.stderr)
        return {}


def fetch_defillama_volume() -> dict:
    """Fetch volume data from DeFiLlama derivatives overview."""
    print("Fetching volume data from DeFiLlama...", end=" ", flush=True)

    volumes = {}

    try:
        req = Request(
            DEFILLAMA_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))

        protocols = data.get("protocols", [])

        for protocol in protocols:
            name = protocol.get("name", "")
            chains = protocol.get("chains", [])

            # Only include if it's on Solana
            if "Solana" not in chains:
                continue

            total_24h = protocol.get("total24h", 0) or 0
            total_7d = protocol.get("total7d", 0) or 0

            volumes[name] = {
                "volume_24h": total_24h,
                "volume_7d": total_7d,
            }

        print(f"found {len(volumes)} Solana protocols")
        return volumes

    except Exception as e:
        print(f"failed: {e}", file=sys.stderr)
        return {}


def fetch_signature_count(program_id: str, hours: int = 24) -> int:
    """Count recent signatures for a program (fast, no tx parsing)."""
    if not program_id:
        return 0

    cutoff_time = int((datetime.now() - timedelta(hours=hours)).timestamp())

    count = 0
    last_sig = None
    max_iterations = 20  # Limit to avoid too many RPC calls

    for _ in range(max_iterations):
        params = [program_id, {"limit": 1000}]
        if last_sig:
            params[1]["before"] = last_sig

        result = rpc_call("getSignaturesForAddress", params)
        if not result or not isinstance(result, list):
            break

        for sig_info in result:
            sig_time = sig_info.get("blockTime", 0)
            if sig_time and sig_time < cutoff_time:
                # Reached cutoff, stop
                return count
            if sig_info.get("err") is None:  # Only count successful txns
                count += 1

        if result:
            last_sig = result[-1].get("signature")
            # Check if last signature is before cutoff
            if result[-1].get("blockTime", 0) < cutoff_time:
                break
        else:
            break

        time.sleep(0.1)  # Rate limiting

    return count


def estimate_traders(tx_count: int) -> int:
    """Estimate unique traders from transaction count."""
    # Based on analysis: ~70% of transactions are from unique wallets per 24h
    return int(tx_count * 0.7)


def estimate_fees(volume_usd: float, fee_rate: float) -> float:
    """Estimate protocol fees from volume."""
    return volume_usd * fee_rate


def collect_all_data(hours: int = 24) -> list:
    """Collect data for all protocols using hybrid approach."""
    # Step 1: Fetch volume from DeFiLlama
    defillama_volumes = fetch_defillama_volume()

    all_metrics = []

    for protocol_name, config in PROTOCOLS.items():
        print(f"\nProcessing {protocol_name}...", end=" ", flush=True)

        # Get volume from DeFiLlama
        defillama_name = config["defillama_name"]
        volume_data = defillama_volumes.get(defillama_name, {})
        volume_24h = volume_data.get("volume_24h", 0)
        volume_7d = volume_data.get("volume_7d", 0)

        # Get tx count from RPC
        program_id = config["program_id"]
        tx_count = 0
        if program_id:
            tx_count = fetch_signature_count(program_id, hours)

        # Estimate derived metrics
        traders = estimate_traders(tx_count)
        fees = estimate_fees(volume_24h, config["fee_rate"])

        metrics = {
            "protocol": protocol_name,
            "transactions": tx_count,
            "traders": traders,
            "volume_usd": volume_24h,
            "volume_7d": volume_7d,
            "fees_usd": fees,
            "fee_rate": config["fee_rate"],
        }

        print(f"{tx_count:,} txns, ${volume_24h:,.0f} vol")
        all_metrics.append(metrics)

    return all_metrics


def print_dashboard(all_metrics: list, hours: int):
    """Print the formatted dashboard."""
    # Filter out protocols with no data
    active_metrics = [m for m in all_metrics if m["volume_usd"] > 0 or m["transactions"] > 0]

    # Calculate totals
    total_txns = sum(m["transactions"] for m in active_metrics)
    total_traders = sum(m["traders"] for m in active_metrics)
    total_volume = sum(m["volume_usd"] for m in active_metrics)
    total_fees = sum(m["fees_usd"] for m in active_metrics)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    print("\n")
    print("=" * 100)
    print(f"SOLANA PERPS DASHBOARD ({hours}h)".center(60) + f"Updated: {now}".rjust(40))
    print("=" * 100)
    print(f"{'Protocol':<15} {'Txns':>12} {'Traders':>10} {'Volume (USD)':>18} {'Fees (USD)':>14} {'Fees/Trader':>12} {'Share':>8}")
    print("-" * 100)

    for m in sorted(active_metrics, key=lambda x: x["volume_usd"], reverse=True):
        traders_count = m["traders"]
        fees_per_trader = m["fees_usd"] / traders_count if traders_count > 0 else 0
        share = (m["volume_usd"] / total_volume * 100) if total_volume > 0 else 0

        print(f"{m['protocol']:<15} {m['transactions']:>12,} {traders_count:>10,} "
              f"${m['volume_usd']:>16,.0f} ${m['fees_usd']:>12,.0f} "
              f"${fees_per_trader:>10,.2f} {share:>7.1f}%")

    print("-" * 100)
    avg_fees_per_trader = total_fees / total_traders if total_traders > 0 else 0
    print(f"{'TOTAL':<15} {total_txns:>12,} {total_traders:>10,} "
          f"${total_volume:>16,.0f} ${total_fees:>12,.0f} "
          f"${avg_fees_per_trader:>10,.2f} {'100.0%':>8}")
    print("=" * 100)

    # Data source attribution
    print("\nData Sources:")
    print("  Volume: DeFiLlama API (https://defillama.com/derivatives)")
    print("  Tx Count: Solana RPC (getSignaturesForAddress)")
    print("  Traders: Estimated (tx_count × 0.7)")
    print("  Fees: Estimated (volume × fee_rate)")

    # Show inactive protocols
    inactive = [m for m in all_metrics if m["volume_usd"] == 0 and m["transactions"] == 0]
    if inactive:
        print(f"\nInactive/No Data: {', '.join(m['protocol'] for m in inactive)}")


def main():
    print("=" * 60)
    print("SOLANA PERPS DASHBOARD (Hybrid DeFiLlama + RPC)")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    hours = 24

    # Collect data using hybrid approach
    all_metrics = collect_all_data(hours)

    # Print dashboard
    print_dashboard(all_metrics, hours)


if __name__ == "__main__":
    main()
