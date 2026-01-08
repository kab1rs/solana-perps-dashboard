#!/usr/bin/env python3
"""
Drift Protocol Real-Time Data Puller (via Helius)

Pulls recent Drift trade data using Helius Enhanced Transactions API.
Use this for real-time data; use drift_puller.py for historical S3 data.

Usage:
    python drift_realtime_puller.py --api-key YOUR_HELIUS_KEY --limit 5000 --analyze
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
import csv

# Drift Program ID
DRIFT_PROGRAM = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"

# Helius API base URL
HELIUS_BASE_URL = "https://api.helius.xyz/v0"

# Known Drift market indices
DRIFT_MARKETS = {
    0: "SOL-PERP",
    1: "BTC-PERP",
    2: "ETH-PERP",
    3: "APT-PERP",
    4: "MATIC-PERP",
    5: "ARB-PERP",
    6: "DOGE-PERP",
    7: "BNB-PERP",
    8: "SUI-PERP",
    9: "1MPEPE-PERP",
    10: "OP-PERP",
    11: "RNDR-PERP",
    12: "XRP-PERP",
    13: "HNT-PERP",
    14: "INJ-PERP",
    15: "LINK-PERP",
    16: "RLB-PERP",
    17: "PYTH-PERP",
    18: "TIA-PERP",
    19: "JTO-PERP",
    20: "SEI-PERP",
    21: "AVAX-PERP",
    22: "WIF-PERP",
    23: "JUP-PERP",
    24: "DYM-PERP",
    25: "TAO-PERP",
    26: "W-PERP",
    27: "KMNO-PERP",
    28: "TNSR-PERP",
    29: "DRIFT-PERP",
}


def fetch_transactions(api_key: str, before_sig: Optional[str] = None, limit: int = 100) -> list:
    """Fetch transactions for Drift program from Helius."""
    url = f"{HELIUS_BASE_URL}/addresses/{DRIFT_PROGRAM}/transactions"
    params = f"?api-key={api_key}&limit={limit}"
    if before_sig:
        params += f"&before={before_sig}"

    try:
        req = Request(url + params, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })
        with urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 429:
            print("Rate limited, waiting 2 seconds...", file=sys.stderr)
            time.sleep(2)
            return fetch_transactions(api_key, before_sig, limit)
        raise
    except Exception as e:
        print(f"Error fetching transactions: {e}", file=sys.stderr)
        return []


def parse_drift_transaction(tx: dict) -> Optional[dict]:
    """Parse a Drift transaction to extract trade details."""
    if not tx:
        return None

    sig = tx.get("signature", "")
    timestamp = tx.get("timestamp", 0)
    fee_payer = tx.get("feePayer", "")
    source = tx.get("source", "")

    # Skip if not from Drift
    if source != "DRIFT":
        return None

    # Get token transfers to estimate volume
    token_transfers = tx.get("tokenTransfers", [])

    volume_usd = 0
    collateral_token = None
    trader_wallet = None  # The actual trader, not the keeper

    # Check for USDC/USDT transfers - the user account is usually the trader
    for transfer in token_transfers:
        mint = transfer.get("mint", "")
        from_user = transfer.get("fromUserAccount", "")
        to_user = transfer.get("toUserAccount", "")

        if mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v":  # USDC
            amount = float(transfer.get("tokenAmount", 0))
            volume_usd += amount
            collateral_token = "USDC"
            # The trader is usually the one sending/receiving USDC
            if from_user and from_user != fee_payer:
                trader_wallet = from_user
            elif to_user and to_user != fee_payer:
                trader_wallet = to_user
        elif mint == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB":  # USDT
            amount = float(transfer.get("tokenAmount", 0))
            volume_usd += amount
            collateral_token = "USDT"
            if from_user and from_user != fee_payer:
                trader_wallet = from_user
            elif to_user and to_user != fee_payer:
                trader_wallet = to_user

    # Check account data for balance changes
    account_data = tx.get("accountData", [])
    for acc in account_data:
        account = acc.get("account", "")
        token_changes = acc.get("tokenBalanceChanges", [])

        for change in token_changes:
            mint = change.get("mint", "")
            if mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v":
                raw = abs(float(change.get("rawTokenAmount", {}).get("tokenAmount", 0)))
                decimals = int(change.get("rawTokenAmount", {}).get("decimals", 6))
                amount = raw / (10 ** decimals)
                if amount > volume_usd:
                    volume_usd = amount
                    collateral_token = "USDC"
                # Track accounts with USDC changes as potential traders
                if account and account != fee_payer and not trader_wallet:
                    trader_wallet = account

    # Try to identify action and extract trader from instruction accounts
    instructions = tx.get("instructions", [])
    action = "unknown"
    market = "UNKNOWN"

    for inst in instructions:
        if inst.get("programId") == DRIFT_PROGRAM:
            data = inst.get("data", "")
            accounts = inst.get("accounts", [])

            # In Drift instructions, accounts[1] is often the user/authority
            # accounts[0] is usually the state, accounts[1] is user
            if len(accounts) >= 2 and not trader_wallet:
                # Skip known system accounts
                potential_trader = accounts[1]
                if (potential_trader != fee_payer and
                    not potential_trader.startswith("Sysvar") and
                    potential_trader != "11111111111111111111111111111111"):
                    trader_wallet = potential_trader

            # Instruction discriminators
            if data.startswith("3Gm"):
                action = "order"
            elif data.startswith("Gk"):
                action = "fill"
            elif data.startswith("9T"):
                action = "settle"
            elif data.startswith("2E"):
                action = "deposit"
            elif data.startswith("6L"):
                action = "withdraw"

    # Use trader_wallet if found, otherwise fall back to fee_payer
    final_wallet = trader_wallet or fee_payer

    # Only return if there's meaningful activity
    if volume_usd > 0 or action in ["fill", "order", "deposit", "withdraw"]:
        return {
            "signature": sig,
            "timestamp": timestamp,
            "datetime": datetime.fromtimestamp(timestamp).isoformat() if timestamp else "",
            "wallet": final_wallet,
            "fee_payer": fee_payer,
            "action": action,
            "market": market,
            "volume_usd": volume_usd,
            "collateral_token": collateral_token,
            "fee_sol": tx.get("fee", 0) / 1e9,
            "success": tx.get("transactionError") is None,
        }

    return None


def pull_drift_realtime(
    api_key: str,
    max_transactions: int = 5000,
    cutoff_date: Optional[datetime] = None,
    output_dir: str = "./drift_realtime_data"
) -> tuple:
    """Pull real-time Drift transactions via Helius."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    all_trades = []
    last_sig = None
    total_fetched = 0
    batch_size = 100

    stats = {
        "total_fetched": 0,
        "drift_trades": 0,
        "unique_wallets": set(),
        "volume_usd": 0,
        "actions": {},
        "start_time": None,
        "end_time": None,
    }

    print(f"Fetching Drift transactions via Helius (max {max_transactions})...")

    while total_fetched < max_transactions:
        txs = fetch_transactions(api_key, last_sig, batch_size)

        if not txs:
            print("No more transactions available")
            break

        for tx in txs:
            total_fetched += 1

            timestamp = tx.get("timestamp", 0)
            if timestamp:
                tx_date = datetime.fromtimestamp(timestamp)
                if stats["start_time"] is None or tx_date > stats["start_time"]:
                    stats["start_time"] = tx_date
                if stats["end_time"] is None or tx_date < stats["end_time"]:
                    stats["end_time"] = tx_date

                if cutoff_date and tx_date < cutoff_date:
                    print(f"Reached cutoff date {cutoff_date}")
                    break

            parsed = parse_drift_transaction(tx)
            if parsed:
                all_trades.append(parsed)
                stats["drift_trades"] += 1
                stats["unique_wallets"].add(parsed["wallet"])
                stats["volume_usd"] += parsed["volume_usd"]

                action = parsed["action"]
                if action not in stats["actions"]:
                    stats["actions"][action] = 0
                stats["actions"][action] += 1

        if txs:
            last_sig = txs[-1].get("signature")

        if total_fetched % 500 == 0:
            print(f"Fetched {total_fetched} transactions, {len(all_trades)} Drift trades, ${stats['volume_usd']:,.0f} volume")

        time.sleep(0.1)

        if cutoff_date and stats["end_time"] and stats["end_time"] < cutoff_date:
            break

    stats["total_fetched"] = total_fetched
    stats["unique_wallets"] = len(stats["unique_wallets"])

    # Save data
    if all_trades:
        all_trades.sort(key=lambda x: x["timestamp"], reverse=True)

        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = output_path / f"drift_realtime_{date_str}.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_trades[0].keys())
            writer.writeheader()
            writer.writerows(all_trades)
        print(f"\nSaved {len(all_trades)} trades to {csv_file}")

        stats_file = output_path / f"drift_realtime_stats_{date_str}.json"
        with open(stats_file, "w") as f:
            stats_copy = stats.copy()
            stats_copy["start_time"] = stats["start_time"].isoformat() if stats["start_time"] else None
            stats_copy["end_time"] = stats["end_time"].isoformat() if stats["end_time"] else None
            json.dump(stats_copy, f, indent=2)

    return stats, all_trades


def analyze_wallets(trades: list, top_n: int = 50) -> dict:
    """Analyze wallet activity."""
    wallets = {}

    for trade in trades:
        wallet = trade.get("wallet", "")
        if not wallet:
            continue

        if wallet not in wallets:
            wallets[wallet] = {
                "wallet": wallet,
                "total_volume": 0,
                "trade_count": 0,
                "actions": {},
            }

        wallets[wallet]["total_volume"] += trade.get("volume_usd", 0)
        wallets[wallet]["trade_count"] += 1

        action = trade.get("action", "unknown")
        if action not in wallets[wallet]["actions"]:
            wallets[wallet]["actions"][action] = 0
        wallets[wallet]["actions"][action] += 1

    sorted_wallets = sorted(wallets.values(), key=lambda x: x["total_volume"], reverse=True)

    return {
        "total_wallets": len(wallets),
        "top_wallets": sorted_wallets[:top_n],
    }


def print_summary(stats: dict, analysis: Optional[dict] = None):
    """Print summary."""
    print("\n" + "="*70)
    print("DRIFT REAL-TIME DATA SUMMARY")
    print("="*70)

    if stats.get("start_time") and stats.get("end_time"):
        print(f"Date range: {stats['end_time']} to {stats['start_time']}")

    print(f"\nTransactions fetched: {stats['total_fetched']:,}")
    print(f"Drift trades found: {stats['drift_trades']:,}")
    print(f"Unique wallets: {stats['unique_wallets']:,}")
    print(f"Total volume: ${stats['volume_usd']:,.0f}")

    print("\nActions:")
    for action, count in sorted(stats["actions"].items(), key=lambda x: -x[1]):
        print(f"  {action}: {count:,}")

    if analysis:
        print("\n" + "-"*70)
        print("TOP WALLETS BY VOLUME")
        print("-"*70)
        print(f"{'Rank':<6}{'Wallet':<46}{'Volume':>14}{'Trades':>8}")
        print("-"*70)

        for i, w in enumerate(analysis["top_wallets"][:20], 1):
            wallet_short = w["wallet"][:44] + ".." if len(w["wallet"]) > 44 else w["wallet"]
            print(f"{i:<6}{wallet_short:<46}${w['total_volume']:>12,.0f}{w['trade_count']:>8}")


def main():
    parser = argparse.ArgumentParser(description="Pull real-time Drift data via Helius")
    parser.add_argument("--api-key", type=str, required=True, help="Helius API key")
    parser.add_argument("--limit", type=int, default=5000, help="Max transactions to fetch")
    parser.add_argument("--days", type=int, help="Only fetch last N days")
    parser.add_argument("--output", type=str, default="./drift_realtime_data", help="Output directory")
    parser.add_argument("--analyze", action="store_true", help="Run wallet analysis")
    parser.add_argument("--top", type=int, default=50, help="Top N wallets to show")

    args = parser.parse_args()

    cutoff_date = None
    if args.days:
        cutoff_date = datetime.now() - timedelta(days=args.days)

    stats, trades = pull_drift_realtime(
        api_key=args.api_key,
        max_transactions=args.limit,
        cutoff_date=cutoff_date,
        output_dir=args.output
    )

    analysis = None
    if args.analyze and trades:
        analysis = analyze_wallets(trades, args.top)

    print_summary(stats, analysis)


if __name__ == "__main__":
    main()
