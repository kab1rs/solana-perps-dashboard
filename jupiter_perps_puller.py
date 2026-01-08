#!/usr/bin/env python3
"""
Jupiter Perpetuals Data Puller

Pulls perp trade data from Jupiter Perpetuals using Helius Enhanced Transactions API.
Requires a Helius API key (free tier available at helius.dev).

Usage:
    python jupiter_perps_puller.py --api-key YOUR_HELIUS_KEY --days 7
    python jupiter_perps_puller.py --api-key YOUR_HELIUS_KEY --limit 5000 --analyze
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

# Jupiter Perpetuals Program ID
JUPITER_PERPS_PROGRAM = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"

# Helius API base URL
HELIUS_BASE_URL = "https://api.helius.xyz/v0"

# Instruction types we care about for perp trading
PERP_INSTRUCTION_TYPES = {
    "increasePosition": "open_long",
    "increaseLongPosition": "open_long",
    "increaseShortPosition": "open_short",
    "decreasePosition": "close",
    "decreaseLongPosition": "close_long",
    "decreaseShortPosition": "close_short",
    "liquidate": "liquidation",
    "liquidateLongPosition": "liquidation",
    "liquidateShortPosition": "liquidation",
    "openPosition": "open",
    "closePosition": "close",
}

# Known Jupiter Perps markets (custody accounts)
JUPITER_MARKETS = {
    "SOL": "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz",
    "ETH": "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn",
    "BTC": "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm",
}


def fetch_transactions(api_key: str, before_sig: Optional[str] = None, limit: int = 100) -> list:
    """Fetch transactions for Jupiter Perps program from Helius."""
    url = f"{HELIUS_BASE_URL}/addresses/{JUPITER_PERPS_PROGRAM}/transactions"
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


def fetch_parsed_transaction(api_key: str, signature: str) -> Optional[dict]:
    """Fetch a single parsed transaction with full details."""
    url = f"{HELIUS_BASE_URL}/transactions"
    params = f"?api-key={api_key}"

    try:
        data = json.dumps({"transactions": [signature]}).encode("utf-8")
        req = Request(url + params, data=data, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))
            return result[0] if result else None
    except Exception as e:
        print(f"Error fetching parsed tx {signature[:20]}...: {e}", file=sys.stderr)
        return None


def parse_perp_transaction(tx: dict) -> Optional[dict]:
    """Parse a transaction to extract perp trade details."""
    if not tx:
        return None

    sig = tx.get("signature", "")
    timestamp = tx.get("timestamp", 0)
    fee_payer = tx.get("feePayer", "")

    # Look for perp-related instructions
    instructions = tx.get("instructions", [])
    events = tx.get("events", {})

    # Try to extract from native transfers and token transfers
    native_transfers = tx.get("nativeTransfers", [])
    token_transfers = tx.get("tokenTransfers", [])

    # Parse account data for position info
    account_data = tx.get("accountData", [])

    # Extract instruction details
    perp_action = None
    for inst in instructions:
        program_id = inst.get("programId", "")
        if program_id == JUPITER_PERPS_PROGRAM:
            # Try to get instruction name from parsed data
            parsed = inst.get("parsed", {})
            inst_type = inst.get("type", "") or parsed.get("type", "")

            if inst_type in PERP_INSTRUCTION_TYPES:
                perp_action = PERP_INSTRUCTION_TYPES[inst_type]
                break

            # Check inner instructions
            inner = inst.get("innerInstructions", [])
            for inner_inst in inner:
                inner_type = inner_inst.get("type", "")
                if inner_type in PERP_INSTRUCTION_TYPES:
                    perp_action = PERP_INSTRUCTION_TYPES[inner_type]
                    break

    # If we couldn't determine action from instructions, try description
    description = tx.get("description", "")
    if not perp_action and description:
        desc_lower = description.lower()
        if "increase" in desc_lower and "position" in desc_lower:
            perp_action = "open"
        elif "decrease" in desc_lower and "position" in desc_lower:
            perp_action = "close"
        elif "liquidat" in desc_lower:
            perp_action = "liquidation"

    # Calculate volume from token transfers
    volume_usd = 0
    collateral_token = None
    for transfer in token_transfers:
        # USDC mint
        if transfer.get("mint") == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v":
            amount = float(transfer.get("tokenAmount", 0))
            volume_usd += amount
            collateral_token = "USDC"
        # USDT mint
        elif transfer.get("mint") == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB":
            amount = float(transfer.get("tokenAmount", 0))
            volume_usd += amount
            collateral_token = "USDT"

    # Also check native SOL transfers (converted at oracle price if available)
    for transfer in native_transfers:
        amount_sol = float(transfer.get("amount", 0)) / 1e9
        # Rough SOL price estimate - in production you'd want oracle price
        volume_usd += amount_sol * 200  # Placeholder

    # Determine market from accounts
    market = "UNKNOWN"
    accounts = []
    for inst in instructions:
        if inst.get("programId") == JUPITER_PERPS_PROGRAM:
            accounts = inst.get("accounts", [])
            break

    for acc in accounts:
        for symbol, custody in JUPITER_MARKETS.items():
            if acc == custody:
                market = symbol
                break

    # Build record
    if perp_action or volume_usd > 0:
        return {
            "signature": sig,
            "timestamp": timestamp,
            "datetime": datetime.fromtimestamp(timestamp).isoformat() if timestamp else "",
            "wallet": fee_payer,
            "action": perp_action or "unknown",
            "market": market,
            "volume_usd": volume_usd,
            "collateral_token": collateral_token,
            "fee": tx.get("fee", 0) / 1e9,
            "success": tx.get("transactionError") is None,
            "description": description[:200] if description else "",
        }

    return None


def pull_jupiter_perps(
    api_key: str,
    max_transactions: int = 1000,
    cutoff_date: Optional[datetime] = None,
    output_dir: str = "./jupiter_data"
) -> dict:
    """
    Pull Jupiter Perps transactions.

    Args:
        api_key: Helius API key
        max_transactions: Maximum number of transactions to fetch
        cutoff_date: Stop fetching when transactions are older than this
        output_dir: Directory to save output files
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    all_trades = []
    last_sig = None
    total_fetched = 0
    batch_size = 100

    stats = {
        "total_fetched": 0,
        "perp_trades": 0,
        "unique_wallets": set(),
        "volume_by_market": {},
        "actions": {},
        "start_time": None,
        "end_time": None,
    }

    print(f"Fetching Jupiter Perps transactions (max {max_transactions})...")

    while total_fetched < max_transactions:
        # Fetch batch of transactions
        txs = fetch_transactions(api_key, last_sig, batch_size)

        if not txs:
            print("No more transactions available")
            break

        for tx in txs:
            total_fetched += 1

            # Check timestamp cutoff
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

            # Parse the transaction
            parsed = parse_perp_transaction(tx)
            if parsed:
                all_trades.append(parsed)
                stats["perp_trades"] += 1
                stats["unique_wallets"].add(parsed["wallet"])

                market = parsed["market"]
                if market not in stats["volume_by_market"]:
                    stats["volume_by_market"][market] = 0
                stats["volume_by_market"][market] += parsed["volume_usd"]

                action = parsed["action"]
                if action not in stats["actions"]:
                    stats["actions"][action] = 0
                stats["actions"][action] += 1

        # Update for pagination
        if txs:
            last_sig = txs[-1].get("signature")

        # Progress update
        if total_fetched % 500 == 0:
            print(f"Fetched {total_fetched} transactions, {len(all_trades)} perp trades")

        # Small delay to avoid rate limits
        time.sleep(0.1)

        # Check if we hit cutoff
        if cutoff_date and stats["end_time"] and stats["end_time"] < cutoff_date:
            break

    stats["total_fetched"] = total_fetched
    stats["unique_wallets"] = len(stats["unique_wallets"])

    # Save data
    if all_trades:
        # Sort by timestamp descending
        all_trades.sort(key=lambda x: x["timestamp"], reverse=True)

        # Save CSV
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = output_path / f"jupiter_perps_{date_str}.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_trades[0].keys())
            writer.writeheader()
            writer.writerows(all_trades)
        print(f"\nSaved {len(all_trades)} trades to {csv_file}")

        # Save stats
        stats_file = output_path / f"jupiter_stats_{date_str}.json"
        with open(stats_file, "w") as f:
            stats_copy = stats.copy()
            stats_copy["start_time"] = stats["start_time"].isoformat() if stats["start_time"] else None
            stats_copy["end_time"] = stats["end_time"].isoformat() if stats["end_time"] else None
            json.dump(stats_copy, f, indent=2)
        print(f"Saved stats to {stats_file}")

    return stats, all_trades


def analyze_jupiter_wallets(trades: list, top_n: int = 50) -> dict:
    """Analyze wallet activity from Jupiter Perps trades."""
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
                "markets": set(),
                "opens": 0,
                "closes": 0,
                "liquidations": 0,
            }

        wallets[wallet]["total_volume"] += trade.get("volume_usd", 0)
        wallets[wallet]["trade_count"] += 1
        wallets[wallet]["markets"].add(trade.get("market", "UNKNOWN"))

        action = trade.get("action", "")
        if "open" in action:
            wallets[wallet]["opens"] += 1
        elif "close" in action:
            wallets[wallet]["closes"] += 1
        elif "liquidation" in action:
            wallets[wallet]["liquidations"] += 1

    # Calculate additional metrics
    for w in wallets.values():
        w["num_markets"] = len(w["markets"])
        w["markets"] = list(w["markets"])
        w["avg_trade_size"] = w["total_volume"] / w["trade_count"] if w["trade_count"] > 0 else 0

    # Sort by volume
    sorted_wallets = sorted(wallets.values(), key=lambda x: x["total_volume"], reverse=True)

    return {
        "total_wallets": len(wallets),
        "top_wallets": sorted_wallets[:top_n],
        "multi_market": len([w for w in wallets.values() if w["num_markets"] > 1]),
    }


def print_jupiter_summary(stats: dict, analysis: Optional[dict] = None):
    """Print summary of Jupiter Perps data."""
    print("\n" + "="*70)
    print("JUPITER PERPS SUMMARY")
    print("="*70)

    if stats.get("start_time") and stats.get("end_time"):
        print(f"Date range: {stats['end_time']} to {stats['start_time']}")

    print(f"\nTransactions fetched: {stats['total_fetched']:,}")
    print(f"Perp trades found: {stats['perp_trades']:,}")
    print(f"Unique wallets: {stats['unique_wallets']:,}")

    print("\nVolume by market:")
    for market, vol in sorted(stats["volume_by_market"].items(), key=lambda x: -x[1]):
        print(f"  {market}: ${vol:,.0f}")

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
    parser = argparse.ArgumentParser(
        description="Pull Jupiter Perpetuals trade data via Helius API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Pull last 1000 transactions
  python jupiter_perps_puller.py --api-key YOUR_KEY --limit 1000

  # Pull last 7 days of data
  python jupiter_perps_puller.py --api-key YOUR_KEY --days 7

  # Pull and analyze
  python jupiter_perps_puller.py --api-key YOUR_KEY --limit 5000 --analyze

Get a free Helius API key at: https://helius.dev
        """
    )

    parser.add_argument(
        "--api-key",
        type=str,
        required=True,
        help="Helius API key (get free at helius.dev)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of transactions to fetch"
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Only fetch transactions from the last N days"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./jupiter_data",
        help="Output directory"
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Run wallet analysis after pulling data"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Number of top wallets to show"
    )

    args = parser.parse_args()

    # Calculate cutoff date if specified
    cutoff_date = None
    if args.days:
        cutoff_date = datetime.now() - timedelta(days=args.days)
        print(f"Will fetch transactions since {cutoff_date}")

    # Pull data
    stats, trades = pull_jupiter_perps(
        api_key=args.api_key,
        max_transactions=args.limit,
        cutoff_date=cutoff_date,
        output_dir=args.output
    )

    # Analyze if requested
    analysis = None
    if args.analyze and trades:
        analysis = analyze_jupiter_wallets(trades, args.top)

    # Print summary
    print_jupiter_summary(stats, analysis)

    if analysis:
        # Save analysis
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        analysis_file = Path(args.output) / f"jupiter_wallet_analysis_{date_str}.json"
        with open(analysis_file, "w") as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"\nSaved wallet analysis to {analysis_file}")


if __name__ == "__main__":
    main()
