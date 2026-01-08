#!/usr/bin/env python3
"""
Cross-Venue Perp DEX Analyzer

Combines wallet activity data from multiple Solana perp DEXs (Drift, Jupiter, etc.)
to identify traders operating across venues.

Usage:
    python cross_venue_analyzer.py --drift drift_data/drift_trades_*.csv --jupiter jupiter_data/jupiter_perps_*.csv
"""

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional
import glob


def load_drift_data(csv_files: list[str]) -> list[dict]:
    """Load and normalize Drift trade data."""
    trades = []
    for file_path in csv_files:
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Normalize to common format
                trades.append({
                    "venue": "drift",
                    "wallet": row.get("taker") or row.get("maker", ""),
                    "role": "taker" if row.get("taker") else "maker",
                    "volume_usd": float(row.get("quoteAssetAmountFilled", 0) or 0),
                    "market": row.get("_market", "").replace("-PERP", ""),
                    "direction": row.get("takerOrderDirection", ""),
                    "timestamp": int(row.get("ts", 0) or 0),
                    "signature": row.get("txSig", ""),
                })
    return trades


def load_jupiter_data(csv_files: list[str]) -> list[dict]:
    """Load and normalize Jupiter Perps trade data."""
    trades = []
    for file_path in csv_files:
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                action = row.get("action", "")
                direction = ""
                if "long" in action:
                    direction = "long"
                elif "short" in action:
                    direction = "short"

                trades.append({
                    "venue": "jupiter",
                    "wallet": row.get("wallet", ""),
                    "role": "taker",  # Jupiter is always taker (vs pool)
                    "volume_usd": float(row.get("volume_usd", 0) or 0),
                    "market": row.get("market", "UNKNOWN"),
                    "direction": direction,
                    "timestamp": int(row.get("timestamp", 0) or 0),
                    "signature": row.get("signature", ""),
                })
    return trades


def analyze_cross_venue(trades: list[dict], min_volume: float = 1000) -> dict:
    """
    Analyze wallet activity across venues.

    Returns detailed stats about cross-venue trading patterns.
    """
    wallets = defaultdict(lambda: {
        "wallet": "",
        "venues": set(),
        "total_volume": 0,
        "total_trades": 0,
        "volume_by_venue": defaultdict(float),
        "trades_by_venue": defaultdict(int),
        "markets_by_venue": defaultdict(set),
        "long_volume": 0,
        "short_volume": 0,
        "first_seen": None,
        "last_seen": None,
    })

    for trade in trades:
        wallet = trade["wallet"]
        if not wallet:
            continue

        w = wallets[wallet]
        w["wallet"] = wallet
        w["venues"].add(trade["venue"])
        w["total_volume"] += trade["volume_usd"]
        w["total_trades"] += 1
        w["volume_by_venue"][trade["venue"]] += trade["volume_usd"]
        w["trades_by_venue"][trade["venue"]] += 1
        w["markets_by_venue"][trade["venue"]].add(trade["market"])

        if trade["direction"] == "long":
            w["long_volume"] += trade["volume_usd"]
        elif trade["direction"] == "short":
            w["short_volume"] += trade["volume_usd"]

        ts = trade["timestamp"]
        if ts:
            if w["first_seen"] is None or ts < w["first_seen"]:
                w["first_seen"] = ts
            if w["last_seen"] is None or ts > w["last_seen"]:
                w["last_seen"] = ts

    # Filter by minimum volume and process
    result_wallets = []
    for w in wallets.values():
        if w["total_volume"] < min_volume:
            continue

        # Convert sets to lists for JSON
        w["venues"] = list(w["venues"])
        w["num_venues"] = len(w["venues"])
        w["volume_by_venue"] = dict(w["volume_by_venue"])
        w["trades_by_venue"] = dict(w["trades_by_venue"])
        w["markets_by_venue"] = {k: list(v) for k, v in w["markets_by_venue"].items()}

        # Calculate metrics
        w["is_cross_venue"] = w["num_venues"] > 1
        if w["long_volume"] + w["short_volume"] > 0:
            w["long_pct"] = w["long_volume"] / (w["long_volume"] + w["short_volume"]) * 100
        else:
            w["long_pct"] = 50

        # Time range
        if w["first_seen"] and w["last_seen"]:
            w["active_days"] = (w["last_seen"] - w["first_seen"]) / 86400
        else:
            w["active_days"] = 0

        result_wallets.append(w)

    # Sort by total volume
    result_wallets.sort(key=lambda x: x["total_volume"], reverse=True)

    # Calculate aggregate stats
    cross_venue_wallets = [w for w in result_wallets if w["is_cross_venue"]]
    single_venue_wallets = [w for w in result_wallets if not w["is_cross_venue"]]

    cross_venue_volume = sum(w["total_volume"] for w in cross_venue_wallets)
    single_venue_volume = sum(w["total_volume"] for w in single_venue_wallets)

    # Venue comparison
    venue_stats = defaultdict(lambda: {"wallets": 0, "volume": 0, "trades": 0})
    for w in result_wallets:
        for venue in w["venues"]:
            venue_stats[venue]["wallets"] += 1
            venue_stats[venue]["volume"] += w["volume_by_venue"].get(venue, 0)
            venue_stats[venue]["trades"] += w["trades_by_venue"].get(venue, 0)

    return {
        "total_wallets": len(result_wallets),
        "cross_venue_wallets": len(cross_venue_wallets),
        "cross_venue_pct": len(cross_venue_wallets) / len(result_wallets) * 100 if result_wallets else 0,
        "cross_venue_volume": cross_venue_volume,
        "cross_venue_volume_pct": cross_venue_volume / (cross_venue_volume + single_venue_volume) * 100 if (cross_venue_volume + single_venue_volume) > 0 else 0,
        "venue_stats": dict(venue_stats),
        "wallets": result_wallets,
        "top_cross_venue": [w for w in result_wallets if w["is_cross_venue"]][:50],
    }


def print_cross_venue_report(analysis: dict):
    """Print a detailed cross-venue analysis report."""
    print("\n" + "="*80)
    print("CROSS-VENUE PERP DEX ANALYSIS")
    print("="*80)

    print(f"\n{'OVERVIEW':^80}")
    print("-"*80)
    print(f"Total wallets analyzed: {analysis['total_wallets']:,}")
    print(f"Cross-venue traders: {analysis['cross_venue_wallets']:,} ({analysis['cross_venue_pct']:.1f}%)")
    print(f"Cross-venue volume: ${analysis['cross_venue_volume']:,.0f} ({analysis['cross_venue_volume_pct']:.1f}% of total)")

    print(f"\n{'VENUE BREAKDOWN':^80}")
    print("-"*80)
    print(f"{'Venue':<15}{'Wallets':>12}{'Volume':>20}{'Trades':>15}")
    print("-"*80)
    for venue, stats in sorted(analysis["venue_stats"].items()):
        print(f"{venue:<15}{stats['wallets']:>12,}${stats['volume']:>18,.0f}{stats['trades']:>15,}")

    print(f"\n{'TOP CROSS-VENUE TRADERS':^80}")
    print("-"*80)
    print(f"{'Wallet':<45}{'Total Vol':>14}{'Drift':>12}{'Jupiter':>12}")
    print("-"*80)

    for w in analysis["top_cross_venue"][:25]:
        wallet_short = w["wallet"][:43] + ".." if len(w["wallet"]) > 43 else w["wallet"]
        drift_vol = w["volume_by_venue"].get("drift", 0)
        jup_vol = w["volume_by_venue"].get("jupiter", 0)
        print(f"{wallet_short:<45}${w['total_volume']:>12,.0f}${drift_vol:>10,.0f}${jup_vol:>10,.0f}")

    # Identify interesting patterns
    print(f"\n{'INTERESTING PATTERNS':^80}")
    print("-"*80)

    # Heavy Drift traders who also use Jupiter
    drift_heavy = [w for w in analysis["top_cross_venue"]
                   if w["volume_by_venue"].get("drift", 0) > w["volume_by_venue"].get("jupiter", 0) * 2]
    if drift_heavy[:5]:
        print("\nDrift-heavy cross-venue traders (>2x volume on Drift):")
        for w in drift_heavy[:5]:
            print(f"  {w['wallet'][:50]}...")
            print(f"    Drift: ${w['volume_by_venue'].get('drift', 0):,.0f} | Jupiter: ${w['volume_by_venue'].get('jupiter', 0):,.0f}")

    # Heavy Jupiter traders who also use Drift
    jup_heavy = [w for w in analysis["top_cross_venue"]
                 if w["volume_by_venue"].get("jupiter", 0) > w["volume_by_venue"].get("drift", 0) * 2]
    if jup_heavy[:5]:
        print("\nJupiter-heavy cross-venue traders (>2x volume on Jupiter):")
        for w in jup_heavy[:5]:
            print(f"  {w['wallet'][:50]}...")
            print(f"    Jupiter: ${w['volume_by_venue'].get('jupiter', 0):,.0f} | Drift: ${w['volume_by_venue'].get('drift', 0):,.0f}")

    # Balanced traders
    balanced = [w for w in analysis["top_cross_venue"]
                if 0.5 < (w["volume_by_venue"].get("drift", 0) / max(w["volume_by_venue"].get("jupiter", 1), 1)) < 2]
    if balanced[:5]:
        print("\nBalanced cross-venue traders (similar volume on both):")
        for w in balanced[:5]:
            print(f"  {w['wallet'][:50]}...")
            print(f"    Drift: ${w['volume_by_venue'].get('drift', 0):,.0f} | Jupiter: ${w['volume_by_venue'].get('jupiter', 0):,.0f}")

    # Directional bias
    print("\nDirectional bias among cross-venue traders:")
    long_biased = [w for w in analysis["top_cross_venue"] if w.get("long_pct", 50) > 65]
    short_biased = [w for w in analysis["top_cross_venue"] if w.get("long_pct", 50) < 35]
    print(f"  Long-biased (>65% long): {len(long_biased)} traders")
    print(f"  Short-biased (>65% short): {len(short_biased)} traders")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze cross-venue perp DEX trading activity",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze Drift and Jupiter data
  python cross_venue_analyzer.py \\
    --drift "drift_data/drift_trades_*.csv" \\
    --jupiter "jupiter_data/jupiter_perps_*.csv"

  # Filter for wallets with >$10k volume
  python cross_venue_analyzer.py \\
    --drift "drift_data/*.csv" \\
    --jupiter "jupiter_data/*.csv" \\
    --min-volume 10000

  # Analyze only Drift data
  python cross_venue_analyzer.py --drift "drift_data/*.csv"
        """
    )

    parser.add_argument(
        "--drift",
        type=str,
        help="Glob pattern for Drift CSV files"
    )
    parser.add_argument(
        "--jupiter",
        type=str,
        help="Glob pattern for Jupiter CSV files"
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=1000,
        help="Minimum volume threshold for wallet analysis"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./analysis",
        help="Output directory"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Number of top wallets to include in output"
    )

    args = parser.parse_args()

    if not args.drift and not args.jupiter:
        parser.error("At least one of --drift or --jupiter must be specified")

    all_trades = []

    # Load Drift data
    if args.drift:
        drift_files = glob.glob(args.drift)
        if drift_files:
            print(f"Loading {len(drift_files)} Drift file(s)...")
            drift_trades = load_drift_data(drift_files)
            all_trades.extend(drift_trades)
            print(f"  Loaded {len(drift_trades):,} Drift trades")
        else:
            print(f"Warning: No files matched pattern '{args.drift}'")

    # Load Jupiter data
    if args.jupiter:
        jupiter_files = glob.glob(args.jupiter)
        if jupiter_files:
            print(f"Loading {len(jupiter_files)} Jupiter file(s)...")
            jupiter_trades = load_jupiter_data(jupiter_files)
            all_trades.extend(jupiter_trades)
            print(f"  Loaded {len(jupiter_trades):,} Jupiter trades")
        else:
            print(f"Warning: No files matched pattern '{args.jupiter}'")

    if not all_trades:
        print("No trades loaded. Check your file patterns.")
        return

    print(f"\nTotal trades loaded: {len(all_trades):,}")

    # Run analysis
    print(f"\nAnalyzing cross-venue activity (min volume: ${args.min_volume:,.0f})...")
    analysis = analyze_cross_venue(all_trades, args.min_volume)

    # Print report
    print_cross_venue_report(analysis)

    # Save results
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"cross_venue_analysis_{date_str}.json"

    # Prepare for JSON serialization
    analysis_json = {
        "total_wallets": analysis["total_wallets"],
        "cross_venue_wallets": analysis["cross_venue_wallets"],
        "cross_venue_pct": analysis["cross_venue_pct"],
        "cross_venue_volume": analysis["cross_venue_volume"],
        "cross_venue_volume_pct": analysis["cross_venue_volume_pct"],
        "venue_stats": analysis["venue_stats"],
        "top_cross_venue": analysis["top_cross_venue"][:args.top],
    }

    with open(output_file, "w") as f:
        json.dump(analysis_json, f, indent=2)
    print(f"\nSaved analysis to {output_file}")


if __name__ == "__main__":
    main()
