#!/usr/bin/env python3
"""
Drift Protocol Historical Data Puller

Pulls trade data from Drift's public S3 bucket for wallet-level analysis.
Supports multiple markets and date ranges.

Usage:
    python drift_puller.py --markets SOL-PERP,BTC-PERP,ETH-PERP --days 7
    python drift_puller.py --markets ALL --start 2024-12-01 --end 2024-12-31
"""

import argparse
import gzip
import io
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError
import csv
import json

# Drift S3 bucket configuration
BASE_URL = "https://drift-historical-data-v2.s3.eu-west-1.amazonaws.com"
PROGRAM_ID = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"

# Available perp markets (as of Jan 2025)
PERP_MARKETS = [
    "SOL-PERP", "BTC-PERP", "ETH-PERP", "DOGE-PERP", "BNB-PERP",
    "ARB-PERP", "APT-PERP", "AVAX-PERP", "LINK-PERP", "LTC-PERP",
    "MATIC-PERP", "INJ-PERP", "JTO-PERP", "JUP-PERP", "DRIFT-PERP",
    "HNT-PERP", "HYPE-PERP", "GOAT-PERP", "MOODENG-PERP", "WIF-PERP",
    "BONK-PERP", "PEPE-PERP", "SUI-PERP", "SEI-PERP", "TIA-PERP",
    "PYTH-PERP", "RNDR-PERP", "OP-PERP", "STX-PERP", "NEAR-PERP"
]

# CSV columns in Drift trade records
TRADE_COLUMNS = [
    "fillerReward", "baseAssetAmountFilled", "quoteAssetAmountFilled",
    "takerFee", "makerRebate", "referrerReward", "quoteAssetAmountSurplus",
    "takerOrderBaseAssetAmount", "takerOrderCumulativeBaseAssetAmountFilled",
    "takerOrderCumulativeQuoteAssetAmountFilled", "makerOrderBaseAssetAmount",
    "makerOrderCumulativeBaseAssetAmountFilled", "makerOrderCumulativeQuoteAssetAmountFilled",
    "oraclePrice", "makerFee", "txSig", "slot", "ts", "action", "actionExplanation",
    "marketIndex", "marketType", "filler", "fillRecordId", "taker", "takerOrderId",
    "takerOrderDirection", "maker", "makerOrderId", "makerOrderDirection",
    "spotFulfillmentMethodFee", "programId"
]


def fetch_trade_records(market: str, date: datetime) -> Optional[list[dict]]:
    """Fetch trade records for a specific market and date."""
    year = date.strftime("%Y")
    date_str = date.strftime("%Y%m%d")
    url = f"{BASE_URL}/program/{PROGRAM_ID}/market/{market}/tradeRecords/{year}/{date_str}"

    try:
        req = Request(url, headers={"Accept-Encoding": "gzip"})
        with urlopen(req, timeout=30) as response:
            # Data is gzip compressed
            compressed = response.read()
            try:
                decompressed = gzip.decompress(compressed)
            except:
                decompressed = compressed

            # Parse CSV
            reader = csv.DictReader(io.StringIO(decompressed.decode("utf-8")))
            records = []
            for row in reader:
                row["_market"] = market
                row["_date"] = date_str
                records.append(row)
            return records

    except HTTPError as e:
        if e.code == 404:
            return None  # No data for this date
        raise
    except Exception as e:
        print(f"Error fetching {market} {date_str}: {e}", file=sys.stderr)
        return None


def get_date_range(start: datetime, end: datetime) -> list[datetime]:
    """Generate list of dates between start and end."""
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def pull_data(
    markets: list[str],
    start_date: datetime,
    end_date: datetime,
    output_dir: str,
    max_workers: int = 10,
    output_format: str = "csv"
) -> dict:
    """
    Pull trade data for specified markets and date range.

    Returns stats about the pull operation.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    dates = get_date_range(start_date, end_date)
    tasks = [(market, date) for market in markets for date in dates]

    all_records = []
    stats = {
        "markets": markets,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_tasks": len(tasks),
        "successful": 0,
        "failed": 0,
        "no_data": 0,
        "total_trades": 0,
        "unique_takers": set(),
        "unique_makers": set(),
        "volume_by_market": {},
    }

    print(f"Pulling data for {len(markets)} markets over {len(dates)} days ({len(tasks)} total requests)")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(fetch_trade_records, market, date): (market, date)
            for market, date in tasks
        }

        completed = 0
        for future in as_completed(future_to_task):
            market, date = future_to_task[future]
            completed += 1

            try:
                records = future.result()
                if records is None:
                    stats["no_data"] += 1
                elif len(records) == 0:
                    stats["no_data"] += 1
                else:
                    stats["successful"] += 1
                    stats["total_trades"] += len(records)
                    all_records.extend(records)

                    # Update stats
                    for r in records:
                        if r.get("taker"):
                            stats["unique_takers"].add(r["taker"])
                        if r.get("maker"):
                            stats["unique_makers"].add(r["maker"])

                        vol = float(r.get("quoteAssetAmountFilled", 0) or 0)
                        if market not in stats["volume_by_market"]:
                            stats["volume_by_market"][market] = 0
                        stats["volume_by_market"][market] += vol

            except Exception as e:
                stats["failed"] += 1
                print(f"Failed {market} {date.strftime('%Y%m%d')}: {e}", file=sys.stderr)

            # Progress
            if completed % 50 == 0 or completed == len(tasks):
                print(f"Progress: {completed}/{len(tasks)} ({stats['total_trades']:,} trades)")

    # Convert sets to counts for JSON serialization
    stats["unique_takers"] = len(stats["unique_takers"])
    stats["unique_makers"] = len(stats["unique_makers"])

    # Save data
    if all_records:
        if output_format == "csv":
            output_file = output_path / f"drift_trades_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            with open(output_file, "w", newline="") as f:
                # Add our custom columns to the fieldnames
                fieldnames = list(all_records[0].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_records)
            print(f"\nSaved {len(all_records):,} trades to {output_file}")

        elif output_format == "jsonl":
            output_file = output_path / f"drift_trades_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.jsonl"
            with open(output_file, "w") as f:
                for record in all_records:
                    f.write(json.dumps(record) + "\n")
            print(f"\nSaved {len(all_records):,} trades to {output_file}")

    # Save stats
    stats_file = output_path / f"drift_stats_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.json"
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    return stats


def analyze_wallets(csv_file: str, top_n: int = 50) -> dict:
    """
    Analyze wallet activity from pulled trade data.

    Returns wallet stats including:
    - Volume by wallet
    - Trade count
    - Markets traded
    - Long/short ratio
    """
    wallets = {}

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            taker = row.get("taker", "")
            maker = row.get("maker", "")
            volume = float(row.get("quoteAssetAmountFilled", 0) or 0)
            market = row.get("_market", "")
            direction = row.get("takerOrderDirection", "")

            # Process taker
            if taker:
                if taker not in wallets:
                    wallets[taker] = {
                        "wallet": taker,
                        "taker_volume": 0,
                        "maker_volume": 0,
                        "taker_trades": 0,
                        "maker_trades": 0,
                        "markets": set(),
                        "long_volume": 0,
                        "short_volume": 0,
                    }
                wallets[taker]["taker_volume"] += volume
                wallets[taker]["taker_trades"] += 1
                wallets[taker]["markets"].add(market)
                if direction == "long":
                    wallets[taker]["long_volume"] += volume
                elif direction == "short":
                    wallets[taker]["short_volume"] += volume

            # Process maker
            if maker:
                if maker not in wallets:
                    wallets[maker] = {
                        "wallet": maker,
                        "taker_volume": 0,
                        "maker_volume": 0,
                        "taker_trades": 0,
                        "maker_trades": 0,
                        "markets": set(),
                        "long_volume": 0,
                        "short_volume": 0,
                    }
                wallets[maker]["maker_volume"] += volume
                wallets[maker]["maker_trades"] += 1
                wallets[maker]["markets"].add(market)

    # Calculate totals and convert sets
    for w in wallets.values():
        w["total_volume"] = w["taker_volume"] + w["maker_volume"]
        w["total_trades"] = w["taker_trades"] + w["maker_trades"]
        w["num_markets"] = len(w["markets"])
        w["markets"] = list(w["markets"])
        w["long_short_ratio"] = (
            w["long_volume"] / w["short_volume"]
            if w["short_volume"] > 0 else float("inf")
        )

    # Sort by total volume
    sorted_wallets = sorted(
        wallets.values(),
        key=lambda x: x["total_volume"],
        reverse=True
    )

    return {
        "total_wallets": len(wallets),
        "top_wallets": sorted_wallets[:top_n],
        "multi_market_traders": len([w for w in wallets.values() if w["num_markets"] > 1]),
    }


def print_wallet_summary(analysis: dict):
    """Pretty print wallet analysis."""
    print("\n" + "="*80)
    print("DRIFT WALLET ANALYSIS")
    print("="*80)
    print(f"\nTotal unique wallets: {analysis['total_wallets']:,}")
    print(f"Multi-market traders: {analysis['multi_market_traders']:,} ({analysis['multi_market_traders']/analysis['total_wallets']*100:.1f}%)")

    print("\n" + "-"*80)
    print("TOP WALLETS BY VOLUME")
    print("-"*80)
    print(f"{'Rank':<6}{'Wallet':<46}{'Volume':>14}{'Trades':>10}{'Markets':>8}")
    print("-"*80)

    for i, w in enumerate(analysis["top_wallets"][:25], 1):
        wallet_short = w["wallet"][:44] + ".." if len(w["wallet"]) > 44 else w["wallet"]
        print(f"{i:<6}{wallet_short:<46}${w['total_volume']:>12,.0f}{w['total_trades']:>10,}{w['num_markets']:>8}")

    print("\n" + "-"*80)
    print("MULTI-MARKET TRADERS (trading 3+ markets)")
    print("-"*80)

    multi = [w for w in analysis["top_wallets"] if w["num_markets"] >= 3][:15]
    for w in multi:
        print(f"\n{w['wallet'][:50]}...")
        print(f"  Volume: ${w['total_volume']:,.0f} | Trades: {w['total_trades']:,}")
        print(f"  Markets: {', '.join(w['markets'][:5])}" + ("..." if len(w['markets']) > 5 else ""))
        if w["long_volume"] + w["short_volume"] > 0:
            long_pct = w["long_volume"] / (w["long_volume"] + w["short_volume"]) * 100
            print(f"  Direction: {long_pct:.0f}% long / {100-long_pct:.0f}% short")


def main():
    parser = argparse.ArgumentParser(
        description="Pull and analyze Drift Protocol trade data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Pull last 7 days of SOL, BTC, ETH perp data
  python drift_puller.py --markets SOL-PERP,BTC-PERP,ETH-PERP --days 7

  # Pull all perp markets for a specific date range
  python drift_puller.py --markets ALL --start 2024-12-01 --end 2024-12-31

  # Pull data and analyze wallets
  python drift_puller.py --markets SOL-PERP,BTC-PERP --days 7 --analyze

  # Just analyze existing data
  python drift_puller.py --analyze-file ./drift_data/drift_trades_20241201_20241207.csv

Available markets: SOL-PERP, BTC-PERP, ETH-PERP, DOGE-PERP, JUP-PERP, etc.
        """
    )

    parser.add_argument(
        "--markets",
        type=str,
        default="SOL-PERP,BTC-PERP,ETH-PERP",
        help="Comma-separated list of markets, or ALL for all perp markets"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to pull (from today backwards)"
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date (YYYY-MM-DD), overrides --days"
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date (YYYY-MM-DD), defaults to yesterday"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./drift_data",
        help="Output directory"
    )
    parser.add_argument(
        "--format",
        choices=["csv", "jsonl"],
        default="csv",
        help="Output format"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel download workers"
    )
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Run wallet analysis after pulling data"
    )
    parser.add_argument(
        "--analyze-file",
        type=str,
        help="Analyze an existing CSV file (skip pulling)"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Number of top wallets to show in analysis"
    )

    args = parser.parse_args()

    # Handle analyze-only mode
    if args.analyze_file:
        print(f"Analyzing {args.analyze_file}...")
        analysis = analyze_wallets(args.analyze_file, args.top)
        print_wallet_summary(analysis)

        # Save analysis
        analysis_file = args.analyze_file.replace(".csv", "_wallet_analysis.json")
        with open(analysis_file, "w") as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"\nSaved analysis to {analysis_file}")
        return

    # Parse markets
    if args.markets.upper() == "ALL":
        markets = PERP_MARKETS
    else:
        markets = [m.strip().upper() for m in args.markets.split(",")]

    # Parse dates
    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
    else:
        start_date = datetime.now() - timedelta(days=args.days)

    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    else:
        end_date = datetime.now() - timedelta(days=1)  # Yesterday (today may be incomplete)

    print(f"\nDrift Data Puller")
    print(f"  Markets: {', '.join(markets)}")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    print(f"  Output: {args.output}")
    print()

    # Pull data
    stats = pull_data(
        markets=markets,
        start_date=start_date,
        end_date=end_date,
        output_dir=args.output,
        max_workers=args.workers,
        output_format=args.format
    )

    # Print summary
    print("\n" + "="*60)
    print("PULL SUMMARY")
    print("="*60)
    print(f"Total trades: {stats['total_trades']:,}")
    print(f"Unique takers: {stats['unique_takers']:,}")
    print(f"Unique makers: {stats['unique_makers']:,}")
    print(f"\nVolume by market:")
    for market, vol in sorted(stats["volume_by_market"].items(), key=lambda x: -x[1]):
        print(f"  {market}: ${vol:,.0f}")

    # Run analysis if requested
    if args.analyze and stats["total_trades"] > 0:
        csv_file = Path(args.output) / f"drift_trades_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        if csv_file.exists():
            print("\nRunning wallet analysis...")
            analysis = analyze_wallets(str(csv_file), args.top)
            print_wallet_summary(analysis)

            # Save analysis
            analysis_file = str(csv_file).replace(".csv", "_wallet_analysis.json")
            with open(analysis_file, "w") as f:
                json.dump(analysis, f, indent=2, default=str)
            print(f"\nSaved analysis to {analysis_file}")


if __name__ == "__main__":
    main()
