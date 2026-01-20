#!/usr/bin/env python3
"""
Pacifica Perpetuals Data Puller

Pulls perp trade data from Pacifica Exchange API.
Since Pacifica is an off-chain CLOB without a bulk history endpoint,
this uses a wallet-first approach:
1. Fetch recent trades from each market to discover active wallets
2. Fetch full trade history for discovered wallets

Usage:
    python pacifica_puller.py --days 7 --min-volume 1000
    python pacifica_puller.py --wallets wallet1,wallet2,wallet3
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# Pacifica API base URL
PACIFICA_BASE_URL = "https://api.pacifica.fi/api/v1"

# Known Pacifica markets
PACIFICA_MARKETS = [
    "SOL", "BTC", "ETH", "WIF", "TRUMP", "BONK", "FARTCOIN",
    "PENGU", "AI16Z", "JUP", "POPCAT", "HYPE", "SUI", "DOGE",
    "PNUT", "XRP", "MELANIA", "GOAT", "RAY", "ONDO"
]


def api_request(endpoint: str, params: dict = None) -> Optional[dict]:
    """Make a request to the Pacifica API."""
    url = f"{PACIFICA_BASE_URL}{endpoint}"
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        url = f"{url}?{query}"

    try:
        req = Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })
        with urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))
            if result.get("success"):
                return result
            else:
                print(f"API error: {result.get('error')}", file=sys.stderr)
                return None
    except HTTPError as e:
        if e.code == 429:
            print("Rate limited, waiting 2 seconds...", file=sys.stderr)
            time.sleep(2)
            return api_request(endpoint, params)
        print(f"HTTP error {e.code}: {e.reason}", file=sys.stderr)
        return None
    except URLError as e:
        print(f"URL error: {e.reason}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return None


def fetch_recent_trades(symbol: str) -> list[dict]:
    """Fetch recent public trades for a market."""
    result = api_request("/trades", {"symbol": symbol})
    if result and "data" in result:
        return result["data"]
    return []


def fetch_market_info() -> list[dict]:
    """Fetch available market info."""
    result = api_request("/markets")
    if result and "data" in result:
        return result["data"]
    return []


def fetch_wallet_trade_history(
    wallet: str,
    symbol: str = None,
    start_time: int = None,
    end_time: int = None,
    limit: int = 100
) -> list[dict]:
    """
    Fetch trade history for a specific wallet.

    Args:
        wallet: Wallet address
        symbol: Optional market filter (e.g., "BTC")
        start_time: Start timestamp in milliseconds
        end_time: End timestamp in milliseconds
        limit: Max records per request (default 100)

    Returns:
        List of trade records
    """
    all_trades = []
    cursor = None
    max_pages = 50  # Safety limit

    params = {
        "account": wallet,
        "limit": limit,
    }
    if symbol:
        params["symbol"] = symbol
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time

    for _ in range(max_pages):
        if cursor:
            params["cursor"] = cursor

        result = api_request("/trades/history", params)
        if not result:
            break

        trades = result.get("data", [])
        all_trades.extend(trades)

        if not result.get("has_more", False):
            break

        cursor = result.get("next_cursor")
        if not cursor:
            break

        time.sleep(0.1)  # Rate limit courtesy

    return all_trades


def discover_wallets_from_recent_trades(markets: list[str] = None) -> set[str]:
    """
    Discover active wallets by fetching recent trades from public endpoints.

    Note: The public /trades endpoint doesn't include wallet addresses,
    so this is limited. For full wallet discovery, you'd need additional
    data sources like on-chain transaction parsing.
    """
    if markets is None:
        markets = PACIFICA_MARKETS

    # The public /trades endpoint doesn't include wallet info
    # This is a placeholder - in practice you'd need:
    # 1. A known list of wallets to track
    # 2. On-chain transaction parsing
    # 3. External data source like Dune Analytics

    print("Note: Pacifica's public trade API doesn't expose wallet addresses.")
    print("Please provide wallet addresses via --wallets or --wallets-file")
    return set()


def normalize_trade(trade: dict, wallet: str) -> dict:
    """Normalize a Pacifica trade record to common format."""
    side = trade.get("side", "")
    direction = ""
    if "long" in side:
        direction = "long"
    elif "short" in side:
        direction = "short"

    event_type = trade.get("event_type", "")
    role = "maker" if "maker" in event_type else "taker"

    # Calculate volume: price * amount
    try:
        price = float(trade.get("price", 0) or trade.get("entry_price", 0) or 0)
        amount = float(trade.get("amount", 0) or 0)
        volume_usd = price * amount
    except (ValueError, TypeError):
        volume_usd = 0

    # Timestamp is in milliseconds, convert to seconds for consistency
    created_at = trade.get("created_at", 0)
    timestamp = created_at // 1000 if created_at > 1e12 else created_at

    return {
        "history_id": str(trade.get("history_id", "")),
        "timestamp": timestamp,
        "datetime": datetime.fromtimestamp(timestamp).isoformat() if timestamp else "",
        "wallet": wallet,
        "market": trade.get("symbol", "UNKNOWN"),
        "direction": direction,
        "side": side,
        "event_type": event_type,
        "role": role,
        "volume_usd": volume_usd,
        "price": float(trade.get("price", 0) or 0),
        "amount": float(trade.get("amount", 0) or 0),
        "entry_price": float(trade.get("entry_price", 0) or 0),
        "fee": float(trade.get("fee", 0) or 0),
        "pnl": float(trade.get("pnl", 0) or 0),
        "cause": trade.get("cause", "normal"),
    }


def pull_pacifica_data(
    wallets: list[str],
    days: int = 7,
    min_volume: float = 0,
    output_dir: str = "./pacifica_data"
) -> tuple[dict, list[dict]]:
    """
    Pull Pacifica trade data for specified wallets.

    Args:
        wallets: List of wallet addresses to fetch
        days: Number of days of history to fetch
        min_volume: Minimum total volume to include wallet in output
        output_dir: Directory to save output files

    Returns:
        Tuple of (stats dict, list of normalized trades)
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    cutoff_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)

    all_trades = []
    wallet_stats = {}

    stats = {
        "wallets_requested": len(wallets),
        "wallets_with_trades": 0,
        "total_trades": 0,
        "total_volume": 0,
        "volume_by_market": {},
        "roles": {"maker": 0, "taker": 0},
        "start_time": None,
        "end_time": None,
    }

    print(f"Fetching Pacifica trades for {len(wallets)} wallet(s)...")
    print(f"Date range: last {days} days")

    for i, wallet in enumerate(wallets, 1):
        print(f"  [{i}/{len(wallets)}] Fetching {wallet[:20]}...")

        trades = fetch_wallet_trade_history(
            wallet=wallet,
            start_time=cutoff_time,
            end_time=end_time
        )

        if not trades:
            continue

        wallet_volume = 0
        wallet_trades = []

        for trade in trades:
            normalized = normalize_trade(trade, wallet)
            wallet_trades.append(normalized)
            wallet_volume += normalized["volume_usd"]

            # Update stats
            market = normalized["market"]
            if market not in stats["volume_by_market"]:
                stats["volume_by_market"][market] = 0
            stats["volume_by_market"][market] += normalized["volume_usd"]

            stats["roles"][normalized["role"]] += 1

            ts = normalized["timestamp"]
            if ts:
                if stats["start_time"] is None or ts < stats["start_time"]:
                    stats["start_time"] = ts
                if stats["end_time"] is None or ts > stats["end_time"]:
                    stats["end_time"] = ts

        if wallet_volume >= min_volume:
            all_trades.extend(wallet_trades)
            stats["wallets_with_trades"] += 1
            stats["total_trades"] += len(wallet_trades)
            stats["total_volume"] += wallet_volume

            wallet_stats[wallet] = {
                "trades": len(wallet_trades),
                "volume": wallet_volume
            }
            print(f"    Found {len(wallet_trades)} trades, ${wallet_volume:,.0f} volume")
        else:
            print(f"    Skipped (volume ${wallet_volume:,.0f} < min ${min_volume:,.0f})")

        time.sleep(0.2)  # Rate limit between wallets

    # Save data
    if all_trades:
        all_trades.sort(key=lambda x: x["timestamp"], reverse=True)

        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = output_path / f"pacifica_trades_{date_str}.csv"

        fieldnames = [
            "history_id", "timestamp", "datetime", "wallet", "market",
            "direction", "side", "event_type", "role", "volume_usd",
            "price", "amount", "entry_price", "fee", "pnl", "cause"
        ]

        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_trades)
        print(f"\nSaved {len(all_trades)} trades to {csv_file}")

        # Save stats
        stats_file = output_path / f"pacifica_stats_{date_str}.json"
        stats_copy = stats.copy()
        stats_copy["start_time"] = datetime.fromtimestamp(stats["start_time"]).isoformat() if stats["start_time"] else None
        stats_copy["end_time"] = datetime.fromtimestamp(stats["end_time"]).isoformat() if stats["end_time"] else None
        stats_copy["wallet_stats"] = wallet_stats

        with open(stats_file, "w") as f:
            json.dump(stats_copy, f, indent=2)
        print(f"Saved stats to {stats_file}")

    return stats, all_trades


def print_pacifica_summary(stats: dict):
    """Print summary of Pacifica data pull."""
    print("\n" + "="*70)
    print("PACIFICA PERPS SUMMARY")
    print("="*70)

    if stats.get("start_time") and stats.get("end_time"):
        start = datetime.fromtimestamp(stats["start_time"]) if isinstance(stats["start_time"], (int, float)) else stats["start_time"]
        end = datetime.fromtimestamp(stats["end_time"]) if isinstance(stats["end_time"], (int, float)) else stats["end_time"]
        print(f"Date range: {start} to {end}")

    print(f"\nWallets requested: {stats['wallets_requested']}")
    print(f"Wallets with trades: {stats['wallets_with_trades']}")
    print(f"Total trades: {stats['total_trades']:,}")
    print(f"Total volume: ${stats['total_volume']:,.0f}")

    print("\nVolume by market:")
    for market, vol in sorted(stats["volume_by_market"].items(), key=lambda x: -x[1]):
        print(f"  {market}: ${vol:,.0f}")

    print("\nRole breakdown:")
    for role, count in stats["roles"].items():
        print(f"  {role}: {count:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Pull Pacifica Perpetuals trade data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Pull trades for specific wallets
  python pacifica_puller.py --wallets wallet1,wallet2,wallet3 --days 7

  # Pull from a file of wallet addresses (one per line)
  python pacifica_puller.py --wallets-file wallets.txt --days 30

  # Filter by minimum volume
  python pacifica_puller.py --wallets-file wallets.txt --min-volume 10000

Note: Pacifica is an off-chain CLOB, so wallet addresses must be provided.
The public trade API doesn't expose trader addresses.
        """
    )

    parser.add_argument(
        "--wallets",
        type=str,
        help="Comma-separated list of wallet addresses"
    )
    parser.add_argument(
        "--wallets-file",
        type=str,
        help="File containing wallet addresses (one per line)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days of history to fetch (default: 7)"
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=0,
        help="Minimum volume threshold for including wallets"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./pacifica_data",
        help="Output directory"
    )

    args = parser.parse_args()

    # Collect wallet addresses
    wallets = []

    if args.wallets:
        wallets.extend([w.strip() for w in args.wallets.split(",") if w.strip()])

    if args.wallets_file:
        try:
            with open(args.wallets_file, "r") as f:
                for line in f:
                    wallet = line.strip()
                    if wallet and not wallet.startswith("#"):
                        wallets.append(wallet)
        except FileNotFoundError:
            print(f"Error: Wallet file '{args.wallets_file}' not found", file=sys.stderr)
            sys.exit(1)

    if not wallets:
        print("Error: No wallet addresses provided.", file=sys.stderr)
        print("Use --wallets or --wallets-file to specify wallets to track.", file=sys.stderr)
        sys.exit(1)

    # Remove duplicates while preserving order
    seen = set()
    unique_wallets = []
    for w in wallets:
        if w not in seen:
            seen.add(w)
            unique_wallets.append(w)
    wallets = unique_wallets

    print(f"Pulling Pacifica data for {len(wallets)} unique wallet(s)")

    # Pull data
    stats, trades = pull_pacifica_data(
        wallets=wallets,
        days=args.days,
        min_volume=args.min_volume,
        output_dir=args.output
    )

    # Print summary
    print_pacifica_summary(stats)


if __name__ == "__main__":
    main()
