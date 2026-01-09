#!/usr/bin/env python3
"""
Solana Perps Insights Dashboard

Shows unique insights on Solana perp DEXes that aren't easily available elsewhere.
Data refreshed every 15 minutes via GitHub Actions.
"""

import json
import streamlit as st
import pandas as pd
from pathlib import Path

# Page config - dark theme friendly
st.set_page_config(
    page_title="Solana Perps Insights",
    page_icon="📊",
    layout="wide",
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e1e;
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .positive { color: #00ff88; }
    .negative { color: #ff4444; }
    .funding-positive { background-color: rgba(0, 255, 136, 0.1); }
    .funding-negative { background-color: rgba(255, 68, 68, 0.1); }
</style>
""", unsafe_allow_html=True)


def load_cache():
    """Load cached data from JSON file."""
    cache_path = Path(__file__).parent / "data" / "cache.json"
    if not cache_path.exists():
        return None
    with open(cache_path) as f:
        return json.load(f)


def format_change(value):
    """Format change value with arrow and color."""
    if value > 0:
        return f"▲ {value:.1f}%"
    elif value < 0:
        return f"▼ {abs(value):.1f}%"
    return "—"


def format_funding(rate):
    """Format funding rate as percentage with color indicator."""
    pct = rate * 100
    if pct > 0:
        return f"+{pct:.4f}%"
    return f"{pct:.4f}%"


# Load cached data
cache = load_cache()

if cache is None:
    st.error("No cached data available. Please wait for the first data update.")
    st.stop()

# Header
st.title("📊 Solana Perps Insights")
updated_at = cache.get("updated_at", "Unknown")
st.caption(f"Data updated: {updated_at} | Refreshes every 15 minutes")

# Calculate totals
protocol_df = pd.DataFrame(cache["protocols"])
protocol_df = protocol_df[protocol_df["volume_24h"] > 0].sort_values("volume_24h", ascending=False)

total_volume = protocol_df["volume_24h"].sum()
total_traders = protocol_df["traders"].sum()
total_fees = protocol_df["fees"].sum()
total_txns = protocol_df["transactions"].sum()
total_oi = cache.get("total_open_interest", 0)

# Top metrics row
st.header("Overview")
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("24h Volume", f"${total_volume:,.0f}")
with col2:
    st.metric("Open Interest", f"${total_oi:,.0f}")
with col3:
    st.metric("Traders (24h est)", f"{total_traders:,}")
with col4:
    st.metric("Fees Generated", f"${total_fees:,.0f}")
with col5:
    st.metric("Transactions", f"{total_txns:,}")

st.divider()

# Protocol comparison with changes
st.header("Protocol Comparison")

display_df = protocol_df.copy()
display_df["Market Share"] = (display_df["volume_24h"] / total_volume * 100).round(1).astype(str) + "%"

# Add change indicators
display_df["24h Change"] = display_df["change_1d"].apply(format_change)
display_df["7d Change"] = display_df["change_7d"].apply(format_change)

# Format numbers
display_df["Volume 24h"] = display_df["volume_24h"].apply(lambda x: f"${x:,.0f}")
display_df["Volume 7d"] = display_df["volume_7d"].apply(lambda x: f"${x:,.0f}")
display_df["Fees"] = display_df["fees"].apply(lambda x: f"${x:,.0f}")
display_df["Traders"] = display_df["traders"].apply(lambda x: f"{x:,}")
display_df = display_df.rename(columns={"protocol": "Protocol"})

st.dataframe(
    display_df[["Protocol", "Volume 24h", "24h Change", "7d Change", "Market Share", "Traders", "Fees"]],
    width="stretch",
    hide_index=True,
)

st.divider()

# Best Venue by Asset
st.header("Best Venue by Asset")
st.caption("Compare where to trade each asset across Solana perp DEXes")

drift_markets = cache.get("drift_markets", {})
jupiter_markets = cache.get("jupiter_markets", {})

# Build comparison for common assets
common_assets = ["SOL", "BTC", "ETH"]
venue_data = []

for asset in common_assets:
    drift_key = f"{asset}-PERP"
    drift_info = drift_markets.get(drift_key, {})

    jupiter_vol = jupiter_markets.get("volumes", {}).get(asset, 0)
    drift_vol = drift_info.get("volume", 0)
    drift_funding = drift_info.get("funding_rate", 0)
    drift_oi = drift_info.get("open_interest", 0)

    # Determine best venue
    best_volume = "Jupiter" if jupiter_vol > drift_vol else "Drift"

    venue_data.append({
        "Asset": asset,
        "Drift Volume": f"${drift_vol:,.0f}",
        "Jupiter Volume": f"${jupiter_vol:,.0f}",
        "Best Volume": best_volume,
        "Drift Funding": format_funding(drift_funding),
        "Drift OI": f"${drift_oi * drift_info.get('last_price', 0):,.0f}",
    })

venue_df = pd.DataFrame(venue_data)
st.dataframe(venue_df, width="stretch", hide_index=True)

st.divider()

# Market Breakdowns
st.header("Market Deep Dive")

col1, col2 = st.columns(2)

# Drift Markets with funding rates
with col1:
    drift_traders = cache.get("drift_traders_1h", 0)
    st.subheader(f"Drift Markets ({drift_traders:,} traders/1h)")
    st.caption("All 85 PERP markets with funding rates")

    if drift_markets:
        drift_data = []
        total_vol = sum(m["volume"] for m in drift_markets.values())

        # Sort by volume and take top 20
        sorted_markets = sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)[:20]

        for market, info in sorted_markets:
            share = (info["volume"] / total_vol * 100) if total_vol > 0 else 0
            funding = info.get("funding_rate", 0)
            oi_usd = info.get("open_interest", 0) * info.get("last_price", 0)

            drift_data.append({
                "Market": market,
                "Volume 24h": f"${info['volume']:,.0f}",
                "Funding Rate": format_funding(funding),
                "Open Interest": f"${oi_usd:,.0f}",
                "Share": f"{share:.1f}%",
            })

        st.dataframe(pd.DataFrame(drift_data), width="stretch", hide_index=True)
    else:
        st.write("No market data available")

# Jupiter Markets
with col2:
    jupiter_traders = cache.get("jupiter_traders_6h", 0)
    st.subheader(f"Jupiter Markets ({jupiter_traders:,} traders/6h)")
    st.caption("Market breakdown by trade count")

    jupiter_trades = jupiter_markets.get("trades", {})
    jupiter_volumes = jupiter_markets.get("volumes", {})

    if jupiter_trades:
        jupiter_data = []
        total_trades = sum(jupiter_trades.values())

        for market in sorted(jupiter_trades.keys(), key=lambda x: jupiter_trades[x], reverse=True):
            trades = jupiter_trades[market]
            vol = jupiter_volumes.get(market, 0)
            share = (trades / total_trades * 100) if total_trades > 0 else 0
            avg_size = vol / trades if trades > 0 else 0

            jupiter_data.append({
                "Market": market,
                "Trades": f"{trades:,}",
                "Volume": f"${vol:,.0f}",
                "Avg Trade": f"${avg_size:,.0f}",
                "Share": f"{share:.1f}%",
            })

        st.dataframe(pd.DataFrame(jupiter_data), width="stretch", hide_index=True)
    else:
        st.write("No market data available")

st.divider()

# Unique Insights Section
st.header("Unique Insights")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Funding Extremes")
    st.caption("Markets with highest/lowest funding")

    if drift_markets:
        sorted_by_funding = sorted(
            [(k, v) for k, v in drift_markets.items() if v.get("volume", 0) > 10000],
            key=lambda x: x[1].get("funding_rate", 0)
        )

        if sorted_by_funding:
            # Most negative (shorts pay)
            lowest = sorted_by_funding[0]
            st.write(f"**Shorts Pay Most:** {lowest[0]}")
            st.write(f"Rate: {format_funding(lowest[1].get('funding_rate', 0))}")

            # Most positive (longs pay)
            highest = sorted_by_funding[-1]
            st.write(f"**Longs Pay Most:** {highest[0]}")
            st.write(f"Rate: {format_funding(highest[1].get('funding_rate', 0))}")

with col2:
    st.subheader("Market Concentration")
    st.caption("How volume is distributed")

    if drift_markets:
        total_vol = sum(m["volume"] for m in drift_markets.values())
        sorted_by_vol = sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)

        top3_vol = sum(m["volume"] for _, m in sorted_by_vol[:3])
        top3_pct = (top3_vol / total_vol * 100) if total_vol > 0 else 0

        st.write(f"**Top 3 markets:** {top3_pct:.1f}% of volume")
        st.write(f"SOL-PERP alone: {(sorted_by_vol[0][1]['volume'] / total_vol * 100):.1f}%")
        st.write(f"Active markets: {len([m for m in drift_markets.values() if m['volume'] > 1000])}")

with col3:
    st.subheader("OI Concentration")
    st.caption("Where leverage is building")

    if drift_markets:
        # Calculate OI in USD terms
        oi_data = [(k, v.get("open_interest", 0) * v.get("last_price", 0))
                   for k, v in drift_markets.items()]
        sorted_by_oi = sorted(oi_data, key=lambda x: x[1], reverse=True)

        if sorted_by_oi:
            st.write(f"**#1:** {sorted_by_oi[0][0]} (${sorted_by_oi[0][1]:,.0f})")
            if len(sorted_by_oi) > 1:
                st.write(f"**#2:** {sorted_by_oi[1][0]} (${sorted_by_oi[1][1]:,.0f})")
            if len(sorted_by_oi) > 2:
                st.write(f"**#3:** {sorted_by_oi[2][0]} (${sorted_by_oi[2][1]:,.0f})")

# Footer
st.divider()
st.caption("""
**Data Sources:**
- Volume & Changes: DeFiLlama API
- Market Data & Funding: Drift REST API
- Jupiter Markets: Dune Analytics
- Traders: Dune Analytics (sampled, scaled to 24h)

**Unique Insights:** Cross-protocol comparison, funding rates, OI concentration - data not easily found elsewhere.
""")
