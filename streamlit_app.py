#!/usr/bin/env python3
"""
Solana Perps Dashboard - Streamlit Web App

Reads pre-computed data from data/cache.json (updated every 15 min by GitHub Actions).
"""

import json
import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path

# Page config
st.set_page_config(
    page_title="Solana Perps Dashboard",
    page_icon="📊",
    layout="wide",
)

# Title
st.title("📊 Solana Perps Dashboard")


def load_cache():
    """Load cached data from JSON file."""
    cache_path = Path(__file__).parent / "data" / "cache.json"
    if not cache_path.exists():
        return None
    with open(cache_path) as f:
        return json.load(f)


# Load cached data
cache = load_cache()

if cache is None:
    st.error("No cached data available. Please wait for the first data update.")
    st.stop()

# Show last update time
updated_at = cache.get("updated_at", "Unknown")
st.caption(f"Data updated: {updated_at}")

# Protocol Summary Section
st.header("Protocol Summary (24h)")

protocol_df = pd.DataFrame(cache["protocols"])
protocol_df = protocol_df[protocol_df["volume_24h"] > 0].sort_values("volume_24h", ascending=False)

# Calculate totals
total_volume = protocol_df["volume_24h"].sum()
total_traders = protocol_df["traders"].sum()
total_fees = protocol_df["fees"].sum()
total_txns = protocol_df["transactions"].sum()

# Display metrics in columns
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Volume", f"${total_volume:,.0f}")
with col2:
    st.metric("Total Traders", f"{total_traders:,}")
with col3:
    st.metric("Total Fees", f"${total_fees:,.0f}")
with col4:
    st.metric("Transactions", f"{total_txns:,}")

# Protocol table
display_df = protocol_df.copy()
display_df["Share"] = (display_df["volume_24h"] / total_volume * 100).round(1).astype(str) + "%"
display_df["Volume 24h"] = display_df["volume_24h"].apply(lambda x: f"${x:,.0f}")
display_df["Fees"] = display_df["fees"].apply(lambda x: f"${x:,.0f}")
display_df["Transactions"] = display_df["transactions"].apply(lambda x: f"{x:,}")
display_df["Traders"] = display_df["traders"].apply(lambda x: f"{x:,}")
display_df = display_df.rename(columns={"protocol": "Protocol"})

st.dataframe(
    display_df[["Protocol", "Volume 24h", "Transactions", "Traders", "Fees", "Share"]],
    width="stretch",
    hide_index=True,
)

# Market Breakdowns
st.header("Market Breakdowns")

col1, col2 = st.columns(2)

# Drift Markets
with col1:
    drift_traders = cache.get("drift_traders_1h", 0)
    st.subheader(f"Drift Markets ({drift_traders:,} traders in 1h)")

    drift_markets = cache.get("drift_markets", {})
    if drift_markets:
        drift_data = []
        total_vol = sum(m["volume"] for m in drift_markets.values())

        for market, info in sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)[:15]:
            share = (info["volume"] / total_vol * 100) if total_vol > 0 else 0
            drift_data.append({
                "Market": market,
                "Volume 24h": f"${info['volume']:,.0f}",
                "Open Interest": f"{info['open_interest']:,.0f}",
                "Share": f"{share:.1f}%",
            })

        st.dataframe(pd.DataFrame(drift_data), width="stretch", hide_index=True)
    else:
        st.write("No market data available")

# Jupiter Markets
with col2:
    jupiter_traders = cache.get("jupiter_traders_6h", 0)
    st.subheader(f"Jupiter Markets ({jupiter_traders:,} traders in 6h)")

    jupiter_markets = cache.get("jupiter_markets", {})
    jupiter_trades = jupiter_markets.get("trades", {})
    jupiter_volumes = jupiter_markets.get("volumes", {})

    if jupiter_trades:
        jupiter_data = []
        total_trades = sum(jupiter_trades.values())

        for market in sorted(jupiter_trades.keys(), key=lambda x: jupiter_trades[x], reverse=True):
            trades = jupiter_trades[market]
            vol = jupiter_volumes.get(market, 0)
            share = (trades / total_trades * 100) if total_trades > 0 else 0
            jupiter_data.append({
                "Market": market,
                "Trades": f"{trades:,}",
                "Volume": f"${vol:,.0f}",
                "Share": f"{share:.1f}%",
            })

        st.dataframe(pd.DataFrame(jupiter_data), width="stretch", hide_index=True)
    else:
        st.write("No market data available")

# Footer
st.divider()
st.caption("""
**Data Sources:**
- Volume: DeFiLlama API
- Markets: Drift REST API / Dune Analytics
- Transactions: Solana RPC
- Traders: Dune Analytics (6h sample, scaled to 24h)
- Fees: Estimated (volume × fee_rate)

Data refreshed every 15 minutes via GitHub Actions.
""")
