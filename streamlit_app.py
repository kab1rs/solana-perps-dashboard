#!/usr/bin/env python3
"""
Solana Perps Dashboard - Streamlit Web App

Run locally: streamlit run streamlit_app.py
Deploy: Push to GitHub and connect to Streamlit Cloud
"""

import streamlit as st
import pandas as pd
from datetime import datetime

# Import data fetching functions from the main dashboard
from solana_perps_dashboard import (
    fetch_defillama_volume,
    fetch_drift_markets_from_api,
    fetch_drift_accurate_traders,
    fetch_jupiter_accurate_traders,
    fetch_jupiter_market_breakdown,
    fetch_signature_count,
    distribute_volume_by_trades,
    calculate_market_fees,
    PROTOCOLS,
)

# Page config
st.set_page_config(
    page_title="Solana Perps Dashboard",
    page_icon="📊",
    layout="wide",
)

# Title
st.title("📊 Solana Perps Dashboard")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Add refresh button
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load all dashboard data with caching."""
    # Fetch volumes from DeFiLlama (fast)
    defillama_volumes = fetch_defillama_volume()

    # Fetch accurate trader counts (1h sample for speed, scale to 24h estimate)
    try:
        drift_traders_1h = fetch_drift_accurate_traders(hours=1)
    except Exception:
        drift_traders_1h = 0

    try:
        jupiter_traders_1h = fetch_jupiter_accurate_traders(hours=1)
    except Exception:
        jupiter_traders_1h = 0

    # Build protocol metrics
    protocol_data = []
    for protocol_name, config in PROTOCOLS.items():
        volume_data = defillama_volumes.get(config["defillama_name"], {})
        volume_24h = volume_data.get("volume_24h", 0)

        program_id = config["program_id"]
        tx_count = fetch_signature_count(program_id, 24) if program_id else 0

        # Scale 1h traders to 24h estimate (not linear due to overlap)
        if protocol_name == "Drift":
            traders = int(drift_traders_1h * 6)  # ~6x for 24h
        elif protocol_name == "Jupiter Perps":
            traders = int(jupiter_traders_1h * 6)  # ~6x for 24h
        else:
            traders = 0

        fees = volume_24h * config["fee_rate"]

        protocol_data.append({
            "Protocol": protocol_name,
            "Volume 24h": volume_24h,
            "Transactions": tx_count,
            "Traders": traders,
            "Fees": fees,
        })

    # Fetch Drift market breakdown from API (fast)
    try:
        drift_markets = fetch_drift_markets_from_api()
    except Exception:
        drift_markets = {}

    # Fetch Jupiter market breakdown from Dune
    try:
        jupiter_trades = fetch_jupiter_market_breakdown(hours=1)
    except Exception:
        jupiter_trades = {}

    jupiter_volume = next(
        (p["Volume 24h"] for p in protocol_data if p["Protocol"] == "Jupiter Perps"), 0
    )
    jupiter_volumes = distribute_volume_by_trades(jupiter_volume, jupiter_trades)
    jupiter_fees = calculate_market_fees(jupiter_volumes, PROTOCOLS["Jupiter Perps"]["fee_rate"])

    return {
        "protocols": protocol_data,
        "drift_markets": drift_markets,
        "drift_traders_1h": drift_traders_1h,
        "jupiter_trades": jupiter_trades,
        "jupiter_volumes": jupiter_volumes,
        "jupiter_fees": jupiter_fees,
        "jupiter_traders_1h": jupiter_traders_1h,
    }


# Load data
data = load_data()

# Protocol Summary Section
st.header("Protocol Summary (24h)")

protocol_df = pd.DataFrame(data["protocols"])
protocol_df = protocol_df[protocol_df["Volume 24h"] > 0].sort_values("Volume 24h", ascending=False)

# Calculate totals
total_volume = protocol_df["Volume 24h"].sum()
total_traders = protocol_df["Traders"].sum()
total_fees = protocol_df["Fees"].sum()

# Display metrics in columns
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Volume", f"${total_volume:,.0f}")
with col2:
    st.metric("Total Traders", f"{total_traders:,}")
with col3:
    st.metric("Total Fees", f"${total_fees:,.0f}")
with col4:
    st.metric("Active Protocols", len(protocol_df))

# Protocol table
protocol_df["Share"] = (protocol_df["Volume 24h"] / total_volume * 100).round(1).astype(str) + "%"
protocol_df["Volume 24h"] = protocol_df["Volume 24h"].apply(lambda x: f"${x:,.0f}")
protocol_df["Fees"] = protocol_df["Fees"].apply(lambda x: f"${x:,.0f}")
protocol_df["Transactions"] = protocol_df["Transactions"].apply(lambda x: f"{x:,}")
protocol_df["Traders"] = protocol_df["Traders"].apply(lambda x: f"{x:,}")

st.dataframe(
    protocol_df[["Protocol", "Volume 24h", "Transactions", "Traders", "Fees", "Share"]],
    use_container_width=True,
    hide_index=True,
)

# Market Breakdowns
st.header("Market Breakdowns")

col1, col2 = st.columns(2)

# Drift Markets
with col1:
    st.subheader(f"Drift Markets ({data['drift_traders_1h']:,} traders in 1h)")

    drift_markets = data["drift_markets"]
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

        st.dataframe(pd.DataFrame(drift_data), use_container_width=True, hide_index=True)

# Jupiter Markets
with col2:
    st.subheader(f"Jupiter Markets ({data['jupiter_traders_1h']:,} traders in 1h)")

    jupiter_trades = data["jupiter_trades"]
    jupiter_volumes = data["jupiter_volumes"]
    jupiter_fees = data["jupiter_fees"]

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

        st.dataframe(pd.DataFrame(jupiter_data), use_container_width=True, hide_index=True)

# Footer
st.divider()
st.caption("""
**Data Sources:**
- Volume: DeFiLlama API (protocol) / Drift API (markets)
- Transactions: Solana RPC
- Traders: Dune Analytics (6h sample, scaled)
- Fees: Estimated (volume × fee_rate)
""")
