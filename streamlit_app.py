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
    """Load dashboard data using fast APIs only."""
    # Fetch volumes from DeFiLlama (fast)
    defillama_volumes = fetch_defillama_volume()

    # Build protocol metrics
    protocol_data = []
    for protocol_name, config in PROTOCOLS.items():
        volume_data = defillama_volumes.get(config["defillama_name"], {})
        volume_24h = volume_data.get("volume_24h", 0)
        fees = volume_24h * config["fee_rate"]

        protocol_data.append({
            "Protocol": protocol_name,
            "Volume 24h": volume_24h,
            "Fees": fees,
        })

    # Fetch Drift market breakdown from API (fast)
    try:
        drift_markets = fetch_drift_markets_from_api()
    except Exception:
        drift_markets = {}

    return {
        "protocols": protocol_data,
        "drift_markets": drift_markets,
    }


# Load data with spinner
with st.spinner("Loading data..."):
    data = load_data()

# Protocol Summary Section
st.header("Protocol Summary (24h)")

protocol_df = pd.DataFrame(data["protocols"])
protocol_df = protocol_df[protocol_df["Volume 24h"] > 0].sort_values("Volume 24h", ascending=False)

# Calculate totals
total_volume = protocol_df["Volume 24h"].sum()
total_fees = protocol_df["Fees"].sum()

# Display metrics in columns
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Volume", f"${total_volume:,.0f}")
with col2:
    st.metric("Total Fees", f"${total_fees:,.0f}")
with col3:
    st.metric("Active Protocols", len(protocol_df))

# Protocol table
protocol_df["Share"] = (protocol_df["Volume 24h"] / total_volume * 100).round(1).astype(str) + "%"
protocol_df["Volume 24h"] = protocol_df["Volume 24h"].apply(lambda x: f"${x:,.0f}")
protocol_df["Fees"] = protocol_df["Fees"].apply(lambda x: f"${x:,.0f}")

st.dataframe(
    protocol_df[["Protocol", "Volume 24h", "Fees", "Share"]],
    width="stretch",
    hide_index=True,
)

# Drift Market Breakdown
st.header("Drift Market Breakdown")

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

    st.dataframe(pd.DataFrame(drift_data), width="stretch", hide_index=True)
else:
    st.write("No market data available")

# Footer
st.divider()
st.caption("""
**Data Sources:**
- Volume: DeFiLlama API
- Markets: Drift REST API
- Fees: Estimated (volume × fee_rate)
""")
