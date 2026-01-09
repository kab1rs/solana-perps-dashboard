#!/usr/bin/env python3
"""
Solana Perps Insights Dashboard

Shows unique insights on Solana perp DEXes that aren't easily available elsewhere.
Data refreshed every 15 minutes via GitHub Actions.
"""

import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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


def format_volume(value):
    """Format large numbers with B/M suffix."""
    if value >= 1e9:
        return f"${value/1e9:.1f}B"
    elif value >= 1e6:
        return f"${value/1e6:.0f}M"
    return f"${value:,.0f}"


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
st.header("Solana Perps Overview")
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("24h Volume", format_volume(total_volume))
with col2:
    st.metric("Open Interest", format_volume(total_oi))
with col3:
    st.metric("Traders (24h)", f"{total_traders:,}")
with col4:
    st.metric("Fees Generated", f"${total_fees:,.0f}")
with col5:
    st.metric("Transactions", f"{total_txns:,}")

st.divider()

# Cross-Chain Comparison
st.header("Cross-Chain Comparison")
st.caption("How Solana perps compare to other chains")

global_derivatives = cache.get("global_derivatives", [])

if global_derivatives:
    # Calculate global total
    global_total = sum(p["volume_24h"] for p in global_derivatives)
    solana_total = total_volume

    col1, col2 = st.columns([2, 1])

    with col1:
        # Create comparison table
        comparison_data = []
        for p in global_derivatives[:10]:
            chains = ", ".join(p.get("chains", [])[:2])
            is_solana = "Solana" in p.get("chains", [])
            comparison_data.append({
                "Protocol": p["name"],
                "Chain": chains,
                "Volume 24h": format_volume(p["volume_24h"]),
                "Market Share": f"{p['volume_24h']/global_total*100:.1f}%",
                "24h Change": format_change(p.get("change_1d", 0)),
                "Solana": "✓" if is_solana else "",
            })

        comp_df = pd.DataFrame(comparison_data)
        st.dataframe(comp_df, width="stretch", hide_index=True)

    with col2:
        # Pie chart of top protocols
        top_5 = global_derivatives[:5]
        others_vol = sum(p["volume_24h"] for p in global_derivatives[5:])

        pie_data = {
            "Protocol": [p["name"] for p in top_5] + ["Others"],
            "Volume": [p["volume_24h"] for p in top_5] + [others_vol],
        }
        pie_df = pd.DataFrame(pie_data)

        fig = px.pie(
            pie_df,
            values="Volume",
            names="Protocol",
            title="Global Perps Market Share",
            hole=0.4,
        )
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
            margin=dict(t=50, b=50, l=20, r=20),
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Solana's position
    solana_rank = next(
        (i + 1 for i, p in enumerate(global_derivatives)
         if "Solana" in p.get("chains", [])),
        None
    )
    solana_share = (solana_total / global_total * 100) if global_total > 0 else 0

    st.info(f"**Solana perps rank #{solana_rank or '?'}** globally with **{solana_share:.1f}%** market share (${format_volume(solana_total)} / {format_volume(global_total)})")

st.divider()

# Solana Protocol Comparison with Chart
st.header("Solana Protocol Breakdown")

col1, col2 = st.columns([1, 1])

with col1:
    display_df = protocol_df.copy()
    display_df["Market Share"] = (display_df["volume_24h"] / total_volume * 100).round(1).astype(str) + "%"
    display_df["24h Change"] = display_df["change_1d"].apply(format_change)
    display_df["7d Change"] = display_df["change_7d"].apply(format_change)
    display_df["Volume 24h"] = display_df["volume_24h"].apply(lambda x: f"${x:,.0f}")
    display_df["Fees"] = display_df["fees"].apply(lambda x: f"${x:,.0f}")
    display_df["Traders"] = display_df["traders"].apply(lambda x: f"{x:,}")
    display_df = display_df.rename(columns={"protocol": "Protocol"})

    st.dataframe(
        display_df[["Protocol", "Volume 24h", "24h Change", "7d Change", "Market Share", "Traders", "Fees"]],
        width="stretch",
        hide_index=True,
    )

with col2:
    # Solana protocols pie chart
    fig = px.pie(
        protocol_df,
        values="volume_24h",
        names="protocol",
        title="Solana Perps Market Share",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
        margin=dict(t=50, b=50, l=20, r=20),
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Best Venue by Asset
st.header("Best Venue by Asset")
st.caption("Compare where to trade each asset across Solana perp DEXes")

drift_markets = cache.get("drift_markets", {})
jupiter_markets = cache.get("jupiter_markets", {})

# Dynamically derive common assets from available markets
drift_asset_names = {m.replace("-PERP", "") for m in drift_markets.keys() if m.endswith("-PERP")}
jupiter_asset_names = set(jupiter_markets.get("volumes", {}).keys())
common_assets = sorted(drift_asset_names & jupiter_asset_names)

# Fallback: if no common assets found, use top assets by combined volume
if not common_assets:
    combined = {}
    for asset in drift_asset_names | jupiter_asset_names:
        drift_vol = drift_markets.get(f"{asset}-PERP", {}).get("volume", 0)
        jup_vol = jupiter_markets.get("volumes", {}).get(asset, 0)
        combined[asset] = drift_vol + jup_vol
    common_assets = sorted(combined.keys(), key=lambda x: combined[x], reverse=True)[:8]

# Limit to top 8 assets for display
common_assets = common_assets[:8] if len(common_assets) > 8 else common_assets

venue_data = []

for asset in common_assets:
    drift_key = f"{asset}-PERP"
    drift_info = drift_markets.get(drift_key, {})

    jupiter_vol = jupiter_markets.get("volumes", {}).get(asset, 0)
    drift_vol = drift_info.get("volume", 0)
    drift_funding = drift_info.get("funding_rate", 0)
    drift_oi = drift_info.get("open_interest", 0)

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

# Funding Rate Heatmap
st.header("Funding Rate Overview")

col1, col2 = st.columns([2, 1])

with col1:
    if drift_markets:
        # Get top markets by volume for funding display
        sorted_markets = sorted(
            [(k, v) for k, v in drift_markets.items() if v.get("volume", 0) > 10000],
            key=lambda x: x[1]["volume"],
            reverse=True
        )[:12]

        funding_data = []
        for market, info in sorted_markets:
            funding = info.get("funding_rate", 0) * 100  # Convert to percentage
            funding_data.append({
                "Market": market.replace("-PERP", ""),
                "Funding %": funding,
                "Direction": "Longs Pay" if funding > 0 else "Shorts Pay" if funding < 0 else "Neutral",
            })

        funding_df = pd.DataFrame(funding_data)

        # Create bar chart
        colors = ["#ff4444" if f > 0 else "#00ff88" for f in funding_df["Funding %"]]
        fig = go.Figure(data=[
            go.Bar(
                x=funding_df["Market"],
                y=funding_df["Funding %"],
                marker_color=colors,
                text=[f"{f:.4f}%" for f in funding_df["Funding %"]],
                textposition="outside",
            )
        ])
        fig.update_layout(
            title="Funding Rates (Top Markets)",
            xaxis_title="Market",
            yaxis_title="Funding Rate %",
            height=350,
            margin=dict(t=50, b=50),
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Funding Extremes")

    if drift_markets:
        sorted_by_funding = sorted(
            [(k, v) for k, v in drift_markets.items() if v.get("volume", 0) > 10000],
            key=lambda x: x[1].get("funding_rate", 0)
        )

        if sorted_by_funding:
            lowest = sorted_by_funding[0]
            st.markdown(f"**Shorts Pay Most:**")
            st.markdown(f"🟢 {lowest[0]}: {format_funding(lowest[1].get('funding_rate', 0))}")

            highest = sorted_by_funding[-1]
            st.markdown(f"**Longs Pay Most:**")
            st.markdown(f"🔴 {highest[0]}: {format_funding(highest[1].get('funding_rate', 0))}")

st.divider()

# Market Deep Dive
st.header("Market Deep Dive")

col1, col2 = st.columns(2)

with col1:
    drift_traders = cache.get("drift_traders_1h", 0)
    st.subheader(f"Drift Markets ({drift_traders:,} traders/1h)")

    if drift_markets:
        drift_data = []
        total_vol = sum(m["volume"] for m in drift_markets.values())

        sorted_markets = sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)[:15]

        for market, info in sorted_markets:
            share = (info["volume"] / total_vol * 100) if total_vol > 0 else 0
            funding = info.get("funding_rate", 0)
            oi_usd = info.get("open_interest", 0) * info.get("last_price", 0)

            drift_data.append({
                "Market": market,
                "Volume 24h": f"${info['volume']:,.0f}",
                "Funding": format_funding(funding),
                "Open Interest": f"${oi_usd:,.0f}",
                "Share": f"{share:.1f}%",
            })

        st.dataframe(pd.DataFrame(drift_data), width="stretch", hide_index=True)

with col2:
    jupiter_traders = cache.get("jupiter_traders_1h", 0)
    st.subheader(f"Jupiter Markets ({jupiter_traders:,} traders/1h)")

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

st.divider()

# Cross-Platform Wallet Analysis
st.header("Cross-Platform Traders")
st.caption("Wallet overlap between Drift and Jupiter Perps (1h window)")

wallet_data = cache.get("wallet_overlap", {})

if wallet_data.get("error"):
    st.warning(f"Wallet data unavailable: {wallet_data.get('error', 'Query timeout')}")
else:
    multi = wallet_data.get("multi_platform", 0)
    drift_only = wallet_data.get("drift_only", 0)
    jupiter_only = wallet_data.get("jupiter_only", 0)
    total = multi + drift_only + jupiter_only

    if total > 0:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Multi-Platform",
                f"{multi:,}",
                help="Wallets active on BOTH Drift and Jupiter"
            )

        with col2:
            st.metric(
                "Drift Exclusive",
                f"{drift_only:,}",
                help="Wallets active ONLY on Drift"
            )

        with col3:
            st.metric(
                "Jupiter Exclusive",
                f"{jupiter_only:,}",
                help="Wallets active ONLY on Jupiter"
            )

        with col4:
            overlap_pct = (multi / total * 100) if total > 0 else 0
            st.metric(
                "Overlap Rate",
                f"{overlap_pct:.1f}%",
                help="Percentage of traders using both platforms"
            )

        # Visualization: Platform distribution pie chart
        col1, col2 = st.columns([1, 1])

        with col1:
            fig = px.pie(
                values=[multi, drift_only, jupiter_only],
                names=["Both Platforms", "Drift Only", "Jupiter Only"],
                title="Trader Distribution by Platform",
                color_discrete_sequence=["#8B5CF6", "#3B82F6", "#10B981"],
                hole=0.4,
            )
            fig.update_layout(height=300, margin=dict(t=50, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Bar chart showing totals per platform
            fig = go.Figure(data=[
                go.Bar(
                    x=["Drift Total", "Jupiter Total"],
                    y=[multi + drift_only, multi + jupiter_only],
                    marker_color=["#3B82F6", "#10B981"],
                    text=[f"{multi + drift_only:,}", f"{multi + jupiter_only:,}"],
                    textposition="outside",
                )
            ])
            fig.update_layout(
                title="Total Traders per Platform (1h)",
                yaxis_title="Unique Wallets",
                height=300,
                margin=dict(t=50, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No wallet data available for the current period")

st.divider()

# Unique Insights Section
st.header("Quick Insights")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.subheader("Market Concentration")
    if drift_markets:
        total_vol = sum(m["volume"] for m in drift_markets.values())
        sorted_by_vol = sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)

        top3_vol = sum(m["volume"] for _, m in sorted_by_vol[:3])
        top3_pct = (top3_vol / total_vol * 100) if total_vol > 0 else 0

        st.metric("Top 3 Markets", f"{top3_pct:.1f}%", "of total volume")
        st.write(f"SOL-PERP: {(sorted_by_vol[0][1]['volume'] / total_vol * 100):.1f}%")
        st.write(f"Active markets: {len([m for m in drift_markets.values() if m['volume'] > 1000])}")

with col2:
    st.subheader("OI Leaders")
    if drift_markets:
        oi_data = [(k, v.get("open_interest", 0) * v.get("last_price", 0))
                   for k, v in drift_markets.items()]
        sorted_by_oi = sorted(oi_data, key=lambda x: x[1], reverse=True)[:3]

        for i, (market, oi) in enumerate(sorted_by_oi, 1):
            st.write(f"**#{i}** {market}: ${oi:,.0f}")

with col3:
    st.subheader("Active Traders (1h)")
    drift_1h = cache.get("drift_traders_1h", 0)
    jupiter_1h = cache.get("jupiter_traders_1h", 0)
    if drift_1h > 0 or jupiter_1h > 0:
        st.metric("Drift", f"{drift_1h:,}")
        st.metric("Jupiter", f"{jupiter_1h:,}")
    else:
        st.write("No trader data available")

with col4:
    st.subheader("Liquidations (1h)")
    liquidations = cache.get("liquidations_1h", {})
    if liquidations.get("error"):
        st.warning("Data unavailable")
        st.caption("Query timeout")
    elif liquidations.get("count", 0) > 0:
        st.metric("Events", f"{liquidations['count']:,}")
        st.write(f"Txns: {liquidations.get('txns', 0):,}")
    else:
        st.info("No liquidations")
    st.caption("Source: Drift")

# Footer
st.divider()
st.caption("""
**Data Sources:** DeFiLlama (volume), Drift REST API (markets, funding, OI), Dune Analytics (traders, Jupiter markets)

**Unique Insights:** Cross-chain comparison, funding rates, OI concentration - data aggregated from multiple sources.
""")
