#!/usr/bin/env python3
"""
Solana Perps Insights Dashboard

Shows unique insights on Solana perp DEXes that aren't easily available elsewhere.
Data refreshed every 15 minutes via GitHub Actions.
"""

import json
from datetime import datetime, timezone

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

# Sidebar navigation
with st.sidebar:
    st.title("Navigation")
    st.markdown("""
- [Overview](#solana-perps-overview)
- [Cross-Chain](#cross-chain-comparison)
- [Protocol Breakdown](#solana-protocol-breakdown)
- [Best Venue](#best-venue-by-asset)
- [Funding Rates](#funding-rate-overview)
- [Market Deep Dive](#market-deep-dive)
- [Cross-Platform](#cross-platform-traders)
- [Quick Insights](#quick-insights)
    """)
    st.divider()

    # Data freshness indicator in sidebar
    def get_cache_age_display(cache_data):
        """Get formatted cache age with status indicator."""
        if not cache_data:
            return "No data", "red", None
        updated_at = cache_data.get("updated_at", "")
        try:
            updated_time = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            age_minutes = (datetime.now(timezone.utc) - updated_time).total_seconds() / 60
            if age_minutes < 20:
                return f"{int(age_minutes)}m ago", "green", age_minutes
            elif age_minutes < 45:
                return f"{int(age_minutes)}m ago", "yellow", age_minutes
            else:
                return f"{int(age_minutes)}m ago", "red", age_minutes
        except (ValueError, TypeError):
            return "Unknown", "gray", None

    # Will be populated after cache loads
    st.caption("Data refreshes every 15 min")


def load_cache():
    """Load cached data from JSON file."""
    cache_path = Path(__file__).parent / "data" / "cache.json"
    if not cache_path.exists():
        return None
    with open(cache_path) as f:
        return json.load(f)


def load_history():
    """Load historical data from JSON file."""
    history_path = Path(__file__).parent / "data" / "history.json"
    if not history_path.exists():
        return None
    try:
        with open(history_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


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


def get_time_window_data(cache: dict, window: str) -> dict:
    """Get data for selected time window with fallback to legacy format."""
    time_windows = cache.get("time_windows", {})

    if window in time_windows:
        return time_windows[window]

    # Fallback to legacy format for backward compatibility
    if window == "1h":
        return {
            "drift_traders": cache.get("drift_traders_1h", 0),
            "jupiter_traders": cache.get("jupiter_traders_1h", 0),
            "liquidations": cache.get("liquidations_1h", {}),
            "wallet_overlap": cache.get("wallet_overlap", {})
        }

    return {
        "drift_traders": 0,
        "jupiter_traders": 0,
        "liquidations": {"count": 0, "txns": 0, "error": "No data"},
        "wallet_overlap": {"multi_platform": 0, "drift_only": 0, "jupiter_only": 0, "error": "No data"}
    }


# Load cached data
cache = load_cache()

if cache is None:
    st.error("No cached data available. Please wait for the first data update.")
    st.stop()

# Header
st.title("Solana Perps Insights")

# Show data freshness status prominently
age_text, age_status, age_minutes = get_cache_age_display(cache)
status_colors = {"green": "#00ff88", "yellow": "#ffaa00", "red": "#ff4444", "gray": "#888888"}
status_icon = {"green": "🟢", "yellow": "🟡", "red": "🔴", "gray": "⚪"}

col_header1, col_header2 = st.columns([3, 1])
with col_header1:
    st.caption(f"Data refreshes every 15 minutes")
with col_header2:
    st.markdown(
        f"<span style='color: {status_colors[age_status]}'>{status_icon[age_status]} Updated: {age_text}</span>",
        unsafe_allow_html=True
    )

# Show warning banner if data is stale
if age_minutes and age_minutes > 30:
    st.warning(f"Data is {int(age_minutes)} minutes old. Cache may be stale or refresh failed.")

# Time window selector
time_window = st.radio(
    "Time Window",
    options=["1h", "4h", "8h", "24h"],
    index=3,  # Default to 24h
    horizontal=True,
    help="Select time window for trader counts, liquidations, and wallet overlap data"
)

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

# Determine column count based on available data
show_transactions = total_txns > 0
num_cols = 5 if show_transactions else 4

cols = st.columns(num_cols)
with cols[0]:
    st.metric("24h Volume", format_volume(total_volume), help="Source: DeFiLlama. Sum of all Solana perps protocols.")
with cols[1]:
    st.metric("Drift Open Interest", format_volume(total_oi), help="Source: Drift API. Jupiter OI not yet available.")
with cols[2]:
    st.metric("Traders (24h)", f"{total_traders:,}", help="Source: Dune Analytics. Drift + Jupiter + Pacifica. Note: Pacifica uses off-chain matching, so count shows active on-chain users (deposits/settlements) which may undercount actual traders.")
with cols[3]:
    st.metric("Fees Generated", f"${total_fees:,.0f}", help="Estimated from volume × protocol fee rates.")
if show_transactions:
    with cols[4]:
        st.metric("Transactions", f"{total_txns:,}", help="Source: Solana RPC. Program signature counts.")

st.divider()

# Historical Trends Section
history = load_history()
if history and history.get("snapshots") and len(history["snapshots"]) >= 2:
    with st.expander("Historical Trends (7 days)", expanded=False):
        snapshots = history["snapshots"]

        # Parse timestamps and build dataframe
        trend_data = []
        for s in snapshots:
            try:
                ts = datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00"))
                trend_data.append({
                    "timestamp": ts,
                    "volume": s.get("total_volume_24h", 0),
                    "traders": s.get("total_traders_24h", 0),
                    "open_interest": s.get("total_open_interest", 0),
                })
            except (ValueError, KeyError):
                continue

        if trend_data:
            trend_df = pd.DataFrame(trend_data)

            col1, col2 = st.columns(2)

            with col1:
                # Volume trend
                fig_vol = go.Figure()
                fig_vol.add_trace(go.Scatter(
                    x=trend_df["timestamp"],
                    y=trend_df["volume"],
                    mode="lines+markers",
                    name="24h Volume",
                    line=dict(color="#8B5CF6", width=2),
                    marker=dict(size=4),
                ))
                fig_vol.update_layout(
                    title="Volume Trend",
                    yaxis_title="24h Volume (USD)",
                    height=250,
                    margin=dict(t=40, b=40, l=60, r=20),
                    yaxis_tickformat="$,.0s",
                )
                st.plotly_chart(fig_vol, use_container_width=True)

            with col2:
                # Open Interest trend
                fig_oi = go.Figure()
                fig_oi.add_trace(go.Scatter(
                    x=trend_df["timestamp"],
                    y=trend_df["open_interest"],
                    mode="lines+markers",
                    name="Open Interest",
                    line=dict(color="#10B981", width=2),
                    marker=dict(size=4),
                ))
                fig_oi.update_layout(
                    title="Open Interest Trend",
                    yaxis_title="Open Interest (USD)",
                    height=250,
                    margin=dict(t=40, b=40, l=60, r=20),
                    yaxis_tickformat="$,.0s",
                )
                st.plotly_chart(fig_oi, use_container_width=True)

            # Traders trend (full width)
            fig_traders = go.Figure()
            fig_traders.add_trace(go.Scatter(
                x=trend_df["timestamp"],
                y=trend_df["traders"],
                mode="lines+markers",
                name="Traders",
                line=dict(color="#F59E0B", width=2),
                marker=dict(size=4),
                fill="tozeroy",
                fillcolor="rgba(245, 158, 11, 0.1)",
            ))
            fig_traders.update_layout(
                title="Active Traders Trend",
                yaxis_title="24h Traders",
                height=200,
                margin=dict(t=40, b=40, l=60, r=20),
            )
            st.plotly_chart(fig_traders, use_container_width=True)

            # Show change summary
            if len(trend_df) >= 2:
                latest = trend_df.iloc[-1]
                oldest = trend_df.iloc[0]
                vol_change = ((latest["volume"] - oldest["volume"]) / oldest["volume"] * 100) if oldest["volume"] > 0 else 0
                oi_change = ((latest["open_interest"] - oldest["open_interest"]) / oldest["open_interest"] * 100) if oldest["open_interest"] > 0 else 0
                trader_change = ((latest["traders"] - oldest["traders"]) / oldest["traders"] * 100) if oldest["traders"] > 0 else 0

                st.caption(f"Changes over {len(trend_df)} snapshots: Volume {vol_change:+.1f}% | OI {oi_change:+.1f}% | Traders {trader_change:+.1f}%")

st.divider()

# Cross-Chain Comparison
st.header("Cross-Chain Comparison")
st.caption("How Solana perps compare to other chains")

global_derivatives = cache.get("global_derivatives", [])

if global_derivatives:
    # Calculate global total
    global_total = sum(p["volume_24h"] for p in global_derivatives)
    solana_total = total_volume

    # Find Solana protocol rankings
    solana_protocols = []
    for i, p in enumerate(global_derivatives):
        if "Solana" in p.get("chains", []):
            solana_protocols.append({
                "name": p["name"],
                "rank": i + 1,
                "volume": p["volume_24h"],
                "share": p["volume_24h"] / global_total * 100 if global_total > 0 else 0,
            })

    # Show Solana ranking summary at top
    if solana_protocols:
        cols = st.columns(len(solana_protocols) + 1)
        with cols[0]:
            st.metric(
                "Solana Global Rank",
                f"#{solana_protocols[0]['rank']}",
                help="Highest-ranked Solana protocol globally"
            )
        for i, sp in enumerate(solana_protocols):
            with cols[i + 1]:
                st.metric(
                    sp["name"],
                    f"#{sp['rank']}",
                    f"{sp['share']:.1f}% share",
                    help=f"Volume: {format_volume(sp['volume'])}"
                )

    col1, col2 = st.columns([2, 1])

    with col1:
        # Create comparison table with rank column
        comparison_data = []
        for i, p in enumerate(global_derivatives[:15]):
            chains = ", ".join(p.get("chains", [])[:2])
            is_solana = "Solana" in p.get("chains", [])
            comparison_data.append({
                "Rank": f"#{i + 1}",
                "Protocol": p["name"],
                "Chain": chains,
                "Volume 24h": format_volume(p["volume_24h"]),
                "Market Share": f"{p['volume_24h']/global_total*100:.1f}%",
                "24h": format_change(p.get("change_1d", 0)),
                "7d": format_change(p.get("change_7d", 0)),
            })

        comp_df = pd.DataFrame(comparison_data)

        # Style the dataframe to highlight Solana rows
        def highlight_solana(row):
            is_solana = any(sp["name"] == row["Protocol"] for sp in solana_protocols)
            if is_solana:
                return ["background-color: rgba(139, 92, 246, 0.2)"] * len(row)
            return [""] * len(row)

        styled_df = comp_df.style.apply(highlight_solana, axis=1)
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    with col2:
        # Bar chart for clearer comparison (better than pie for rankings)
        with st.spinner("Loading chart..."):
            top_10 = global_derivatives[:10]
            colors = ["#8B5CF6" if "Solana" in p.get("chains", []) else "#4B5563" for p in top_10]

            fig = go.Figure(data=[
                go.Bar(
                    x=[p["name"][:10] for p in top_10],
                    y=[p["volume_24h"] for p in top_10],
                    marker_color=colors,
                    text=[format_volume(p["volume_24h"]) for p in top_10],
                    textposition="outside",
                )
            ])
            fig.update_layout(
                title="Top 10 Perps Protocols",
                yaxis_title="24h Volume",
                height=400,
                margin=dict(t=50, b=80),
                xaxis_tickangle=-45,
            )
            fig.add_annotation(
                text="Purple = Solana",
                xref="paper", yref="paper",
                x=1, y=1,
                showarrow=False,
                font=dict(size=10, color="#8B5CF6"),
            )
            st.plotly_chart(fig, use_container_width=True)

    # Summary box
    solana_share = (solana_total / global_total * 100) if global_total > 0 else 0
    st.info(f"**Solana perps:** {format_volume(solana_total)} total volume ({solana_share:.1f}% global share) across {len(solana_protocols)} protocols")

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
    # Add asterisk to Pacifica traders to indicate it's an estimate
    def format_traders(row):
        count = row["traders"]
        if row["protocol"] == "Pacifica" and count > 0:
            return f"{count:,}*"
        return f"{count:,}"
    display_df["Traders"] = display_df.apply(format_traders, axis=1)
    display_df = display_df.rename(columns={"protocol": "Protocol"})

    st.dataframe(
        display_df[["Protocol", "Volume 24h", "24h Change", "7d Change", "Market Share", "Traders", "Fees"]],
        width="stretch",
        hide_index=True,
    )
    # Add footnote for Pacifica if present
    if "Pacifica" in display_df["Protocol"].values:
        st.caption("*Pacifica uses off-chain matching. Trader count shows on-chain users only and may differ from actual traders.")

with col2:
    # Solana protocols pie chart
    with st.spinner("Loading chart..."):
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

# Define extreme funding threshold (0.1% = 87.6% annualized)
EXTREME_FUNDING_THRESHOLD = 0.001  # 0.1% per funding period

def is_valid_funding_market(info: dict) -> bool:
    """Filter for valid funding rate markets: min OI and reasonable funding."""
    oi_usd = info.get("open_interest", 0) * info.get("last_price", 0)
    funding = abs(info.get("funding_rate", 0))
    return oi_usd >= 10000 and funding < 0.05  # $10k OI min, <5% funding

def get_annualized_funding(rate: float) -> float:
    """Calculate annualized funding rate (assuming 1h funding periods)."""
    return rate * 24 * 365 * 100  # Convert to percentage

# Check for extreme funding rates and show alert
if drift_markets:
    extreme_markets = []
    for market, info in drift_markets.items():
        if info.get("volume", 0) > 10000 and is_valid_funding_market(info):
            funding = info.get("funding_rate", 0)
            if abs(funding) >= EXTREME_FUNDING_THRESHOLD:
                annualized = get_annualized_funding(funding)
                extreme_markets.append({
                    "market": market,
                    "funding": funding,
                    "annualized": annualized,
                    "direction": "longs" if funding > 0 else "shorts"
                })

    if extreme_markets:
        # Sort by absolute funding rate
        extreme_markets.sort(key=lambda x: abs(x["funding"]), reverse=True)
        alert_msg = "**Extreme Funding Rates Detected:**\n"
        for em in extreme_markets[:3]:  # Show top 3
            direction_icon = "🔴" if em["direction"] == "longs" else "🟢"
            alert_msg += f"- {direction_icon} **{em['market']}**: {em['funding']*100:.4f}% ({em['annualized']:.1f}% APR) - {em['direction']} pay\n"
        st.warning(alert_msg)

col1, col2 = st.columns([2, 1])

with col1:
    if drift_markets:
        with st.spinner("Loading chart..."):
            # Get markets sorted by absolute funding rate (most extreme first)
            sorted_markets = sorted(
                [(k, v) for k, v in drift_markets.items()
                 if v.get("volume", 0) > 10000 and is_valid_funding_market(v)],
                key=lambda x: abs(x[1].get("funding_rate", 0)),
                reverse=True
            )[:12]

            funding_data = []
            for market, info in sorted_markets:
                funding = info.get("funding_rate", 0) * 100  # Convert to percentage
                annualized = get_annualized_funding(info.get("funding_rate", 0))
                is_extreme = abs(info.get("funding_rate", 0)) >= EXTREME_FUNDING_THRESHOLD
                funding_data.append({
                    "Market": market.replace("-PERP", ""),
                    "Funding %": funding,
                    "Annualized": annualized,
                    "Direction": "Longs Pay" if funding > 0 else "Shorts Pay" if funding < 0 else "Neutral",
                    "Extreme": is_extreme,
                })

            funding_df = pd.DataFrame(funding_data)

            # Create bar chart with extreme highlighting
            def get_funding_color(row):
                if row["Extreme"]:
                    return "#ff0000" if row["Funding %"] > 0 else "#00ff00"  # Brighter for extreme
                return "#ff4444" if row["Funding %"] > 0 else "#00ff88"

            colors = [get_funding_color(row) for _, row in funding_df.iterrows()]

            fig = go.Figure(data=[
                go.Bar(
                    x=funding_df["Market"],
                    y=funding_df["Funding %"],
                    marker_color=colors,
                    marker_line_width=[3 if e else 0 for e in funding_df["Extreme"]],
                    marker_line_color="white",
                    text=[f"{f:.4f}%" for f in funding_df["Funding %"]],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>Funding: %{y:.4f}%<br>Annualized: %{customdata:.1f}%<extra></extra>",
                    customdata=funding_df["Annualized"],
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
            # Add threshold lines for extreme funding
            fig.add_hline(y=0.1, line_dash="dot", line_color="orange", opacity=0.5)
            fig.add_hline(y=-0.1, line_dash="dot", line_color="orange", opacity=0.5)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("White border = extreme funding (>0.1%). Orange dotted lines = threshold. Hover for annualized rates.")

with col2:
    st.subheader("Funding Extremes")

    if drift_markets:
        # Filter for valid markets (min OI, reasonable funding)
        valid_markets = [(k, v) for k, v in drift_markets.items()
                         if v.get("volume", 0) > 10000 and is_valid_funding_market(v)]
        sorted_by_funding = sorted(valid_markets, key=lambda x: x[1].get("funding_rate", 0))

        if sorted_by_funding:
            lowest = sorted_by_funding[0]
            lowest_rate = lowest[1].get('funding_rate', 0)
            lowest_apr = get_annualized_funding(lowest_rate)
            st.markdown(f"**Shorts Pay Most:**")
            st.markdown(f"🟢 {lowest[0]}: {format_funding(lowest_rate)}")
            st.caption(f"({lowest_apr:.1f}% APR)")

            highest = sorted_by_funding[-1]
            highest_rate = highest[1].get('funding_rate', 0)
            highest_apr = get_annualized_funding(highest_rate)
            st.markdown(f"**Longs Pay Most:**")
            st.markdown(f"🔴 {highest[0]}: {format_funding(highest_rate)}")
            st.caption(f"({highest_apr:.1f}% APR)")

st.divider()

# Market Deep Dive
st.header("Market Deep Dive")

col1, col2, col3 = st.columns(3)

window_data = get_time_window_data(cache, time_window)

with col1:
    drift_traders = window_data.get("drift_traders", 0)
    st.subheader(f"Drift ({drift_traders:,} traders/{time_window})")

    if drift_markets:
        drift_data = []
        total_vol = sum(m["volume"] for m in drift_markets.values())

        sorted_markets = sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)[:12]

        for market, info in sorted_markets:
            share = (info["volume"] / total_vol * 100) if total_vol > 0 else 0
            funding = info.get("funding_rate", 0)
            oi_usd = info.get("open_interest", 0) * info.get("last_price", 0)

            drift_data.append({
                "Market": market,
                "Volume 24h": f"${info['volume']:,.0f}",
                "Funding": format_funding(funding),
                "OI": f"${oi_usd:,.0f}",
                "Share": f"{share:.1f}%",
            })

        st.dataframe(pd.DataFrame(drift_data), width="stretch", hide_index=True)

with col2:
    jupiter_traders = window_data.get("jupiter_traders", 0)
    st.subheader(f"Jupiter ({jupiter_traders:,} traders/{time_window})")

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

with col3:
    pacifica_traders = window_data.get("pacifica_traders", 0)
    st.subheader(f"Pacifica ({pacifica_traders:,} traders/{time_window})")

    # Get Pacifica volume from protocols list
    pacifica_protocol = next(
        (p for p in cache.get("protocols", []) if p.get("protocol") == "Pacifica"),
        None
    )

    if pacifica_protocol:
        pacifica_vol = pacifica_protocol.get("volume_24h", 0)
        pacifica_vol_7d = pacifica_protocol.get("volume_7d", 0)
        pacifica_fees = pacifica_protocol.get("fees", 0)
        change_1d = pacifica_protocol.get("change_1d", 0)

        # Summary metrics
        st.metric("24h Volume", format_volume(pacifica_vol), f"{change_1d:+.1f}%")

        # Additional stats
        pacifica_stats = [
            {"Metric": "7d Volume", "Value": format_volume(pacifica_vol_7d)},
            {"Metric": "24h Fees", "Value": f"${pacifica_fees:,.0f}"},
            {"Metric": f"Traders ({time_window})", "Value": f"{pacifica_traders:,}"},
        ]

        # Add 24h traders if different from selected window
        if time_window != "24h":
            traders_24h = cache.get("time_windows", {}).get("24h", {}).get("pacifica_traders", 0)
            pacifica_stats.append({"Metric": "Traders (24h)", "Value": f"{traders_24h:,}"})

        st.dataframe(pd.DataFrame(pacifica_stats), width="stretch", hide_index=True)

        st.caption("Market-level breakdown unavailable (off-chain CLOB)")
    else:
        st.info("No Pacifica data available")

st.divider()

# Cross-Platform Wallet Analysis
st.header("Cross-Platform Traders")
st.caption(f"Wallet overlap between Drift, Jupiter, and Pacifica ({time_window} window)")

wallet_data = get_time_window_data(cache, time_window).get("wallet_overlap", {})

if wallet_data.get("error"):
    st.warning(f"Wallet data unavailable for {time_window} window")
    if "timeout" in wallet_data.get("error", "").lower() or "skipped" in wallet_data.get("error", "").lower():
        st.caption("Wallet overlap queries time out beyond 4h due to data volume. Try 1h or 4h window.")
    else:
        st.caption(wallet_data.get("error", "Unknown error"))
else:
    # Extract all overlap categories
    drift_only = wallet_data.get("drift_only", 0)
    jupiter_only = wallet_data.get("jupiter_only", 0)
    pacifica_only = wallet_data.get("pacifica_only", 0)
    drift_jupiter = wallet_data.get("drift_jupiter", 0)
    drift_pacifica = wallet_data.get("drift_pacifica", 0)
    jupiter_pacifica = wallet_data.get("jupiter_pacifica", 0)
    all_three = wallet_data.get("all_three", 0)

    # Calculate totals per platform
    drift_total = drift_only + drift_jupiter + drift_pacifica + all_three
    jupiter_total = jupiter_only + drift_jupiter + jupiter_pacifica + all_three
    pacifica_total = pacifica_only + drift_pacifica + jupiter_pacifica + all_three
    multi_platform = drift_jupiter + drift_pacifica + jupiter_pacifica + all_three
    total_unique = drift_only + jupiter_only + pacifica_only + drift_jupiter + drift_pacifica + jupiter_pacifica + all_three

    if total_unique > 0:
        # Top metrics row
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "All 3 Platforms",
                f"{all_three:,}",
                help="Wallets active on Drift, Jupiter, AND Pacifica"
            )

        with col2:
            st.metric(
                "Multi-Platform",
                f"{multi_platform:,}",
                help="Wallets active on 2+ platforms"
            )

        with col3:
            st.metric(
                "Drift Only",
                f"{drift_only:,}",
                help="Wallets active ONLY on Drift"
            )

        with col4:
            st.metric(
                "Jupiter Only",
                f"{jupiter_only:,}",
                help="Wallets active ONLY on Jupiter"
            )

        with col5:
            st.metric(
                "Pacifica Only",
                f"{pacifica_only:,}",
                help="Wallets active ONLY on Pacifica"
            )

        # Visualization row
        col1, col2 = st.columns([1, 1])

        with col1:
            with st.spinner("Loading chart..."):
                # Pie chart showing distribution
                pie_values = [drift_only, jupiter_only, pacifica_only, drift_jupiter, drift_pacifica, jupiter_pacifica, all_three]
                pie_names = ["Drift Only", "Jupiter Only", "Pacifica Only", "Drift+Jupiter", "Drift+Pacifica", "Jupiter+Pacifica", "All Three"]
                pie_colors = ["#3B82F6", "#10B981", "#F59E0B", "#8B5CF6", "#6366F1", "#14B8A6", "#EC4899"]

                # Filter out zero values for cleaner chart
                filtered = [(v, n, c) for v, n, c in zip(pie_values, pie_names, pie_colors) if v > 0]
                if filtered:
                    values, names, colors = zip(*filtered)
                    fig = px.pie(
                        values=values,
                        names=names,
                        title="Trader Distribution",
                        color_discrete_sequence=colors,
                        hole=0.4,
                    )
                    fig.update_layout(height=350, margin=dict(t=50, b=20, l=20, r=20))
                    st.plotly_chart(fig, use_container_width=True)

        with col2:
            with st.spinner("Loading chart..."):
                # Bar chart showing totals per platform
                fig = go.Figure(data=[
                    go.Bar(
                        x=["Drift", "Jupiter", "Pacifica"],
                        y=[drift_total, jupiter_total, pacifica_total],
                        marker_color=["#3B82F6", "#10B981", "#F59E0B"],
                        text=[f"{drift_total:,}", f"{jupiter_total:,}", f"{pacifica_total:,}"],
                        textposition="outside",
                    )
                ])
                fig.update_layout(
                    title=f"Total Traders per Platform ({time_window})",
                    yaxis_title="Unique Wallets",
                    height=350,
                    margin=dict(t=50, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

        # Overlap details expander
        with st.expander("Overlap Details"):
            overlap_details = [
                {"Combination": "Drift + Jupiter (not Pacifica)", "Count": drift_jupiter},
                {"Combination": "Drift + Pacifica (not Jupiter)", "Count": drift_pacifica},
                {"Combination": "Jupiter + Pacifica (not Drift)", "Count": jupiter_pacifica},
                {"Combination": "All Three Platforms", "Count": all_three},
            ]
            st.dataframe(pd.DataFrame(overlap_details), hide_index=True)

            overlap_pct = (multi_platform / total_unique * 100) if total_unique > 0 else 0
            st.caption(f"**{overlap_pct:.1f}%** of traders use 2+ platforms ({multi_platform:,} of {total_unique:,} unique wallets)")
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
    st.subheader(f"Active Traders ({time_window})")
    insights_window = get_time_window_data(cache, time_window)
    drift_count = insights_window.get("drift_traders", 0)
    jupiter_count = insights_window.get("jupiter_traders", 0)
    pacifica_count = insights_window.get("pacifica_traders", 0)
    flashtrade_count = insights_window.get("flashtrade_traders", 0)
    adrena_count = insights_window.get("adrena_traders", 0)
    total_traders = drift_count + jupiter_count + pacifica_count + flashtrade_count + adrena_count
    if total_traders > 0:
        # Show as compact table for all 5 protocols
        trader_data = [
            {"Protocol": "Drift", "Traders": f"{drift_count:,}"},
            {"Protocol": "Jupiter", "Traders": f"{jupiter_count:,}"},
            {"Protocol": "Pacifica", "Traders": f"{pacifica_count:,}"},
            {"Protocol": "FlashTrade", "Traders": f"{flashtrade_count:,}"},
            {"Protocol": "Adrena", "Traders": f"{adrena_count:,}"},
        ]
        st.dataframe(pd.DataFrame(trader_data), hide_index=True, height=212)
    else:
        st.write("No trader data available")

with col4:
    st.subheader(f"Liquidations ({time_window})")
    liquidations = insights_window.get("liquidations", {})
    if liquidations.get("error"):
        st.warning(f"Liquidations unavailable for {time_window}")
        if "timeout" in liquidations.get("error", "").lower() or "skipped" in liquidations.get("error", "").lower():
            st.caption("Liquidation queries time out beyond 8h. Try a shorter window.")
        else:
            st.caption(liquidations.get("error", "Unknown error"))
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
