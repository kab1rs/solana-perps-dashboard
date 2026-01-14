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

# Custom CSS - Solana color scheme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');

    /* Solana dark theme - purple tinted background */
    .stApp { background-color: #0e0e1a !important; }
    [data-testid="stHeader"] { background-color: #0e0e1a !important; }
    [data-testid="stSidebar"] { background-color: #12121f !important; border-right: 1px solid #2d2d44 !important; }
    [data-testid="stSidebar"] [data-testid="stMarkdown"] { color: #9ca3af !important; }

    /* Typography - IBM Plex Mono */
    html, body, [class*="css"] {
        font-family: 'IBM Plex Mono', 'SF Mono', monospace !important;
    }

    /* Headings - balanced Solana colors */
    h1 { color: #9945FF !important; font-size: 1.8rem !important; font-weight: 600 !important; }
    h2 { color: #c4b5fd !important; font-size: 1.3rem !important; font-weight: 500 !important; margin-top: 1.5rem !important; }
    h3 { color: #e5e7eb !important; font-size: 1rem !important; font-weight: 500 !important; }

    /* Metrics styling - white values, colored deltas */
    [data-testid="stMetricValue"] {
        color: #f0f0f0 !important;
        font-size: 1.6rem !important;
        font-weight: 600 !important;
    }
    [data-testid="stMetricDelta"][data-testid-delta="positive"] { color: #14F195 !important; }
    [data-testid="stMetricDelta"][data-testid-delta="negative"] { color: #f85149 !important; }
    [data-testid="stMetricDelta"] { font-size: 0.85rem !important; }
    [data-testid="stMetricLabel"] { color: #9ca3af !important; font-size: 0.85rem !important; }

    /* Table styling */
    .stDataFrame {
        background-color: #12121f !important;
        border: 1px solid #2d2d44 !important;
        border-radius: 8px !important;
    }
    .stDataFrame thead th {
        background-color: #1a1a2e !important;
        color: #9ca3af !important;
        font-weight: 500 !important;
        border-bottom: 1px solid #2d2d44 !important;
    }
    .stDataFrame tbody td {
        color: #e5e7eb !important;
        border-bottom: 1px solid #2d2d44 !important;
    }
    .stDataFrame tbody tr:hover td {
        background-color: #1a1a2e !important;
    }

    /* Dividers - subtle purple */
    hr { border-color: #2d2d44 !important; opacity: 0.5 !important; margin: 1.5rem 0 !important; }

    /* Tabs styling - purple accent */
    .stTabs [data-baseweb="tab-list"] { background-color: transparent !important; gap: 0 !important; }
    .stTabs [data-baseweb="tab"] {
        background-color: transparent !important;
        color: #9ca3af !important;
        border-radius: 4px 4px 0 0 !important;
        padding: 0.5rem 1rem !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1a1a2e !important;
        color: #c4b5fd !important;
        border-bottom: 2px solid #9945FF !important;
    }

    /* Radio buttons (time selector) - purple accent */
    .stRadio > div { gap: 0.5rem !important; }
    .stRadio label {
        background-color: #1a1a2e !important;
        border: 1px solid #2d2d44 !important;
        border-radius: 4px !important;
        padding: 0.3rem 0.8rem !important;
        color: #9ca3af !important;
    }
    .stRadio label[data-checked="true"] {
        background-color: #2d2d44 !important;
        color: #e5e7eb !important;
        border-color: #9945FF !important;
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #12121f !important;
        border: 1px solid #2d2d44 !important;
        border-radius: 4px !important;
        color: #9ca3af !important;
    }

    /* Info/warning boxes */
    .stAlert { background-color: #1a1a2e !important; border: 1px solid #2d2d44 !important; }

    /* Positive/negative colors - Solana green/red */
    .positive { color: #14F195 !important; }
    .negative { color: #f85149 !important; }

    /* Caption text */
    .stCaption, small { color: #6b7280 !important; }

    /* Reduce padding */
    .block-container { padding: 1rem 2rem !important; max-width: 1400px !important; }

    /* Plotly chart backgrounds */
    .js-plotly-plot .plotly .bg { fill: #12121f !important; }
</style>
""", unsafe_allow_html=True)

# Sidebar navigation - simplified
with st.sidebar:
    st.markdown("### Solana Perps")
    st.markdown("""
[Overview](#overview) · [Markets](#markets) · [P&L](#p-l-leaderboard) · [Traders](#traders)
    """)
    st.markdown("---")

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
        return f"${value/1e9:.2f}B"
    elif value >= 1e6:
        return f"${value/1e6:.1f}M"
    elif value >= 1e3:
        return f"${value/1e3:.0f}K"
    return f"${value:,.0f}"


def format_wallet(address: str) -> str:
    """Truncate wallet to 6...4 format."""
    if address and len(address) > 10:
        return f"{address[:6]}...{address[-4:]}"
    return address or ""


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

# Header - clean and minimal with freshness indicator
st.title("Solana Perps")
age_text, age_status, age_minutes = get_cache_age_display(cache)
freshness_icon = "🟢" if age_status == "green" else "🟡" if age_status == "yellow" else "🔴"
st.caption(f"{freshness_icon} Updated {age_text} · Data refreshes every 15 min")

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

# Get previous values from history for trend indicators
history = load_history()
vol_delta, oi_delta, traders_delta, fees_delta = None, None, None, None
if history and history.get("snapshots") and len(history["snapshots"]) >= 2:
    prev_snapshot = history["snapshots"][-2]  # Second to last snapshot
    prev_vol = prev_snapshot.get("total_volume_24h", 0)
    prev_oi = prev_snapshot.get("total_open_interest", 0)
    prev_traders = prev_snapshot.get("total_traders_24h", 0)
    if prev_vol > 0:
        vol_delta = f"{((total_volume - prev_vol) / prev_vol * 100):+.1f}%"
    if prev_oi > 0:
        oi_delta = f"{((total_oi - prev_oi) / prev_oi * 100):+.1f}%"
    if prev_traders > 0:
        traders_delta = f"{((total_traders - prev_traders) / prev_traders * 100):+.1f}%"

# Overview metrics - compact row with trend indicators
st.header("Overview")
cols = st.columns(4)
with cols[0]:
    st.metric("24h Volume", format_volume(total_volume), delta=vol_delta)
with cols[1]:
    st.metric("Open Interest", format_volume(total_oi), delta=oi_delta)
with cols[2]:
    st.metric("Traders", f"{total_traders:,}", delta=traders_delta)
with cols[3]:
    st.metric("Fees", format_volume(total_fees))

st.divider()

# Historical Trends Section - Combined multi-line chart
if history and history.get("snapshots") and len(history["snapshots"]) >= 2:
    with st.expander("Historical Trends (7 days)", expanded=True):
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

            # Combined chart with dual y-axes
            from plotly.subplots import make_subplots
            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # Volume (primary y-axis)
            fig.add_trace(
                go.Scatter(
                    x=trend_df["timestamp"],
                    y=trend_df["volume"],
                    mode="lines+markers",
                    name="Volume",
                    line=dict(color="#9945FF", width=2),
                    marker=dict(size=4),
                ),
                secondary_y=False,
            )

            # Open Interest (primary y-axis)
            fig.add_trace(
                go.Scatter(
                    x=trend_df["timestamp"],
                    y=trend_df["open_interest"],
                    mode="lines+markers",
                    name="Open Interest",
                    line=dict(color="#c4b5fd", width=2),
                    marker=dict(size=4),
                ),
                secondary_y=False,
            )

            # Traders (secondary y-axis)
            fig.add_trace(
                go.Scatter(
                    x=trend_df["timestamp"],
                    y=trend_df["traders"],
                    mode="lines+markers",
                    name="Traders",
                    line=dict(color="#14F195", width=2),
                    marker=dict(size=4),
                    fill="tozeroy",
                    fillcolor="rgba(20, 241, 149, 0.05)",
                ),
                secondary_y=True,
            )

            fig.update_layout(
                title="7-Day Trends",
                height=300,
                margin=dict(t=40, b=40, l=60, r=60),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#c9d1d9'),
            )
            fig.update_xaxes(gridcolor='#1e2330', zerolinecolor='#1e2330')
            fig.update_yaxes(title_text="USD", tickformat="$,.0s", gridcolor='#1e2330', secondary_y=False)
            fig.update_yaxes(title_text="Traders", gridcolor='#1e2330', secondary_y=True)

            st.plotly_chart(fig, use_container_width=True)

            # Show change summary
            if len(trend_df) >= 2:
                latest = trend_df.iloc[-1]
                oldest = trend_df.iloc[0]
                vol_change = ((latest["volume"] - oldest["volume"]) / oldest["volume"] * 100) if oldest["volume"] > 0 else 0
                oi_change = ((latest["open_interest"] - oldest["open_interest"]) / oldest["open_interest"] * 100) if oldest["open_interest"] > 0 else 0
                trader_change = ((latest["traders"] - oldest["traders"]) / oldest["traders"] * 100) if oldest["traders"] > 0 else 0

                st.caption(f"7-day change: Volume {vol_change:+.1f}% · OI {oi_change:+.1f}% · Traders {trader_change:+.1f}%")

st.divider()

# Market Share - consolidated Cross-Chain + Protocol view
st.header("Market Share")

global_derivatives = cache.get("global_derivatives", [])

if global_derivatives:
    global_total = sum(p["volume_24h"] for p in global_derivatives)
    solana_share = (total_volume / global_total * 100) if global_total > 0 else 0

    # Find Solana rankings
    solana_protocols = []
    for i, p in enumerate(global_derivatives):
        if "Solana" in p.get("chains", []):
            solana_protocols.append({"name": p["name"], "rank": i + 1})

    # Quick stats row
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Global Rank", f"#{solana_protocols[0]['rank']}" if solana_protocols else "—")
    with col2:
        st.metric("Global Share", f"{solana_share:.1f}%")
    with col3:
        st.metric("Protocols", f"{len(solana_protocols)}")

    # Two tables side by side
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Global Rankings")
        comparison_data = []
        for i, p in enumerate(global_derivatives[:12]):
            is_sol = "Solana" in p.get("chains", [])
            comparison_data.append({
                "#": i + 1,
                "Protocol": p["name"],
                "Volume": format_volume(p["volume_24h"]),
                "Share": f"{p['volume_24h']/global_total*100:.1f}%",
                "Chain": "SOL" if is_sol else p.get("chains", [""])[0][:3].upper(),
            })
        st.dataframe(pd.DataFrame(comparison_data), hide_index=True, use_container_width=True)

    with col2:
        st.subheader("Solana Breakdown")
        display_df = protocol_df.copy()
        display_df["Share"] = (display_df["volume_24h"] / total_volume * 100).round(1).astype(str) + "%"
        display_df["24h"] = display_df["change_1d"].apply(format_change)
        display_df["Volume"] = display_df["volume_24h"].apply(format_volume)
        display_df["Traders"] = display_df["traders"].apply(lambda x: f"{x:,}")
        display_df = display_df.rename(columns={"protocol": "Protocol"})
        st.dataframe(
            display_df[["Protocol", "Volume", "Share", "24h", "Traders"]],
            hide_index=True,
            use_container_width=True,
        )

st.divider()

# Markets - Protocol comparison with funding rates
st.header("Markets")

drift_markets = cache.get("drift_markets", {})
jupiter_markets = cache.get("jupiter_markets", {})
window_data = get_time_window_data(cache, time_window)

# Helper for annualized funding
def get_annualized_funding(rate: float) -> float:
    return rate * 24 * 365 * 100

# Three protocol columns
col1, col2, col3 = st.columns(3)

with col1:
    drift_traders = window_data.get("drift_traders", 0)
    st.subheader(f"Drift ({drift_traders:,} traders)")

    if drift_markets:
        drift_data = []
        total_vol = sum(m["volume"] for m in drift_markets.values())
        sorted_markets = sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)[:10]

        for market, info in sorted_markets:
            share = (info["volume"] / total_vol * 100) if total_vol > 0 else 0
            funding = info.get("funding_rate", 0)
            oi_usd = info.get("open_interest", 0) * info.get("last_price", 0)
            drift_data.append({
                "Market": market.replace("-PERP", ""),
                "Volume": format_volume(info["volume"]),
                "Funding": format_funding(funding),
                "OI": format_volume(oi_usd),
            })

        st.dataframe(pd.DataFrame(drift_data), hide_index=True, use_container_width=True)

        # Funding extremes inline
        valid_markets = [(k, v) for k, v in drift_markets.items() if v.get("volume", 0) > 10000]
        if valid_markets:
            sorted_by_funding = sorted(valid_markets, key=lambda x: x[1].get("funding_rate", 0))
            lowest = sorted_by_funding[0]
            highest = sorted_by_funding[-1]
            st.caption(f"Funding: {lowest[0].replace('-PERP', '')} {format_funding(lowest[1].get('funding_rate', 0))} → {highest[0].replace('-PERP', '')} {format_funding(highest[1].get('funding_rate', 0))}")

with col2:
    jupiter_traders = window_data.get("jupiter_traders", 0)
    st.subheader(f"Jupiter ({jupiter_traders:,} traders)")

    jupiter_trades = jupiter_markets.get("trades", {})
    jupiter_volumes = jupiter_markets.get("volumes", {})

    if jupiter_trades:
        jupiter_data = []
        total_trades = sum(jupiter_trades.values())

        for market in sorted(jupiter_trades.keys(), key=lambda x: jupiter_trades[x], reverse=True):
            trades = jupiter_trades[market]
            vol = jupiter_volumes.get(market, 0)
            avg_size = vol / trades if trades > 0 else 0
            jupiter_data.append({
                "Market": market,
                "Volume": format_volume(vol),
                "Trades": f"{trades:,}",
                "Avg": format_volume(avg_size),
            })

        st.dataframe(pd.DataFrame(jupiter_data), hide_index=True, use_container_width=True)
    else:
        st.info("No Jupiter market data")

with col3:
    pacifica_traders = window_data.get("pacifica_traders", 0)
    st.subheader(f"Pacifica ({pacifica_traders:,} traders)")

    pacifica_protocol = next(
        (p for p in cache.get("protocols", []) if p.get("protocol") == "Pacifica"),
        None
    )

    if pacifica_protocol:
        pacifica_vol = pacifica_protocol.get("volume_24h", 0)
        change_1d = pacifica_protocol.get("change_1d", 0)
        st.metric("24h Volume", format_volume(pacifica_vol), f"{change_1d:+.1f}%")

        pacifica_stats = [
            {"Metric": "7d Volume", "Value": format_volume(pacifica_protocol.get("volume_7d", 0))},
            {"Metric": "Fees (24h)", "Value": format_volume(pacifica_protocol.get("fees", 0))},
            {"Metric": "Traders", "Value": f"{pacifica_traders:,}"},
        ]
        st.dataframe(pd.DataFrame(pacifica_stats), hide_index=True, use_container_width=True)
        st.caption("Off-chain CLOB - no per-market data")
    else:
        st.info("No Pacifica data")

st.divider()

# Traders Section - Wallet overlap and activity
st.header("Traders")

wallet_data = get_time_window_data(cache, time_window).get("wallet_overlap", {})

if wallet_data.get("error"):
    st.warning(f"Wallet data unavailable for {time_window} window")
else:
    drift_only = wallet_data.get("drift_only", 0)
    jupiter_only = wallet_data.get("jupiter_only", 0)
    pacifica_only = wallet_data.get("pacifica_only", 0)
    drift_jupiter = wallet_data.get("drift_jupiter", 0)
    drift_pacifica = wallet_data.get("drift_pacifica", 0)
    jupiter_pacifica = wallet_data.get("jupiter_pacifica", 0)
    all_three = wallet_data.get("all_three", 0)

    drift_total = drift_only + drift_jupiter + drift_pacifica + all_three
    jupiter_total = jupiter_only + drift_jupiter + jupiter_pacifica + all_three
    pacifica_total = pacifica_only + drift_pacifica + jupiter_pacifica + all_three
    multi_platform = drift_jupiter + drift_pacifica + jupiter_pacifica + all_three
    total_unique = drift_only + jupiter_only + pacifica_only + drift_jupiter + drift_pacifica + jupiter_pacifica + all_three

    if total_unique > 0:
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Unique", f"{total_unique:,}")
        with col2:
            st.metric("Multi-Platform", f"{multi_platform:,}")
        with col3:
            overlap_pct = (multi_platform / total_unique * 100)
            st.metric("Overlap %", f"{overlap_pct:.1f}%")
        with col4:
            st.metric("All 3 Platforms", f"{all_three:,}")

        # Horizontal bar chart (replaces pie chart)
        col1, col2 = st.columns([2, 1])

        with col1:
            # Horizontal bar for platform totals
            fig = go.Figure(data=[
                go.Bar(
                    y=["Pacifica", "Jupiter", "Drift"],
                    x=[pacifica_total, jupiter_total, drift_total],
                    orientation='h',
                    marker_color=["#9945FF", "#c4b5fd", "#14F195"],  # Purple primary, green accent
                    text=[f"{pacifica_total:,}", f"{jupiter_total:,}", f"{drift_total:,}"],
                    textposition="outside",
                )
            ])
            fig.update_layout(
                title=f"Traders per Platform ({time_window})",
                xaxis_title="Unique Wallets",
                height=200,
                margin=dict(t=40, b=30, l=80, r=60),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#c9d1d9'),
            )
            fig.update_xaxes(gridcolor='#1e2330', zerolinecolor='#1e2330')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Overlap breakdown table
            overlap_data = [
                {"Category": "Drift only", "Count": f"{drift_only:,}"},
                {"Category": "Jupiter only", "Count": f"{jupiter_only:,}"},
                {"Category": "Pacifica only", "Count": f"{pacifica_only:,}"},
                {"Category": "Drift+Jupiter", "Count": f"{drift_jupiter:,}"},
                {"Category": "Drift+Pacifica", "Count": f"{drift_pacifica:,}"},
                {"Category": "Jup+Pacifica", "Count": f"{jupiter_pacifica:,}"},
                {"Category": "All three", "Count": f"{all_three:,}"},
            ]
            st.dataframe(pd.DataFrame(overlap_data), hide_index=True, use_container_width=True, height=250)

st.divider()

# P&L Leaderboard Section - Unified table with wallet links
st.header("P&L Leaderboard")

pnl_data = cache.get("pnl_leaderboard", {})
pacifica_pnl = pnl_data.get("pacifica", {})
jupiter_pnl = pnl_data.get("jupiter", {})

# Toggle for Winners/Losers
pnl_view = st.radio("View", ["Top Winners", "Top Losers"], horizontal=True, label_visibility="collapsed")

# Build unified table from both protocols
unified_data = []

if pnl_view == "Top Winners":
    # Add Pacifica winners
    for t in pacifica_pnl.get("top_winners", [])[:25]:
        addr = t.get("address", "")
        pnl = t.get("pnl_24h", 0)
        vol = t.get("volume_24h", 0)
        unified_data.append({
            "pnl_sort": pnl,
            "Wallet": f"[{addr[:6]}...{addr[-4:]}](https://solana.fm/address/{addr})" if addr else "",
            "Protocol": "Pacifica",
            "P&L": f"${pnl:+,.0f}",
            "Period": "24h",
            "Volume": format_volume(vol),
        })

    # Add Jupiter winners
    for t in jupiter_pnl.get("top_winners", [])[:25]:
        addr = t.get("address", "")
        pnl = t.get("pnl_weekly", 0)
        vol = t.get("volume_weekly", 0)
        unified_data.append({
            "pnl_sort": pnl,
            "Wallet": f"[{addr[:6]}...{addr[-4:]}](https://solana.fm/address/{addr})" if addr else "",
            "Protocol": "Jupiter",
            "P&L": f"${pnl:+,.0f}",
            "Period": "Weekly",
            "Volume": format_volume(vol),
        })
else:
    # Add Pacifica losers
    for t in pacifica_pnl.get("top_losers", [])[:25]:
        addr = t.get("address", "")
        pnl = t.get("pnl_24h", 0)
        vol = t.get("volume_24h", 0)
        unified_data.append({
            "pnl_sort": pnl,
            "Wallet": f"[{addr[:6]}...{addr[-4:]}](https://solana.fm/address/{addr})" if addr else "",
            "Protocol": "Pacifica",
            "P&L": f"${pnl:+,.0f}",
            "Period": "24h",
            "Volume": format_volume(vol),
        })

    # Add Jupiter losers
    for t in jupiter_pnl.get("top_losers", [])[:25]:
        addr = t.get("address", "")
        pnl = t.get("pnl_weekly", 0)
        vol = t.get("volume_weekly", 0)
        unified_data.append({
            "pnl_sort": pnl,
            "Wallet": f"[{addr[:6]}...{addr[-4:]}](https://solana.fm/address/{addr})" if addr else "",
            "Protocol": "Jupiter",
            "P&L": f"${pnl:+,.0f}",
            "Period": "Weekly",
            "Volume": format_volume(vol),
        })

if unified_data:
    # Sort by P&L (descending for winners, ascending for losers)
    unified_data.sort(key=lambda x: x["pnl_sort"], reverse=(pnl_view == "Top Winners"))

    # Create dataframe and display with markdown links
    df = pd.DataFrame(unified_data)
    df = df.drop(columns=["pnl_sort"])  # Remove sort column
    df.insert(0, "#", range(1, len(df) + 1))

    st.dataframe(
        df,
        hide_index=True,
        use_container_width=True,
        height=500,
        column_config={
            "Wallet": st.column_config.LinkColumn("Wallet", display_text=r"\[(.+)\]"),
            "#": st.column_config.NumberColumn("#", width="small"),
            "Protocol": st.column_config.TextColumn("Protocol", width="small"),
            "Period": st.column_config.TextColumn("Period", width="small"),
        }
    )

    # Summary
    total_pacifica = pacifica_pnl.get("total_traders", 0)
    total_jupiter = jupiter_pnl.get("total_traders", 0)
    st.caption(f"Pacifica: 24h P&L ({total_pacifica:,} traders) · Jupiter: Weekly P&L ({total_jupiter:,} traders)")
else:
    st.info("No P&L data available")

# Footer
st.divider()
st.caption("Data: DeFiLlama · Drift API · Dune Analytics · Pacifica API · Jupiter API")
