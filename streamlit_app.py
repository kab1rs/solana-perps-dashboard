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
    page_icon="◈",
    layout="wide",
)

# Premium "Terminal Luxe" CSS Design System
st.markdown("""
<style>
    /* ══════════════════════════════════════════════════════════════════════════
       SOLANA PERPS INSIGHTS - TERMINAL LUXE DESIGN SYSTEM
       A premium trading terminal aesthetic with deep blacks and neon accents
       ══════════════════════════════════════════════════════════════════════════ */

    /* Import distinctive fonts */
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

    /* CSS Variables - Design Tokens */
    :root {
        /* Colors - Rich dark palette with neon accents */
        --bg-void: #06060a;
        --bg-deep: #0a0a0f;
        --bg-surface: #0f0f16;
        --bg-elevated: #14141d;
        --bg-card: rgba(20, 20, 32, 0.7);

        /* Accent colors */
        --accent-solana: #9945FF;
        --accent-cyan: #00FFA3;
        --accent-magenta: #DC1FFF;
        --accent-blue: #4F9DFF;
        --accent-amber: #FFB800;

        /* Semantic colors */
        --positive: #00FFA3;
        --negative: #FF4F6F;
        --warning: #FFB800;
        --muted: #6B7280;

        /* Text colors */
        --text-primary: #F8FAFC;
        --text-secondary: #94A3B8;
        --text-muted: #64748B;

        /* Borders & Effects */
        --border-subtle: rgba(255, 255, 255, 0.06);
        --border-glow: rgba(153, 69, 255, 0.3);
        --glow-solana: 0 0 30px rgba(153, 69, 255, 0.15);
        --glow-cyan: 0 0 20px rgba(0, 255, 163, 0.1);

        /* Typography */
        --font-display: 'Outfit', sans-serif;
        --font-mono: 'JetBrains Mono', monospace;

        /* Spacing */
        --radius-sm: 6px;
        --radius-md: 10px;
        --radius-lg: 16px;
        --radius-xl: 24px;
    }

    /* ═══════════ GLOBAL STYLES ═══════════ */

    .stApp {
        background: linear-gradient(135deg, var(--bg-void) 0%, var(--bg-deep) 50%, #0d0a14 100%);
        font-family: var(--font-display);
    }

    /* Subtle grid pattern overlay */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-image:
            linear-gradient(rgba(153, 69, 255, 0.02) 1px, transparent 1px),
            linear-gradient(90deg, rgba(153, 69, 255, 0.02) 1px, transparent 1px);
        background-size: 60px 60px;
        pointer-events: none;
        z-index: 0;
    }

    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1400px;
    }

    /* ═══════════ TYPOGRAPHY ═══════════ */

    h1, h2, h3, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        font-family: var(--font-display) !important;
        font-weight: 600 !important;
        letter-spacing: -0.02em;
    }

    h1, .stMarkdown h1 {
        font-size: 2.5rem !important;
        background: linear-gradient(135deg, var(--text-primary) 0%, var(--accent-solana) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem !important;
    }

    h2, .stMarkdown h2 {
        font-size: 1.4rem !important;
        color: var(--text-primary) !important;
        display: flex;
        align-items: center;
        gap: 12px;
        padding-bottom: 0.75rem;
        border-bottom: 1px solid var(--border-subtle);
        margin-top: 1.5rem !important;
    }

    h2::before {
        content: '◈';
        color: var(--accent-solana);
        font-size: 0.9em;
    }

    h3, .stMarkdown h3 {
        font-size: 1.1rem !important;
        color: var(--text-secondary) !important;
        font-weight: 500 !important;
    }

    p, span, div {
        font-family: var(--font-display);
    }

    /* Caption styling */
    .stCaption, small, .caption {
        font-family: var(--font-mono) !important;
        font-size: 0.75rem !important;
        color: var(--text-muted) !important;
        letter-spacing: 0.02em;
    }

    /* ═══════════ SIDEBAR ═══════════ */

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, var(--bg-surface) 0%, var(--bg-deep) 100%);
        border-right: 1px solid var(--border-subtle);
    }

    section[data-testid="stSidebar"] .stMarkdown h1 {
        font-size: 1.3rem !important;
        background: linear-gradient(135deg, var(--accent-solana) 0%, var(--accent-cyan) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        padding: 1rem 0;
        border-bottom: 1px solid var(--border-subtle);
        margin-bottom: 1rem !important;
    }

    section[data-testid="stSidebar"] a {
        font-family: var(--font-mono) !important;
        font-size: 0.85rem !important;
        color: var(--text-secondary) !important;
        text-decoration: none;
        display: block;
        padding: 8px 12px;
        margin: 2px 0;
        border-radius: var(--radius-sm);
        transition: all 0.2s ease;
        border-left: 2px solid transparent;
    }

    section[data-testid="stSidebar"] a:hover {
        color: var(--accent-cyan) !important;
        background: rgba(0, 255, 163, 0.05);
        border-left-color: var(--accent-cyan);
    }

    section[data-testid="stSidebar"] hr {
        border-color: var(--border-subtle);
        margin: 1.5rem 0;
    }

    /* ═══════════ METRIC CARDS ═══════════ */

    div[data-testid="stMetric"] {
        background: var(--bg-card);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid var(--border-subtle);
        border-radius: var(--radius-lg);
        padding: 1.25rem 1.5rem;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }

    div[data-testid="stMetric"]::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
        background: linear-gradient(90deg, transparent, var(--accent-solana), transparent);
        opacity: 0;
        transition: opacity 0.3s ease;
    }

    div[data-testid="stMetric"]:hover {
        border-color: var(--border-glow);
        box-shadow: var(--glow-solana);
        transform: translateY(-2px);
    }

    div[data-testid="stMetric"]:hover::before {
        opacity: 1;
    }

    div[data-testid="stMetric"] label {
        font-family: var(--font-mono) !important;
        font-size: 0.75rem !important;
        color: var(--text-muted) !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 500 !important;
    }

    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-family: var(--font-display) !important;
        font-size: 1.75rem !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.02em;
    }

    div[data-testid="stMetric"] div[data-testid="stMetricDelta"] {
        font-family: var(--font-mono) !important;
        font-size: 0.8rem !important;
    }

    div[data-testid="stMetric"] div[data-testid="stMetricDelta"] svg {
        display: none;
    }

    div[data-testid="stMetric"] div[data-testid="stMetricDelta"][data-testid*="Up"],
    div[data-testid="stMetric"] div[data-testid="stMetricDelta"]:has(svg[data-icon="arrowUp"]) {
        color: var(--positive) !important;
    }

    /* ═══════════ DATA TABLES ═══════════ */

    div[data-testid="stDataFrame"] {
        background: var(--bg-card);
        border-radius: var(--radius-lg);
        border: 1px solid var(--border-subtle);
        overflow: hidden;
    }

    div[data-testid="stDataFrame"] table {
        font-family: var(--font-mono) !important;
        font-size: 0.85rem !important;
    }

    div[data-testid="stDataFrame"] th {
        background: var(--bg-elevated) !important;
        color: var(--text-muted) !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        font-size: 0.7rem !important;
        letter-spacing: 0.06em;
        padding: 12px 16px !important;
        border-bottom: 1px solid var(--border-subtle) !important;
    }

    div[data-testid="stDataFrame"] td {
        color: var(--text-secondary) !important;
        padding: 10px 16px !important;
        border-bottom: 1px solid var(--border-subtle) !important;
    }

    div[data-testid="stDataFrame"] tr:hover td {
        background: rgba(153, 69, 255, 0.05) !important;
        color: var(--text-primary) !important;
    }

    /* ═══════════ BUTTONS & RADIO ═══════════ */

    div[data-testid="stRadio"] > div {
        background: var(--bg-card);
        border-radius: var(--radius-lg);
        padding: 0.5rem;
        border: 1px solid var(--border-subtle);
        gap: 0.25rem;
    }

    div[data-testid="stRadio"] label {
        font-family: var(--font-mono) !important;
        font-size: 0.85rem !important;
        padding: 0.6rem 1.2rem !important;
        border-radius: var(--radius-md) !important;
        transition: all 0.2s ease !important;
        color: var(--text-secondary) !important;
    }

    div[data-testid="stRadio"] label:hover {
        background: rgba(153, 69, 255, 0.1) !important;
        color: var(--text-primary) !important;
    }

    div[data-testid="stRadio"] label[data-checked="true"] {
        background: linear-gradient(135deg, rgba(153, 69, 255, 0.3) 0%, rgba(0, 255, 163, 0.15) 100%) !important;
        color: var(--text-primary) !important;
        font-weight: 500 !important;
        box-shadow: inset 0 0 0 1px var(--accent-solana);
    }

    /* Hide radio circles */
    div[data-testid="stRadio"] input[type="radio"] {
        display: none;
    }

    /* ═══════════ ALERTS & INFO BOXES ═══════════ */

    div[data-testid="stAlert"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-subtle) !important;
        border-radius: var(--radius-md) !important;
        font-family: var(--font-display) !important;
    }

    .stAlert[data-baseweb*="info"] {
        border-left: 3px solid var(--accent-blue) !important;
    }

    .stAlert[data-baseweb*="warning"] {
        border-left: 3px solid var(--warning) !important;
    }

    .stAlert[data-baseweb*="error"] {
        border-left: 3px solid var(--negative) !important;
    }

    /* ═══════════ DIVIDERS ═══════════ */

    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg,
            transparent,
            var(--border-subtle) 20%,
            var(--border-subtle) 80%,
            transparent
        );
        margin: 2rem 0;
    }

    /* ═══════════ SPINNERS ═══════════ */

    .stSpinner > div {
        border-top-color: var(--accent-solana) !important;
    }

    /* ═══════════ CUSTOM CLASSES ═══════════ */

    .positive { color: var(--positive) !important; }
    .negative { color: var(--negative) !important; }
    .muted { color: var(--text-muted) !important; }

    .data-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 10px;
        background: var(--bg-elevated);
        border-radius: 100px;
        font-family: var(--font-mono);
        font-size: 0.7rem;
        color: var(--text-muted);
        border: 1px solid var(--border-subtle);
    }

    .data-badge::before {
        content: '';
        width: 6px;
        height: 6px;
        background: var(--positive);
        border-radius: 50%;
        animation: pulse 2s infinite;
    }

    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.4; }
    }

    .section-intro {
        font-family: var(--font-mono);
        font-size: 0.8rem;
        color: var(--text-muted);
        margin-bottom: 1.5rem;
        padding-left: 1rem;
        border-left: 2px solid var(--border-subtle);
    }

    .footer-credits {
        font-family: var(--font-mono);
        font-size: 0.75rem;
        color: var(--text-muted);
        padding: 2rem;
        text-align: center;
        border-top: 1px solid var(--border-subtle);
        margin-top: 3rem;
    }

    .footer-credits a {
        color: var(--accent-solana);
        text-decoration: none;
    }

    .footer-credits a:hover {
        text-decoration: underline;
    }

    /* ═══════════ ANIMATIONS ═══════════ */

    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(10px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    .main .block-container > div {
        animation: fadeInUp 0.4s ease-out;
    }

    /* Stagger animation for columns */
    .main .block-container [data-testid="column"]:nth-child(1) { animation-delay: 0s; }
    .main .block-container [data-testid="column"]:nth-child(2) { animation-delay: 0.05s; }
    .main .block-container [data-testid="column"]:nth-child(3) { animation-delay: 0.1s; }
    .main .block-container [data-testid="column"]:nth-child(4) { animation-delay: 0.15s; }
    .main .block-container [data-testid="column"]:nth-child(5) { animation-delay: 0.2s; }

</style>
""", unsafe_allow_html=True)

# Sidebar navigation
with st.sidebar:
    st.markdown("""
    <div style="padding: 0.5rem 0 1.5rem 0;">
        <div style="font-family: 'Outfit', sans-serif; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.15em; color: #64748B; margin-bottom: 0.5rem;">Dashboard</div>
        <div style="font-family: 'Outfit', sans-serif; font-size: 1.5rem; font-weight: 600; background: linear-gradient(135deg, #9945FF, #00FFA3); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">Solana Perps</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.1em; color: #64748B; margin-bottom: 0.75rem; padding-left: 0.5rem;">Navigate</div>
    """, unsafe_allow_html=True)

    st.markdown("""
<style>
.nav-link {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    color: #94A3B8;
    text-decoration: none;
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 12px;
    margin: 3px 0;
    border-radius: 8px;
    transition: all 0.2s ease;
    border-left: 2px solid transparent;
}
.nav-link:hover {
    color: #00FFA3;
    background: rgba(0, 255, 163, 0.06);
    border-left-color: #00FFA3;
}
.nav-link .icon {
    font-size: 1rem;
    width: 20px;
    text-align: center;
}
</style>

<a href="#solana-perps-overview" class="nav-link"><span class="icon">◈</span> Overview</a>
<a href="#cross-chain-comparison" class="nav-link"><span class="icon">⬡</span> Cross-Chain</a>
<a href="#solana-protocol-breakdown" class="nav-link"><span class="icon">◐</span> Protocols</a>
<a href="#best-venue-by-asset" class="nav-link"><span class="icon">⟁</span> Best Venue</a>
<a href="#funding-rate-overview" class="nav-link"><span class="icon">⏱</span> Funding Rates</a>
<a href="#market-deep-dive" class="nav-link"><span class="icon">◉</span> Market Deep Dive</a>
<a href="#cross-platform-traders" class="nav-link"><span class="icon">⊕</span> Cross-Platform</a>
<a href="#quick-insights" class="nav-link"><span class="icon">✦</span> Quick Insights</a>
    """, unsafe_allow_html=True)

    st.divider()

    # Live status indicator
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 8px; padding: 12px; background: rgba(20, 20, 32, 0.5); border-radius: 10px; border: 1px solid rgba(255,255,255,0.06);">
        <div style="width: 8px; height: 8px; background: #00FFA3; border-radius: 50%; animation: pulse 2s infinite;"></div>
        <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.7rem; color: #94A3B8;">Auto-refresh: 15 min</span>
    </div>
    <style>
    @keyframes pulse {
        0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(0, 255, 163, 0.4); }
        50% { opacity: 0.6; box-shadow: 0 0 0 4px rgba(0, 255, 163, 0); }
    }
    </style>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# PLOTLY THEME CONFIGURATION - Terminal Luxe Design System
# ══════════════════════════════════════════════════════════════════════════

PLOTLY_THEME = {
    "bg_color": "rgba(10, 10, 15, 0.0)",
    "paper_color": "rgba(10, 10, 15, 0.0)",
    "grid_color": "rgba(255, 255, 255, 0.04)",
    "text_color": "#94A3B8",
    "title_color": "#F8FAFC",
    "accent_solana": "#9945FF",
    "accent_cyan": "#00FFA3",
    "accent_blue": "#4F9DFF",
    "positive": "#00FFA3",
    "negative": "#FF4F6F",
    "font_family": "JetBrains Mono, monospace",
}

# Protocol-specific colors
PROTOCOL_COLORS = {
    "Drift": "#4F9DFF",
    "Jupiter": "#00FFA3",
    "Pacifica": "#DC1FFF",
    "Adrena": "#FFB800",
    "FlashTrade": "#FF6B6B",
    "default": "#9945FF",
}

# Sequential color palette for charts
CHART_COLORS = ["#9945FF", "#00FFA3", "#4F9DFF", "#DC1FFF", "#FFB800", "#FF6B6B", "#6366F1", "#14B8A6"]


def apply_plotly_theme(fig):
    """Apply the Terminal Luxe theme to a Plotly figure."""
    fig.update_layout(
        font_family=PLOTLY_THEME["font_family"],
        font_color=PLOTLY_THEME["text_color"],
        font_size=11,
        title_font_size=14,
        title_font_color=PLOTLY_THEME["title_color"],
        title_font_family="Outfit, sans-serif",
        paper_bgcolor=PLOTLY_THEME["paper_color"],
        plot_bgcolor=PLOTLY_THEME["bg_color"],
        margin=dict(t=60, b=40, l=40, r=40),
        xaxis=dict(
            gridcolor=PLOTLY_THEME["grid_color"],
            linecolor=PLOTLY_THEME["grid_color"],
            tickfont=dict(size=10),
            title_font=dict(size=11, color=PLOTLY_THEME["text_color"]),
        ),
        yaxis=dict(
            gridcolor=PLOTLY_THEME["grid_color"],
            linecolor=PLOTLY_THEME["grid_color"],
            tickfont=dict(size=10),
            title_font=dict(size=11, color=PLOTLY_THEME["text_color"]),
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=10),
            borderwidth=0,
        ),
        hoverlabel=dict(
            bgcolor="#14141d",
            bordercolor="#9945FF",
            font_size=11,
            font_family=PLOTLY_THEME["font_family"],
        ),
    )
    return fig


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

# Header with premium styling
st.markdown("""
<div style="margin-bottom: 1.5rem;">
    <h1 style="font-family: 'Outfit', sans-serif; font-size: 2.75rem; font-weight: 700; background: linear-gradient(135deg, #F8FAFC 0%, #9945FF 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; letter-spacing: -0.03em;">Solana Perps Insights</h1>
    <p style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #64748B; margin-top: 0.5rem;">Real-time analytics across Drift, Jupiter & more</p>
</div>
""", unsafe_allow_html=True)

updated_at = cache.get("updated_at", "Unknown")

# Parse time for display
try:
    updated_time = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    age_minutes = (datetime.now(timezone.utc) - updated_time).total_seconds() / 60
    time_display = updated_time.strftime("%b %d, %H:%M UTC")
    is_stale = age_minutes > 30
except (ValueError, TypeError):
    time_display = updated_at
    is_stale = False
    age_minutes = 0

# Status bar with timestamp
status_color = "#FFB800" if is_stale else "#00FFA3"
st.markdown(f"""
<div style="display: flex; align-items: center; gap: 1.5rem; padding: 1rem 1.25rem; background: rgba(20, 20, 32, 0.6); border-radius: 12px; border: 1px solid rgba(255,255,255,0.06); margin-bottom: 1.5rem; backdrop-filter: blur(10px);">
    <div style="display: flex; align-items: center; gap: 8px;">
        <div style="width: 8px; height: 8px; background: {status_color}; border-radius: 50%;"></div>
        <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: #94A3B8;">Last update: <span style="color: #F8FAFC;">{time_display}</span></span>
    </div>
    <div style="width: 1px; height: 20px; background: rgba(255,255,255,0.1);"></div>
    <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: #64748B;">Sources: DeFiLlama · Drift API · Dune · Solana RPC</span>
</div>
""", unsafe_allow_html=True)

if is_stale:
    st.warning(f"Data is {int(age_minutes)} minutes old. Cache may be stale.")

# Time window selector with label
st.markdown("""
<div style="font-family: 'JetBrains Mono', monospace; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.1em; color: #64748B; margin-bottom: 0.5rem;">Time Window</div>
""", unsafe_allow_html=True)

time_window = st.radio(
    "Time Window",
    options=["1h", "4h", "8h", "24h"],
    index=0,
    horizontal=True,
    help="Select time window for trader counts, liquidations, and wallet overlap data",
    label_visibility="collapsed"
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
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("24h Volume", format_volume(total_volume), help="Source: DeFiLlama. Sum of all Solana perps protocols.")
with col2:
    st.metric("Drift Open Interest", format_volume(total_oi), help="Source: Drift API. Jupiter OI not yet available.")
with col3:
    st.metric("Traders (24h)", f"{total_traders:,}", help="Source: Dune Analytics. Drift + Jupiter + Pacifica. Note: Pacifica uses off-chain matching, so count shows active on-chain users (deposits/settlements) which may undercount actual traders.")
with col4:
    st.metric("Fees Generated", f"${total_fees:,.0f}", help="Estimated from volume × protocol fee rates.")
with col5:
    st.metric("Transactions", f"{total_txns:,}", help="Source: Solana RPC. Program signature counts.")

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
            colors = [PLOTLY_THEME["accent_solana"] if "Solana" in p.get("chains", []) else "rgba(100, 116, 139, 0.5)" for p in top_10]

            fig = go.Figure(data=[
                go.Bar(
                    x=[p["name"][:10] for p in top_10],
                    y=[p["volume_24h"] for p in top_10],
                    marker_color=colors,
                    marker_line_color=[PLOTLY_THEME["accent_solana"] if "Solana" in p.get("chains", []) else "rgba(100, 116, 139, 0.3)" for p in top_10],
                    marker_line_width=1,
                    text=[format_volume(p["volume_24h"]) for p in top_10],
                    textposition="outside",
                    textfont=dict(size=9, color=PLOTLY_THEME["text_color"]),
                    hovertemplate="<b>%{x}</b><br>Volume: %{text}<extra></extra>",
                )
            ])
            fig.update_layout(
                title=dict(text="Top 10 Perps Protocols", font=dict(size=14)),
                yaxis_title="24h Volume",
                height=400,
                xaxis_tickangle=-45,
                bargap=0.3,
            )
            fig.add_annotation(
                text="<span style='color:#9945FF'>■</span> Solana",
                xref="paper", yref="paper",
                x=1, y=1.05,
                showarrow=False,
                font=dict(size=10, color=PLOTLY_THEME["text_color"]),
            )
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

    # Summary box with premium styling
    solana_share = (solana_total / global_total * 100) if global_total > 0 else 0
    st.markdown(f"""
    <div style="display: flex; align-items: center; gap: 1rem; padding: 1rem 1.5rem; background: linear-gradient(135deg, rgba(153, 69, 255, 0.1) 0%, rgba(0, 255, 163, 0.05) 100%); border-radius: 12px; border: 1px solid rgba(153, 69, 255, 0.2); margin-top: 1rem;">
        <span style="font-size: 1.5rem;">◈</span>
        <div>
            <span style="font-family: 'Outfit', sans-serif; font-size: 0.95rem; color: #F8FAFC; font-weight: 500;">Solana Perps:</span>
            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.9rem; color: #00FFA3; margin-left: 0.5rem;">{format_volume(solana_total)}</span>
            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #94A3B8;"> total volume</span>
            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #64748B;"> · </span>
            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.9rem; color: #9945FF;">{solana_share:.1f}%</span>
            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #94A3B8;"> global share</span>
            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #64748B;"> · </span>
            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #94A3B8;">{len(solana_protocols)} protocols</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

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
    # Solana protocols pie chart with premium styling
    with st.spinner("Loading chart..."):
        # Use protocol-specific colors
        protocol_colors = [PROTOCOL_COLORS.get(p, PROTOCOL_COLORS["default"]) for p in protocol_df["protocol"]]

        fig = go.Figure(data=[go.Pie(
            labels=protocol_df["protocol"],
            values=protocol_df["volume_24h"],
            hole=0.55,
            marker=dict(
                colors=protocol_colors,
                line=dict(color=PLOTLY_THEME["bg_color"], width=2)
            ),
            textinfo="label+percent",
            textposition="outside",
            textfont=dict(size=10, color=PLOTLY_THEME["text_color"]),
            hovertemplate="<b>%{label}</b><br>Volume: $%{value:,.0f}<br>Share: %{percent}<extra></extra>",
        )])

        fig.update_layout(
            title=dict(text="Market Share Distribution", font=dict(size=14)),
            showlegend=False,
            height=350,
            annotations=[dict(
                text="<b>SOL</b><br>Perps",
                x=0.5, y=0.5,
                font=dict(size=14, color=PLOTLY_THEME["title_color"], family="Outfit, sans-serif"),
                showarrow=False
            )]
        )
        apply_plotly_theme(fig)
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

def is_valid_funding_market(info: dict) -> bool:
    """Filter for valid funding rate markets: min OI and reasonable funding."""
    oi_usd = info.get("open_interest", 0) * info.get("last_price", 0)
    funding = abs(info.get("funding_rate", 0))
    return oi_usd >= 10000 and funding < 0.05  # $10k OI min, <5% funding

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
                funding_data.append({
                    "Market": market.replace("-PERP", ""),
                    "Funding %": funding,
                    "Direction": "Longs Pay" if funding > 0 else "Shorts Pay" if funding < 0 else "Neutral",
                })

            funding_df = pd.DataFrame(funding_data)

            # Create bar chart with themed colors
            colors = [PLOTLY_THEME["negative"] if f > 0 else PLOTLY_THEME["positive"] for f in funding_df["Funding %"]]
            fig = go.Figure(data=[
                go.Bar(
                    x=funding_df["Market"],
                    y=funding_df["Funding %"],
                    marker_color=colors,
                    marker_line_color=colors,
                    marker_line_width=1,
                    text=[f"{f:.4f}%" for f in funding_df["Funding %"]],
                    textposition="outside",
                    textfont=dict(size=9, color=PLOTLY_THEME["text_color"]),
                    hovertemplate="<b>%{x}</b><br>Funding: %{y:.4f}%<extra></extra>",
                )
            ])
            fig.update_layout(
                title=dict(text="Funding Rates by Market", font=dict(size=14)),
                xaxis_title="",
                yaxis_title="Funding Rate %",
                height=350,
                bargap=0.4,
            )
            fig.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.2)", line_width=1)
            fig.add_annotation(
                text="<span style='color:#FF4F6F'>■</span> Longs Pay  <span style='color:#00FFA3'>■</span> Shorts Pay",
                xref="paper", yref="paper",
                x=0.5, y=1.08,
                showarrow=False,
                font=dict(size=9, color=PLOTLY_THEME["text_color"]),
            )
            apply_plotly_theme(fig)
            st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("""
    <div style="font-family: 'Outfit', sans-serif; font-size: 1.1rem; font-weight: 500; color: #F8FAFC; margin-bottom: 1rem;">Funding Extremes</div>
    """, unsafe_allow_html=True)

    if drift_markets:
        # Filter for valid markets (min OI, reasonable funding)
        valid_markets = [(k, v) for k, v in drift_markets.items()
                         if v.get("volume", 0) > 10000 and is_valid_funding_market(v)]
        sorted_by_funding = sorted(valid_markets, key=lambda x: x[1].get("funding_rate", 0))

        if sorted_by_funding:
            lowest = sorted_by_funding[0]
            highest = sorted_by_funding[-1]

            st.markdown(f"""
            <div style="background: rgba(20, 20, 32, 0.6); border-radius: 12px; padding: 1rem; border: 1px solid rgba(255,255,255,0.06); margin-bottom: 1rem;">
                <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; color: #64748B; margin-bottom: 0.5rem;">Shorts Pay Most</div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span style="width: 10px; height: 10px; background: #00FFA3; border-radius: 50%;"></span>
                    <span style="font-family: 'Outfit', sans-serif; font-size: 1.1rem; font-weight: 600; color: #F8FAFC;">{lowest[0].replace("-PERP", "")}</span>
                    <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.9rem; color: #00FFA3; margin-left: auto;">{format_funding(lowest[1].get('funding_rate', 0))}</span>
                </div>
            </div>

            <div style="background: rgba(20, 20, 32, 0.6); border-radius: 12px; padding: 1rem; border: 1px solid rgba(255,255,255,0.06);">
                <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; color: #64748B; margin-bottom: 0.5rem;">Longs Pay Most</div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span style="width: 10px; height: 10px; background: #FF4F6F; border-radius: 50%;"></span>
                    <span style="font-family: 'Outfit', sans-serif; font-size: 1.1rem; font-weight: 600; color: #F8FAFC;">{highest[0].replace("-PERP", "")}</span>
                    <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.9rem; color: #FF4F6F; margin-left: auto;">{format_funding(highest[1].get('funding_rate', 0))}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)

st.divider()

# Market Deep Dive
st.header("Market Deep Dive")

col1, col2 = st.columns(2)

with col1:
    window_data = get_time_window_data(cache, time_window)
    drift_traders = window_data.get("drift_traders", 0)
    st.subheader(f"Drift Markets ({drift_traders:,} traders/{time_window})")

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
    jupiter_traders = window_data.get("jupiter_traders", 0)
    st.subheader(f"Jupiter Markets ({jupiter_traders:,} traders/{time_window})")

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
st.caption(f"Wallet overlap between Drift and Jupiter Perps ({time_window} window)")

wallet_data = get_time_window_data(cache, time_window).get("wallet_overlap", {})

if wallet_data.get("error"):
    st.warning(f"Wallet data unavailable for {time_window} window")
    if "timeout" in wallet_data.get("error", "").lower() or "skipped" in wallet_data.get("error", "").lower():
        st.caption("Wallet overlap queries time out beyond 4h due to data volume. Try 1h or 4h window.")
    else:
        st.caption(wallet_data.get("error", "Unknown error"))
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
            with st.spinner("Loading chart..."):
                fig = go.Figure(data=[go.Pie(
                    labels=["Both Platforms", "Drift Only", "Jupiter Only"],
                    values=[multi, drift_only, jupiter_only],
                    hole=0.55,
                    marker=dict(
                        colors=[PLOTLY_THEME["accent_solana"], PROTOCOL_COLORS["Drift"], PROTOCOL_COLORS["Jupiter"]],
                        line=dict(color=PLOTLY_THEME["bg_color"], width=2)
                    ),
                    textinfo="label+percent",
                    textposition="outside",
                    textfont=dict(size=10, color=PLOTLY_THEME["text_color"]),
                    hovertemplate="<b>%{label}</b><br>Traders: %{value:,}<br>Share: %{percent}<extra></extra>",
                )])
                fig.update_layout(
                    title=dict(text="Trader Distribution", font=dict(size=14)),
                    showlegend=False,
                    height=300,
                    annotations=[dict(
                        text=f"<b>{total:,}</b><br>traders",
                        x=0.5, y=0.5,
                        font=dict(size=12, color=PLOTLY_THEME["title_color"], family="Outfit, sans-serif"),
                        showarrow=False
                    )]
                )
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            with st.spinner("Loading chart..."):
                # Bar chart showing totals per platform
                fig = go.Figure(data=[
                    go.Bar(
                        x=["Drift", "Jupiter"],
                        y=[multi + drift_only, multi + jupiter_only],
                        marker_color=[PROTOCOL_COLORS["Drift"], PROTOCOL_COLORS["Jupiter"]],
                        marker_line_color=[PROTOCOL_COLORS["Drift"], PROTOCOL_COLORS["Jupiter"]],
                        marker_line_width=1,
                        text=[f"{multi + drift_only:,}", f"{multi + jupiter_only:,}"],
                        textposition="outside",
                        textfont=dict(size=11, color=PLOTLY_THEME["text_color"]),
                        hovertemplate="<b>%{x}</b><br>Traders: %{y:,}<extra></extra>",
                    )
                ])
                fig.update_layout(
                    title=dict(text=f"Total Traders ({time_window})", font=dict(size=14)),
                    yaxis_title="Unique Wallets",
                    height=300,
                    bargap=0.5,
                )
                apply_plotly_theme(fig)
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
    st.subheader(f"Active Traders ({time_window})")
    insights_window = get_time_window_data(cache, time_window)
    drift_count = insights_window.get("drift_traders", 0)
    jupiter_count = insights_window.get("jupiter_traders", 0)
    pacifica_count = insights_window.get("pacifica_traders", 0)
    if drift_count > 0 or jupiter_count > 0 or pacifica_count > 0:
        st.metric("Drift", f"{drift_count:,}")
        st.metric("Jupiter", f"{jupiter_count:,}")
        st.metric(
            "Pacifica",
            f"{pacifica_count:,}",
            help="Pacifica uses hybrid architecture (off-chain CLOB, on-chain settlement). This count represents active on-chain users, not all traders. May undercount due to off-chain activity, or overcount depositors who haven't traded."
        )
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

# Footer with premium styling
st.divider()

st.markdown("""
<div style="padding: 2rem 0; text-align: center;">
    <div style="display: inline-flex; align-items: center; gap: 1.5rem; padding: 1.25rem 2rem; background: rgba(20, 20, 32, 0.5); border-radius: 16px; border: 1px solid rgba(255,255,255,0.06); margin-bottom: 1.5rem;">
        <div style="text-align: left;">
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #64748B; margin-bottom: 4px;">Data Sources</div>
            <div style="font-family: 'Outfit', sans-serif; font-size: 0.85rem; color: #94A3B8;">
                <span style="color: #9945FF;">DeFiLlama</span> ·
                <span style="color: #4F9DFF;">Drift API</span> ·
                <span style="color: #00FFA3;">Dune Analytics</span> ·
                <span style="color: #FFB800;">Solana RPC</span>
            </div>
        </div>
    </div>

    <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: #64748B; line-height: 1.8;">
        Cross-chain comparison · Funding rates · OI concentration · Wallet overlap<br>
        <span style="color: #4B5563;">Aggregated insights updated every 15 minutes</span>
    </div>

    <div style="margin-top: 1.5rem; padding-top: 1.5rem; border-top: 1px solid rgba(255,255,255,0.04);">
        <span style="font-family: 'Outfit', sans-serif; font-size: 0.8rem; color: #4B5563;">
            Built with <span style="color: #9945FF;">◈</span> for the Solana ecosystem
        </span>
    </div>
</div>
""", unsafe_allow_html=True)
