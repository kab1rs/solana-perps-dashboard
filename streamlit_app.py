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
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# TERMINAL THEME CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# Theme options - can be changed via sidebar
TERMINAL_THEMES = {
    "matrix": {
        "name": "Matrix Green",
        "accent": "#00FF41",
        "accent_dim": "#00aa2a",
        "positive": "#00FF41",
        "negative": "#FF0040",
        "warning": "#FFB000",
    },
    "amber": {
        "name": "Amber Retro",
        "accent": "#FFB000",
        "accent_dim": "#aa7500",
        "positive": "#00FF41",
        "negative": "#FF0040",
        "warning": "#FFB000",
    },
    "solana": {
        "name": "Solana Purple",
        "accent": "#9945FF",
        "accent_dim": "#6b30b3",
        "positive": "#00FFA3",
        "negative": "#FF4F6F",
        "warning": "#FFB800",
    },
    "cyan": {
        "name": "Cyber Cyan",
        "accent": "#00FFA3",
        "accent_dim": "#00aa6d",
        "positive": "#00FFA3",
        "negative": "#FF4F6F",
        "warning": "#FFB800",
    },
}

# Get theme from URL query params or session state (settings persistence)
query_params = st.query_params

# Initialize from URL params if available, otherwise use defaults
if "terminal_theme" not in st.session_state:
    saved_theme = query_params.get("theme", "matrix")
    st.session_state.terminal_theme = saved_theme if saved_theme in TERMINAL_THEMES else "matrix"
if "crt_effects" not in st.session_state:
    st.session_state.crt_effects = query_params.get("crt", "1") == "1"
if "show_alerts" not in st.session_state:
    st.session_state.show_alerts = query_params.get("alerts", "1") == "1"

theme = TERMINAL_THEMES[st.session_state.terminal_theme]
crt_enabled = st.session_state.crt_effects

# Terminal CSS Design System
st.markdown(f"""
<style>
    /* ══════════════════════════════════════════════════════════════════════════
       SOLANA PERPS TERMINAL - PURE TERMINAL AESTHETIC
       Monospace everything, box-drawing borders, dense data display
       ══════════════════════════════════════════════════════════════════════════ */

    /* Import monospace font */
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap');

    /* CSS Variables - Terminal Design Tokens */
    :root {{
        /* Background colors - pure black */
        --bg-void: #000000;
        --bg-deep: #0a0a0a;
        --bg-surface: #111111;
        --bg-elevated: #1a1a1a;
        --bg-card: #0d0d0d;

        /* Theme accent colors */
        --accent: {theme['accent']};
        --accent-dim: {theme['accent_dim']};
        --positive: {theme['positive']};
        --negative: {theme['negative']};
        --warning: {theme['warning']};

        /* Text colors */
        --text-primary: #e0e0e0;
        --text-secondary: #888888;
        --text-muted: #555555;
        --text-bright: #ffffff;

        /* Borders */
        --border-color: #333333;
        --border-bright: #444444;

        /* Typography - monospace only */
        --font-mono: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;

        /* No border radius - sharp corners */
        --radius: 0px;
    }}

    /* ═══════════ GLOBAL STYLES ═══════════ */

    .stApp {{
        background: var(--bg-void) !important;
        font-family: var(--font-mono) !important;
    }}

    /* CRT Scanline effect overlay - conditional */
    .stApp::before {{
        content: '';
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: repeating-linear-gradient(
            0deg,
            rgba(0, 0, 0, 0.15),
            rgba(0, 0, 0, 0.15) 1px,
            transparent 1px,
            transparent 2px
        );
        pointer-events: none;
        z-index: 1000;
        opacity: {0.3 if crt_enabled else 0};
        transition: opacity 0.3s ease;
    }}

    /* CRT text glow effect */
    {"" if not crt_enabled else '''
    .stApp {
        text-shadow: 0 0 2px ''' + theme['accent'] + '''20;
    }
    '''}

    /* CRT screen flicker animation */
    {"" if not crt_enabled else '''
    @keyframes flicker {
        0%, 100% { opacity: 1; }
        92% { opacity: 1; }
        93% { opacity: 0.8; }
        94% { opacity: 1; }
    }
    .stApp {
        animation: flicker 8s infinite;
    }
    '''}

    .main .block-container {{
        padding-top: 1rem;
        padding-bottom: 2rem;
        max-width: 1600px;
    }}

    /* ═══════════ TYPOGRAPHY - ALL MONOSPACE ═══════════ */

    * {{
        font-family: var(--font-mono) !important;
    }}

    h1, h2, h3, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {{
        font-family: var(--font-mono) !important;
        font-weight: 600 !important;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }}

    h1, .stMarkdown h1 {{
        font-size: 1.2rem !important;
        color: var(--accent) !important;
        margin-bottom: 0.5rem !important;
    }}

    h2, .stMarkdown h2 {{
        font-size: 0.9rem !important;
        color: var(--accent) !important;
        padding: 0.5rem 0;
        border-bottom: 1px solid var(--border-color);
        margin-top: 1rem !important;
        margin-bottom: 0.75rem !important;
    }}

    h2::before {{
        content: '► ';
        color: var(--accent);
    }}

    h3, .stMarkdown h3 {{
        font-size: 0.85rem !important;
        color: var(--text-secondary) !important;
        font-weight: 500 !important;
    }}

    h3::before {{
        content: '> ';
        color: var(--accent-dim);
    }}

    p, span, div, label {{
        font-family: var(--font-mono) !important;
        font-size: 0.8rem;
    }}

    /* Caption styling */
    .stCaption, small, .caption {{
        font-family: var(--font-mono) !important;
        font-size: 0.7rem !important;
        color: var(--text-muted) !important;
    }}

    /* ═══════════ SIDEBAR - TERMINAL STYLE ═══════════ */

    section[data-testid="stSidebar"] {{
        background: var(--bg-deep) !important;
        border-right: 1px solid var(--border-color);
    }}

    section[data-testid="stSidebar"] > div {{
        background: var(--bg-deep) !important;
    }}

    section[data-testid="stSidebar"] .stMarkdown h1 {{
        font-size: 0.9rem !important;
        color: var(--accent) !important;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        padding: 0.75rem 0;
        border-bottom: 1px solid var(--border-color);
        margin-bottom: 0.75rem !important;
    }}

    section[data-testid="stSidebar"] a {{
        font-family: var(--font-mono) !important;
        font-size: 0.75rem !important;
        color: var(--text-secondary) !important;
        text-decoration: none;
        display: block;
        padding: 6px 8px;
        margin: 1px 0;
        border-left: 2px solid transparent;
        transition: all 0.1s ease;
    }}

    section[data-testid="stSidebar"] a:hover {{
        color: var(--accent) !important;
        background: rgba(255, 255, 255, 0.03);
        border-left-color: var(--accent);
    }}

    section[data-testid="stSidebar"] hr {{
        border-color: var(--border-color);
        margin: 1rem 0;
    }}

    /* ═══════════ METRIC CARDS - TERMINAL PANELS ═══════════ */

    div[data-testid="stMetric"] {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color);
        border-radius: var(--radius);
        padding: 0.75rem 1rem;
        position: relative;
    }}

    div[data-testid="stMetric"]::before {{
        content: '┌─';
        position: absolute;
        top: -1px;
        left: -1px;
        color: var(--border-bright);
        font-size: 0.7rem;
    }}

    div[data-testid="stMetric"]::after {{
        content: '─┐';
        position: absolute;
        top: -1px;
        right: -1px;
        color: var(--border-bright);
        font-size: 0.7rem;
    }}

    div[data-testid="stMetric"] label {{
        font-family: var(--font-mono) !important;
        font-size: 0.65rem !important;
        color: var(--text-muted) !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        font-weight: 500 !important;
    }}

    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {{
        font-family: var(--font-mono) !important;
        font-size: 1.3rem !important;
        font-weight: 700 !important;
        color: var(--accent) !important;
        letter-spacing: 0.02em;
    }}

    div[data-testid="stMetric"] div[data-testid="stMetricDelta"] {{
        font-family: var(--font-mono) !important;
        font-size: 0.7rem !important;
    }}

    div[data-testid="stMetric"] div[data-testid="stMetricDelta"] svg {{
        display: none;
    }}

    /* ═══════════ DATA TABLES - DENSE TERMINAL ═══════════ */

    div[data-testid="stDataFrame"] {{
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: var(--radius);
        overflow: hidden;
    }}

    div[data-testid="stDataFrame"] table {{
        font-family: var(--font-mono) !important;
        font-size: 0.75rem !important;
    }}

    div[data-testid="stDataFrame"] th {{
        background: var(--bg-elevated) !important;
        color: var(--accent) !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.65rem !important;
        letter-spacing: 0.08em;
        padding: 8px 12px !important;
        border-bottom: 1px solid var(--border-color) !important;
    }}

    div[data-testid="stDataFrame"] td {{
        color: var(--text-primary) !important;
        padding: 6px 12px !important;
        border-bottom: 1px solid var(--border-color) !important;
    }}

    div[data-testid="stDataFrame"] tr:hover td {{
        background: rgba(255, 255, 255, 0.03) !important;
        color: var(--text-bright) !important;
    }}

    /* ═══════════ BUTTONS & RADIO - TERMINAL STYLE ═══════════ */

    div[data-testid="stRadio"] > div {{
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: var(--radius);
        padding: 0.25rem;
        gap: 0;
    }}

    div[data-testid="stRadio"] label {{
        font-family: var(--font-mono) !important;
        font-size: 0.75rem !important;
        padding: 0.4rem 0.8rem !important;
        border-radius: var(--radius) !important;
        color: var(--text-secondary) !important;
        transition: all 0.1s ease !important;
    }}

    div[data-testid="stRadio"] label:hover {{
        background: rgba(255, 255, 255, 0.05) !important;
        color: var(--text-primary) !important;
    }}

    div[data-testid="stRadio"] label[data-checked="true"] {{
        background: var(--bg-elevated) !important;
        color: var(--accent) !important;
        font-weight: 600 !important;
        border: 1px solid var(--accent-dim);
    }}

    /* Hide radio circles */
    div[data-testid="stRadio"] input[type="radio"] {{
        display: none;
    }}

    /* ═══════════ SELECT BOX - TERMINAL STYLE ═══════════ */

    div[data-testid="stSelectbox"] > div > div {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: var(--radius) !important;
    }}

    div[data-testid="stSelectbox"] label {{
        font-size: 0.7rem !important;
        color: var(--text-muted) !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }}

    /* ═══════════ ALERTS & INFO BOXES ═══════════ */

    div[data-testid="stAlert"] {{
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: var(--radius) !important;
        font-family: var(--font-mono) !important;
        font-size: 0.75rem !important;
    }}

    .stAlert[data-baseweb*="info"] {{
        border-left: 3px solid var(--accent) !important;
    }}

    .stAlert[data-baseweb*="warning"] {{
        border-left: 3px solid var(--warning) !important;
    }}

    .stAlert[data-baseweb*="error"] {{
        border-left: 3px solid var(--negative) !important;
    }}

    /* ═══════════ DIVIDERS - TERMINAL STYLE ═══════════ */

    hr {{
        border: none;
        height: 1px;
        background: var(--border-color);
        margin: 1.5rem 0;
    }}

    /* ═══════════ SPINNERS ═══════════ */

    .stSpinner > div {{
        border-top-color: var(--accent) !important;
    }}

    /* ═══════════ TERMINAL CUSTOM CLASSES ═══════════ */

    .terminal-box {{
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        padding: 1rem;
        font-family: var(--font-mono);
        font-size: 0.8rem;
        color: var(--text-primary);
        position: relative;
    }}

    .terminal-header {{
        color: var(--accent);
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 0.5rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--border-color);
    }}

    .terminal-value {{
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--accent);
    }}

    .terminal-label {{
        font-size: 0.65rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }}

    .positive {{ color: var(--positive) !important; }}
    .negative {{ color: var(--negative) !important; }}
    .muted {{ color: var(--text-muted) !important; }}
    .accent {{ color: var(--accent) !important; }}

    /* Blinking cursor effect */
    @keyframes blink {{
        0%, 50% {{ opacity: 1; }}
        51%, 100% {{ opacity: 0; }}
    }}

    .cursor {{
        display: inline-block;
        width: 8px;
        height: 14px;
        background: var(--accent);
        animation: blink 1s infinite;
        vertical-align: middle;
        margin-left: 2px;
    }}

    /* Status indicator */
    .status-dot {{
        display: inline-block;
        width: 6px;
        height: 6px;
        border-radius: 50%;
        margin-right: 6px;
    }}

    .status-dot.live {{ background: var(--positive); }}
    .status-dot.stale {{ background: var(--warning); }}
    .status-dot.error {{ background: var(--negative); }}

    /* Terminal-style loading animation */
    @keyframes terminal-loading {{
        0% {{ content: '[    ]'; }}
        20% {{ content: '[=   ]'; }}
        40% {{ content: '[==  ]'; }}
        60% {{ content: '[=== ]'; }}
        80% {{ content: '[====]'; }}
        100% {{ content: '[    ]'; }}
    }}

    @keyframes pulse {{
        0%, 100% {{ opacity: 0.4; }}
        50% {{ opacity: 1; }}
    }}

    .terminal-loading {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        color: var(--accent);
        font-size: 0.75rem;
    }}

    .terminal-loading::before {{
        content: '▶';
        animation: pulse 1s infinite;
    }}

    /* Streamlit spinner override - terminal style */
    .stSpinner > div {{
        border-color: var(--accent) !important;
    }}

    .stSpinner > div > div {{
        background-color: transparent !important;
    }}

    div[data-testid="stSpinner"] {{
        color: var(--accent) !important;
    }}

    div[data-testid="stSpinner"]::before {{
        content: '> LOADING...';
        color: var(--accent);
        font-size: 0.75rem;
        letter-spacing: 0.1em;
    }}

    /* Hide default streamlit elements */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    header {{ visibility: hidden; }}

</style>
""", unsafe_allow_html=True)

# Sidebar navigation - Terminal Style
with st.sidebar:
    # Terminal-style header
    st.markdown(f"""
    <div style="padding: 0.5rem 0 1rem 0; border-bottom: 1px solid #333;">
        <div style="font-size: 0.6rem; text-transform: uppercase; letter-spacing: 0.15em; color: #555; margin-bottom: 0.25rem;">┌── TERMINAL ──┐</div>
        <div style="font-size: 1rem; font-weight: 700; color: {theme['accent']};">SOL/PERPS<span class="cursor"></span></div>
        <div style="font-size: 0.65rem; color: #555; margin-top: 0.25rem;">v1.0.0</div>
    </div>
    """, unsafe_allow_html=True)

    # Theme selector
    st.markdown("""
    <div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin: 1rem 0 0.5rem 0;">[ THEME ]</div>
    """, unsafe_allow_html=True)

    theme_names = {k: v["name"] for k, v in TERMINAL_THEMES.items()}
    selected_theme = st.selectbox(
        "Theme",
        options=list(theme_names.keys()),
        format_func=lambda x: theme_names[x],
        index=list(theme_names.keys()).index(st.session_state.terminal_theme),
        label_visibility="collapsed"
    )

    if selected_theme != st.session_state.terminal_theme:
        st.session_state.terminal_theme = selected_theme
        st.query_params["theme"] = selected_theme
        st.rerun()

    # Display settings
    st.markdown("""
    <div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin: 1rem 0 0.5rem 0;">[ DISPLAY ]</div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        crt_toggle = st.checkbox("CRT", value=st.session_state.crt_effects, help="Scanlines & glow")
        if crt_toggle != st.session_state.crt_effects:
            st.session_state.crt_effects = crt_toggle
            st.query_params["crt"] = "1" if crt_toggle else "0"
            st.rerun()
    with col2:
        alerts_toggle = st.checkbox("Alerts", value=st.session_state.show_alerts, help="Show alerts panel")
        if alerts_toggle != st.session_state.show_alerts:
            st.session_state.show_alerts = alerts_toggle
            st.query_params["alerts"] = "1" if alerts_toggle else "0"
            st.rerun()

    st.markdown("""
    <div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin: 1rem 0 0.5rem 0;">[ NAVIGATE ]</div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
<style>
.nav-link {{
    font-size: 0.75rem;
    color: #888;
    text-decoration: none;
    display: block;
    padding: 6px 8px;
    margin: 1px 0;
    border-left: 2px solid transparent;
    transition: all 0.1s ease;
}}
.nav-link:hover {{
    color: {theme['accent']};
    background: rgba(255, 255, 255, 0.03);
    border-left-color: {theme['accent']};
}}
.nav-link::before {{
    content: '> ';
    color: #444;
}}
.nav-link:hover::before {{
    color: {theme['accent']};
}}
</style>

<a href="#solana-perps-overview" class="nav-link">OVERVIEW</a>
<a href="#cross-chain-comparison" class="nav-link">CROSS_CHAIN</a>
<a href="#solana-protocol-breakdown" class="nav-link">PROTOCOLS</a>
<a href="#best-venue-by-asset" class="nav-link">BEST_VENUE</a>
<a href="#funding-rate-overview" class="nav-link">FUNDING</a>
<a href="#market-deep-dive" class="nav-link">MARKETS</a>
<a href="#cross-platform-traders" class="nav-link">WALLETS</a>
<a href="#whale-activity" class="nav-link">WHALES</a>
<a href="#liquidations-rpc" class="nav-link">LIQUIDATIONS</a>
<a href="#quick-insights" class="nav-link">INSIGHTS</a>
    """, unsafe_allow_html=True)

    st.divider()

    # System status - terminal style
    st.markdown(f"""
    <div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin-bottom: 0.5rem;">[ SYSTEM ]</div>
    <div style="padding: 8px; background: #0d0d0d; border: 1px solid #333; font-size: 0.7rem;">
        <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 4px;">
            <span class="status-dot live"></span>
            <span style="color: {theme['positive']};">ONLINE</span>
        </div>
        <div style="color: #555;">REFRESH: 15min</div>
        <div style="color: #555;">SOURCES: 4</div>
    </div>
    """, unsafe_allow_html=True)

    # Keyboard shortcuts info
    with st.expander("⌨ SHORTCUTS", expanded=False):
        st.markdown(f"""
        <div style="font-size: 0.65rem; font-family: var(--font-mono); line-height: 1.8;">
            <div style="margin-bottom: 8px; color: #555;">─ NAVIGATION ─</div>
            <div><span style="color: {theme['accent']}; background: #1a1a1a; padding: 2px 6px; border: 1px solid #333;">↑</span> <span style="color: #888;">Scroll up</span></div>
            <div><span style="color: {theme['accent']}; background: #1a1a1a; padding: 2px 6px; border: 1px solid #333;">↓</span> <span style="color: #888;">Scroll down</span></div>
            <div><span style="color: {theme['accent']}; background: #1a1a1a; padding: 2px 6px; border: 1px solid #333;">Home</span> <span style="color: #888;">Top of page</span></div>
            <div><span style="color: {theme['accent']}; background: #1a1a1a; padding: 2px 6px; border: 1px solid #333;">End</span> <span style="color: #888;">Bottom</span></div>
            <div style="margin: 8px 0; color: #555;">─ SIDEBAR ─</div>
            <div><span style="color: {theme['accent']}; background: #1a1a1a; padding: 2px 6px; border: 1px solid #333;">C</span> <span style="color: #888;">Close sidebar</span></div>
            <div style="margin: 8px 0; color: #555;">─ REFRESH ─</div>
            <div><span style="color: {theme['accent']}; background: #1a1a1a; padding: 2px 6px; border: 1px solid #333;">R</span> <span style="color: #888;">Reload page</span></div>
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
# PLOTLY THEME CONFIGURATION - Terminal Style
# ══════════════════════════════════════════════════════════════════════════

def get_plotly_theme():
    """Get Plotly theme based on current terminal theme."""
    current_theme = TERMINAL_THEMES[st.session_state.terminal_theme]
    return {
        "bg_color": "rgba(0, 0, 0, 0)",
        "paper_color": "rgba(0, 0, 0, 0)",
        "grid_color": "rgba(255, 255, 255, 0.05)",
        "text_color": "#888888",
        "title_color": current_theme["accent"],
        "accent": current_theme["accent"],
        "accent_dim": current_theme["accent_dim"],
        "positive": current_theme["positive"],
        "negative": current_theme["negative"],
        "font_family": "JetBrains Mono, monospace",
    }

PLOTLY_THEME = get_plotly_theme()

# Protocol-specific colors - terminal style
PROTOCOL_COLORS = {
    "Drift": "#4F9DFF",
    "Jupiter": "#00FFA3",
    "Pacifica": "#DC1FFF",
    "Adrena": "#FFB800",
    "FlashTrade": "#FF6B6B",
    "default": theme["accent"],
}

# Sequential color palette for charts
CHART_COLORS = [theme["accent"], "#4F9DFF", "#00FFA3", "#DC1FFF", "#FFB800", "#FF6B6B", "#6366F1", "#14B8A6"]


def apply_plotly_theme(fig):
    """Apply the Terminal theme to a Plotly figure."""
    plotly_theme = get_plotly_theme()
    fig.update_layout(
        font_family=plotly_theme["font_family"],
        font_color=plotly_theme["text_color"],
        font_size=10,
        title_font_size=12,
        title_font_color=plotly_theme["title_color"],
        title_font_family=plotly_theme["font_family"],
        paper_bgcolor=plotly_theme["paper_color"],
        plot_bgcolor=plotly_theme["bg_color"],
        margin=dict(t=50, b=40, l=40, r=40),
        xaxis=dict(
            gridcolor=plotly_theme["grid_color"],
            linecolor="#333333",
            tickfont=dict(size=9, color="#888888"),
            title_font=dict(size=10, color="#888888"),
            zerolinecolor="#333333",
        ),
        yaxis=dict(
            gridcolor=plotly_theme["grid_color"],
            linecolor="#333333",
            tickfont=dict(size=9, color="#888888"),
            title_font=dict(size=10, color="#888888"),
            zerolinecolor="#333333",
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=9, color="#888888"),
            borderwidth=0,
        ),
        hoverlabel=dict(
            bgcolor="#1a1a1a",
            bordercolor=plotly_theme["accent"],
            font_size=10,
            font_family=plotly_theme["font_family"],
        ),
    )
    return fig


def terminal_section_header(title: str) -> str:
    """Generate a terminal-style section header."""
    title_upper = title.upper().replace(" ", "_")
    padding = 80 - len(title_upper) - 6  # Account for brackets and dashes
    return f"""
<div style="margin: 1.5rem 0 1rem 0;">
    <div style="color: {theme['accent']}; font-size: 0.75rem; font-weight: 600; letter-spacing: 0.05em;">
        ┌─[ {title_upper} ]{"─" * max(padding, 1)}┐
    </div>
</div>
"""


def ascii_bar(value: float, max_value: float, width: int = 20) -> str:
    """Generate an ASCII progress bar."""
    if max_value == 0:
        return "░" * width
    filled = int((value / max_value) * width)
    return "█" * filled + "░" * (width - filled)


def ascii_bar_html(value: float, max_value: float, width: int = 15, color: str = None, total: float = None) -> str:
    """Generate an HTML-styled ASCII progress bar.

    Args:
        value: The value to display
        max_value: Max value for bar scaling (bar fills proportionally)
        width: Number of characters for the bar
        color: Bar color
        total: If provided, shows share percentage (value/total) instead of relative to max
    """
    if color is None:
        color = theme["accent"]
    if max_value == 0:
        return f'<span style="color: #333;">{"░" * width}</span>'
    filled = int((value / max_value) * width)
    # Use total for percentage if provided, otherwise use max_value
    pct_base = total if total is not None else max_value
    pct = (value / pct_base) * 100 if pct_base > 0 else 0
    bar = f'<span style="color: {color};">{"█" * filled}</span><span style="color: #333;">{"░" * (width - filled)}</span>'
    return f'{bar} <span style="color: #888;">{pct:.1f}%</span>'


def ascii_sparkline(values: list, width: int = 10) -> str:
    """Generate an ASCII sparkline from values."""
    if not values:
        return "─" * width
    chars = "▁▂▃▄▅▆▇█"
    min_val = min(values)
    max_val = max(values)
    if max_val == min_val:
        return chars[4] * len(values)
    return "".join(chars[int((v - min_val) / (max_val - min_val) * 7)] for v in values[-width:])


def render_terminal_table(headers: list, rows: list, col_styles: dict = None) -> str:
    """Render a terminal-style HTML table with box drawing characters."""
    if col_styles is None:
        col_styles = {}

    # Build header row
    header_cells = "".join(f'<th style="padding: 8px 12px; text-align: left; color: {theme["accent"]}; font-size: 0.7rem; letter-spacing: 0.05em; border-bottom: 1px solid #333;">{h}</th>' for h in headers)

    # Build data rows
    data_rows = ""
    for row in rows:
        cells = ""
        for i, cell in enumerate(row):
            style = col_styles.get(i, "color: #888;")
            cells += f'<td style="padding: 6px 12px; font-size: 0.75rem; border-bottom: 1px solid #222; {style}">{cell}</td>'
        data_rows += f'<tr style="transition: background 0.1s;">{cells}</tr>'

    return f'''
    <div style="background: #0a0a0a; border: 1px solid #333; overflow: hidden; margin: 0.5rem 0;">
        <table style="width: 100%; border-collapse: collapse; font-family: 'JetBrains Mono', monospace;">
            <thead><tr style="background: #111;">{header_cells}</tr></thead>
            <tbody>{data_rows}</tbody>
        </table>
    </div>
    '''


def collapsible_section(title: str, content_key: str, default_open: bool = True) -> bool:
    """Create a collapsible section header. Returns True if section is open."""
    if f"collapse_{content_key}" not in st.session_state:
        st.session_state[f"collapse_{content_key}"] = default_open

    is_open = st.session_state[f"collapse_{content_key}"]
    indicator = "▼" if is_open else "►"

    col1, col2 = st.columns([20, 1])
    with col1:
        st.markdown(f"""
        <div style="color: {theme['accent']}; font-size: 0.75rem; font-weight: 600; letter-spacing: 0.05em; cursor: pointer;">
            {indicator} {title.upper().replace(" ", "_")}
        </div>
        """, unsafe_allow_html=True)
    with col2:
        if st.button("toggle", key=f"btn_{content_key}", label_visibility="collapsed"):
            st.session_state[f"collapse_{content_key}"] = not is_open
            st.rerun()

    return is_open


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


# Load cached data with terminal-style loading state
cache = load_cache()

if cache is None:
    st.markdown(f"""
    <div style="text-align: center; padding: 4rem 2rem; background: #0a0a0a; border: 1px solid #ff4444;">
        <div style="color: #ff4444; font-size: 1.2rem; margin-bottom: 1rem;">
            ╔══════════════════════════════════════╗
        </div>
        <div style="color: #ff4444; font-size: 0.9rem; margin-bottom: 0.5rem;">
            ║  ERROR: CACHE_NOT_FOUND             ║
        </div>
        <div style="color: #ff4444; font-size: 1.2rem; margin-bottom: 1rem;">
            ╚══════════════════════════════════════╝
        </div>
        <div style="color: #888; font-size: 0.75rem; margin-top: 1rem;">
            > No cached data available<br>
            > Waiting for first data update...<br>
            > Cache updates every 15 minutes via GitHub Actions
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# Header - Terminal Style (clean, no ASCII art)
st.markdown(f"""
<div style="background: #0a0a0a; border: 1px solid {theme['accent']}40; padding: 1.5rem; margin-bottom: 1rem;">
    <div style="text-align: center;">
        <div style="color: {theme['accent']}; font-size: 2rem; font-weight: 700; letter-spacing: 0.1em; margin-bottom: 0.25rem;">
            SOLANA PERPS
        </div>
        <div style="color: #555; font-size: 0.7rem; letter-spacing: 0.2em; margin-bottom: 1rem;">
            ════════════════════════════════════════
        </div>
        <div style="color: {theme['accent']}; font-size: 0.75rem; letter-spacing: 0.15em;">
            PERPETUALS ANALYTICS TERMINAL
        </div>
        <div style="color: #444; font-size: 0.65rem; margin-top: 0.5rem;">
            v1.0.0
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

updated_at = cache.get("updated_at", "Unknown")

# Parse time for display
try:
    updated_time = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    age_minutes = (datetime.now(timezone.utc) - updated_time).total_seconds() / 60
    time_display = updated_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    is_stale = age_minutes > 30
except (ValueError, TypeError):
    time_display = updated_at
    is_stale = False
    age_minutes = 0

# Terminal-style system status bar
status_indicator = "●" if not is_stale else "○"
status_text = "LIVE" if not is_stale else "STALE"
status_color = theme['positive'] if not is_stale else theme['warning']

st.markdown(f"""
<div style="background: #0d0d0d; border: 1px solid #333; padding: 0.5rem 1rem; margin-bottom: 1rem; font-size: 0.7rem;">
    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 1rem;">
        <div style="display: flex; align-items: center; gap: 1.5rem;">
            <span style="color: {status_color};">[{status_indicator}] {status_text}</span>
            <span style="color: #555;">│</span>
            <span style="color: #888;">UPDATED: <span style="color: #e0e0e0;">{time_display}</span></span>
        </div>
        <div style="display: flex; align-items: center; gap: 1.5rem;">
            <span style="color: #555;">SOURCES:</span>
            <span style="color: #4F9DFF;">DEFILLAMA</span>
            <span style="color: #00FFA3;">DRIFT</span>
            <span style="color: #FFB800;">DUNE</span>
            <span style="color: #9945FF;">RPC</span>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

if is_stale:
    st.warning(f"⚠ DATA STALE: {int(age_minutes)} minutes since last update")

# Alerts Panel - Show unusual market conditions
if st.session_state.show_alerts:
    drift_markets_alert = cache.get("drift_markets", {})
    alerts = []

    # Check for extreme funding rates (>0.1% or <-0.1%)
    for market, info in drift_markets_alert.items():
        funding = info.get("funding_rate", 0)
        vol = info.get("volume", 0)
        if vol > 100000:  # Only check markets with decent volume
            if funding > 0.001:  # >0.1%
                alerts.append({
                    "type": "funding_high",
                    "icon": "⚠",
                    "msg": f"{market.replace('-PERP', '')} funding HIGH: {funding*100:+.3f}% (longs pay)",
                    "color": theme["negative"]
                })
            elif funding < -0.001:  # <-0.1%
                alerts.append({
                    "type": "funding_low",
                    "icon": "💰",
                    "msg": f"{market.replace('-PERP', '')} funding LOW: {funding*100:+.3f}% (shorts pay)",
                    "color": theme["positive"]
                })

    # Check for big volume changes
    for _, row in pd.DataFrame(cache.get("protocols", [])).iterrows():
        change = row.get("change_1d", 0)
        if change > 50:
            alerts.append({
                "type": "volume_spike",
                "icon": "📈",
                "msg": f"{row['protocol']} volume +{change:.0f}% in 24h",
                "color": theme["positive"]
            })
        elif change < -30:
            alerts.append({
                "type": "volume_drop",
                "icon": "📉",
                "msg": f"{row['protocol']} volume {change:.0f}% in 24h",
                "color": theme["warning"]
            })

    if alerts:
        alerts_html = "".join([
            f'<div style="display: flex; align-items: center; gap: 8px; padding: 4px 0;">'
            f'<span>{a["icon"]}</span>'
            f'<span style="color: {a["color"]};">{a["msg"]}</span>'
            f'</div>'
            for a in alerts[:5]  # Limit to 5 alerts
        ])

        st.markdown(f"""
        <div style="background: #0a0a0a; border: 1px solid {theme['warning']}40; padding: 0.75rem; margin-bottom: 1rem; font-size: 0.7rem;">
            <div style="color: {theme['warning']}; font-size: 0.65rem; letter-spacing: 0.1em; margin-bottom: 0.5rem; text-transform: uppercase;">
                ⚡ ALERTS ({len(alerts)})
            </div>
            {alerts_html}
        </div>
        """, unsafe_allow_html=True)

# Time window selector - terminal style
st.markdown(f"""
<div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin-bottom: 0.5rem;">[ TIME_WINDOW ]</div>
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

# Get top protocol and SOL price for sidebar
top_protocol = protocol_df.iloc[0] if len(protocol_df) > 0 else None
drift_markets = cache.get("drift_markets", {})
sol_price = drift_markets.get("SOL-PERP", {}).get("last_price", 0)
sol_funding = drift_markets.get("SOL-PERP", {}).get("funding_rate", 0)

# Add mini stats to sidebar
with st.sidebar:
    st.markdown(f"""
    <div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin: 1rem 0 0.5rem 0;">[ QUICK_STATS ]</div>
    <div style="background: #0a0a0a; border: 1px solid #333; padding: 0.5rem; font-size: 0.7rem;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <span style="color: #555;">SOL_PRICE</span>
            <span style="color: {theme['accent']};">${sol_price:,.2f}</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <span style="color: #555;">SOL_FUND</span>
            <span style="color: {theme['positive'] if sol_funding < 0 else theme['negative']};">{sol_funding*100:+.4f}%</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <span style="color: #555;">VOL_24H</span>
            <span style="color: #e0e0e0;">{format_volume(total_volume)}</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
            <span style="color: #555;">OPEN_INT</span>
            <span style="color: #e0e0e0;">{format_volume(total_oi)}</span>
        </div>
        <div style="display: flex; justify-content: space-between;">
            <span style="color: #555;">TRADERS</span>
            <span style="color: #e0e0e0;">{total_traders:,}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Top movers
    if len(protocol_df) > 0:
        top_gainer = protocol_df.loc[protocol_df["change_1d"].idxmax()]
        st.markdown(f"""
        <div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin: 1rem 0 0.5rem 0;">[ TOP_MOVER_24H ]</div>
        <div style="background: #0a0a0a; border: 1px solid #333; padding: 0.5rem; font-size: 0.7rem;">
            <div style="color: {PROTOCOL_COLORS.get(top_gainer['protocol'], theme['accent'])}; font-weight: 600;">{top_gainer['protocol']}</div>
            <div style="color: {theme['positive']}; font-size: 0.85rem;">▲ {top_gainer['change_1d']:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)

# Terminal-style section header
st.markdown(f"""
<div style="margin: 1.5rem 0 1rem 0;">
    <div style="color: {theme['accent']}; font-size: 0.8rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em;">
        ┌─[ SOLANA_PERPS_OVERVIEW ]─────────────────────────────────────────────────────────┐
    </div>
</div>
""", unsafe_allow_html=True)

# Metrics in terminal box style
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("VOL_24H", format_volume(total_volume), help="Source: DeFiLlama")
with col2:
    st.metric("OPEN_INT", format_volume(total_oi), help="Source: Drift API")
with col3:
    st.metric("TRADERS", f"{total_traders:,}", help="Drift + Jupiter + Pacifica")
with col4:
    st.metric("FEES", f"${total_fees:,.0f}", help="Estimated fees")
with col5:
    st.metric("TXN_COUNT", f"{total_txns:,}", help="Program signatures")

st.markdown(f"""
<div style="color: {theme['accent']}; font-size: 0.8rem; margin-bottom: 0.5rem;">
    └───────────────────────────────────────────────────────────────────────────────────────┘
</div>
""", unsafe_allow_html=True)

st.divider()

# Cross-Chain Comparison
st.markdown(terminal_section_header("Cross-Chain Comparison"), unsafe_allow_html=True)
st.caption("> Comparing Solana perps to other chains")

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
        # Create terminal-style comparison table
        max_global_vol = global_derivatives[0]["volume_24h"] if global_derivatives else 1
        comparison_rows = []
        for i, p in enumerate(global_derivatives[:12]):
            is_solana = "Solana" in p.get("chains", [])
            chain = p.get("chains", ["?"])[0][:3].upper()
            vol = p["volume_24h"]
            change_1d = p.get("change_1d", 0)
            share = vol / global_total * 100 if global_total > 0 else 0

            # Highlight Solana protocols
            name_color = theme["accent"] if is_solana else "#888"
            change_color = theme["positive"] if change_1d > 0 else theme["negative"] if change_1d < 0 else "#555"

            comparison_rows.append([
                f'<span style="color: #555;">#{i+1:02d}</span>',
                f'<span style="color: {name_color}; font-weight: {"600" if is_solana else "400"};">{p["name"][:12]}</span>',
                f'<span style="color: #555;">{chain}</span>',
                f'<span style="color: #e0e0e0;">{format_volume(vol)}</span>',
                ascii_bar_html(vol, max_global_vol, width=8, color=theme["accent"] if is_solana else "#555", total=global_total),
                f'<span style="color: {change_color};">{"▲" if change_1d > 0 else "▼" if change_1d < 0 else "─"}{abs(change_1d):.1f}%</span>',
            ])

        st.markdown(render_terminal_table(
            headers=["#", "PROTOCOL", "CHAIN", "VOL_24H", "SHARE", "Δ24H"],
            rows=comparison_rows
        ), unsafe_allow_html=True)

    with col2:
        # Bar chart for clearer comparison (better than pie for rankings)
        with st.spinner("Loading chart..."):
            top_10 = global_derivatives[:10]
            colors = [theme["accent"] if "Solana" in p.get("chains", []) else "rgba(100, 116, 139, 0.5)" for p in top_10]

            fig = go.Figure(data=[
                go.Bar(
                    x=[p["name"][:10] for p in top_10],
                    y=[p["volume_24h"] for p in top_10],
                    marker_color=colors,
                    marker_line_color=[theme["accent"] if "Solana" in p.get("chains", []) else "rgba(100, 116, 139, 0.3)" for p in top_10],
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

    # Summary box - terminal style
    solana_share = (solana_total / global_total * 100) if global_total > 0 else 0
    st.markdown(f"""
    <div style="background: #0d0d0d; border: 1px solid #333; padding: 0.75rem 1rem; margin-top: 1rem; font-size: 0.75rem;">
        <span style="color: {theme['accent']};">SOLANA_SUMMARY:</span>
        <span style="color: {theme['positive']}; margin-left: 1rem;">{format_volume(solana_total)}</span>
        <span style="color: #555;"> VOL</span>
        <span style="color: #333; margin: 0 0.5rem;">│</span>
        <span style="color: {theme['accent']};">{solana_share:.1f}%</span>
        <span style="color: #555;"> GLOBAL_SHARE</span>
        <span style="color: #333; margin: 0 0.5rem;">│</span>
        <span style="color: #888;">{len(solana_protocols)} PROTOCOLS</span>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Solana Protocol Comparison with Chart
st.markdown(terminal_section_header("Solana Protocol Breakdown"), unsafe_allow_html=True)

col1, col2 = st.columns([1, 1])

with col1:
    # Build terminal-style table with ASCII bars
    max_vol = protocol_df["volume_24h"].max()
    table_rows = []
    for _, row in protocol_df.iterrows():
        proto = row["protocol"]
        vol = row["volume_24h"]
        change_1d = row["change_1d"]
        traders = row["traders"]
        fees = row["fees"]

        # Color for change
        change_color = theme["positive"] if change_1d > 0 else theme["negative"] if change_1d < 0 else "#888"
        change_str = f'<span style="color: {change_color};">{"▲" if change_1d > 0 else "▼" if change_1d < 0 else "─"} {abs(change_1d):.1f}%</span>'

        # Protocol color
        proto_color = PROTOCOL_COLORS.get(proto, theme["accent"])

        # Traders with asterisk for Pacifica
        traders_str = f"{traders:,}*" if proto == "Pacifica" else f"{traders:,}"

        table_rows.append([
            f'<span style="color: {proto_color};">{proto}</span>',
            f'<span style="color: #e0e0e0;">${vol:,.0f}</span>',
            ascii_bar_html(vol, max_vol, width=12, color=proto_color, total=total_volume),
            change_str,
            f'<span style="color: #888;">{traders_str}</span>',
            f'<span style="color: #666;">${fees:,.0f}</span>',
        ])

    st.markdown(render_terminal_table(
        headers=["PROTOCOL", "VOL_24H", "SHARE", "Δ24H", "TRADERS", "FEES"],
        rows=table_rows
    ), unsafe_allow_html=True)

    # Footnote
    if "Pacifica" in protocol_df["protocol"].values:
        st.caption("* Pacifica: on-chain users only")

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
                font=dict(size=14, color=PLOTLY_THEME["title_color"], family="JetBrains Mono, monospace"),
                showarrow=False
            )]
        )
        apply_plotly_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# Best Venue by Asset
st.markdown(terminal_section_header("Best Venue by Asset"), unsafe_allow_html=True)
st.caption("> Compare trading venues for each asset")

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

venue_rows = []
max_combined_vol = max(
    (drift_markets.get(f"{a}-PERP", {}).get("volume", 0) + jupiter_markets.get("volumes", {}).get(a, 0))
    for a in common_assets
) if common_assets else 1

for asset in common_assets:
    drift_key = f"{asset}-PERP"
    drift_info = drift_markets.get(drift_key, {})

    jupiter_vol = jupiter_markets.get("volumes", {}).get(asset, 0)
    drift_vol = drift_info.get("volume", 0)
    drift_funding = drift_info.get("funding_rate", 0)
    drift_oi = drift_info.get("open_interest", 0)
    drift_price = drift_info.get("last_price", 0)

    # Determine winner
    if jupiter_vol > drift_vol:
        winner = f'<span style="color: {PROTOCOL_COLORS["Jupiter"]};">JUP</span>'
    elif drift_vol > jupiter_vol:
        winner = f'<span style="color: {PROTOCOL_COLORS["Drift"]};">DRIFT</span>'
    else:
        winner = '<span style="color: #555;">TIE</span>'

    # Funding color
    fund_color = theme["positive"] if drift_funding < 0 else theme["negative"] if drift_funding > 0 else "#888"

    venue_rows.append([
        f'<span style="color: #e0e0e0; font-weight: 600;">{asset}</span>',
        f'<span style="color: {PROTOCOL_COLORS["Drift"]};">${drift_vol:,.0f}</span>',
        f'<span style="color: {PROTOCOL_COLORS["Jupiter"]};">${jupiter_vol:,.0f}</span>',
        winner,
        f'<span style="color: {fund_color};">{drift_funding*100:+.4f}%</span>',
        f'<span style="color: #888;">${drift_oi * drift_price:,.0f}</span>',
    ])

st.markdown(render_terminal_table(
    headers=["ASSET", "DRIFT_VOL", "JUP_VOL", "BEST", "FUNDING", "OI"],
    rows=venue_rows
), unsafe_allow_html=True)

st.divider()

# Funding Rate Heatmap
st.markdown(terminal_section_header("Funding Rates"), unsafe_allow_html=True)

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
    st.markdown(f"""
    <div style="font-size: 0.7rem; font-weight: 600; color: {theme['accent']}; margin-bottom: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em;">FUNDING_EXTREMES</div>
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
            <div style="background: #0d0d0d; border: 1px solid #333; padding: 0.75rem; margin-bottom: 0.5rem; font-size: 0.75rem;">
                <div style="color: #555; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">SHORTS_PAY_MOST</div>
                <div style="display: flex; align-items: center; justify-content: space-between;">
                    <span style="color: #e0e0e0;">{lowest[0].replace("-PERP", "")}</span>
                    <span style="color: {theme['positive']};">{format_funding(lowest[1].get('funding_rate', 0))}</span>
                </div>
            </div>

            <div style="background: #0d0d0d; border: 1px solid #333; padding: 0.75rem; font-size: 0.75rem;">
                <div style="color: #555; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">LONGS_PAY_MOST</div>
                <div style="display: flex; align-items: center; justify-content: space-between;">
                    <span style="color: #e0e0e0;">{highest[0].replace("-PERP", "")}</span>
                    <span style="color: {theme['negative']};">{format_funding(highest[1].get('funding_rate', 0))}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)

st.divider()

# Market Deep Dive
st.markdown(terminal_section_header("Market Deep Dive"), unsafe_allow_html=True)

window_data = get_time_window_data(cache, time_window)
pacifica_markets = cache.get("pacifica_markets", {})

col1, col2, col3 = st.columns(3)

with col1:
    drift_traders = window_data.get("drift_traders", 0)
    st.markdown(f"""
    <div style="color: {PROTOCOL_COLORS['Drift']}; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.5rem;">
        DRIFT <span style="color: #888; font-weight: 400;">({drift_traders:,}/{time_window})</span>
    </div>
    """, unsafe_allow_html=True)

    if drift_markets:
        total_vol = sum(m["volume"] for m in drift_markets.values())
        max_vol = max(m["volume"] for m in drift_markets.values())
        sorted_markets = sorted(drift_markets.items(), key=lambda x: x[1]["volume"], reverse=True)[:10]

        drift_rows = []
        for market, info in sorted_markets:
            vol = info["volume"]
            funding = info.get("funding_rate", 0)
            fund_color = theme["positive"] if funding < 0 else theme["negative"] if funding > 0 else "#888"

            drift_rows.append([
                f'<span style="color: #e0e0e0;">{market.replace("-PERP", "")}</span>',
                f'<span style="color: #888;">${vol/1e6:.1f}M</span>',
                ascii_bar_html(vol, max_vol, width=8, color=PROTOCOL_COLORS["Drift"], total=total_vol),
                f'<span style="color: {fund_color};">{funding*100:+.3f}%</span>',
            ])

        st.markdown(render_terminal_table(
            headers=["MKT", "VOL", "SHARE", "FUND"],
            rows=drift_rows
        ), unsafe_allow_html=True)

with col2:
    jupiter_traders = window_data.get("jupiter_traders", 0)
    st.markdown(f"""
    <div style="color: {PROTOCOL_COLORS['Jupiter']}; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.5rem;">
        JUPITER <span style="color: #888; font-weight: 400;">({jupiter_traders:,}/{time_window})</span>
    </div>
    """, unsafe_allow_html=True)

    jupiter_trades = jupiter_markets.get("trades", {})
    jupiter_volumes = jupiter_markets.get("volumes", {})

    if jupiter_trades:
        total_trades = sum(jupiter_trades.values())
        max_trades = max(jupiter_trades.values())

        jupiter_rows = []
        for market in sorted(jupiter_trades.keys(), key=lambda x: jupiter_trades[x], reverse=True)[:10]:
            trades = jupiter_trades[market]
            vol = jupiter_volumes.get(market, 0)

            jupiter_rows.append([
                f'<span style="color: #e0e0e0;">{market}</span>',
                f'<span style="color: #888;">{trades:,}</span>',
                f'<span style="color: #888;">${vol/1e6:.1f}M</span>',
                ascii_bar_html(trades, max_trades, width=8, color=PROTOCOL_COLORS["Jupiter"], total=total_trades),
            ])

        st.markdown(render_terminal_table(
            headers=["MKT", "TRADES", "VOL", "SHARE"],
            rows=jupiter_rows
        ), unsafe_allow_html=True)

with col3:
    pacifica_traders = window_data.get("pacifica_traders", 0)
    st.markdown(f"""
    <div style="color: {PROTOCOL_COLORS['Pacifica']}; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.5rem;">
        PACIFICA <span style="color: #888; font-weight: 400;">({pacifica_traders:,}/{time_window})</span>
    </div>
    """, unsafe_allow_html=True)

    if pacifica_markets:
        sorted_pac_markets = sorted(pacifica_markets.items(), key=lambda x: x[1].get("max_leverage", 0), reverse=True)[:10]
        max_lev = max(m.get("max_leverage", 0) for _, m in sorted_pac_markets)

        pacifica_rows = []
        for market, info in sorted_pac_markets:
            funding = info.get("funding_rate", 0)
            leverage = info.get("max_leverage", 0)
            fund_color = theme["positive"] if funding < 0 else theme["negative"] if funding > 0 else "#888"

            pacifica_rows.append([
                f'<span style="color: #e0e0e0;">{market}</span>',
                f'<span style="color: {fund_color};">{funding*100:+.3f}%</span>',
                f'<span style="color: {PROTOCOL_COLORS["Pacifica"]};">{leverage}x</span>',
            ])

        st.markdown(render_terminal_table(
            headers=["MKT", "FUND", "LEV"],
            rows=pacifica_rows
        ), unsafe_allow_html=True)
        st.caption("49 markets · Volume not available")
    else:
        st.caption("Market data loading...")

st.divider()

# Cross-Platform Wallet Analysis (always 24h for consistency)
st.markdown(terminal_section_header("Cross-Platform Traders (24h)"), unsafe_allow_html=True)
st.caption("> Wallet overlap: Drift × Jupiter × Pacifica")

# Always use 24h wallet data for consistency (Pacifica API only provides 24h granularity)
wallet_data = cache.get("time_windows", {}).get("24h", {}).get("wallet_overlap", {})

if wallet_data.get("error"):
    st.warning(f"Wallet data unavailable: {wallet_data.get('error', 'Unknown error')}")
else:
    # Extract all overlap categories
    drift_only = wallet_data.get("drift_only", 0)
    jupiter_only = wallet_data.get("jupiter_only", 0)
    pacifica_only = wallet_data.get("pacifica_only", 0)
    drift_jupiter = wallet_data.get("drift_jupiter", 0)
    drift_pacifica = wallet_data.get("drift_pacifica", 0)
    jupiter_pacifica = wallet_data.get("jupiter_pacifica", 0)
    all_three = wallet_data.get("all_three", 0)
    multi = wallet_data.get("multi_platform", 0)

    # Calculate totals per platform
    drift_total = drift_only + drift_jupiter + drift_pacifica + all_three
    jupiter_total = jupiter_only + drift_jupiter + jupiter_pacifica + all_three
    pacifica_total = pacifica_only + drift_pacifica + jupiter_pacifica + all_three
    total = drift_only + jupiter_only + pacifica_only + drift_jupiter + drift_pacifica + jupiter_pacifica + all_three

    if total > 0:
        # Top metrics row
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "All Three",
                f"{all_three:,}",
                help="Wallets active on Drift, Jupiter, AND Pacifica"
            )

        with col2:
            st.metric(
                "Multi-Platform",
                f"{multi:,}",
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

        # Pair overlaps row
        st.markdown(f"""
        <div style="font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.1em; color: #555; margin: 1rem 0 0.5rem 0;">[ PLATFORM_PAIRS ]</div>
        """, unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Drift + Jupiter", f"{drift_jupiter:,}", help="On both Drift and Jupiter (not Pacifica)")
        with col2:
            st.metric("Drift + Pacifica", f"{drift_pacifica:,}", help="On both Drift and Pacifica (not Jupiter)")
        with col3:
            st.metric("Jupiter + Pacifica", f"{jupiter_pacifica:,}", help="On both Jupiter and Pacifica (not Drift)")
        with col4:
            overlap_pct = (multi / total * 100) if total > 0 else 0
            st.metric("Overlap Rate", f"{overlap_pct:.1f}%", help="Percentage of traders using 2+ platforms")

        # Visualization row
        col1, col2 = st.columns([1, 1])

        with col1:
            with st.spinner("Loading chart..."):
                # Pie chart with all categories
                labels = []
                values = []
                colors = []

                if all_three > 0:
                    labels.append("All Three")
                    values.append(all_three)
                    colors.append(theme["accent"])
                if drift_jupiter > 0:
                    labels.append("Drift+Jupiter")
                    values.append(drift_jupiter)
                    colors.append("#7C3AED")
                if drift_pacifica > 0:
                    labels.append("Drift+Pacifica")
                    values.append(drift_pacifica)
                    colors.append("#8B5CF6")
                if jupiter_pacifica > 0:
                    labels.append("Jupiter+Pacifica")
                    values.append(jupiter_pacifica)
                    colors.append("#A78BFA")
                if drift_only > 0:
                    labels.append("Drift Only")
                    values.append(drift_only)
                    colors.append(PROTOCOL_COLORS["Drift"])
                if jupiter_only > 0:
                    labels.append("Jupiter Only")
                    values.append(jupiter_only)
                    colors.append(PROTOCOL_COLORS["Jupiter"])
                if pacifica_only > 0:
                    labels.append("Pacifica Only")
                    values.append(pacifica_only)
                    colors.append(PROTOCOL_COLORS["Pacifica"])

                fig = go.Figure(data=[go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.55,
                    marker=dict(
                        colors=colors,
                        line=dict(color=PLOTLY_THEME["bg_color"], width=2)
                    ),
                    textinfo="label+percent",
                    textposition="outside",
                    textfont=dict(size=9, color=PLOTLY_THEME["text_color"]),
                    hovertemplate="<b>%{label}</b><br>Traders: %{value:,}<br>Share: %{percent}<extra></extra>",
                )])
                fig.update_layout(
                    title=dict(text="Trader Distribution", font=dict(size=14)),
                    showlegend=False,
                    height=320,
                    annotations=[dict(
                        text=f"<b>{total:,}</b><br>traders",
                        x=0.5, y=0.5,
                        font=dict(size=12, color=PLOTLY_THEME["title_color"], family="JetBrains Mono, monospace"),
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
                        x=["Drift", "Jupiter", "Pacifica"],
                        y=[drift_total, jupiter_total, pacifica_total],
                        marker_color=[PROTOCOL_COLORS["Drift"], PROTOCOL_COLORS["Jupiter"], PROTOCOL_COLORS["Pacifica"]],
                        marker_line_color=[PROTOCOL_COLORS["Drift"], PROTOCOL_COLORS["Jupiter"], PROTOCOL_COLORS["Pacifica"]],
                        marker_line_width=1,
                        text=[f"{drift_total:,}", f"{jupiter_total:,}", f"{pacifica_total:,}"],
                        textposition="outside",
                        textfont=dict(size=11, color=PLOTLY_THEME["text_color"]),
                        hovertemplate="<b>%{x}</b><br>Traders: %{y:,}<extra></extra>",
                    )
                ])
                fig.update_layout(
                    title=dict(text="Total Traders by Platform (24h)", font=dict(size=14)),
                    yaxis_title="Unique Wallets",
                    height=320,
                    bargap=0.4,
                )
                apply_plotly_theme(fig)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No wallet data available for the current period")

st.divider()

# Whale Activity Section
st.markdown(terminal_section_header("Whale Activity"), unsafe_allow_html=True)
st.caption("> Top traders from P&L leaderboards - recent activity via RPC")

whale_data = cache.get("whale_activity", {})

if whale_data.get("error"):
    st.warning(f"Whale data unavailable: {whale_data.get('error', 'Unknown error')}")
else:
    whales = whale_data.get("whales", [])
    active_count = whale_data.get("active_last_1h", 0)
    total_whales = whale_data.get("total_whales", 0)

    if whales:
        col1, col2, col3 = st.columns([1, 1, 1])

        with col1:
            st.metric(
                "Tracked Whales",
                f"{total_whales}",
                help="Top traders from Pacifica and Jupiter P&L leaderboards"
            )

        with col2:
            st.metric(
                "Active (1h)",
                f"{active_count}",
                f"{(active_count/total_whales*100):.0f}%" if total_whales > 0 else "0%",
                help="Whales with transactions in the last hour"
            )

        with col3:
            # Calculate total recent txns
            total_recent_txns = sum(w.get("txn_count_1h", 0) for w in whales)
            st.metric(
                "Whale Txns (1h)",
                f"{total_recent_txns}",
                help="Total transactions from tracked whales in last hour"
            )

        # Whale activity table
        whale_rows = []
        for whale in whales[:10]:  # Show top 10
            addr = whale.get("address", "")
            short_addr = f"{addr[:4]}...{addr[-4:]}" if len(addr) > 8 else addr
            source = whale.get("source", "").upper()[:3]
            pnl = whale.get("pnl_24h", 0)
            vol = whale.get("volume", 0)
            txn_1h = whale.get("txn_count_1h", 0)
            is_active = whale.get("is_active", False)

            # Activity indicator
            status = f'<span style="color: {theme["positive"]};">●</span>' if is_active else f'<span style="color: #333;">○</span>'

            # PnL color
            pnl_color = theme["positive"] if pnl > 0 else theme["negative"] if pnl < 0 else "#888"
            pnl_str = f"+${pnl:,.0f}" if pnl > 0 else f"-${abs(pnl):,.0f}" if pnl < 0 else "$0"

            source_color = PROTOCOL_COLORS.get("Pacifica", theme["accent"]) if source == "PAC" else PROTOCOL_COLORS.get("Jupiter", theme["accent"])

            whale_rows.append([
                status,
                f'<span style="color: #e0e0e0;">{short_addr}</span>',
                f'<span style="color: {source_color};">{source}</span>',
                f'<span style="color: {pnl_color};">{pnl_str}</span>',
                f'<span style="color: #888;">${vol:,.0f}</span>',
                f'<span style="color: {theme["accent"] if txn_1h > 0 else "#555"};">{txn_1h}</span>',
            ])

        st.markdown(render_terminal_table(
            headers=["", "ADDRESS", "SRC", "P&L", "VOLUME", "TXN_1H"],
            rows=whale_rows
        ), unsafe_allow_html=True)

        st.caption(f"● Active in last 1h | Data updated: {whale_data.get('timestamp', 'Unknown')[:16]}")
    else:
        st.info("No whale data available")

st.divider()

# Real-time Liquidations Section (RPC-based)
st.markdown(terminal_section_header("Liquidations (RPC)"), unsafe_allow_html=True)
st.caption("> Real-time liquidation detection via Solana RPC")

liq_rpc_data = cache.get("liquidations_rpc", {})

if liq_rpc_data.get("error"):
    st.warning(f"RPC liquidations unavailable: {liq_rpc_data.get('error', 'Unknown error')}")
else:
    drift_liq = liq_rpc_data.get("drift", {})
    jupiter_liq = liq_rpc_data.get("jupiter", {})
    total_1h = liq_rpc_data.get("total_count_1h", 0)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Drift Liquidations (1h)",
            f"{drift_liq.get('count_1h', 0)}",
            help="Liquidations detected on Drift in last hour"
        )

    with col2:
        st.metric(
            "Jupiter Liquidations (1h)",
            f"{jupiter_liq.get('count_1h', 0)}",
            help="Liquidations detected on Jupiter Perps in last hour"
        )

    with col3:
        st.metric(
            "Total (1h)",
            f"{total_1h}",
            help="Combined liquidations across protocols"
        )

    with col4:
        total_checked = drift_liq.get("checked_transactions", 0) + jupiter_liq.get("checked_transactions", 0)
        st.metric(
            "Txns Scanned",
            f"{total_checked}",
            help="Number of recent transactions scanned for liquidations"
        )

    # Show recent liquidations table
    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"""
        <div style="color: {PROTOCOL_COLORS['Drift']}; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.5rem;">
            DRIFT LIQUIDATIONS
        </div>
        """, unsafe_allow_html=True)

        drift_liqs = drift_liq.get("liquidations", [])[:5]
        if drift_liqs:
            drift_liq_rows = []
            for liq in drift_liqs:
                sig = liq.get("signature", "")
                short_sig = f"{sig[:6]}...{sig[-6:]}" if len(sig) > 12 else sig
                ts = liq.get("timestamp", "")
                time_str = ts[11:16] if ts and len(ts) > 16 else "?"  # Extract HH:MM
                liq_type = liq.get("type", "?").upper()[:4]

                drift_liq_rows.append([
                    f'<span style="color: #888;">{time_str}</span>',
                    f'<span style="color: #e0e0e0;">{short_sig}</span>',
                    f'<span style="color: {theme["warning"]};">{liq_type}</span>',
                ])

            st.markdown(render_terminal_table(
                headers=["TIME", "SIGNATURE", "TYPE"],
                rows=drift_liq_rows
            ), unsafe_allow_html=True)
        else:
            st.info("No recent Drift liquidations")

    with col2:
        st.markdown(f"""
        <div style="color: {PROTOCOL_COLORS['Jupiter']}; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.5rem;">
            JUPITER LIQUIDATIONS
        </div>
        """, unsafe_allow_html=True)

        jupiter_liqs = jupiter_liq.get("liquidations", [])[:5]
        if jupiter_liqs:
            jup_liq_rows = []
            for liq in jupiter_liqs:
                sig = liq.get("signature", "")
                short_sig = f"{sig[:6]}...{sig[-6:]}" if len(sig) > 12 else sig
                ts = liq.get("timestamp", "")
                time_str = ts[11:16] if ts and len(ts) > 16 else "?"

                jup_liq_rows.append([
                    f'<span style="color: #888;">{time_str}</span>',
                    f'<span style="color: #e0e0e0;">{short_sig}</span>',
                ])

            st.markdown(render_terminal_table(
                headers=["TIME", "SIGNATURE"],
                rows=jup_liq_rows
            ), unsafe_allow_html=True)
        else:
            st.info("No recent Jupiter liquidations")

    st.caption(f"Data updated: {liq_rpc_data.get('timestamp', 'Unknown')[:16]} | Source: Solana RPC")

st.divider()

# Unique Insights Section
st.markdown(terminal_section_header("Quick Insights"), unsafe_allow_html=True)

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

# Footer - Terminal Style
st.divider()

st.markdown(f"""
<div style="padding: 1rem 0; text-align: center;">
    <div style="background: #0a0a0a; border: 1px solid #333; padding: 0.75rem 1rem; display: inline-block; text-align: left; font-size: 0.65rem; margin-bottom: 1rem;">
        <div style="color: {theme['accent']}; margin-bottom: 0.5rem; letter-spacing: 0.1em;">[ SYSTEM_INFO ]</div>
        <div style="color: #666;">DATA_SOURCES: <span style="color: #888;">DEFILLAMA | DRIFT_API | DUNE | RPC</span></div>
        <div style="color: #666;">REFRESH_RATE: <span style="color: #888;">15min</span></div>
        <div style="color: #666;">FEATURES: <span style="color: #888;">CROSS_CHAIN | FUNDING | OI | WALLETS</span></div>
    </div>

    <div style="font-size: 0.7rem; color: #555;">
        <span style="color: {theme['accent']};">></span> SOLANA_PERPS_TERMINAL v1.0.0
        <span style="color: #333; margin: 0 0.5rem;">|</span>
        <span style="color: #555;">Built for the Solana ecosystem</span>
    </div>
</div>
""", unsafe_allow_html=True)
