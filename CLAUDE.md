# Solana Perps Dashboard

Real-time analytics dashboard for Solana perpetual DEXes (Drift, Jupiter Perps, FlashTrade, Adrena).

## Architecture

- **Frontend**: Streamlit (`streamlit_app.py`)
- **Data fetching**: `solana_perps_dashboard.py` - all API/Dune query functions
- **Caching**: `update_cache.py` runs via GitHub Actions every 15 mins, writes to `data/cache.json`

## Data Sources

| Source | Data | Speed |
|--------|------|-------|
| DeFiLlama API | Protocol volumes (24h/7d), global derivatives rankings | Fast |
| Drift REST API | Per-market volume, funding rates, open interest, prices | Fast |
| Dune Analytics | Trader counts, Jupiter markets, wallet overlap, liquidations | Slow (2-5 min) |
| Solana RPC | Transaction/signature counts | Fast |

## Key Features

- Cross-chain comparison with global perps rankings
- Time window selector (1h/4h/8h/24h) for trader/liquidation metrics
- Best venue by asset comparison (Drift vs Jupiter)
- Funding rate heatmap
- Cross-platform wallet tracking (who trades on both Drift AND Jupiter)
- Market deep dive per protocol

## Known Limitations

- **Dune query timeouts**: Complex queries fail at longer windows
  - Wallet overlap: only works for 1h/4h (8h+ times out)
  - Liquidations: only works for 1h-8h (24h times out)
- **Drift uses keepers**: User wallets are in `account_arguments[3-5]`, not `signer`
- **Jupiter**: Users sign directly, so `signer` = wallet
- **FlashTrade/Adrena**: Limited data (no program IDs configured)

## Cache Structure

```python
cache = {
    "time_windows": {
        "1h": {"drift_traders": N, "jupiter_traders": N, "liquidations": {...}, "wallet_overlap": {...}},
        "4h": {...},
        "8h": {...},
        "24h": {...}
    },
    "protocols": [...],      # Volume, fees, traders per protocol
    "drift_markets": {...},  # Per-market data from Drift API
    "jupiter_markets": {...},# Per-market trades/volumes
    "global_derivatives": [...] # Top 15 global perps for comparison
}
```

## Common Tasks

- **Run cache update**: `python update_cache.py`
- **Run dashboard locally**: `streamlit run streamlit_app.py`
- **Test a Dune query**: Functions in `solana_perps_dashboard.py` can be called directly

## Repo

GitHub: https://github.com/kab1rs/solana-perps-dashboard
