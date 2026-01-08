#!/usr/bin/env python3
"""
Audit script for Solana Perps Dashboard
Verifies trader counts, volume, and fees by examining raw transactions.
"""

import json
import sys
from urllib.request import urlopen, Request

RPC_URL = "https://ellipsis.rpcpool.com/7ba0a839-324a-417c-8b44-f37b444f43ee"

PROTOCOLS = {
    "Jupiter Perps": "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu",
    "Drift": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
}

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"


def rpc_call(method: str, params: list) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    req = Request(
        RPC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
    )
    with urlopen(req, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
        return result.get("result", {})


def audit_protocol(name: str, program_id: str, sample_size: int = 20):
    """Audit a protocol by examining sample transactions."""
    print(f"\n{'='*80}")
    print(f"AUDITING: {name}")
    print(f"Program ID: {program_id}")
    print(f"{'='*80}")

    # Fetch recent signatures
    sigs = rpc_call("getSignaturesForAddress", [program_id, {"limit": sample_size}])
    print(f"\nFetched {len(sigs)} signatures")

    all_signers = set()
    all_fee_payers = set()
    total_fees_lamports = 0
    total_volume = 0
    tx_details = []

    for i, sig_info in enumerate(sigs[:sample_size]):
        sig = sig_info.get("signature")
        if sig_info.get("err"):
            continue

        tx = rpc_call("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        if not tx:
            continue

        meta = tx.get("meta", {})
        message = tx.get("transaction", {}).get("message", {})
        account_keys = message.get("accountKeys", [])

        # Get fee payer (first account)
        fee_payer = None
        signer = None
        for acc in account_keys:
            if isinstance(acc, dict):
                if acc.get("signer"):
                    if fee_payer is None:
                        fee_payer = acc.get("pubkey")
                    signer = acc.get("pubkey")
            elif isinstance(acc, str) and fee_payer is None:
                fee_payer = acc

        all_fee_payers.add(fee_payer)
        if signer:
            all_signers.add(signer)

        # Get fee
        fee_lamports = meta.get("fee", 0)
        total_fees_lamports += fee_lamports

        # Get volume from token balance changes
        volume = 0
        pre_balances = meta.get("preTokenBalances", [])
        post_balances = meta.get("postTokenBalances", [])

        balance_map = {}
        for bal in pre_balances:
            mint = bal.get("mint", "")
            idx = bal.get("accountIndex", 0)
            amount = float(bal.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
            balance_map[(mint, idx)] = {"pre": amount, "post": 0}

        for bal in post_balances:
            mint = bal.get("mint", "")
            idx = bal.get("accountIndex", 0)
            amount = float(bal.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
            if (mint, idx) in balance_map:
                balance_map[(mint, idx)]["post"] = amount
            else:
                balance_map[(mint, idx)] = {"pre": 0, "post": amount}

        for (mint, idx), vals in balance_map.items():
            if mint in [USDC_MINT, USDT_MINT]:
                change = abs(vals["post"] - vals["pre"])
                if change > volume:
                    volume = change

        total_volume += volume

        tx_details.append({
            "sig": sig[:20] + "...",
            "fee_payer": fee_payer[:12] + "..." if fee_payer else "N/A",
            "signer": signer[:12] + "..." if signer else "N/A",
            "fee_lamports": fee_lamports,
            "volume": volume,
        })

    # Print sample transactions
    print(f"\n{'SAMPLE TRANSACTIONS':^80}")
    print("-" * 80)
    print(f"{'Signature':<24} {'Fee Payer':<16} {'Signer':<16} {'Fee (lamp)':<12} {'Volume':<12}")
    print("-" * 80)

    for tx in tx_details[:10]:
        print(f"{tx['sig']:<24} {tx['fee_payer']:<16} {tx['signer']:<16} {tx['fee_lamports']:<12,} ${tx['volume']:<11,.2f}")

    # Summary
    print(f"\n{'AUDIT SUMMARY':^80}")
    print("-" * 80)
    print(f"Transactions sampled: {len(tx_details)}")
    print(f"Unique fee payers: {len(all_fee_payers)}")
    print(f"Unique signers: {len(all_signers)}")
    print(f"Total fees (lamports): {total_fees_lamports:,}")
    print(f"Total fees (SOL): {total_fees_lamports / 1e9:.6f}")
    print(f"Avg fee per tx (lamports): {total_fees_lamports / max(len(tx_details), 1):,.0f}")
    print(f"Total volume (USD): ${total_volume:,.2f}")
    print(f"Avg volume per tx: ${total_volume / max(len(tx_details), 1):,.2f}")

    # Check if fee payer == signer
    print(f"\n{'TRADER vs FEE PAYER ANALYSIS':^80}")
    print("-" * 80)
    same_count = 0
    for tx in tx_details:
        if tx['fee_payer'] == tx['signer']:
            same_count += 1
    print(f"Transactions where fee_payer == signer: {same_count}/{len(tx_details)} ({100*same_count/max(len(tx_details),1):.1f}%)")
    print(f"This suggests {'user-initiated' if same_count > len(tx_details)/2 else 'keeper/bot'} transactions")

    # List unique wallets
    print(f"\nUnique Fee Payers ({len(all_fee_payers)}):")
    for fp in list(all_fee_payers)[:5]:
        print(f"  {fp}")
    if len(all_fee_payers) > 5:
        print(f"  ... and {len(all_fee_payers) - 5} more")

    return {
        "txns": len(tx_details),
        "unique_fee_payers": len(all_fee_payers),
        "unique_signers": len(all_signers),
        "total_fees_sol": total_fees_lamports / 1e9,
        "total_volume": total_volume,
    }


def main():
    print("=" * 80)
    print("SOLANA PERPS DASHBOARD AUDIT")
    print("=" * 80)

    results = {}
    for name, program_id in PROTOCOLS.items():
        results[name] = audit_protocol(name, program_id, sample_size=30)

    print("\n" + "=" * 80)
    print("ISSUES IDENTIFIED")
    print("=" * 80)

    print("""
1. TRADER COUNT ISSUE:
   - Current implementation counts 'signers' from accountKeys
   - For keeper-based protocols (like Drift), the signer is often the keeper bot
   - Real traders are in instruction accounts, not as transaction signers
   - Fix: Parse instruction accounts to find actual user wallets

2. VOLUME ISSUE:
   - Volume is estimated from USDC/USDT balance changes
   - Many perp trades don't involve direct stablecoin transfers
   - Drift uses margin accounts; volume is in position size, not transfers
   - Fix: Parse instruction data for actual trade size, or use protocol APIs

3. FEES ISSUE:
   - Currently only capturing Solana transaction fees (lamports)
   - NOT capturing protocol trading fees (which are much larger)
   - Protocol fees are typically 0.01-0.1% of trade volume
   - Fix: Identify fee account transfers or parse instruction data
""")


if __name__ == "__main__":
    main()
