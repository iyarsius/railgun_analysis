# Railgun RAIL Staking Analysis

Python script to reconstruct Railgun RAIL staking activity from Ethereum mainnet using a public RPC.

It generates:
- daily `stake in`
- daily `pending claim`
- daily `claimable`
- CSV exports
- PNG charts
- top stake / unlock / claim tables for `2026-04-05` to `2026-04-10`

## Contract

- Staking contract: `0xEE6A649Aa3766bD117e12C161726b693A1B2Ee20`
- Public RPC default: `https://ethereum-rpc.publicnode.com`

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python analyze_rail_staking.py
```

Outputs are written to `output/`.

## Notes

- `Unlock` and `Claim` events do not emit amounts, so amounts are reconstructed from `Stake(account, stakeID, amount)`.
- The script also reconciles known stakes via the public `stakes(account, stakeID)` getter to backfill missing history when needed.
