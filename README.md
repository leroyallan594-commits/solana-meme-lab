# Solana Meme Research Lab (read-only prototype)

A beginner-friendly local Streamlit dashboard for collecting public market/risk snapshots, enforcing conservative screening checks, assigning a transparent score, logging alert outcomes, and tracking local price-based exit references.

## Safety and limitations

- No private keys, wallet connection, signing, swaps, or auto-trading.
- Uses public DEX Screener endpoints, the public Solana JSON-RPC endpoint, and RugCheck's public token-report endpoint.
- A provider not returning a critical field is treated as **UNKNOWN**, not as a pass. Unknown critical checks block Watchlist/Strong alerts.
- The DEX Screener latest-profile and boost feeds are not a complete feed of newly created tokens. This prototype is a research sample, not a complete market crawler.
- RugCheck response formats and rate limits may change. Review the linked token report manually and validate thresholds against logged results.
- The score uses proxies: DEX Screener buy/sell transaction counts are not unique wallets, and links do not prove authentic social activity.
- Liquidity lock percentages alone do not establish expiry, pool ownership, or exit feasibility.
- RugCheck's report has no deployer rug-history count. Creator history is therefore **advisory** by default (`require_creator_history_clear: false`): the `rugged` flag and any creator-rug risk name are still hard rejects, and alerts say the creator was not verified. Set it to `true` and no alert can ever fire.
- Insider/cluster share is a lower bound built from the `insider` flag on RugCheck's top-20 holders. If RugCheck reports insider networks without a supply share, the check stays UNKNOWN and blocks the alert.
- Dev sell tracking is not implemented.
- Price/stop-loss/take-profit notifications are reference alerts; no trades are sent and chart prices may not be executable.

## Install on Kali Linux

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
mkdir -p ~/Projects/solana-meme-lab
# Copy app.py, config.yaml, requirements.txt and README.md into that directory.
cd ~/Projects/solana-meme-lab
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m streamlit run app.py
```

Open the local Streamlit URL printed in the terminal, usually `http://localhost:8501`.

## Use

1. Open the dashboard and click **Scan now**. It makes read-only API/RPC requests and writes a local SQLite database at `data/meme_intel.db`.
2. Wait for repeated scans before expecting holder-growth scores; the app needs comparable snapshots at least eight minutes apart.
3. Inspect the hard-check details for each token. A score is not a price prediction. An incomplete set of safety checks blocks Watchlist/Strong alerts.
4. Add a token mint and entry price under **Track a holding** to log price-threshold/observed-liquidity reference alerts during future scans.
5. Review alert performance after 1 hour, 24 hours and 7 days. A horizon is only recorded if a scan runs inside its window (1h: 1-3h after the alert, 24h: 24-30h, 7d: 7d to 7d12h). Scan at least hourly or the columns stay empty rather than holding a wrong price. Export CSVs for weekly analysis.
6. Edit `config.yaml` to alter starting thresholds. Save changes and restart Streamlit. Treat values as hypotheses until a sufficiently large sample supports them.

## Changes in 0.2

Checked against live RugCheck and DexScreener responses and tested offline with mocked data:

- LP lock is read from `markets[].lp.lpLockedPct` (the old code looked for a top-level field that does not exist, so the check was always UNKNOWN).
- Pool, vault and burn accounts are excluded from top-10 / top-1 holder percentages by matching `topHolders` against the market addresses. Before, a bonding curve holding about 76% was counted as a holder.
- Cluster check uses `graphInsidersDetected`, `insiderNetworks` and the per-holder `insider` flag (the old key names do not exist in the report).
- `require_creator_history_clear` and `require_cluster_data` now do something; they were ignored before. Unverifiable checks that are not required show as ADVISORY.
- Outcome prices (1h / 24h / 7d) are only stored inside their window, and every scan refreshes older alerted tokens with one price call each. Before, a scan 30 hours later wrote the same price into both the 1h and 24h columns, and only the 15 most recent tokens were tracked.
- Raw RugCheck JSON is stored only for tokens that pass, so the database stops growing by megabytes per scan.

## Data sources

- DEX Screener API: https://docs.dexscreener.com/api/reference
- Solana RPC and Mint account fields: https://solana.com/docs/rpc/http/getaccountinfo and https://solana.com/docs/tokens
- RugCheck API report documentation: https://api.rugcheck.xyz/swagger/index.html

This software is for research and education, not financial advice. Meme tokens can lose most or all of their value.
