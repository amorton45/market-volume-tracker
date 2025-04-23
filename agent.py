#!/usr/bin/env python3
"""
Collect daily volumes for US equities, selected option underlyings,
and the top-250 cryptocurrencies.
"""

import csv, datetime, os, sys, requests, pandas as pd

POLYGON  = "https://api.polygon.io"
COINGECKO = "https://api.coingecko.com/api/v3"
API_KEY  = os.getenv("POLYGON_API_KEY")

def safe_json(resp: requests.Response, endpoint: str) -> dict:
    """Return resp.json() or raise with a helpful message."""
    try:
        return resp.json()
    except ValueError:
        snippet = resp.text[:200].replace("\n", " ")
        raise RuntimeError(
            f"{endpoint} returned non-JSON ({resp.status_code}): {snippet}"
        )

# --- get previous trading day ------------------------------------------------
d = datetime.date.today() - datetime.timedelta(days=1)
while d.weekday() > 4:                # loop back to Friday if weekend
    d -= datetime.timedelta(days=1)
ds = d.strftime("%Y-%m-%d")

rows = []                              # master list to append to CSV

# 1) Equities (grouped daily bars) -------------------------------------------
u = f"{POLYGON}/v2/aggs/grouped/locale/us/market/stocks/{ds}?apiKey={API_KEY}"
stocks = safe_json(requests.get(u, timeout=30), u).get("results", [])
rows += [
    {"date": ds, "asset_type": "equity", "symbol": s["T"], "volume": s["v"]}
    for s in stocks
]

# 2) Options (sum day.volume for each contract) ------------------------------
tickers = ["SPY", "AAPL", "TSLA", "QQQ"]
for t in tickers:
    u = f"{POLYGON}/v3/snapshot/options/{t}?apiKey={API_KEY}"
    try:
        chain = safe_json(requests.get(u, timeout=30), u).get("results", [])
        vol = sum(c.get("day", {}).get("volume", 0) for c in chain)
    except RuntimeError as e:
        print(f"⚠️  Skipping options for {t}: {e}", file=sys.stderr)
        vol = None
    rows.append({"date": ds, "asset_type": "option", "symbol": t, "volume": vol})

# 3) Cryptocurrencies --------------------------------------------------------
u = f"{COINGECKO}/coins/markets?vs_currency=usd&order=volume_desc&per_page=250&page=1&sparkline=false"
cryptos = safe_json(requests.get(u, timeout=30), u)
rows += [
    {"date": ds, "asset_type": "crypto", "symbol": c["symbol"].upper(), "volume": c["total_volume"]}
    for c in cryptos
]

# --- append to CSV -----------------------------------------------------------
df = pd.DataFrame(rows)
csv_file = "daily_volumes.csv"
header = not os.path.exists(csv_file)
df.to_csv(csv_file, mode="a", index=False, header=header)
print(f"✅ wrote {len(df)} rows for {ds}")
