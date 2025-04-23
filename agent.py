#!/usr/bin/env python3
import csv, datetime, os, requests, pandas as pd

POLYGON = "https://api.polygon.io"
CG = "https://api.coingecko.com/api/v3"
API_KEY = os.getenv("POLYGON_API_KEY")

# yesterday (handles weekends/holidays by stepping back until weekday)
date = datetime.date.today() - datetime.timedelta(days=1)
while date.weekday() > 4:  # 0=Mon, 6=Sun
    date -= datetime.timedelta(days=1)
ds = date.strftime("%Y-%m-%d")

out = []

# 1) US equities -----------------------------------------------------------
url = f"{POLYGON}/v2/aggs/grouped/locale/us/market/stocks/{ds}?apiKey={API_KEY}"
stocks = requests.get(url, timeout=30).json().get("results", [])
for s in stocks:
    out.append({"date": ds, "asset_type": "equity", "symbol": s["T"], "volume": s["v"]})

# 2) Options  (pick a list of heavily-traded underlyings to stay in free tier)
tickers = ["SPY", "AAPL", "TSLA", "QQQ"]
for t in tickers:
    url = f"{POLYGON}/v2/snapshot/options/{t}?apiKey={API_KEY}"
    chain = requests.get(url, timeout=30).json().get("results", {}).get("options", [])
    total = sum(o.get("day", {}).get("volume", 0) for o in chain)
    out.append({"date": ds, "asset_type": "option", "symbol": t, "volume": total})

# 3) Cryptocurrencies (top 250 by volume)
cg_url = f"{CG}/coins/markets?vs_currency=usd&order=volume_desc&per_page=250&page=1&sparkline=false"
cryptos = requests.get(cg_url, timeout=30).json()
for c in cryptos:
    out.append({"date": ds, "asset_type": "crypto", "symbol": c['symbol'].upper(), "volume": c['total_volume']})

# -------------------------------------------------------------------------
df = pd.DataFrame(out)
csv_path = "daily_volumes.csv"
header = not os.path.exists(csv_path)
df.to_csv(csv_path, mode="a", index=False, header=header)
print(f"Wrote {len(df)} rows for {ds}")
