#!/usr/bin/env python3
"""
Daily market-volume collector.

• U.S. equities (NYSE + Nasdaq): total shares traded per day
  Source: Polygon /v2/aggs/grouped/locale/us/market/stocks/{date}   :contentReference[oaicite:1]{index=1}

• Crypto (all coins): total USD notional traded per day
  Source: CoinGecko /global/market_cap_chart?vs_currency=usd&days=max   :contentReference[oaicite:2]{index=2}

• Options (SPY, AAPL, TSLA, QQQ): daily contract volume (yesterday only)

Back-fills automatically from 2023-01-01 on the first run; afterwards it
adds only the latest day.  Free-tier rate limits are respected.
"""

from __future__ import annotations
import datetime as dt
import os, sys, time, requests, pandas as pd

# --------------------------------------------------------------------------- #
# constants & config
POLYGON      = "https://api.polygon.io"
COINGECKO    = "https://api.coingecko.com/api/v3"
API_KEY      = os.getenv("POLYGON_API_KEY")          # required for equities & options
BACKFILL_BEG = dt.date(2023, 1, 1)                   # inclusive
POLY_SLEEP   = 12                                    # ≤5 calls/min on free Polygon tier

CSV_FILE     = "daily_volumes.csv"

# --------------------------------------------------------------------------- #
def safe_json(r: requests.Response, url: str) -> dict:
    try:
        return r.json()
    except ValueError:
        raise RuntimeError(f"{url} – non-JSON ({r.status_code}): {r.text[:120]}")


def weekdays(start: dt.date, end: dt.date) -> list[dt.date]:
    cur, out = start, []
    while cur <= end:
        if cur.weekday() < 5:        # Mon-Fri
            out.append(cur)
        cur += dt.timedelta(days=1)
    return out


def ensure_api_key() -> None:
    if not API_KEY:
        sys.exit("❌  Set POLYGON_API_KEY as a repository secret.")


# --------------------------------------------------------------------------- #
# 1. figure out missing dates
yesterday = dt.date.today() - dt.timedelta(days=1)
while yesterday.weekday() > 4:             # roll back if Sat/Sun
    yesterday -= dt.timedelta(days=1)

if os.path.exists(CSV_FILE):
    df_existing = pd.read_csv(CSV_FILE, usecols=["date", "asset_type"])
    done_equity = set(df_existing[df_existing.asset_type == "equity_market"].date)
    done_crypto = set(df_existing[df_existing.asset_type == "crypto_market"].date)
else:
    done_equity = done_crypto = set()

equity_dates_needed = [
    d for d in weekdays(BACKFILL_BEG, yesterday)
    if d.strftime("%Y-%m-%d") not in done_equity
]
crypto_dates_needed = [
    d for d in weekdays(BACKFILL_BEG, yesterday)
    if d.strftime("%Y-%m-%d") not in done_crypto
]

rows: list[dict] = []

# --------------------------------------------------------------------------- #
# 2. EQUITIES – total market volume
if equity_dates_needed:
    ensure_api_key()
    for ix, d in enumerate(equity_dates_needed, 1):
        ds = d.strftime("%Y-%m-%d")
        url = f"{POLYGON}/v2/aggs/grouped/locale/us/market/stocks/{ds}?apiKey={API_KEY}"
        data = safe_json(requests.get(url, timeout=30), url)
        total_vol = sum(bar.get("v", 0) for bar in data.get("results", []))
        rows.append(
            {"date": ds, "asset_type": "equity_market",
             "symbol": "TOTAL_US", "volume": total_vol}
        )
        if ix < len(equity_dates_needed):
            time.sleep(POLY_SLEEP)            # stay under 5 req/min
    print(f"🗂  Added {len(equity_dates_needed)} equity-market day(s).")

# --------------------------------------------------------------------------- #
# 3. CRYPTO – total market volume
if crypto_dates_needed:
    days_since_start = (yesterday - BACKFILL_BEG).days + 2  # +2 for safety
    url = (f"{COINGECKO}/global/market_cap_chart"
           f"?vs_currency=usd&days={days_since_start}")
    data = safe_json(requests.get(url, timeout=60), url)
    vol_series = {
        dt.datetime.utcfromtimestamp(ts / 1000).date(): vol
        for ts, vol in data.get("total_volumes", [])
    }
    for d in crypto_dates_needed:
        vol = vol_series.get(d)
        if vol is not None:
            rows.append(
                {"date": d.strftime("%Y-%m-%d"),
                 "asset_type": "crypto_market",
                 "symbol": "TOTAL_CRYPTO",
                 "volume": vol}
            )
    print(f"🗂  Added {len(crypto_dates_needed)} crypto-market day(s).")

# --------------------------------------------------------------------------- #
# 4. OPTIONS – yesterday only (unchanged logic)
ds_y = yesterday.strftime("%Y-%m-%d")
tickers = ["SPY", "AAPL", "TSLA", "QQQ"]
ensure_api_key()
for t in tickers:
    url = f"{POLYGON}/v3/snapshot/options/{t}?apiKey={API_KEY}"
    try:
        chain = safe_json(requests.get(url, timeout=30), url).get("results", [])
        vol = sum(c.get("day", {}).get("volume", 0) for c in chain)
    except RuntimeError as e:
        print(f"⚠️  {e} – skipping {t}")
        vol = None
    rows.append({"date": ds_y, "asset_type": "option", "symbol": t, "volume": vol})

# --------------------------------------------------------------------------- #
# 5. write / append CSV
if rows:
    df_new = pd.DataFrame(rows)
    header_flag = not os.path.exists(CSV_FILE)
    df_new.to_csv(CSV_FILE, mode="a", index=False, header=header_flag)
    print(f"✅  Wrote {len(df_new)} new rows to {CSV_FILE}")
else:
    print("ℹ️  Nothing new to write.")
