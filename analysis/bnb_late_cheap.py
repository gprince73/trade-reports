"""BNB 15M markets where one side traded under 5c in the final 15 seconds.

For every settled KXBNB15M market since START, pulls the public Kalshi trade
prints in [close - 15s, close), keeps markets where either side printed
below CHEAP_CENTS, and reports how they settled. "cheap_side_won" marks the
Chop CWST outcome: the side the market had given up on came back and settled.

Needs outbound access to api.elections.kalshi.com.

    python3 analysis/bnb_late_cheap.py [--start 2026-09-01] [--out bnb_late_cheap.csv]
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

API = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = "KXBNB15M"
WINDOW_SECONDS = 15
CHEAP_CENTS = 5


def _get(path: str, params: dict) -> dict:
    for attempt in range(5):
        r = requests.get(f"{API}{path}", params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return {}


def _paged(path: str, params: dict, key: str):
    cursor = None
    while True:
        page = _get(path, {**params, "limit": 1000, **({"cursor": cursor} if cursor else {})})
        yield from page.get(key, [])
        cursor = page.get("cursor")
        if not cursor:
            return


def _cents(obj: dict, field: str) -> float | None:
    # The API has served prices both as integer cents and as dollar strings.
    if obj.get(f"{field}_dollars") is not None:
        return float(obj[f"{field}_dollars"]) * 100
    if obj.get(field) is not None:
        return float(obj[field])
    return None


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def settled_markets(start: datetime) -> list[dict]:
    return list(_paged("/markets", {
        "series_ticker": SERIES,
        "status": "settled",
        "min_close_ts": int(start.timestamp()),
    }, "markets"))


def late_trades(ticker: str, close: datetime) -> list[dict]:
    lo = close - timedelta(seconds=WINDOW_SECONDS)
    return list(_paged("/markets/trades", {
        "ticker": ticker,
        "min_ts": int(lo.timestamp()),
        "max_ts": int(close.timestamp()),
    }, "trades"))


def analyse(start: datetime) -> pd.DataFrame:
    rows = []
    markets = settled_markets(start)
    print(f"{len(markets)} settled {SERIES} markets since {start.date()}")
    for m in markets:
        close = _ts(m["close_time"])
        trades = late_trades(m["ticker"], close)
        yes_px = [p for t in trades if (p := _cents(t, "yes_price")) is not None]
        if not yes_px:
            continue
        min_yes, max_yes = min(yes_px), max(yes_px)
        min_no = 100 - max_yes
        cheap_side = None
        if min_yes < CHEAP_CENTS:
            cheap_side = "yes"
        elif min_no < CHEAP_CENTS:
            cheap_side = "no"
        if cheap_side is None:
            continue
        result = m.get("result")
        rows.append({
            "ticker": m["ticker"],
            "close_time": close,
            "strike": m.get("floor_strike"),
            "settle_value": m.get("expiration_value"),
            "result": result,
            "cheap_side": cheap_side,
            "min_yes_cents": min_yes,
            "min_no_cents": min_no,
            "late_trades": len(trades),
            "cheap_side_won": result == cheap_side,
        })
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-09-01")
    ap.add_argument("--out", default="bnb_late_cheap.csv")
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    df = analyse(start)
    df.to_csv(args.out, index=False)

    print(f"\nmarkets with a side trading <{CHEAP_CENTS}c in the final {WINDOW_SECONDS}s: {len(df)}")
    if df.empty:
        return
    print("\nsettled result:")
    print(df["result"].value_counts().to_string())
    print("\ncheap side x result:")
    print(pd.crosstab(df["cheap_side"], df["result"], margins=True).to_string())
    print(f"\ncheap side came back and won (Chop CWST): {int(df['cheap_side_won'].sum())}")
    print(f"\nrows written to {args.out}")


if __name__ == "__main__":
    main()
