"""CWST-family lab strategy analysis, focused on BNB.

Parses the `[lab <strategy>] SETTLED ...` messages that the ingestion
pipeline currently drops into `bot_name`, rebuilds a structured trade
table (strategy, ticker, asset, side, result, qty, entry price, pnl),
and prints the tables behind analysis/cwst_bnb_report.md.

Run from the repo root:
    python3 analysis/cwst_bnb.py
"""
from __future__ import annotations

import re

import pandas as pd

EVENTS_PATH = "published_data/events.parquet"

LAB_PATTERN = re.compile(
    r"\[lab (?P<strategy>[^\]]+)\] SETTLED (?P<ticker>\S+) (?P<side>YES|NO) "
    r"(?P<result>WIN|LOSS) (?P<pnl>[+-]?\d+\.\d+) "
    r"\(held (?P<qty>\d+) @ (?P<price>[\d.]+)\) day (?P<day_pnl>[+-]?\d+\.\d+)"
)


def load_lab_trades(events_path: str = EVENTS_PATH) -> pd.DataFrame:
    df = pd.read_parquet(events_path)
    lab = df[df["bot_name"].str.contains(r"\[lab ", na=False)]

    rows = []
    for _, r in lab.iterrows():
        m = LAB_PATTERN.search(r["bot_name"])
        if m:
            rows.append({**m.groupdict(), "timestamp": r["timestamp"]})

    out = pd.DataFrame(rows)
    for col in ("pnl", "price", "day_pnl"):
        out[col] = out[col].astype(float)
    out["qty"] = out["qty"].astype(int)
    out["asset"] = out["ticker"].str.extract(r"^KX([A-Z]+?)(?:15M|D)-")[0]
    out["hourly"] = out["strategy"].str.endswith(" H")
    out["strat_base"] = out["strategy"].str.replace(r" H$", "", regex=True)
    out["win"] = out["result"] == "WIN"
    out["filled"] = out["qty"] > 0
    return out


def strategy_summary(trades: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    filled = trades[trades["filled"]]
    g = filled.groupby(by).agg(
        n=("win", "size"),
        wins=("win", "sum"),
        pnl=("pnl", "sum"),
        avg_price=("price", "mean"),
    )
    g["win_rate"] = g["wins"] / g["n"]
    # For a binary contract bought at `price`, breakeven win rate == price.
    g["breakeven_wr"] = g["avg_price"]
    return g.round(3)


def main() -> None:
    pd.set_option("display.width", 200)
    trades = load_lab_trades()
    cwst = trades[trades["strat_base"].str.startswith("cwst")]
    bnb = trades[trades["asset"] == "BNB"]

    print(f"parsed {len(trades)} lab settlements "
          f"({trades['timestamp'].min()} -> {trades['timestamp'].max()})")

    print("\n=== cwst family, filled trades, by strategy ===")
    print(strategy_summary(cwst, ["strat_base"]).to_string())

    print("\n=== cwst family, filled trades, by strategy x asset ===")
    print(strategy_summary(cwst, ["strat_base", "asset"]).to_string())

    print("\n=== cwst family on BTC+SOL, by side ===")
    ref = cwst[cwst["asset"].isin(["BTC", "SOL"])]
    print(strategy_summary(ref, ["side"]).to_string())

    print("\n=== BNB lab trades, filled, by strategy x side ===")
    print(strategy_summary(bnb, ["strat_base", "side"]).to_string())

    print("\n=== every BNB lab settlement ===")
    cols = ["timestamp", "strategy", "ticker", "side", "result", "qty", "price", "pnl"]
    print(bnb.sort_values("timestamp")[cols].to_string(index=False))

    # Settlement direction of BNB contracts the main Pacman bots touched,
    # read from the "-- settled YES/NO" suffix in bot_name.
    df = pd.read_parquet(EVENTS_PATH)
    settled = df[df["bot_name"].str.contains("-- settled", na=False)].copy()
    settled["settled"] = settled["bot_name"].str.extract(r"settled (YES|NO)")
    ct = pd.crosstab(settled["asset"], settled["settled"])
    ct["yes_share"] = (ct["YES"] / (ct["YES"] + ct["NO"])).round(3)
    print("\n=== Pacman-touched contracts: settlement direction by asset ===")
    print(ct.to_string())


if __name__ == "__main__":
    main()
