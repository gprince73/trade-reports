"""Chop-CWST backtest: does the "given up on, then recovers" shape pay?

Shape (YES version; NO is the mirror image):
  * earlier in the settlement window the gap was comfortably on the YES side,
  * at T_EVAL seconds before settlement the raw gap has collapsed through the
    strike and the YES contract trades for pennies,
  * but the running settlement average (GCP) is still at/above the win line,
    so only a modest recovery is needed,
  * and the market settles YES anyway.

Gates evaluated at T_EVAL (defaults match the proposed strategy):
  1. required future average  <= REQ_AVG_MAX  (equiv. GCP > -0.55t at t15)
  2. required climb (req avg - raw) < CLIMB_MAX
  3. raw range over the last RANGE_SECS seconds <= RANGE_MAX
  0. raw gap <= RAW_MAX at T_EVAL: the side is actually losing on the tape at
     evaluation (stand-in for "trades cheap" when no tape file is given)
Optional earlier-ownership gate: gap > OWN_MIN somewhere in [OWN_FROM, T_EVAL).

Per-second variant: after T_EVAL the gates are recomputed every second and the
candidate is "pulled" the first second the climb cap is breached, which is
what a bot cancelling resting bids would do.

Input: one or more data_feed5-style pipe-delimited files
    Date | Time | Strike | PriceProxy | GrowingCP | Gap | SD_max | Secs | Feeds
Gap and GrowingCP are dollar distances from strike; --tick converts to t.
Optional tape file (CSV) with columns: ticker, secs_left, yes_cents, volume.

    python3 analysis/chop_cwst.py --feed BNB=path/to/data_feed5_bnb.txt \
        --tick BNB=0.10 --feed SOL=... --tick SOL=0.01 [--tape tape.csv]
    python3 analysis/chop_cwst.py --selftest
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

WINDOW = 60            # seconds in the settlement average
WIN_LINE = -0.5        # ticks; averages above this round to the strike -> YES
T_EVAL = 15
RAW_MAX = -1.0         # ticks; raw must be through the strike on the losing side
REQ_AVG_MAX = -0.35    # == GCP > -0.55t at t15
CLIMB_MAX = 8.0
RANGE_SECS = 3
RANGE_MAX = 3.0
OWN_MIN = 2.0          # earlier-ownership gate, ticks
OWN_FROM = 60
CHEAP_CENTS = 5
RUNGS = (4, 3, 2, 1)
CLIMB_BUCKETS = [-np.inf, 0, 2, 4, 6, 8, np.inf]


# ---------------------------------------------------------------- gate math

def required_future_avg(gcp: float, secs_left: int) -> float:
    """Average the remaining seconds must reach for the window mean to hit WIN_LINE."""
    known = WINDOW - secs_left
    if secs_left <= 0:
        return np.nan
    return (WIN_LINE * WINDOW - known * gcp) / secs_left


def required_climb(gcp: float, raw: float, secs_left: int) -> float:
    return required_future_avg(gcp, secs_left) - raw


# ---------------------------------------------------------------- loading

def load_feed(path: str, tick: float) -> pd.DataFrame:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from data.csv_loader import _parse_dollar, _parse_feeds  # noqa: WPS433

    rows, header = [], None
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or s.startswith("---"):
                continue
            cols = [c.strip() for c in s.split("|")]
            if header is None:
                header = cols
                continue
            if len(cols) >= len(header):
                rows.append(cols[: len(header)])
    df = pd.DataFrame(rows, columns=header)
    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"])
    for c in ("Strike", "PriceProxy", "GrowingCP", "Gap"):
        df[c] = df[c].apply(_parse_dollar)
    df["Secs"] = pd.to_numeric(df["Secs"], errors="coerce")
    df = df.dropna(subset=["Gap", "GrowingCP"]).copy()
    df["raw_t"] = df["Gap"] / tick
    df["gcp_t"] = df["GrowingCP"] / tick
    # Window key: the 15-minute boundary each row settles into.
    df["expiry"] = df["datetime"].dt.ceil("15min")
    df["secs_left"] = (df["expiry"] - df["datetime"]).dt.total_seconds().astype(int)
    return df


def load_tape(path: str | None) -> pd.DataFrame | None:
    if not path:
        return None
    t = pd.read_csv(path)
    need = {"ticker", "secs_left", "yes_cents"}
    missing = need - set(t.columns)
    if missing:
        raise SystemExit(f"tape file missing columns: {sorted(missing)}")
    return t


# ---------------------------------------------------------------- evaluation

@dataclass
class Params:
    t_eval: int = T_EVAL
    raw_max: float = RAW_MAX
    req_avg_max: float = REQ_AVG_MAX
    climb_max: float = CLIMB_MAX
    range_secs: int = RANGE_SECS
    range_max: float = RANGE_MAX
    own_min: float | None = OWN_MIN
    own_from: int = OWN_FROM


def evaluate_window(w: pd.DataFrame, side: str, p: Params) -> dict | None:
    """Evaluate one settlement window for one side. `w` has secs_left, raw_t, gcp_t.

    For side == "NO" the gaps are negated so the same YES-shaped gates apply.
    """
    sign = 1 if side == "YES" else -1
    w = w[(w["secs_left"] >= 0) & (w["secs_left"] <= WINDOW)].sort_values("secs_left", ascending=False)
    if w.empty or w["secs_left"].min() > 2:
        return None
    raw = (w["raw_t"] * sign).to_numpy()
    gcp = (w["gcp_t"] * sign).to_numpy()
    secs = w["secs_left"].to_numpy()

    def at(s: int):
        i = np.where(secs == s)[0]
        return int(i[0]) if len(i) else None

    i_eval = at(p.t_eval)
    if i_eval is None:
        return None
    final_gcp = gcp[-1]
    settled_side_win = final_gcp >= WIN_LINE

    req_avg = required_future_avg(gcp[i_eval], p.t_eval)
    climb = req_avg - raw[i_eval]
    rng_mask = (secs <= p.t_eval + p.range_secs) & (secs > p.t_eval)
    rng = raw[rng_mask].max() - raw[rng_mask].min() if rng_mask.any() else np.nan
    own_mask = (secs <= p.own_from) & (secs > p.t_eval)
    owned = bool((raw[own_mask] > p.own_min).any()) if (p.own_min is not None and own_mask.any()) else True

    g0 = raw[i_eval] <= p.raw_max
    g1 = req_avg <= p.req_avg_max
    g2 = climb < p.climb_max
    g3 = rng <= p.range_max
    passed = bool(g0 and g1 and g2 and g3 and owned)

    # Per-second variant: when, if ever, does the climb cap get breached after T_EVAL?
    pulled_at = None
    if passed:
        for i in range(i_eval + 1, len(secs)):
            if secs[i] <= 0:
                break
            if required_climb(gcp[i], raw[i], int(secs[i])) >= p.climb_max:
                pulled_at = int(secs[i])
                break

    return {
        "side": side,
        "raw_eval": raw[i_eval],
        "gcp_eval": gcp[i_eval],
        "req_avg": req_avg,
        "climb": climb,
        "range3": rng,
        "owned": owned,
        "g0_losing": g0,
        "g1_req_avg": g1,
        "g2_climb": g2,
        "g3_range": g3,
        "passed": passed,
        "pulled_at": pulled_at,
        "final_gcp": final_gcp,
        "side_won": bool(settled_side_win),
    }


def run(feeds: dict[str, tuple[str, float]], tape: pd.DataFrame | None, p: Params, start: str) -> pd.DataFrame:
    out = []
    for asset, (path, tick) in feeds.items():
        df = load_feed(path, tick)
        df = df[df["datetime"] >= pd.Timestamp(start)]
        for expiry, w in df.groupby("expiry"):
            for side in ("YES", "NO"):
                r = evaluate_window(w, side, p)
                if r is None:
                    continue
                r.update({"asset": asset, "expiry": expiry})
                out.append(r)
    res = pd.DataFrame(out)
    if res.empty:
        return res
    res["climb_bucket"] = pd.cut(res["climb"], CLIMB_BUCKETS)

    if tape is not None:
        res = attach_tape(res, tape, p)
    return res


def attach_tape(res: pd.DataFrame, tape: pd.DataFrame, p: Params) -> pd.DataFrame:
    """Mark whether the side traded cheap in the final T_EVAL seconds and which rungs filled."""
    tape = tape.copy()
    tape["expiry"] = pd.to_datetime(tape["expiry"]) if "expiry" in tape else pd.NaT
    late = tape[(tape["secs_left"] < p.t_eval) & (tape["secs_left"] >= 0)]
    cheap_rows = []
    for (exp, side), g in res.groupby(["expiry", "side"]):
        lt = late[late["expiry"] == exp]
        px = lt["yes_cents"] if side == "YES" else 100 - lt["yes_cents"]
        min_px = px.min() if len(px) else np.nan
        cheap_rows.append({
            "expiry": exp, "side": side,
            "min_cents_late": min_px,
            "traded_cheap": bool(min_px < CHEAP_CENTS) if pd.notna(min_px) else False,
            "rungs_filled": sum(int((px <= r).any()) for r in RUNGS) if len(px) else 0,
        })
    return res.merge(pd.DataFrame(cheap_rows), on=["expiry", "side"], how="left")


# ---------------------------------------------------------------- reporting

def report(res: pd.DataFrame) -> None:
    pd.set_option("display.width", 220)
    if res.empty:
        print("no windows evaluated")
        return
    print(f"windows evaluated: {res['expiry'].nunique()}  (assets: {sorted(res['asset'].unique())})")

    print("\n=== candidates passing all gates at t_eval, by asset x side ===")
    c = res[res["passed"]]
    t = c.groupby(["asset", "side"]).agg(n=("side_won", "size"), side_won=("side_won", "sum"))
    t["win_rate"] = (t["side_won"] / t["n"]).round(3)
    t["breakeven_at_4c"] = 0.05
    print(t.to_string() if len(t) else "  none")

    print("\n=== candidates, win rate by required-climb bucket (all assets) ===")
    t = c.groupby(["climb_bucket"], observed=True).agg(n=("side_won", "size"), side_won=("side_won", "sum"))
    t["win_rate"] = (t["side_won"] / t["n"]).round(3)
    print(t.to_string() if len(t) else "  none")

    print("\n=== per-second variant: candidates that would have been pulled before settlement ===")
    t = c.groupby(["asset", "side"]).agg(
        n=("passed", "size"),
        pulled=("pulled_at", lambda s: s.notna().sum()),
        pulled_and_won=("pulled_at", lambda s: int((s.notna() & c.loc[s.index, "side_won"]).sum())),
        kept_and_won=("pulled_at", lambda s: int((s.isna() & c.loc[s.index, "side_won"]).sum())),
    )
    print(t.to_string() if len(t) else "  none")

    if "traded_cheap" in res:
        print(f"\n=== tape: windows where the side traded <{CHEAP_CENTS}c in the final {T_EVAL}s ===")
        ch = res[res["traded_cheap"]]
        t = ch.groupby(["asset", "side"]).agg(n=("side_won", "size"), side_won=("side_won", "sum"),
                                              passed_gates=("passed", "sum"))
        t["win_rate"] = (t["side_won"] / t["n"]).round(3)
        print(t.to_string() if len(t) else "  none")
        both = ch[ch["passed"]]
        if len(both):
            print("\n  cheap AND passed gates:")
            t = both.groupby(["asset", "side"]).agg(n=("side_won", "size"), side_won=("side_won", "sum"),
                                                     avg_rungs_filled=("rungs_filled", "mean"))
            t["win_rate"] = (t["side_won"] / t["n"]).round(3)
            print(t.to_string())


# ---------------------------------------------------------------- self test

def _synthetic_example() -> pd.DataFrame:
    """Rough replay of KXBNB15M-26OCT141600-00 in ticks: owned YES, collapsed, recovered."""
    secs = np.arange(60, -1, -1)
    raw = np.piecewise(
        secs.astype(float),
        [secs > 51, (secs <= 51) & (secs > 23), (secs <= 23) & (secs > 10), secs <= 10],
        [-3.0, 2.5, -3.0, 3.2],
    )
    # GCP = running mean from 60s out
    gcp = np.cumsum(raw) / np.arange(1, len(raw) + 1)
    return pd.DataFrame({"secs_left": secs, "raw_t": raw, "gcp_t": gcp})


def selftest() -> None:
    w = _synthetic_example()
    r = evaluate_window(w, "YES", Params(own_min=OWN_MIN))
    assert r is not None
    print({k: (round(v, 2) if isinstance(v, float) else v) for k, v in r.items()})
    assert r["owned"], "earlier ownership should be detected"
    assert r["g1_req_avg"], "running average should still be near the win line at t15"
    assert r["g2_climb"] and r["g3_range"]
    assert r["passed"] and r["side_won"], "example shape should be a passing, winning candidate"
    n = evaluate_window(w, "NO", Params())
    assert n is not None and not n["passed"] and not n["g0_losing"], "NO is ahead at t15, must not pass"
    assert abs(required_future_avg(-0.55, 15) - (-0.35)) < 1e-9
    print("selftest ok")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", action="append", default=[], help="ASSET=path (repeatable)")
    ap.add_argument("--tick", action="append", default=[], help="ASSET=tick_size_dollars (repeatable)")
    ap.add_argument("--tape", help="CSV with expiry, secs_left, yes_cents[, volume]")
    ap.add_argument("--start", default="2026-09-01")
    ap.add_argument("--t-eval", type=int, default=T_EVAL)
    ap.add_argument("--climb-max", type=float, default=CLIMB_MAX)
    ap.add_argument("--no-own-gate", action="store_true")
    ap.add_argument("--out", default="chop_cwst_results.csv")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()

    if a.selftest:
        selftest()
        return

    ticks = dict(kv.split("=") for kv in a.tick)
    feeds = {}
    for kv in a.feed:
        asset, path = kv.split("=", 1)
        if asset not in ticks:
            raise SystemExit(f"--tick {asset}=<dollars> is required")
        feeds[asset] = (path, float(ticks[asset]))
    if not feeds:
        raise SystemExit("pass at least one --feed ASSET=path")

    p = Params(t_eval=a.t_eval, climb_max=a.climb_max, own_min=None if a.no_own_gate else OWN_MIN)
    res = run(feeds, load_tape(a.tape), p, a.start)
    res.to_csv(a.out, index=False)
    report(res)
    print(f"\nper-window rows written to {a.out}")


if __name__ == "__main__":
    main()
