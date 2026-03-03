"""Analytics engine: daily summary reports from parsed trade events."""
from __future__ import annotations

from typing import List

import pandas as pd

from ingestion.models import EventType, TradeEvent


def events_to_dataframe(events: List[TradeEvent]) -> pd.DataFrame:
    """Convert a list of TradeEvent objects to a flat DataFrame."""
    rows = []
    for e in events:
        row = {
            "timestamp": e.timestamp,
            "event_type": e.event_type.value,
            "bot_name": e.bot_name,
            "asset": e.asset,
            "timeframe": e.timeframe,
            "contract": e.contract,
            "contract_expiry": e.contract_expiry,
            "side": e.side.value if e.side else None,
            "tier": e.tier,
            "gap": e.gap,
            "hurdle": e.hurdle,
            "exp_move": e.exp_move,
            "strike": e.strike,
            "net_pnl": e.net_pnl,
            "session_wins": e.session_wins,
            "session_losses": e.session_losses,
            "session_pnl": e.session_pnl,
            "flips": e.flips,
            "num_fills": len(e.fills),
            "has_penny_fill": any(f.price_cents == 2 for f in e.fills),
            "penny_fill_qty": sum(f.quantity for f in e.fills if f.price_cents == 2),
            "penny_fill_pnl": sum(f.pnl for f in e.fills if f.price_cents == 2),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date
    return df


def fills_to_dataframe(events: List[TradeEvent]) -> pd.DataFrame:
    """Flatten all individual fills into a DataFrame."""
    rows = []
    for e in events:
        for f in e.fills:
            rows.append({
                "timestamp": e.timestamp,
                "event_type": e.event_type.value,
                "bot_name": e.bot_name,
                "asset": e.asset,
                "timeframe": e.timeframe,
                "contract": e.contract,
                "contract_expiry": e.contract_expiry,
                "fill_side": f.side.value,
                "fill_qty": f.quantity,
                "fill_price_cents": f.price_cents,
                "fill_pnl": f.pnl,
                "fill_is_win": f.is_win,
            })
    return pd.DataFrame(rows)


def daily_summary_by_bot(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a daily summary grouped by bot_name.

    Columns: bot_name, total_signals, total_wins, total_losses, total_jackpots,
             win_rate, participation_rate, net_pnl
    """
    if df.empty:
        return pd.DataFrame()

    signals = df[df["event_type"] == "SIGNAL"]
    outcomes = df[df["event_type"].isin(["WIN", "LOSS", "JACKPOT"])]

    # Count signals per bot
    sig_counts = signals.groupby("bot_name").size().rename("total_signals")

    # Count outcomes per bot
    wins_mask = outcomes["event_type"].isin(["WIN", "JACKPOT"])
    losses_mask = outcomes["event_type"] == "LOSS"

    wins = outcomes[wins_mask].groupby("bot_name").size().rename("total_wins")
    losses = outcomes[losses_mask].groupby("bot_name").size().rename("total_losses")
    jackpots = outcomes[outcomes["event_type"] == "JACKPOT"].groupby("bot_name").size().rename("total_jackpots")

    # Net P&L per bot
    pnl = outcomes.groupby("bot_name")["net_pnl"].sum().rename("net_pnl")

    # YES/NO side breakdown
    yes_out = outcomes[outcomes["side"] == "YES"]
    no_out = outcomes[outcomes["side"] == "NO"]

    yes_wins = yes_out[yes_out["event_type"].isin(["WIN", "JACKPOT"])].groupby("bot_name").size().rename("yes_wins")
    yes_losses = yes_out[yes_out["event_type"] == "LOSS"].groupby("bot_name").size().rename("yes_losses")
    yes_pnl = yes_out.groupby("bot_name")["net_pnl"].sum().rename("yes_pnl")

    no_wins = no_out[no_out["event_type"].isin(["WIN", "JACKPOT"])].groupby("bot_name").size().rename("no_wins")
    no_losses = no_out[no_out["event_type"] == "LOSS"].groupby("bot_name").size().rename("no_losses")
    no_pnl = no_out.groupby("bot_name")["net_pnl"].sum().rename("no_pnl")

    # Combine
    summary = pd.DataFrame({
        "total_signals": sig_counts,
        "total_wins": wins,
        "total_losses": losses,
        "total_jackpots": jackpots,
        "net_pnl": pnl,
        "yes_wins": yes_wins,
        "yes_losses": yes_losses,
        "yes_pnl": yes_pnl,
        "no_wins": no_wins,
        "no_losses": no_losses,
        "no_pnl": no_pnl,
    }).fillna(0)

    # Derived metrics
    total_outcomes = summary["total_wins"] + summary["total_losses"]
    summary["win_rate"] = (
        summary["total_wins"] / total_outcomes.replace(0, float("nan"))
    ).fillna(0)
    summary["participation_rate"] = (
        total_outcomes / summary["total_signals"].replace(0, float("nan"))
    ).fillna(0)

    # YES/NO win rates
    yes_total = summary["yes_wins"] + summary["yes_losses"]
    summary["yes_win_rate"] = (summary["yes_wins"] / yes_total.replace(0, float("nan"))).fillna(0)
    no_total = summary["no_wins"] + summary["no_losses"]
    summary["no_win_rate"] = (summary["no_wins"] / no_total.replace(0, float("nan"))).fillna(0)

    summary = summary.reset_index()
    summary = summary.sort_values("net_pnl", ascending=False)
    return summary


def daily_summary_by_asset(df: pd.DataFrame) -> pd.DataFrame:
    """Summary grouped by asset."""
    if df.empty:
        return pd.DataFrame()

    outcomes = df[df["event_type"].isin(["WIN", "LOSS", "JACKPOT"])]

    wins = outcomes[outcomes["event_type"].isin(["WIN", "JACKPOT"])].groupby("asset").size().rename("total_wins")
    losses = outcomes[outcomes["event_type"] == "LOSS"].groupby("asset").size().rename("total_losses")
    pnl = outcomes.groupby("asset")["net_pnl"].sum().rename("net_pnl")

    # YES/NO side breakdown
    yes_out = outcomes[outcomes["side"] == "YES"]
    no_out = outcomes[outcomes["side"] == "NO"]

    yes_wins = yes_out[yes_out["event_type"].isin(["WIN", "JACKPOT"])].groupby("asset").size().rename("yes_wins")
    yes_losses = yes_out[yes_out["event_type"] == "LOSS"].groupby("asset").size().rename("yes_losses")
    yes_pnl = yes_out.groupby("asset")["net_pnl"].sum().rename("yes_pnl")

    no_wins = no_out[no_out["event_type"].isin(["WIN", "JACKPOT"])].groupby("asset").size().rename("no_wins")
    no_losses = no_out[no_out["event_type"] == "LOSS"].groupby("asset").size().rename("no_losses")
    no_pnl = no_out.groupby("asset")["net_pnl"].sum().rename("no_pnl")

    summary = pd.DataFrame({
        "total_wins": wins,
        "total_losses": losses,
        "net_pnl": pnl,
        "yes_wins": yes_wins,
        "yes_losses": yes_losses,
        "yes_pnl": yes_pnl,
        "no_wins": no_wins,
        "no_losses": no_losses,
        "no_pnl": no_pnl,
    }).fillna(0)

    total = summary["total_wins"] + summary["total_losses"]
    summary["win_rate"] = (summary["total_wins"] / total.replace(0, float("nan"))).fillna(0)

    # YES/NO win rates
    yes_total = summary["yes_wins"] + summary["yes_losses"]
    summary["yes_win_rate"] = (summary["yes_wins"] / yes_total.replace(0, float("nan"))).fillna(0)
    no_total = summary["no_wins"] + summary["no_losses"]
    summary["no_win_rate"] = (summary["no_wins"] / no_total.replace(0, float("nan"))).fillna(0)

    return summary.reset_index().sort_values("net_pnl", ascending=False)


def penny_trade_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summary of $0.02 trades (fills at 2¢)."""
    if df.empty:
        return pd.DataFrame()

    penny = df[df["has_penny_fill"] == True].copy()
    if penny.empty:
        return pd.DataFrame()

    grouped = penny.groupby(["bot_name", "asset", "event_type"]).agg(
        count=("contract", "size"),
        total_penny_qty=("penny_fill_qty", "sum"),
        total_penny_pnl=("penny_fill_pnl", "sum"),
        total_net_pnl=("net_pnl", "sum"),
    ).reset_index()

    return grouped.sort_values("total_penny_pnl", ascending=False)


def results_by_price(fills_df: pd.DataFrame) -> pd.DataFrame:
    """Summary of trade results grouped by fill price tier.

    Returns: DataFrame with columns:
        fill_price_cents, bot_name, total_trades, wins, losses, win_rate, total_pnl
    """
    if fills_df.empty:
        return pd.DataFrame()

    # Only outcome fills (from WIN/LOSS/JACKPOT events)
    outcomes = fills_df[fills_df["event_type"].isin(["WIN", "LOSS", "JACKPOT"])].copy()
    if outcomes.empty:
        return pd.DataFrame()

    grouped = outcomes.groupby(["fill_price_cents", "bot_name"]).agg(
        total_fills=("fill_qty", "size"),
        total_qty=("fill_qty", "sum"),
        wins=("fill_is_win", "sum"),
        total_pnl=("fill_pnl", "sum"),
    ).reset_index()

    grouped["losses"] = grouped["total_fills"] - grouped["wins"]
    grouped["win_rate"] = (
        grouped["wins"] / grouped["total_fills"].replace(0, float("nan"))
    ).fillna(0)

    # Format price label
    grouped["price_label"] = grouped["fill_price_cents"].apply(
        lambda c: f"${c / 100:.2f}"
    )

    return grouped.sort_values(
        ["fill_price_cents", "total_pnl"], ascending=[True, False]
    )


# ---------------------------------------------------------------------------
# Gap Analysis
# ---------------------------------------------------------------------------

ASSET_GAP_BUCKETS: dict[str, dict] = {
    "BTC": {
        "edges":  [0, 10, 20, 30, 50, 100, float("inf")],
        "labels": ["$0-10", "$10-20", "$20-30", "$30-50", "$50-100", ">$100"],
    },
    "ETH": {
        "edges":  [0, 0.5, 1.0, 2, 3, 5, 10, float("inf")],
        "labels": ["$0-0.50", "$0.50-1", "$1-2", "$2-3", "$3-5", "$5-10", ">$10"],
    },
    "SOL": {
        "edges":  [0, 0.025, 0.05, 0.10, 0.20, 0.50, float("inf")],
        "labels": ["<2.5\u00a2", "2.5-5\u00a2", "5-10\u00a2", "10-20\u00a2", "20-50\u00a2", ">50\u00a2"],
    },
    "XRP": {
        "edges":  [0, 0.001, 0.002, 0.004, 0.0065, 0.01, 0.02, float("inf")],
        "labels": ["<0.1\u00a2", "0.1-0.2\u00a2", "0.2-0.4\u00a2", "0.4-0.65\u00a2",
                    "0.65-1\u00a2", "1-2\u00a2", ">2\u00a2"],
    },
    # Fallback for any new asset
    "_DEFAULT": {
        "edges":  [0, 1, 5, 10, 25, 50, 100, float("inf")],
        "labels": ["$0-1", "$1-5", "$5-10", "$10-25", "$25-50", "$50-100", ">$100"],
    },
}


def _get_gap_buckets(asset: str) -> tuple:
    """Return (edges, labels) for a given asset, falling back to _DEFAULT."""
    cfg = ASSET_GAP_BUCKETS.get(asset, ASSET_GAP_BUCKETS["_DEFAULT"])
    return cfg["edges"], cfg["labels"]


def gap_analysis_tables(df: pd.DataFrame) -> dict:
    """Analyse trade outcomes by signal gap bucket, per asset.

    Gap is only present on SIGNAL events, so we JOIN signals → outcomes
    on (contract, bot_name) to get the gap context for each outcome.

    Returns dict keyed by asset name. Each value is a tuple of
    (combined_df, wins_dist_df, losses_dist_df) using that asset's
    scaled gap buckets.
    """
    if df.empty:
        return {}

    # --- 1. Separate signals (with gap) and outcomes ---
    signals = df[
        (df["event_type"] == "SIGNAL") & df["gap"].notna() & df["contract"].notna()
    ].copy()
    outcomes = df[
        df["event_type"].isin(["WIN", "LOSS", "JACKPOT"]) & df["contract"].notna()
    ].copy()

    if signals.empty:
        return {}

    # Deduplicate: keep last signal per (contract, bot_name) to handle FLIP SIGNALs
    signals = signals.sort_values("timestamp")
    signals = signals.drop_duplicates(subset=["contract", "bot_name"], keep="last")

    # Deduplicate outcomes too (rare but possible)
    outcomes = outcomes.sort_values("timestamp")
    outcomes = outcomes.drop_duplicates(subset=["contract", "bot_name"], keep="last")

    # --- 2. Left-join signals → outcomes ---
    merged = signals.merge(
        outcomes[["contract", "bot_name", "event_type", "net_pnl"]],
        on=["contract", "bot_name"],
        how="left",
        suffixes=("", "_outcome"),
    )

    # Shared derived columns
    merged["abs_gap"] = merged["gap"].abs()
    merged["is_win"] = merged["event_type_outcome"].isin(["WIN", "JACKPOT"])
    merged["is_loss"] = merged["event_type_outcome"] == "LOSS"
    merged["has_outcome"] = merged["event_type_outcome"].notna()

    # --- 3. Per-asset bucketing and aggregation ---
    def _build_tables(group: pd.DataFrame, asset: str):
        edges, labels = _get_gap_buckets(asset)
        g = group.copy()
        g["gap_bucket"] = pd.cut(
            g["abs_gap"], bins=edges, labels=labels, right=False,
        )

        # Combined table
        combined = g.groupby("gap_bucket", observed=True).agg(
            total_signals=("gap", "size"),
            participated=("has_outcome", "sum"),
            wins=("is_win", "sum"),
            losses=("is_loss", "sum"),
            net_pnl=("net_pnl_outcome", "sum"),
            avg_gap=("abs_gap", "mean"),
        ).reset_index()

        total_sigs = combined["total_signals"].sum()
        combined["pct_of_signals"] = combined["total_signals"] / max(total_sigs, 1)
        combined["participation_rate"] = (
            combined["participated"] / combined["total_signals"].replace(0, float("nan"))
        ).fillna(0)
        decided = combined["wins"] + combined["losses"]
        combined["win_rate"] = (
            combined["wins"] / decided.replace(0, float("nan"))
        ).fillna(0)

        combined = combined[
            ["gap_bucket", "total_signals", "pct_of_signals", "participated",
             "participation_rate", "wins", "losses", "win_rate", "avg_gap", "net_pnl"]
        ]

        # Win / Loss distribution tables
        def _distribution(mask_col: str) -> pd.DataFrame:
            subset = g[g[mask_col]]
            if subset.empty:
                return pd.DataFrame(columns=["gap_bucket", "count", "pct", "bar"])
            dist = subset.groupby("gap_bucket", observed=True).size().reset_index(name="count")
            total = dist["count"].sum()
            dist["pct"] = dist["count"] / max(total, 1)
            max_pct = dist["pct"].max() if not dist.empty else 1
            dist["bar"] = dist["pct"].apply(
                lambda x: "\u2588" * max(1, int(x / max(max_pct, 0.01) * 20))
            )
            return dist

        return combined, _distribution("is_win"), _distribution("is_loss")

    results: dict = {}
    for asset, group in merged.groupby("asset"):
        if asset == "UNKNOWN":
            continue
        combined, wins_dist, losses_dist = _build_tables(group, asset)
        if not combined.empty:
            results[asset] = (combined, wins_dist, losses_dist)

    return results


def overall_stats(df: pd.DataFrame) -> dict:
    """Compute top-level stats for the dashboard header."""
    if df.empty:
        return {}

    outcomes = df[df["event_type"].isin(["WIN", "LOSS", "JACKPOT"])]
    signals = df[df["event_type"] == "SIGNAL"]

    total_wins = len(outcomes[outcomes["event_type"].isin(["WIN", "JACKPOT"])])
    total_losses = len(outcomes[outcomes["event_type"] == "LOSS"])
    total_jackpots = len(outcomes[outcomes["event_type"] == "JACKPOT"])

    return {
        "total_signals": len(signals),
        "total_outcomes": len(outcomes),
        "total_wins": total_wins,
        "total_losses": total_losses,
        "total_jackpots": total_jackpots,
        "win_rate": total_wins / max(total_wins + total_losses, 1),
        "net_pnl": outcomes["net_pnl"].sum() if not outcomes.empty else 0,
        "unique_bots": df["bot_name"].nunique(),
        "unique_assets": df["asset"].nunique(),
        "date_range": f"{df['timestamp'].min()} - {df['timestamp'].max()}" if not df.empty else "",
    }
