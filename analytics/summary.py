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
    wins = outcomes[outcomes["event_type"].isin(["WIN", "JACKPOT"])].groupby("bot_name").size().rename("total_wins")
    losses = outcomes[outcomes["event_type"] == "LOSS"].groupby("bot_name").size().rename("total_losses")
    jackpots = outcomes[outcomes["event_type"] == "JACKPOT"].groupby("bot_name").size().rename("total_jackpots")

    # Net P&L per bot
    pnl = outcomes.groupby("bot_name")["net_pnl"].sum().rename("net_pnl")

    # Combine
    summary = pd.DataFrame({
        "total_signals": sig_counts,
        "total_wins": wins,
        "total_losses": losses,
        "total_jackpots": jackpots,
        "net_pnl": pnl,
    }).fillna(0)

    # Derived metrics
    total_outcomes = summary["total_wins"] + summary["total_losses"]
    summary["win_rate"] = (
        summary["total_wins"] / total_outcomes.replace(0, float("nan"))
    ).fillna(0)
    summary["participation_rate"] = (
        total_outcomes / summary["total_signals"].replace(0, float("nan"))
    ).fillna(0)

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

    summary = pd.DataFrame({
        "total_wins": wins,
        "total_losses": losses,
        "net_pnl": pnl,
    }).fillna(0)

    total = summary["total_wins"] + summary["total_losses"]
    summary["win_rate"] = (summary["total_wins"] / total.replace(0, float("nan"))).fillna(0)

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
