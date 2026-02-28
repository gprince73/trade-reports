"""Generate 90-second Plotly charts for individual contracts."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import pandas as pd

from data.csv_loader import get_contract_window, get_settlement_strike, load_feed
from ingestion.models import EventType, TradeEvent


def build_contract_chart(
    event: TradeEvent,
    feed_df: pd.DataFrame,
    settlement_price: Optional[float] = None,
) -> Optional[go.Figure]:
    """Build a 90-second chart for a trade event.

    Plots Strike (horizontal), PriceProxy, and GrowingCP over the last 90
    seconds of the contract.

    Args:
        event: The WIN/LOSS/JACKPOT TradeEvent with contract info.
        feed_df: The full data feed DataFrame for the asset.
        settlement_price: Override settlement price. If None, auto-detected.

    Returns:
        A Plotly Figure, or None if insufficient data.
    """
    if event.contract_expiry is None:
        return None

    # Get the 90-second window
    window = get_contract_window(feed_df, event.contract_expiry)
    if window.empty or len(window) < 3:
        return None

    # Determine strike price (from Telegram signal, since CSV may have N/A)
    strike = event.strike
    if strike is None:
        # Fall back to CSV strike if available
        valid_strikes = window.dropna(subset=["Strike"])
        if not valid_strikes.empty:
            strike = float(valid_strikes.iloc[0]["Strike"])

    # Settlement price
    if settlement_price is None:
        settlement_price = get_settlement_strike(feed_df, event.contract_expiry)

    # Build figure
    fig = go.Figure()

    # PriceProxy line
    fig.add_trace(go.Scatter(
        x=window["datetime"],
        y=window["PriceProxy"],
        mode="lines",
        name="PriceProxy",
        line=dict(color="#2196F3", width=2),
    ))

    # GrowingCP line
    fig.add_trace(go.Scatter(
        x=window["datetime"],
        y=window["GrowingCP"],
        mode="lines",
        name="GrowingCP",
        line=dict(color="#FF9800", width=2, dash="dot"),
    ))

    # Strike horizontal line
    if strike is not None:
        fig.add_hline(
            y=strike,
            line_dash="dash",
            line_color="#E91E63",
            line_width=2,
            annotation_text=f"Strike: ${strike:,.2f}",
            annotation_position="top left",
        )

    # Settlement price horizontal line
    if settlement_price is not None:
        fig.add_hline(
            y=settlement_price,
            line_dash="dot",
            line_color="#4CAF50",
            line_width=1.5,
            annotation_text=f"Settlement: ${settlement_price:,.2f}",
            annotation_position="bottom left",
        )

    # Expiry vertical line (use add_shape to avoid Plotly annotation bug with datetime)
    fig.add_shape(
        type="line",
        x0=event.contract_expiry, x1=event.contract_expiry,
        y0=0, y1=1, yref="paper",
        line=dict(dash="dash", color="gray", width=1),
    )
    fig.add_annotation(
        x=event.contract_expiry, y=1, yref="paper",
        text="Expiry", showarrow=False,
        font=dict(size=10, color="gray"),
        yshift=10,
    )

    # Outcome indicator
    outcome_color = {
        EventType.WIN: "#4CAF50",
        EventType.JACKPOT: "#FFD700",
        EventType.LOSS: "#F44336",
    }
    outcome_emoji = {
        EventType.WIN: "WIN",
        EventType.JACKPOT: "JACKPOT",
        EventType.LOSS: "LOSS",
    }

    # Build fill summary text
    fill_lines = []
    for f in event.fills:
        symbol = "+" if f.is_win else "-"
        fill_lines.append(f"{f.side.value} {f.quantity}@{f.price_cents}c = ${f.pnl:+.2f}")
    fill_text = "<br>".join(fill_lines) if fill_lines else ""

    # Title
    side_str = event.side.value if event.side else "?"
    title = (
        f"{outcome_emoji.get(event.event_type, event.event_type.value)} | "
        f"{event.bot_name} {event.asset} | "
        f"Side: {side_str} | "
        f"Net: ${event.net_pnl:+.2f}" if event.net_pnl else
        f"{event.bot_name} {event.asset}"
    )

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=14, color=outcome_color.get(event.event_type, "white")),
        ),
        xaxis_title="Time",
        yaxis_title=f"Price ({event.asset})",
        template="plotly_dark",
        height=450,
        margin=dict(l=60, r=30, t=60, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    # Add annotation with fill details
    if fill_text:
        fig.add_annotation(
            xref="paper", yref="paper",
            x=0.02, y=0.02,
            text=fill_text,
            showarrow=False,
            font=dict(size=10, color="white"),
            bgcolor="rgba(0,0,0,0.6)",
            bordercolor="gray",
            borderwidth=1,
            align="left",
        )

    return fig


def generate_penny_charts(
    events: list[TradeEvent],
    feed_cache: Optional[dict[str, pd.DataFrame]] = None,
) -> list[tuple[TradeEvent, go.Figure]]:
    """Generate charts for all trades with $0.02 fills.

    Args:
        events: All parsed trade events.
        feed_cache: Optional pre-loaded {asset: DataFrame} cache.

    Returns:
        List of (event, figure) tuples.
    """
    if feed_cache is None:
        feed_cache = {}

    charts = []
    for event in events:
        # Only chart WIN/LOSS/JACKPOT with penny fills
        if event.event_type not in (EventType.WIN, EventType.LOSS, EventType.JACKPOT):
            continue
        if not any(f.price_cents == 2 for f in event.fills):
            continue
        if event.asset == "UNKNOWN" or event.timeframe == "1HR":
            continue

        # Load feed data (cached)
        if event.asset not in feed_cache:
            try:
                feed_cache[event.asset] = load_feed(event.asset)
            except (FileNotFoundError, ValueError):
                continue

        fig = build_contract_chart(event, feed_cache[event.asset])
        if fig is not None:
            charts.append((event, fig))

    return charts
