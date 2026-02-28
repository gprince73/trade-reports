"""Streamlit dashboard for Trade Reports.

Dual-mode:
  - Local: parses raw HTML exports + CSV feeds on the fly
  - Cloud: loads pre-processed parquet files from published_data/
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.io as pio

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import TELEGRAM_EXPORT_ROOT, PUBLISHED_DATA_DIR, IS_CLOUD


st.set_page_config(
    page_title="Trade Reports",
    page_icon="\U0001f4ca",
    layout="wide",
)


# ---- Cloud mode: load from parquet ----

@st.cache_data(ttl=300)
def load_cloud_data():
    """Load pre-processed data from published_data/ parquet files."""
    pub = PUBLISHED_DATA_DIR
    if not (pub / "events.parquet").exists():
        return None, None, None, None, None, None, {}

    df = pd.read_parquet(pub / "events.parquet")
    fills_df = pd.read_parquet(pub / "fills.parquet") if (pub / "fills.parquet").exists() else pd.DataFrame()
    bot_summary = pd.read_parquet(pub / "summary_by_bot.parquet") if (pub / "summary_by_bot.parquet").exists() else pd.DataFrame()
    asset_summary = pd.read_parquet(pub / "summary_by_asset.parquet") if (pub / "summary_by_asset.parquet").exists() else pd.DataFrame()
    penny_df = pd.read_parquet(pub / "penny_trades.parquet") if (pub / "penny_trades.parquet").exists() else pd.DataFrame()

    stats = {}
    if (pub / "stats.json").exists():
        raw = json.loads((pub / "stats.json").read_text())
        # Convert numeric strings back
        for k, v in raw.items():
            try:
                stats[k] = float(v) if "." in str(v) else int(v)
            except (ValueError, TypeError):
                stats[k] = v

    return df, fills_df, bot_summary, asset_summary, penny_df, stats


def load_cloud_charts():
    """Load pre-generated chart JSON files."""
    charts_dir = PUBLISHED_DATA_DIR / "charts"
    if not charts_dir.exists():
        return []
    figs = []
    for f in sorted(charts_dir.glob("*.json")):
        fig = pio.from_json(f.read_text())
        figs.append((f.stem, fig))
    return figs


# ---- Local mode: parse live ----

@st.cache_data(ttl=300)
def load_local_data(export_date: date):
    """Load and parse all data for a given export date."""
    from ingestion.html_parser import HTMLMessageSource, get_export_folder
    from analytics.summary import (
        events_to_dataframe,
        fills_to_dataframe,
        daily_summary_by_bot,
        daily_summary_by_asset,
        penny_trade_summary,
        overall_stats,
    )

    folder = get_export_folder(export_date)
    source = HTMLMessageSource(folder)
    events = source.get_events()
    df = events_to_dataframe(events)
    fills_df = fills_to_dataframe(events)
    bot_summary = daily_summary_by_bot(df)
    asset_summary = daily_summary_by_asset(df)
    penny_df = penny_trade_summary(df)
    stats = overall_stats(df)
    return events, df, fills_df, bot_summary, asset_summary, penny_df, stats


# ---- Shared rendering ----

def render_metrics(stats: dict):
    """Render the top-level KPI row."""
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total Signals", int(stats.get("total_signals", 0)))
    col2.metric("Wins", int(stats.get("total_wins", 0)))
    col3.metric("Losses", int(stats.get("total_losses", 0)))
    col4.metric("Jackpots", int(stats.get("total_jackpots", 0)))
    col5.metric("Win Rate", f"{float(stats.get('win_rate', 0)):.1%}")
    col6.metric("Net P&L", f"${float(stats.get('net_pnl', 0)):+,.2f}")


def render_table(df: pd.DataFrame, fmt_cols: dict | None = None):
    """Render a DataFrame with optional column formatting."""
    if df is None or df.empty:
        st.info("No data available.")
        return
    display = df.copy()
    if fmt_cols:
        for col, fmt_fn in fmt_cols.items():
            if col in display.columns:
                display[col] = display[col].apply(fmt_fn)
    st.dataframe(display, use_container_width=True, hide_index=True)


def main():
    st.title("Trade Reports Dashboard")

    if IS_CLOUD:
        main_cloud()
    else:
        main_local()


def main_cloud():
    """Cloud mode: render from pre-processed parquet data."""
    result = load_cloud_data()
    df, fills_df, bot_summary, asset_summary, penny_df, stats = result

    if df is None or stats is None or not stats:
        st.warning("No published data found. Run `publish.py` locally first.")
        return

    # Load metadata
    meta_path = PUBLISHED_DATA_DIR / "metadata.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        st.sidebar.info(f"Report date: {meta.get('export_date', '?')}")

    render_metrics(stats)
    st.divider()

    tab_bot, tab_asset, tab_penny, tab_charts, tab_log = st.tabs([
        "By Bot", "By Asset", "$0.02 Trades", "Charts", "Signal Log",
    ])

    with tab_bot:
        st.subheader("Performance by Bot")
        render_table(bot_summary, {
            "win_rate": lambda x: f"{x:.1%}",
            "participation_rate": lambda x: f"{x:.1%}",
            "net_pnl": lambda x: f"${x:+,.2f}",
        })

    with tab_asset:
        st.subheader("Performance by Asset")
        render_table(asset_summary, {
            "win_rate": lambda x: f"{x:.1%}",
            "net_pnl": lambda x: f"${x:+,.2f}",
        })

    with tab_penny:
        st.subheader("$0.02 Trade Analysis")
        render_table(penny_df, {
            "total_penny_pnl": lambda x: f"${x:+,.2f}",
            "total_net_pnl": lambda x: f"${x:+,.2f}",
        })

    with tab_charts:
        st.subheader("Contract Charts ($0.02 Trades)")
        charts = load_cloud_charts()
        if charts:
            for name, fig in charts:
                st.plotly_chart(fig, use_container_width=True, key=f"cloud_{name}")
        else:
            st.info("No charts available.")

    with tab_log:
        st.subheader("Full Signal Log")
        if df is not None and not df.empty:
            log_col1, log_col2, log_col3 = st.columns(3)
            type_filter = log_col1.multiselect(
                "Event Type",
                options=sorted(df["event_type"].unique()),
                default=sorted(df["event_type"].unique()),
            )
            bot_filter = log_col2.multiselect(
                "Bot", options=sorted(df["bot_name"].unique()), default=[],
            )
            asset_filter = log_col3.multiselect(
                "Asset ", options=sorted(df["asset"].unique()), default=[],
            )

            filtered = df[df["event_type"].isin(type_filter)]
            if bot_filter:
                filtered = filtered[filtered["bot_name"].isin(bot_filter)]
            if asset_filter:
                filtered = filtered[filtered["asset"].isin(asset_filter)]

            display_cols = [
                "timestamp", "event_type", "bot_name", "asset", "side",
                "tier", "strike", "net_pnl", "contract",
            ]
            existing_cols = [c for c in display_cols if c in filtered.columns]
            st.dataframe(
                filtered[existing_cols].sort_values("timestamp", ascending=False),
                use_container_width=True, hide_index=True,
            )


def main_local():
    """Local mode: parse raw HTML exports on the fly."""
    from ingestion.models import EventType
    from charts.contract_chart import generate_penny_charts

    st.sidebar.header("Settings")

    # Discover available export folders
    available_dates = []
    if TELEGRAM_EXPORT_ROOT.exists():
        for folder in sorted(TELEGRAM_EXPORT_ROOT.iterdir(), reverse=True):
            if folder.is_dir() and folder.name.startswith("ChatExport_"):
                try:
                    d = date.fromisoformat(folder.name.replace("ChatExport_", ""))
                    available_dates.append(d)
                except ValueError:
                    pass

    if not available_dates:
        st.error("No Telegram export folders found.")
        return

    selected_date = st.sidebar.selectbox(
        "Export Date", available_dates,
        format_func=lambda d: d.strftime("%Y-%m-%d (%A)"),
    )

    filter_date = st.sidebar.date_input("Filter to specific day (optional)", value=None)

    try:
        events, df, fills_df, bot_summary, asset_summary, penny_df, stats = load_local_data(selected_date)
    except FileNotFoundError as e:
        st.error(str(e))
        return

    if df.empty:
        st.warning("No trade events found in this export.")
        return

    # Apply date filter
    if filter_date:
        df = df[df["date"] == filter_date]
        events = [e for e in events if e.timestamp.date() == filter_date]
        bot_summary = bot_summary[bot_summary["bot_name"].isin(df["bot_name"].unique())] if not bot_summary.empty else bot_summary
        from analytics.summary import overall_stats as _stats
        stats = _stats(df)

    render_metrics(stats)
    st.divider()

    tab_bot, tab_asset, tab_penny, tab_charts, tab_log = st.tabs([
        "By Bot", "By Asset", "$0.02 Trades", "Charts", "Signal Log",
    ])

    with tab_bot:
        st.subheader("Performance by Bot")
        render_table(bot_summary, {
            "win_rate": lambda x: f"{x:.1%}",
            "participation_rate": lambda x: f"{x:.1%}",
            "net_pnl": lambda x: f"${x:+,.2f}",
        })

    with tab_asset:
        st.subheader("Performance by Asset")
        render_table(asset_summary, {
            "win_rate": lambda x: f"{x:.1%}",
            "net_pnl": lambda x: f"${x:+,.2f}",
        })

    with tab_penny:
        st.subheader("$0.02 Trade Analysis")
        render_table(penny_df, {
            "total_penny_pnl": lambda x: f"${x:+,.2f}",
            "total_net_pnl": lambda x: f"${x:+,.2f}",
        })

    with tab_charts:
        st.subheader("Contract Charts ($0.02 Trades)")
        chart_col1, chart_col2 = st.columns(2)
        asset_filter = chart_col1.multiselect(
            "Asset", options=sorted(df["asset"].unique()),
            default=sorted(df["asset"].unique()),
        )
        outcome_filter = chart_col2.multiselect(
            "Outcome", options=["WIN", "LOSS", "JACKPOT"],
            default=["WIN", "LOSS", "JACKPOT"],
        )

        chart_events = [
            e for e in events
            if e.event_type in (EventType.WIN, EventType.LOSS, EventType.JACKPOT)
            and any(f.price_cents == 2 for f in e.fills)
            and e.asset in asset_filter
            and e.event_type.value in outcome_filter
        ]

        if chart_events:
            with st.spinner(f"Generating {len(chart_events)} charts..."):
                charts = generate_penny_charts(chart_events)
            if charts:
                for event, fig in charts:
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_{event.contract}_{event.timestamp}")
            else:
                st.info("No matching CSV data found for these trades.")
        else:
            st.info("No $0.02 trades match the selected filters.")

    with tab_log:
        st.subheader("Full Signal Log")
        log_col1, log_col2, log_col3 = st.columns(3)
        type_filter = log_col1.multiselect(
            "Event Type", options=sorted(df["event_type"].unique()),
            default=sorted(df["event_type"].unique()),
        )
        bot_filter = log_col2.multiselect(
            "Bot", options=sorted(df["bot_name"].unique()), default=[],
        )
        asset_log_filter = log_col3.multiselect(
            "Asset ", options=sorted(df["asset"].unique()), default=[],
        )

        filtered = df[df["event_type"].isin(type_filter)]
        if bot_filter:
            filtered = filtered[filtered["bot_name"].isin(bot_filter)]
        if asset_log_filter:
            filtered = filtered[filtered["asset"].isin(asset_log_filter)]

        display_cols = [
            "timestamp", "event_type", "bot_name", "asset", "side",
            "tier", "strike", "net_pnl", "contract",
        ]
        existing_cols = [c for c in display_cols if c in filtered.columns]
        st.dataframe(
            filtered[existing_cols].sort_values("timestamp", ascending=False),
            use_container_width=True, hide_index=True,
        )


if __name__ == "__main__":
    main()
