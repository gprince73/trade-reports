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
        return None, None, {}

    df = pd.read_parquet(pub / "events.parquet")
    fills_df = pd.read_parquet(pub / "fills.parquet") if (pub / "fills.parquet").exists() else pd.DataFrame()

    stats = {}
    if (pub / "stats.json").exists():
        raw = json.loads((pub / "stats.json").read_text())
        for k, v in raw.items():
            try:
                stats[k] = float(v) if "." in str(v) else int(v)
            except (ValueError, TypeError):
                stats[k] = v

    return df, fills_df, stats


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
    from analytics.summary import events_to_dataframe, fills_to_dataframe

    folder = get_export_folder(export_date)
    source = HTMLMessageSource(folder)
    events = source.get_events()
    df = events_to_dataframe(events)
    fills_df = fills_to_dataframe(events)
    return events, df, fills_df


# ---- Shared helpers ----

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


def apply_date_range(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Filter DataFrame to a date range."""
    if df is None or df.empty:
        return df
    mask = (pd.to_datetime(df["timestamp"]).dt.date >= start) & (pd.to_datetime(df["timestamp"]).dt.date <= end)
    return df[mask].copy()


def apply_date_range_fills(fills_df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Filter fills DataFrame to a date range."""
    if fills_df is None or fills_df.empty:
        return fills_df
    mask = (pd.to_datetime(fills_df["timestamp"]).dt.date >= start) & (pd.to_datetime(fills_df["timestamp"]).dt.date <= end)
    return fills_df[mask].copy()


def bot_multiselect(df: pd.DataFrame, key: str) -> list[str]:
    """Render a bot filter multiselect. Returns selected bot names (empty = all)."""
    all_bots = sorted(df["bot_name"].unique())
    return st.multiselect("Filter by Bot", options=all_bots, default=[], key=key)


def filter_by_bots(df: pd.DataFrame, bots: list[str]) -> pd.DataFrame:
    """Apply bot filter. Empty list = no filter (all bots)."""
    if not bots:
        return df
    return df[df["bot_name"].isin(bots)]


# ---- Tab renderers ----

def tab_by_bot(df: pd.DataFrame):
    from analytics.summary import daily_summary_by_bot
    st.subheader("Performance by Bot")
    bots = bot_multiselect(df, key="bot_filter_bybot")
    filtered = filter_by_bots(df, bots)
    bot_summary = daily_summary_by_bot(filtered)
    render_table(bot_summary, {
        "win_rate": lambda x: f"{x:.1%}",
        "participation_rate": lambda x: f"{x:.1%}",
        "net_pnl": lambda x: f"${x:+,.2f}",
    })


def tab_by_asset(df: pd.DataFrame):
    from analytics.summary import daily_summary_by_asset
    st.subheader("Performance by Asset")
    bots = bot_multiselect(df, key="bot_filter_byasset")
    filtered = filter_by_bots(df, bots)
    asset_summary = daily_summary_by_asset(filtered)
    render_table(asset_summary, {
        "win_rate": lambda x: f"{x:.1%}",
        "net_pnl": lambda x: f"${x:+,.2f}",
    })


def tab_by_price(df: pd.DataFrame, fills_df: pd.DataFrame):
    from analytics.summary import results_by_price
    st.subheader("Results by Price")
    bots = bot_multiselect(df, key="bot_filter_byprice")

    if fills_df is None or fills_df.empty:
        st.info("No fill data available.")
        return

    filtered_fills = filter_by_bots(fills_df, bots)
    price_df = results_by_price(filtered_fills)

    if price_df.empty:
        st.info("No results data available.")
        return

    # Get unique prices for filtering
    all_prices = sorted(price_df["fill_price_cents"].unique())
    price_labels = {c: f"${c/100:.2f}" for c in all_prices}
    selected_prices = st.multiselect(
        "Filter by Price",
        options=all_prices,
        default=all_prices,
        format_func=lambda c: price_labels[c],
        key="price_filter",
    )
    if selected_prices:
        price_df = price_df[price_df["fill_price_cents"].isin(selected_prices)]

    render_table(price_df[["price_label", "bot_name", "total_fills", "total_qty", "wins", "losses", "win_rate", "total_pnl"]], {
        "win_rate": lambda x: f"{x:.1%}",
        "total_pnl": lambda x: f"${x:+,.2f}",
    })


def tab_penny_trades(df: pd.DataFrame):
    from analytics.summary import penny_trade_summary
    st.subheader("$0.02 Trade Analysis")
    bots = bot_multiselect(df, key="bot_filter_penny")
    filtered = filter_by_bots(df, bots)
    penny = penny_trade_summary(filtered)
    render_table(penny, {
        "total_penny_pnl": lambda x: f"${x:+,.2f}",
        "total_net_pnl": lambda x: f"${x:+,.2f}",
    })


def tab_charts_cloud():
    st.subheader("Contract Charts ($0.02 Trades)")
    charts = load_cloud_charts()
    if charts:
        for name, fig in charts:
            st.plotly_chart(fig, use_container_width=True, key=f"cloud_{name}")
    else:
        st.info("No charts available.")


def tab_charts_local(events: list, df: pd.DataFrame):
    from ingestion.models import EventType
    from charts.contract_chart import generate_penny_charts

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


def tab_signal_log(df: pd.DataFrame):
    st.subheader("Full Signal Log")
    log_col1, log_col2, log_col3 = st.columns(3)
    type_filter = log_col1.multiselect(
        "Event Type", options=sorted(df["event_type"].unique()),
        default=sorted(df["event_type"].unique()),
    )
    bot_filter = log_col2.multiselect(
        "Bot", options=sorted(df["bot_name"].unique()), default=[],
        key="bot_filter_log",
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


# ---- Main entry ----

def main():
    st.title("Trade Reports Dashboard")
    if IS_CLOUD:
        main_cloud()
    else:
        main_local()


def main_cloud():
    """Cloud mode: render from pre-processed parquet data."""
    from analytics.summary import overall_stats

    result = load_cloud_data()
    df, fills_df, _ = result

    if df is None or df.empty:
        st.warning("No published data found. Run `publish.py` locally first.")
        return

    # Sidebar: date range
    st.sidebar.header("Settings")
    meta_path = PUBLISHED_DATA_DIR / "metadata.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        st.sidebar.info(f"Export date: {meta.get('export_date', '?')}")

    all_dates = sorted(pd.to_datetime(df["timestamp"]).dt.date.unique())
    if len(all_dates) > 1:
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(all_dates[0], all_dates[-1]),
            min_value=all_dates[0],
            max_value=all_dates[-1],
            key="cloud_date_range",
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            df = apply_date_range(df, date_range[0], date_range[1])
            fills_df = apply_date_range_fills(fills_df, date_range[0], date_range[1])

    stats = overall_stats(df)
    render_metrics(stats)
    st.divider()

    tab_bot, tab_asset, tab_price, tab_penny, tab_charts, tab_log = st.tabs([
        "By Bot", "By Asset", "By Price", "$0.02 Trades", "Charts", "Signal Log",
    ])

    with tab_bot:
        tab_by_bot(df)
    with tab_asset:
        tab_by_asset(df)
    with tab_price:
        tab_by_price(df, fills_df)
    with tab_penny:
        tab_penny_trades(df)
    with tab_charts:
        tab_charts_cloud()
    with tab_log:
        tab_signal_log(df)


def main_local():
    """Local mode: parse raw HTML exports on the fly."""
    from analytics.summary import overall_stats

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

    try:
        events, df, fills_df = load_local_data(selected_date)
    except FileNotFoundError as e:
        st.error(str(e))
        return

    if df.empty:
        st.warning("No trade events found in this export.")
        return

    # Date range filter
    all_dates = sorted(pd.to_datetime(df["timestamp"]).dt.date.unique())
    if len(all_dates) > 1:
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(all_dates[0], all_dates[-1]),
            min_value=all_dates[0],
            max_value=all_dates[-1],
            key="local_date_range",
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            df = apply_date_range(df, date_range[0], date_range[1])
            fills_df = apply_date_range_fills(fills_df, date_range[0], date_range[1])
            events = [e for e in events if date_range[0] <= e.timestamp.date() <= date_range[1]]

    stats = overall_stats(df)
    render_metrics(stats)
    st.divider()

    tab_bot, tab_asset, tab_price, tab_penny, tab_charts, tab_log = st.tabs([
        "By Bot", "By Asset", "By Price", "$0.02 Trades", "Charts", "Signal Log",
    ])

    with tab_bot:
        tab_by_bot(df)
    with tab_asset:
        tab_by_asset(df)
    with tab_price:
        tab_by_price(df, fills_df)
    with tab_penny:
        tab_penny_trades(df)
    with tab_charts:
        tab_charts_local(events, df)
    with tab_log:
        tab_signal_log(df)


if __name__ == "__main__":
    main()
