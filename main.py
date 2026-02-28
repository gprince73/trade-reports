"""Entry point: run analytics and optionally launch dashboard."""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from config import OUTPUT_DIR
from ingestion.html_parser import HTMLMessageSource, get_export_folder
from analytics.summary import (
    events_to_dataframe,
    daily_summary_by_bot,
    daily_summary_by_asset,
    penny_trade_summary,
    overall_stats,
)
from charts.contract_chart import generate_penny_charts


def run_report(export_date: date, save_charts: bool = True):
    """Run the full analytics pipeline and print summary."""
    # 1. Parse HTML
    folder = get_export_folder(export_date)
    print(f"Loading export from: {folder}")
    source = HTMLMessageSource(folder)
    events = source.get_events()
    print(f"Parsed {len(events)} trade events")

    # 2. Build DataFrames
    df = events_to_dataframe(events)
    if df.empty:
        print("No events found.")
        return

    # 3. Summary stats
    stats = overall_stats(df)
    print("\n=== OVERALL STATS ===")
    print(f"  Signals:  {stats['total_signals']}")
    print(f"  Wins:     {stats['total_wins']}")
    print(f"  Losses:   {stats['total_losses']}")
    print(f"  Jackpots: {stats['total_jackpots']}")
    print(f"  Win Rate: {stats['win_rate']:.1%}")
    print(f"  Net P&L:  ${stats['net_pnl']:+,.2f}")
    print(f"  Bots:     {stats['unique_bots']}")

    # 4. Bot summary
    bot_df = daily_summary_by_bot(df)
    print("\n=== BY BOT ===")
    print(bot_df.to_string(index=False))

    # 5. Asset summary
    asset_df = daily_summary_by_asset(df)
    print("\n=== BY ASSET ===")
    print(asset_df.to_string(index=False))

    # 6. Penny trade summary
    penny_df = penny_trade_summary(df)
    if not penny_df.empty:
        print("\n=== $0.02 TRADES ===")
        print(penny_df.to_string(index=False))

    # 7. Generate charts
    if save_charts:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        print(f"\nGenerating charts (saving to {OUTPUT_DIR})...")
        charts = generate_penny_charts(events)
        for event, fig in charts:
            safe_name = f"{event.contract}_{event.event_type.value}".replace("-", "_")
            outpath = OUTPUT_DIR / f"{safe_name}.html"
            fig.write_html(str(outpath))
        print(f"  Saved {len(charts)} charts")

    return df, events


def main():
    parser = argparse.ArgumentParser(description="Trade Reports CLI")
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Export date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--no-charts",
        action="store_true",
        help="Skip chart generation.",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch Streamlit dashboard instead of CLI report.",
    )
    args = parser.parse_args()

    if args.dashboard:
        import subprocess
        dashboard_path = Path(__file__).parent / "app" / "dashboard.py"
        subprocess.run([sys.executable, "-m", "streamlit", "run", str(dashboard_path)])
        return

    export_date = date.fromisoformat(args.date)
    run_report(export_date, save_charts=not args.no_charts)


if __name__ == "__main__":
    main()
