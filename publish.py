"""Publish trade reports: process data, export for cloud, git push, notify Telegram."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

from config import PUBLISHED_DATA_DIR, PROJECT_ROOT
from ingestion.html_parser import HTMLMessageSource, get_export_folder
from ingestion.models import EventType
from analytics.summary import (
    events_to_dataframe,
    fills_to_dataframe,
    daily_summary_by_bot,
    daily_summary_by_asset,
    penny_trade_summary,
    overall_stats,
)
from charts.contract_chart import generate_penny_charts


def export_data(export_date: date) -> dict:
    """Run full pipeline and export processed data for Streamlit Cloud."""
    # 1. Parse
    folder = get_export_folder(export_date)
    print(f"Parsing: {folder}")
    source = HTMLMessageSource(folder)
    events = source.get_events()
    print(f"  {len(events)} events parsed")

    # 2. Build DataFrames
    df = events_to_dataframe(events)
    fills_df = fills_to_dataframe(events)
    stats = overall_stats(df)

    if df.empty:
        print("  No events found. Aborting.")
        return stats

    # 3. Export to published_data/
    out = PUBLISHED_DATA_DIR
    out.mkdir(parents=True, exist_ok=True)

    # Save DataFrames as parquet
    df.to_parquet(out / "events.parquet", index=False)
    fills_df.to_parquet(out / "fills.parquet", index=False)
    print(f"  Exported events.parquet ({len(df)} rows)")
    print(f"  Exported fills.parquet ({len(fills_df)} rows)")

    # Save summary tables
    daily_summary_by_bot(df).to_parquet(out / "summary_by_bot.parquet", index=False)
    daily_summary_by_asset(df).to_parquet(out / "summary_by_asset.parquet", index=False)
    penny_df = penny_trade_summary(df)
    if not penny_df.empty:
        penny_df.to_parquet(out / "penny_trades.parquet", index=False)

    # Save overall stats as JSON
    json_stats = {k: (str(v) if not isinstance(v, (int, float)) else v) for k, v in stats.items()}
    (out / "stats.json").write_text(json.dumps(json_stats, indent=2))

    # 4. Generate charts and save as JSON
    charts_dir = out / "charts"
    charts_dir.mkdir(exist_ok=True)

    # Only generate for dates where CSV data likely exists (last 3 days)
    late_events = [e for e in events if e.timestamp.date() >= date(2026, 2, 25)]
    charts = generate_penny_charts(late_events)
    for event, fig in charts:
        safe_name = f"{event.contract}_{event.event_type.value}".replace("-", "_")
        fig.write_json(str(charts_dir / f"{safe_name}.json"))
    print(f"  Exported {len(charts)} chart JSON files")

    # Save metadata
    meta = {
        "export_date": export_date.isoformat(),
        "total_events": len(events),
        "chart_count": len(charts),
    }
    (out / "metadata.json").write_text(json.dumps(meta, indent=2))

    return stats


def git_push(date_str: str) -> bool:
    """Stage published_data, commit, and push."""
    try:
        subprocess.run(
            ["git", "add", "published_data/", ".streamlit/"],
            cwd=str(PROJECT_ROOT), check=True,
        )
        subprocess.run(
            ["git", "commit", "-m", f"Update report: {date_str}"],
            cwd=str(PROJECT_ROOT), check=True,
        )
        subprocess.run(
            ["git", "push"],
            cwd=str(PROJECT_ROOT), check=True,
        )
        print("  Git push successful")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Git push failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Publish Trade Reports")
    parser.add_argument(
        "--date", type=str, default=date.today().isoformat(),
        help="Export date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--no-push", action="store_true",
        help="Skip git push (export only).",
    )
    parser.add_argument(
        "--no-notify", action="store_true",
        help="Skip Telegram notification.",
    )
    args = parser.parse_args()

    export_date = date.fromisoformat(args.date)
    date_str = export_date.isoformat()

    # Step 1: Export
    print(f"\n=== Publishing report for {date_str} ===\n")
    stats = export_data(export_date)

    if not stats:
        return

    # Step 2: Git push
    if not args.no_push:
        print("\nPushing to GitHub...")
        git_push(date_str)
    else:
        print("\nSkipping git push (--no-push)")

    # Step 3: Telegram notification
    if not args.no_notify:
        print("\nSending Telegram notification...")
        from notifications.telegram_bot import send_report_notification
        sent = send_report_notification(stats, date_str)
        if sent:
            print("  Telegram message sent!")
        else:
            print("  Telegram notification skipped or failed.")
    else:
        print("\nSkipping notification (--no-notify)")

    print(f"\nDone. Dashboard: {PROJECT_ROOT / 'config.py'}")
    from config import STREAMLIT_APP_URL
    print(f"View at: {STREAMLIT_APP_URL}")


if __name__ == "__main__":
    main()
