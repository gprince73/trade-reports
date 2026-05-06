"""Send trade report notifications to Telegram via bot."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from telegram import Bot
from telegram.constants import ParseMode

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, STREAMLIT_APP_URL


async def _send_message(text: str, chat_id: str, token: str) -> bool:
    """Send a message via Telegram Bot API."""
    bot = Bot(token=token)
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
        )
        return True
    except Exception as e:
        print(f"Telegram send failed: {e}")
        return False


def build_alert_data(
    stats: dict,
    date_str: str,
    app_url: Optional[str] = None,
) -> dict:
    """Build structured alert data and the Telegram message.

    Returns a dict with structured fields + the formatted HTML message.
    """
    app_url = app_url or STREAMLIT_APP_URL

    wins = stats.get("total_wins", 0)
    losses = stats.get("total_losses", 0)
    jackpots = stats.get("total_jackpots", 0)
    win_rate = stats.get("win_rate", 0)
    net_pnl = stats.get("net_pnl", 0)
    signals = stats.get("total_signals", 0)

    pnl_emoji = "\U0001f4b0" if net_pnl >= 0 else "\U0001f4c9"

    message = (
        f"<b>Daily Trade Report - {date_str}</b>\n"
        f"\n"
        f"{pnl_emoji} <b>Net P&L: ${net_pnl:+,.2f}</b>\n"
        f"\n"
        f"Signals: {signals}\n"
        f"Wins: {wins} | Losses: {losses} | Jackpots: {jackpots}\n"
        f"Win Rate: {win_rate:.1%}\n"
        f"\n"
        f'<a href="{app_url}">View Full Dashboard</a>'
    )

    return {
        "date": date_str,
        "sent_at": datetime.now().isoformat(timespec="seconds"),
        "net_pnl": net_pnl,
        "signals": signals,
        "wins": wins,
        "losses": losses,
        "jackpots": jackpots,
        "win_rate": win_rate,
        "message_html": message,
    }


def save_alert(alert_data: dict, output_dir: Path) -> None:
    """Persist alert data to alerts.json in the output directory."""
    alerts_path = Path(output_dir) / "alerts.json"

    existing = []
    if alerts_path.exists():
        try:
            existing = json.loads(alerts_path.read_text())
        except (json.JSONDecodeError, ValueError):
            existing = []

    # Deduplicate: remove any alert with the same date
    existing = [a for a in existing if a.get("date") != alert_data["date"]]

    # Prepend new alert (most recent first)
    existing.insert(0, alert_data)

    alerts_path.write_text(json.dumps(existing, indent=2))


def send_report_notification(
    stats: dict,
    date_str: str,
    app_url: Optional[str] = None,
    token: Optional[str] = None,
    chat_id: Optional[str] = None,
) -> tuple[bool, dict]:
    """Post a daily report summary to the Telegram group.

    Args:
        stats: Dict from analytics.summary.overall_stats().
        date_str: The report date (e.g. "2026-02-27").
        app_url: Streamlit app URL. Defaults to config value.
        token: Bot token. Defaults to config value.
        chat_id: Target chat ID. Defaults to config value.

    Returns:
        Tuple of (success_bool, alert_data_dict).
    """
    token = token or TELEGRAM_BOT_TOKEN
    chat_id = chat_id or TELEGRAM_CHAT_ID

    alert_data = build_alert_data(stats, date_str, app_url)

    if not token or not chat_id:
        print("Telegram bot token or chat_id not configured. Skipping notification.")
        return False, alert_data

    sent = asyncio.run(_send_message(alert_data["message_html"], chat_id, token))
    return sent, alert_data
