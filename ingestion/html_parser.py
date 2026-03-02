"""Parse Telegram HTML export files into structured TradeEvent objects."""
from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

from bs4 import BeautifulSoup, Tag

from config import TELEGRAM_EXPORT_ROOT
from ingestion.interface import MessageSource
from ingestion.message_parser import parse_text_to_event
from ingestion.models import TradeEvent


# ---------------------------------------------------------------------------
# HTML-specific patterns
# ---------------------------------------------------------------------------

# Timestamp in the title attr: "03.02.2026 08:54:32 UTC-06:00"
TS_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2})")


# ---------------------------------------------------------------------------
# HTML-specific helpers
# ---------------------------------------------------------------------------

def _parse_message(div: Tag, last_sender: str) -> tuple[Optional[TradeEvent], str]:
    """Parse a single message div into a TradeEvent.

    Returns (event_or_None, sender_name).
    """
    # Skip service messages
    classes = div.get("class", [])
    if "service" in classes:
        return None, last_sender

    # Extract timestamp
    date_div = div.find("div", class_="date")
    if not date_div:
        return None, last_sender
    title = date_div.get("title", "")
    ts_match = TS_RE.search(title)
    if not ts_match:
        return None, last_sender
    timestamp = datetime.strptime(ts_match.group(1), "%d.%m.%Y %H:%M:%S")

    # Extract sender (may be absent in "joined" messages)
    sender_div = div.find("div", class_="from_name")
    if sender_div:
        last_sender = sender_div.get_text(strip=True)

    # Extract message text
    text_div = div.find("div", class_="text")
    if not text_div:
        return None, last_sender

    # Get raw HTML text (preserves <br> as newlines)
    raw_html = text_div.decode_contents()
    # Convert <br> to newlines for regex, strip HTML tags for plain text
    raw_text = raw_html.replace("<br>", "\n").replace("<br/>", "\n")
    # Remove HTML tags but keep content
    plain_text = re.sub(r"<[^>]+>", "", raw_text).strip()

    # Delegate to shared parser
    event = parse_text_to_event(plain_text, timestamp)
    return event, last_sender


def _sort_html_files(files: list[Path]) -> list[Path]:
    """Sort messages.html, messages2.html, ..., messages16.html."""
    def sort_key(p: Path):
        stem = p.stem  # "messages" or "messages2"
        num = stem.replace("messages", "")
        return int(num) if num else 0
    return sorted(files, key=sort_key)


class HTMLMessageSource(MessageSource):
    """Parse Telegram HTML exports from a dated subfolder."""

    def __init__(self, export_folder: Path):
        self.export_folder = export_folder

    def get_events(self, target_date: date | None = None) -> List[TradeEvent]:
        html_files = list(self.export_folder.glob("messages*.html"))
        if not html_files:
            raise FileNotFoundError(
                f"No messages*.html files in {self.export_folder}"
            )
        html_files = _sort_html_files(html_files)

        all_events: list[TradeEvent] = []
        last_sender = ""

        for fpath in html_files:
            with open(fpath, "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f, "lxml")

            for div in soup.find_all("div", class_="message"):
                event, last_sender = _parse_message(div, last_sender)
                if event is not None:
                    if target_date is None or event.timestamp.date() == target_date:
                        all_events.append(event)

        return all_events


def get_export_folder(export_date: date | None = None) -> Path:
    """Resolve the export folder for a given date."""
    if export_date is None:
        export_date = date.today()
    folder_name = f"ChatExport_{export_date.isoformat()}"
    folder = TELEGRAM_EXPORT_ROOT / folder_name
    if not folder.exists():
        raise FileNotFoundError(f"Export folder not found: {folder}")
    return folder
