"""Parse Telegram HTML export files into structured TradeEvent objects."""
from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

from bs4 import BeautifulSoup, Tag

from config import TELEGRAM_EXPORT_ROOT, LOCAL_TZ
from ingestion.interface import MessageSource
from ingestion.models import ContractInfo, EventType, Fill, Side, TradeEvent


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Timestamp in the title attr: "03.02.2026 08:54:32 UTC-06:00"
TS_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2})")

# Signal direction emoji → side mapping
SIGNAL_EMOJI = {
    "\U0001f534": Side.NO,    # 🔴
    "\U0001f7e2": Side.YES,   # 🟢
    "\u2b07\ufe0f": Side.NO,  # ⬇️
    "\u2b06\ufe0f": Side.YES, # ⬆️
    "\u2b07": Side.NO,        # ⬇ (without variation selector)
    "\u2b06": Side.YES,       # ⬆ (without variation selector)
}

# Event type detection (order matters — check JACKPOT before WIN)
EVENT_PATTERNS = [
    (re.compile(r"JACKPOT", re.IGNORECASE), EventType.JACKPOT),
    (re.compile(r"PARTIAL\s+LOSS", re.IGNORECASE), EventType.LOSS),
    (re.compile(r"PARTIAL\s+WIN", re.IGNORECASE), EventType.WIN),
    (re.compile(r"\bLOSS\b"), EventType.LOSS),
    (re.compile(r"\bWIN\b"), EventType.WIN),
    (re.compile(r"FLIP\s+SIGNAL", re.IGNORECASE), EventType.SIGNAL),
    (re.compile(r"\bSIGNAL\b"), EventType.SIGNAL),
]

# Field extraction from <br>-delimited lines
SIDE_RE = re.compile(r"Side:\s*(YES|NO)")
TIER_RE = re.compile(r"Tier\s*(\d+)")
GAP_RE = re.compile(r"Gap:\s*\$([+-]?[\d,.]+)")
HURDLE_RE = re.compile(r"Hurdle:\s*([\d.]+)x")
EXPMOVE_RE = re.compile(r"ExpMove:\s*\$([\d,.]+)")
CONTRACT_RE = re.compile(r"Contract:\s*([A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+)")
STRIKE_RE = re.compile(r"Strike:\s*([\d,.]+)")
NET_RE = re.compile(r"Net:\s*\$([+-]?[\d,.]+)")
SESSION_RE = re.compile(r"Session:\s*(\d+)W-(\d+)L\s*\|\s*\$([+-]?[\d,.]+)")
FLIPS_RE = re.compile(r"Flips:\s*(\d+)")

# Fill lines:  "✅ NO 20@90¢ → $+2.00"  or  "❌ YES 83@2c -> $-1.66"
FILL_RE = re.compile(
    r"([✅❌])\s*(YES|NO)\s+(\d+)@(\d+)[¢c]\s*(?:→|->|&gt;)\s*\$([+-]?[\d,.]+)"
)

# Contract ID decoder: KXBTC15M-26FEB031015-15
CONTRACT_ID_RE = re.compile(
    r"KX([A-Z]+)(15M|D)-(\d{2})([A-Z]{3})(\d{2})(\d{4})-(\d{2})"
)

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Known assets
KNOWN_ASSETS = {"BTC", "ETH", "SOL", "XRP"}


def _parse_currency(s: str) -> float:
    """Convert '$+1,234.56' or '$-4.00' to float."""
    return float(s.replace(",", "").replace("+", ""))


def decode_contract(contract_str: str) -> Optional[ContractInfo]:
    """Decode a Kalshi contract ID into its components."""
    m = CONTRACT_ID_RE.search(contract_str)
    if not m:
        return None
    asset = m.group(1)
    timeframe = m.group(2)
    year_suffix = int(m.group(3))   # e.g. 26
    month_str = m.group(4)          # e.g. FEB
    day = int(m.group(5))           # e.g. 03
    hhmm = m.group(6)              # e.g. 1015
    ss = m.group(7)                 # e.g. 15

    year = 2000 + year_suffix
    month = MONTH_MAP.get(month_str, 1)
    hour = int(hhmm[:2])
    minute = int(hhmm[2:])
    second = int(ss)

    expiry = datetime(year, month, day, hour, minute, second)
    return ContractInfo(
        asset=asset,
        timeframe="1HR" if timeframe == "D" else timeframe,
        date=expiry.date(),
        expiry_time=expiry,
        raw=contract_str,
    )


def _extract_bot_and_asset(text: str, event_type: EventType) -> tuple[str, str, str]:
    """Extract bot name, asset, and timeframe from message text.

    Returns (bot_name, asset, timeframe).
    """
    # Only use the FIRST LINE of the text for bot name extraction
    first_line = text.split("\n")[0].strip()

    # Remove ALL leading emojis and symbols (broad Unicode sweep)
    cleaned = first_line
    cleaned = re.sub(r"^[\s\U0001f300-\U0001f9ff\u2600-\u2bff\ufe0f\u200d]+", "", cleaned)

    # Remove the event type keywords (order matters: longer first)
    for kw in [
        "FLIP SIGNAL", "JACKPOT 1HR", "JACKPOT", "PARTIAL LOSS",
        "PARTIAL WIN", "LOSS", "WIN", "SIGNAL", "Started",
    ]:
        cleaned = re.sub(rf"^\s*{re.escape(kw)}\s*", "", cleaned, count=1)

    # Strip any remaining leading emojis after keyword removal
    cleaned = re.sub(r"^[\s\U0001f300-\U0001f9ff\u2600-\u2bff\ufe0f\u200d]+", "", cleaned)

    # Truncate at ":" (handles summary lines like "Ferny 3.1 BTC: $+148.98 | 1W-0L")
    if ":" in cleaned:
        cleaned = cleaned.split(":")[0].strip()

    # Decode HTML entities
    cleaned = cleaned.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

    # What remains is "Fernando-OG BTC" or "Ferny 3.1 ETH" etc.
    cleaned = cleaned.strip()

    # Split off the asset (last token)
    tokens = cleaned.split()
    asset = "UNKNOWN"
    timeframe = "15M"

    # Check if last token is a known asset
    if tokens and tokens[-1].upper() in KNOWN_ASSETS:
        asset = tokens[-1].upper()
        bot_name = " ".join(tokens[:-1])
    else:
        bot_name = cleaned
        # Try to extract asset from contract string
        contract_match = CONTRACT_RE.search(text)
        if contract_match:
            cinfo = decode_contract(contract_match.group(1))
            if cinfo:
                asset = cinfo.asset

    # Determine timeframe from bot name or contract
    if "1HR" in bot_name.upper() or "1hr" in bot_name:
        timeframe = "1HR"
        # Clean "1HR" from bot name if it's a suffix
        bot_name = re.sub(r"\s*1HR\s*", " ", bot_name, flags=re.IGNORECASE).strip()
    else:
        contract_match = CONTRACT_RE.search(text)
        if contract_match:
            cinfo = decode_contract(contract_match.group(1))
            if cinfo:
                timeframe = cinfo.timeframe

    # Fix stray hyphens (e.g., "Fernando- -OG" -> "Fernando-OG")
    bot_name = re.sub(r"-\s+-", "-", bot_name)

    if not bot_name:
        bot_name = "Unknown"

    return bot_name, asset, timeframe


def _classify_event(text: str) -> Optional[EventType]:
    """Determine the event type from message text."""
    # Check for startup first
    if "Started" in text and "\U0001f680" in text:
        return EventType.STARTUP

    # Check signal emojis (with or without SIGNAL keyword)
    first_chars = text[:5]
    for emoji in SIGNAL_EMOJI:
        if emoji in first_chars:
            # Could be a signal (⬇️/⬆️ without SIGNAL keyword) or have SIGNAL keyword
            for pattern, etype in EVENT_PATTERNS:
                if pattern.search(text):
                    return etype
            # If no keyword found but has directional emoji, it's a signal
            return EventType.SIGNAL

    # Check keyword patterns
    for pattern, etype in EVENT_PATTERNS:
        if pattern.search(text):
            return etype

    return None


def _parse_fills(text: str) -> list[Fill]:
    """Extract all fill lines from a WIN/LOSS/JACKPOT message."""
    fills = []
    for m in FILL_RE.finditer(text):
        fills.append(Fill(
            side=Side.YES if m.group(2) == "YES" else Side.NO,
            quantity=int(m.group(3)),
            price_cents=int(m.group(4)),
            pnl=_parse_currency(m.group(5)),
            is_win=(m.group(1) == "✅"),
        ))
    return fills


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
    # Also keep the HTML version for fill parsing (some use &gt; for >)
    html_text = raw_html.replace("<br>", "\n").replace("<br/>", "\n")

    # Classify
    event_type = _classify_event(plain_text)
    if event_type is None or event_type == EventType.STARTUP:
        return None, last_sender

    # Extract fields
    bot_name, asset, timeframe = _extract_bot_and_asset(plain_text, event_type)

    # Side
    side = None
    side_match = SIDE_RE.search(plain_text)
    if side_match:
        side = Side.YES if side_match.group(1) == "YES" else Side.NO
    else:
        # Infer from directional emoji
        for emoji, s in SIGNAL_EMOJI.items():
            if emoji in plain_text[:5]:
                side = s
                break

    # Tier
    tier = None
    tier_match = TIER_RE.search(plain_text)
    if tier_match:
        tier = int(tier_match.group(1))

    # Gap
    gap = None
    gap_match = GAP_RE.search(plain_text)
    if gap_match:
        gap = _parse_currency(gap_match.group(1))

    # Hurdle
    hurdle = None
    hurdle_match = HURDLE_RE.search(plain_text)
    if hurdle_match:
        hurdle = float(hurdle_match.group(1))

    # ExpMove
    exp_move = None
    exp_match = EXPMOVE_RE.search(plain_text)
    if exp_match:
        exp_move = _parse_currency(exp_match.group(1))

    # Contract
    contract = None
    contract_expiry = None
    contract_match = CONTRACT_RE.search(plain_text)
    if contract_match:
        contract = contract_match.group(1)
        cinfo = decode_contract(contract)
        if cinfo:
            contract_expiry = cinfo.expiry_time

    # Strike
    strike = None
    strike_match = STRIKE_RE.search(plain_text)
    if strike_match:
        strike = _parse_currency(strike_match.group(1))

    # Fills (for WIN/LOSS/JACKPOT)
    fills = _parse_fills(html_text)

    # Net P&L
    net_pnl = None
    net_match = NET_RE.search(plain_text)
    if net_match:
        net_pnl = _parse_currency(net_match.group(1))

    # Session
    session_wins = None
    session_losses = None
    session_pnl = None
    session_match = SESSION_RE.search(plain_text)
    if session_match:
        session_wins = int(session_match.group(1))
        session_losses = int(session_match.group(2))
        session_pnl = _parse_currency(session_match.group(3))

    # Flips
    flips = None
    flips_match = FLIPS_RE.search(plain_text)
    if flips_match:
        flips = int(flips_match.group(1))

    event = TradeEvent(
        timestamp=timestamp,
        event_type=event_type,
        bot_name=bot_name,
        asset=asset,
        timeframe=timeframe,
        contract=contract,
        contract_expiry=contract_expiry,
        side=side,
        tier=tier,
        gap=gap,
        hurdle=hurdle,
        exp_move=exp_move,
        strike=strike,
        fills=fills,
        net_pnl=net_pnl,
        session_wins=session_wins,
        session_losses=session_losses,
        session_pnl=session_pnl,
        flips=flips,
        raw_text=plain_text,
    )
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
