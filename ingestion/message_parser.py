"""Shared message parsing logic used by both HTML and Telethon backends.

All regex patterns, field extraction, and TradeEvent construction live here
so that both message sources produce identical output.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from ingestion.models import ContractInfo, EventType, Fill, Side, TradeEvent


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

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
    (re.compile(r"\bSIGNAL\b", re.IGNORECASE), EventType.SIGNAL),
]

# Field extraction
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

KNOWN_ASSETS = {"BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "BNB"}

# Converny family — name format is "Converny T(x) Signal #(y) ASSET" /
# "Converny T(x) WIN ASSET" / "Converny T(x) LOSS ASSET". We collapse
# all signal counts into a single bot per tier so the dashboard groups
# by tier (Converny T1 / T2 / T3).
CONVERNY_RE = re.compile(r"\bConverny\s+T(\d+)\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Shared functions
# ---------------------------------------------------------------------------

def parse_currency(s: str) -> float:
    """Convert '$+1,234.56' or '$-4.00' to float."""
    return float(s.replace(",", "").replace("+", ""))


def decode_contract(contract_str: str) -> Optional[ContractInfo]:
    """Decode a Kalshi contract ID into its components."""
    m = CONTRACT_ID_RE.search(contract_str)
    if not m:
        return None
    asset = m.group(1)
    timeframe = m.group(2)
    year_suffix = int(m.group(3))
    month_str = m.group(4)
    day = int(m.group(5))
    hhmm = m.group(6)
    ss = m.group(7)

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


def classify_event(text: str) -> Optional[EventType]:
    """Determine the event type from message text."""
    if "Started" in text and "\U0001f680" in text:
        return EventType.STARTUP

    first_chars = text[:5]
    for emoji in SIGNAL_EMOJI:
        if emoji in first_chars:
            for pattern, etype in EVENT_PATTERNS:
                if pattern.search(text):
                    return etype
            return EventType.SIGNAL

    for pattern, etype in EVENT_PATTERNS:
        if pattern.search(text):
            return etype

    return None


def _detect_asset_and_timeframe(text: str, first_line: str) -> tuple[str, str]:
    """Find asset (last token of first line, then second line, then contract)
    and timeframe (from contract). Used by Converny path."""
    asset = "UNKNOWN"
    timeframe = "15M"

    tokens = first_line.split()
    if tokens and tokens[-1].upper() in KNOWN_ASSETS:
        asset = tokens[-1].upper()

    if asset == "UNKNOWN":
        lines = text.split("\n")
        if len(lines) > 1:
            second_tokens = lines[1].strip().split()
            if second_tokens and second_tokens[0].upper() in KNOWN_ASSETS:
                asset = second_tokens[0].upper()

    contract_match = CONTRACT_RE.search(text)
    if contract_match:
        cinfo = decode_contract(contract_match.group(1))
        if cinfo:
            if asset == "UNKNOWN":
                asset = cinfo.asset
            timeframe = cinfo.timeframe

    return asset, timeframe


def extract_bot_and_asset(text: str, event_type: EventType) -> tuple[str, str, str]:
    """Extract bot name, asset, and timeframe from message text.

    Returns (bot_name, asset, timeframe).
    """
    first_line = text.split("\n")[0].strip()

    # Converny family: collapse "Converny T1 Signal #8 ...", "Converny T2 WIN ETH",
    # "Converny T3 LOSS XRP", etc. into "Converny T<n>" so the dashboard groups
    # all signal counts under a single per-tier bot.
    converny_match = CONVERNY_RE.search(first_line)
    if converny_match:
        bot_name = f"Converny T{int(converny_match.group(1))}"
        asset, timeframe = _detect_asset_and_timeframe(text, first_line)
        return bot_name, asset, timeframe

    cleaned = first_line
    cleaned = re.sub(r"^[\s\U0001f300-\U0001f9ff\u2600-\u2bff\ufe0f\u200d]+", "", cleaned)

    for kw in [
        "FLIP SIGNAL", "JACKPOT 1HR", "JACKPOT", "PARTIAL LOSS",
        "PARTIAL WIN", "LOSS", "WIN", "SIGNAL", "Started",
    ]:
        cleaned = re.sub(rf"^\s*{re.escape(kw)}\s*", "", cleaned, count=1)

    cleaned = re.sub(r"^[\s\U0001f300-\U0001f9ff\u2600-\u2bff\ufe0f\u200d]+", "", cleaned)

    if ":" in cleaned:
        cleaned = cleaned.split(":")[0].strip()

    cleaned = cleaned.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    cleaned = cleaned.strip()

    tokens = cleaned.split()
    asset = "UNKNOWN"
    timeframe = "15M"

    if tokens and tokens[-1].upper() in KNOWN_ASSETS:
        asset = tokens[-1].upper()
        bot_name = " ".join(tokens[:-1])
    else:
        bot_name = cleaned
        contract_match = CONTRACT_RE.search(text)
        if contract_match:
            cinfo = decode_contract(contract_match.group(1))
            if cinfo:
                asset = cinfo.asset

    if "1HR" in bot_name.upper() or "1hr" in bot_name:
        timeframe = "1HR"
        bot_name = re.sub(r"\s*1HR\s*", " ", bot_name, flags=re.IGNORECASE).strip()
    else:
        contract_match = CONTRACT_RE.search(text)
        if contract_match:
            cinfo = decode_contract(contract_match.group(1))
            if cinfo:
                timeframe = cinfo.timeframe

    bot_name = re.sub(r"-\s+-", "-", bot_name)

    if not bot_name:
        bot_name = "Unknown"

    return bot_name, asset, timeframe


def parse_fills(text: str) -> list[Fill]:
    """Extract all fill lines from a WIN/LOSS/JACKPOT message."""
    fills = []
    for m in FILL_RE.finditer(text):
        fills.append(Fill(
            side=Side.YES if m.group(2) == "YES" else Side.NO,
            quantity=int(m.group(3)),
            price_cents=int(m.group(4)),
            pnl=parse_currency(m.group(5)),
            is_win=(m.group(1) == "✅"),
        ))
    return fills


def parse_text_to_event(plain_text: str, timestamp: datetime) -> Optional[TradeEvent]:
    """Parse a plain-text message body into a TradeEvent.

    This is the core shared parser. Both the HTML and Telethon backends
    call this after extracting the plain text and timestamp from their
    respective message formats.

    Args:
        plain_text: The message body as plain text (newline-separated).
        timestamp:  The message timestamp (in local/CST time).

    Returns:
        A TradeEvent, or None if the message is not a trade event.
    """
    # Strip Telegram Markdown bold markers (**text**) that appear
    # in the raw API but not in the HTML export.
    plain_text = plain_text.replace("**", "")

    event_type = classify_event(plain_text)
    if event_type is None or event_type == EventType.STARTUP:
        return None

    bot_name, asset, timeframe = extract_bot_and_asset(plain_text, event_type)

    # Side
    side = None
    side_match = SIDE_RE.search(plain_text)
    if side_match:
        side = Side.YES if side_match.group(1) == "YES" else Side.NO
    else:
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
        gap = parse_currency(gap_match.group(1))

    # Hurdle
    hurdle = None
    hurdle_match = HURDLE_RE.search(plain_text)
    if hurdle_match:
        hurdle = float(hurdle_match.group(1))

    # ExpMove
    exp_move = None
    exp_match = EXPMOVE_RE.search(plain_text)
    if exp_match:
        exp_move = parse_currency(exp_match.group(1))

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
        strike = parse_currency(strike_match.group(1))

    # Fills
    fills = parse_fills(plain_text)

    # Infer side from fills if not set (WIN/LOSS/JACKPOT events
    # use 🏆/💀 instead of directional emojis, but fills have side)
    if side is None and fills:
        side = fills[0].side

    # Net P&L
    net_pnl = None
    net_match = NET_RE.search(plain_text)
    if net_match:
        net_pnl = parse_currency(net_match.group(1))

    # Session
    session_wins = None
    session_losses = None
    session_pnl = None
    session_match = SESSION_RE.search(plain_text)
    if session_match:
        session_wins = int(session_match.group(1))
        session_losses = int(session_match.group(2))
        session_pnl = parse_currency(session_match.group(3))

    # Flips
    flips = None
    flips_match = FLIPS_RE.search(plain_text)
    if flips_match:
        flips = int(flips_match.group(1))

    return TradeEvent(
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
