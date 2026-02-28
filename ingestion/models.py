"""Data models for trade events parsed from Telegram messages."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class EventType(str, Enum):
    SIGNAL = "SIGNAL"
    WIN = "WIN"
    LOSS = "LOSS"
    JACKPOT = "JACKPOT"
    STARTUP = "STARTUP"


class Side(str, Enum):
    YES = "YES"
    NO = "NO"


@dataclass
class Fill:
    """A single fill line inside a WIN/LOSS/JACKPOT message."""
    side: Side
    quantity: int
    price_cents: int          # e.g. 2 for @2¢
    pnl: float                # e.g. +294.00 or -4.00
    is_win: bool              # ✅ vs ❌


@dataclass
class TradeEvent:
    """One parsed Telegram bot message."""
    timestamp: datetime
    event_type: EventType
    bot_name: str             # e.g. "Fernando-OG", "Ferny 3.1"
    asset: str                # BTC, ETH, SOL, XRP
    timeframe: str            # "15M" or "1HR"
    contract: Optional[str] = None          # e.g. KXBTC15M-26FEB031015-15
    contract_expiry: Optional[datetime] = None
    side: Optional[Side] = None
    tier: Optional[int] = None
    gap: Optional[float] = None
    hurdle: Optional[float] = None
    exp_move: Optional[float] = None
    strike: Optional[float] = None
    fills: list[Fill] = field(default_factory=list)
    net_pnl: Optional[float] = None
    session_wins: Optional[int] = None
    session_losses: Optional[int] = None
    session_pnl: Optional[float] = None
    flips: Optional[int] = None
    raw_text: str = ""


@dataclass
class ContractInfo:
    """Decoded contract identifier."""
    asset: str                # BTC, ETH, SOL, XRP
    timeframe: str            # 15M or D (daily = 1HR)
    date: datetime            # date portion
    expiry_time: datetime     # full expiry datetime
    raw: str                  # original string
