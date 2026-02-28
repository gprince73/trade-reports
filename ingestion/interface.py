"""Abstract interface for message sources.

Implement this to swap between HTML file parsing and live Telethon ingestion.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List

from ingestion.models import TradeEvent


class MessageSource(ABC):
    """Base class for all message ingestion backends."""

    @abstractmethod
    def get_events(self, target_date: date | None = None) -> List[TradeEvent]:
        """Return parsed trade events, optionally filtered to a single day."""
        ...
