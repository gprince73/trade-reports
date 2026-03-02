"""Pull messages from Telegram via Telethon and parse into TradeEvents.

This replaces the manual HTML-export workflow with live API access.
Requires a one-time phone authentication (see telethon_setup.py).
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import List, Optional

from telethon import TelegramClient
from telethon.tl.types import Message

from config import (
    LOCAL_TZ,
    TELETHON_API_ID,
    TELETHON_API_HASH,
    TELETHON_SESSION_NAME,
    TELETHON_CHAT_ID,
)
from ingestion.interface import MessageSource
from ingestion.message_parser import parse_text_to_event
from ingestion.models import TradeEvent

import pytz

UTC = pytz.utc


class TelethonMessageSource(MessageSource):
    """Fetch messages from a Telegram chat via the Telethon user-client API."""

    def __init__(
        self,
        api_id: int = TELETHON_API_ID,
        api_hash: str = TELETHON_API_HASH,
        session_name: str = TELETHON_SESSION_NAME,
        chat_id: int = TELETHON_CHAT_ID,
    ):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_name = session_name
        self.chat_id = chat_id

    # --- Public interface (sync wrapper) ---

    def get_events(self, target_date: date | None = None) -> List[TradeEvent]:
        """Fetch and parse messages. Blocks until complete."""
        return asyncio.run(self._fetch_and_parse(target_date))

    # --- Async internals ---

    async def _fetch_and_parse(
        self, target_date: Optional[date] = None
    ) -> List[TradeEvent]:
        """Connect, pull messages, parse, disconnect."""
        client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        await client.start()

        try:
            events = await self._iter_messages(client, target_date)
        finally:
            await client.disconnect()

        return events

    async def _iter_messages(
        self,
        client: TelegramClient,
        target_date: Optional[date] = None,
    ) -> List[TradeEvent]:
        """Iterate over chat messages and parse each one."""
        # Build date window for the API query
        # Telethon offset_date is exclusive upper bound (UTC)
        if target_date:
            # Fetch messages from the full target day in LOCAL time
            local_start = LOCAL_TZ.localize(
                datetime.combine(target_date, datetime.min.time())
            )
            local_end = LOCAL_TZ.localize(
                datetime.combine(target_date + timedelta(days=1), datetime.min.time())
            )
            utc_start = local_start.astimezone(UTC)
            utc_end = local_end.astimezone(UTC)
        else:
            utc_start = None
            utc_end = None

        events: list[TradeEvent] = []

        # Fetch newest-first (default) with offset_date as upper bound.
        # reverse=True doesn't work reliably with offset_date in Telethon,
        # so we fetch newest-first and reverse the list at the end.
        kwargs: dict = {"entity": self.chat_id}
        if utc_end:
            kwargs["offset_date"] = utc_end

        async for msg in client.iter_messages(**kwargs):
            if not isinstance(msg, Message) or not msg.text:
                continue

            # Telethon msg.date is always UTC-aware
            msg_utc: datetime = msg.date

            # Stop early once we've gone past the start of the window
            if utc_start and msg_utc < utc_start:
                break

            # Convert UTC → local (CST) and strip tzinfo for consistency
            # with the HTML parser (which produces naive CST timestamps)
            local_dt = msg_utc.astimezone(LOCAL_TZ).replace(tzinfo=None)

            event = parse_text_to_event(msg.text, local_dt)
            if event is not None:
                events.append(event)

        # Reverse so events are in chronological order (oldest first)
        events.reverse()
        return events
