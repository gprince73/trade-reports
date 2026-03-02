"""One-time Telethon authentication helper.

Run this interactively to create a .session file:
    python -m ingestion.telethon_setup

You will be prompted for your phone number and an SMS/Telegram code.
Once complete, the session file persists and no further login is needed.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Add project root to path so config imports work
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import TELETHON_API_ID, TELETHON_API_HASH, TELETHON_SESSION_NAME


async def setup():
    """Interactive login — creates a persistent .session file."""
    from telethon import TelegramClient

    if not TELETHON_API_ID or not TELETHON_API_HASH:
        print("ERROR: TELETHON_API_ID and TELETHON_API_HASH must be set in .env")
        print()
        print("Steps:")
        print("  1. Go to https://my.telegram.org")
        print("  2. Log in with your phone number")
        print("  3. Click 'API Development Tools'")
        print("  4. Create an application (any name/description)")
        print("  5. Copy api_id and api_hash into your .env file:")
        print()
        print("     TELETHON_API_ID=12345678")
        print('     TELETHON_API_HASH=abcdef1234567890abcdef1234567890')
        return

    print("=== Telethon Authentication Setup ===")
    print()
    print(f"  API ID:       {TELETHON_API_ID}")
    print(f"  API Hash:     {TELETHON_API_HASH[:8]}...")
    print(f"  Session file: {TELETHON_SESSION_NAME}.session")
    print()

    client = TelegramClient(TELETHON_SESSION_NAME, TELETHON_API_ID, TELETHON_API_HASH)

    await client.start()

    me = await client.get_me()
    print()
    print(f"Authenticated as: {me.first_name} {me.last_name or ''} (@{me.username or 'N/A'})")
    print(f"Session saved to: {TELETHON_SESSION_NAME}.session")
    print()
    print("You can now use --source telethon in main.py and publish.py")

    await client.disconnect()


def main():
    asyncio.run(setup())


if __name__ == "__main__":
    main()
