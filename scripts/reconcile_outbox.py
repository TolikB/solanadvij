"""Resolve a Telegram outbox event whose delivery outcome is uncertain."""

from __future__ import annotations

import argparse
import asyncio
import os

from sniper_bot.database import Database


async def reconcile(event_id: str, action: str, message_id: str | None) -> None:
    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    database = Database(dsn)
    try:
        state = await database.resolve_uncertain_outbox(
            event_id,
            action=action,
            telegram_message_id=message_id,
        )
        print(f"outbox_event={event_id} state={state}")
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("event_id")
    parser.add_argument("action", choices=("retry", "delivered", "dead"))
    parser.add_argument("--telegram-message-id")
    args = parser.parse_args()
    if args.action == "delivered" and not args.telegram_message_id:
        parser.error("--telegram-message-id is required for delivered")
    asyncio.run(reconcile(args.event_id, args.action, args.telegram_message_id))


if __name__ == "__main__":
    main()
