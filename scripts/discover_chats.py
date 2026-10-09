"""Discover Telegram chat IDs the bot can see, so you can add a signal group
to the allowlist.

Usage
-----
1. Make sure @akolade_bot is a member of the target group (a group admin must
   add it) AND the bot's group privacy is OFF (``@BotFather -> /setprivacy ->
   the bot -> Turn off``) — otherwise the bot won't receive the group's
   messages and it won't show up here.
2. Send any message in the group (a signal is fine).
3. Run::

       .venv/bin/python -m scripts.discover_chats

   It prints every chat the bot has recently seen. Copy the ``id`` of the
   group (a negative number like ``-1001234567890``) and add it to
   ``QSB_ALLOWED_CHAT_IDS`` in ``.env`` (comma-separated), then restart the
   bot.
"""

from __future__ import annotations

import asyncio
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("QSB_TELEGRAM_TOKEN", "")


async def main() -> None:
    if not TOKEN:
        print("QSB_TELEGRAM_TOKEN is not set in .env — nothing to do.")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    seen: dict[int, dict] = {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        for attempt in range(3):
            resp = await client.get(url, params={"timeout": 10})
            data = resp.json()
            if not data.get("ok"):
                print("API error:", data)
                return
            for upd in data.get("result", []):
                msg = upd.get("message") or upd.get("edited_message") or {}
                chat = msg.get("chat") or {}
                cid = chat.get("id")
                if cid is None:
                    continue
                seen.setdefault(cid, {"chat": chat, "text": msg.get("text", "")})
            if seen:
                break
            print(f"poll {attempt + 1}: no messages yet (bot not in any chat yet?)")

    print("\nChats the bot can currently see:\n" + "-" * 60)
    current = os.getenv("QSB_ALLOWED_CHAT_IDS", "")
    for cid, info in sorted(seen.items()):
        chat = info["chat"]
        title = chat.get("title") or chat.get("username") or chat.get("first_name", "")
        kind = chat.get("type", "?")
        preview = (info["text"] or "").replace("\n", " ")[:60]
        marker = "  <-- already allowed" if str(cid) in current.split(",") else ""
        print(f"id={cid}\n   type={kind}  title={title!r}\n   latest: {preview!r}{marker}")
    print("-" * 60)
    if not seen:
        print(
            "No chats found. The bot must be IN the group (added by an admin) and "
            "its privacy mode must be OFF, then someone sends a message there."
        )
    else:
        print("Add the group's id to QSB_ALLOWED_CHAT_IDS in .env, then restart.")


if __name__ == "__main__":
    asyncio.run(main())
