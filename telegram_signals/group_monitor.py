"""MTProto userbot bridge — read signals from a private Telegram group a bot
cannot be added to, using the user's own Telegram account (Telethon).

Why this exists
---------------
A Telegram *bot* only receives messages from chats it is a member of, and a
private group's admin must add it. When the admin won't, the only automatic
option is to log in as a member (a user account) and route each new group
message into the same signal queue the bot uses. Parsing, dedup, and gating
are identical to the bot's own handler, so a signal arriving both via a
manual forward and via this monitor is dropped, never double-traded.

ToS / account-risk warning
--------------------------
This keeps the user's Telegram account connected for automation 24/7.
Telegram may flag or ban accounts that behave like a bot; prefer a spare
account if the group allows one. The session is stored in a local
``.session`` file (``QSB_MONITOR_SESSION``) and never leaves the machine.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from telethon import TelegramClient, events
from telethon.errors import UserAlreadyParticipantError
from telethon.tl.functions.messages import ImportChatInviteRequest

from telegram_signals.handlers import SignalHandler

logger = logging.getLogger(__name__)

# Invite-link prefixes whose trailing hash can be used to join a group via
# ImportChatInviteRequest. Public usernames resolve directly via get_entity.
_INVITE_PREFIXES = (
    "https://t.me/+",
    "https://t.me/joinchat/",
    "t.me/+",
    "t.me/joinchat/",
    "tg://join?invite=",
)


def extract_invite_hash(link: str) -> str | None:
    """Return the invite hash from a join link (``t.me/+HASH``,
    ``t.me/joinchat/HASH``, ``tg://join?invite=HASH``), else ``None`` for
    non-invite links (e.g. a public username)."""
    stripped = link.strip()
    for prefix in _INVITE_PREFIXES:
        if stripped.startswith(prefix):
            return stripped[len(prefix):] or None
    return None


class GroupMonitor:
    """Subscribe the user's account to one or more groups and route their new
    messages into the shared signal handler (parse + dedup + queue)."""

    def __init__(
        self,
        handler: SignalHandler,
        *,
        session_path: str,
        api_id: int,
        api_hash: str,
        phone: str,
        links: tuple[str, ...],
    ) -> None:
        self._handler = handler
        self._links = tuple(links)
        self._client = TelegramClient(session_path, api_id, api_hash)
        self._phone = phone
        self._targets: list[Any] = []
        self._ready = asyncio.Event()
        self._done = asyncio.Event()

    @property
    def target_id(self) -> int | None:
        if not self._targets:
            return None
        return getattr(self._targets[0], "id", None)

    @property
    def ready(self) -> asyncio.Event:
        return self._ready

    async def start(self) -> None:
        """Connect the user session, join/resolve every target group, and start
        listening. First run prompts for a Telegram login code on stdin; the
        session file auto-authenticates on later runs."""
        # Interactive on first login (code via stdin); 2FA password prompted
        # the same way if enabled. Runs on the bot's own asyncio loop.
        await self._client.start(phone=self._phone)
        for link in self._links:
            target = await self._resolve_target(link)
            self._targets.append(target)
        self._client.add_event_handler(
            self._on_message, events.NewMessage(chats=self._targets)
        )
        logger.info(
            "group monitor listening on %s",
            ", ".join(f"{t!r} (id {getattr(t, 'id', '?')})" for t in self._targets),
        )
        self._ready.set()

    async def _resolve_target(self, link: str) -> Any:
        """Return the target group entity, joining it if the account isn't a
        member yet. Works for public usernames and invite links
        (``t.me/+HASH`` / ``t.me/joinchat/HASH`` / ``tg://join?invite=``)."""
        try:
            return await self._client.get_entity(link)
        except ValueError:
            pass  # not a member yet — fall through to join the invite link
        except Exception as exc:
            raise RuntimeError(
                f"could not resolve group link {link!r}: {exc}"
            ) from exc

        hash_ = extract_invite_hash(link)
        if not hash_:
            raise RuntimeError(
                f"not a member of {link!r} and it isn't an invite link to join"
            )
        try:
            result = await self._client(ImportChatInviteRequest(hash_))
        except UserAlreadyParticipantError:
            # Race: the account joined between the check and the join call.
            return await self._client.get_entity(link)
        # The response shape varies by Telethon version: older versions returned
        # an Updates with .chats directly; >=1.44 wraps it in a
        # ChatInviteJoinResultOk carrying the Updates under .updates.
        updates = getattr(result, "updates", result)
        chats = getattr(updates, "chats", None)
        if not chats:
            raise RuntimeError(
                f"joined {link!r} but could not read the chat from the "
                f"response ({type(result).__name__})"
            )
        return chats[0]

    async def _on_message(self, event: Any) -> None:
        msg = event.message
        if msg is None or msg.text is None:
            return
        await self._handler.process_text(
            msg.text, source=f"group:{event.chat_id}/msg:{msg.id}"
        )

    async def run_forever(self) -> None:
        try:
            await self.start()
        except Exception:
            logger.exception("group monitor failed to start")
            self._done.set()
            return
        try:
            await self._client.run_until_disconnected()
        finally:
            self._done.set()

    async def stop(self) -> None:
        try:
            await self._client.disconnect()
        except Exception:
            pass


def should_run(
    links: str | tuple[str, ...],
    api_id: int,
    api_hash: str,
    phone: str,
) -> bool:
    """True when the monitor is configured (at least one link plus the three
    credential fields present). Accepts a single link or an iterable."""
    if isinstance(links, str):
        links = (links,)
    has_link = any(str(link).strip() for link in links)
    return bool(has_link and api_id and api_hash.strip() and phone.strip())
