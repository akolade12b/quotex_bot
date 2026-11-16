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
import json
import logging
import os
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
        all_channels: bool = False,
        auth_bridge: Any = None,
        selection_path: str = "data/monitor_selection.json",
    ) -> None:
        self._handler = handler
        self._links = tuple(links)
        self._all_channels = all_channels
        self._auth_bridge = auth_bridge
        self._selection_path = selection_path
        self._monitored_ids: set[int] = set()
        self._handler_registered = False
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
    def channel_labels(self) -> tuple[str, ...]:
        """Human-readable channel names currently attached to the monitor."""
        return tuple(
            f"{getattr(target, 'title', None) or getattr(target, 'username', None) or 'channel'} "
            f"(id {getattr(target, 'id', '?')})"
            for target in self._targets
        )

    @property
    def channel_options(self) -> tuple[tuple[int, str, bool], ...]:
        """Return ``(id, label, selected)`` for every discovered channel."""
        return tuple(
            (int(getattr(target, "id", 0)),
             f"{getattr(target, 'title', None) or getattr(target, 'username', None) or 'channel'} "
             f"(id {getattr(target, 'id', '?')})",
             int(getattr(target, "id", 0)) in self._monitored_ids)
            for target in self._targets
        )

    @property
    def ready(self) -> asyncio.Event:
        return self._ready

    async def start(self) -> None:
        """Connect the user session, join/resolve every target group, and start
        listening. First run prompts for a Telegram login code on stdin; the
        session file auto-authenticates on later runs."""
        # Interactive on first login (code via stdin); 2FA password prompted
        # the same way if enabled. Runs on the bot's own asyncio loop.
        start_kwargs: dict[str, Any] = {"phone": self._phone}
        if self._auth_bridge is not None:
            start_kwargs["code_callback"] = self._auth_bridge.request_text
            start_kwargs["password"] = self._auth_bridge.request_text
        await self._client.start(**start_kwargs)
        if self._all_channels:
            # A bot token cannot enumerate a user's channels. The Telethon
            # user session can, so attach every broadcast channel in dialogs.
            dialogs = await self._client.get_dialogs()
            self._targets.extend(
                dialog.entity for dialog in dialogs
                if bool(getattr(dialog.entity, "broadcast", False))
            )
        else:
            for link in self._links:
                target = await self._resolve_target(link)
                self._targets.append(target)
        all_ids = {int(getattr(target, "id", 0)) for target in self._targets}
        self._monitored_ids = all_ids
        try:
            with open(self._selection_path, "r", encoding="utf-8") as fh:
                saved = {int(x) for x in json.load(fh)}
            if saved & all_ids:
                self._monitored_ids = saved & all_ids
        except (FileNotFoundError, ValueError, TypeError, OSError):
            pass
        await self._refresh_event_handler()
        logger.info(
            "group monitor listening on %s",
            ", ".join(f"{t!r} (id {getattr(t, 'id', '?')})" for t in self._targets),
        )
        self._ready.set()

    async def _refresh_event_handler(self) -> None:
        """Replace the Telethon subscription with the current selection."""
        if self._handler_registered:
            self._client.remove_event_handler(self._on_message)
            self._handler_registered = False
        targets = [
            target for target in self._targets
            if int(getattr(target, "id", 0)) in self._monitored_ids
        ]
        if targets:
            self._client.add_event_handler(self._on_message, events.NewMessage(chats=targets))
            self._handler_registered = True

    async def set_channel_monitoring(self, channel_id: int, enabled: bool) -> None:
        """Enable/disable one discovered channel and persist the selection."""
        ids = {int(getattr(target, "id", 0)) for target in self._targets}
        if channel_id not in ids:
            raise ValueError("channel is not in the discovered channel list")
        if enabled:
            self._monitored_ids.add(channel_id)
        else:
            self._monitored_ids.discard(channel_id)
        await self._refresh_event_handler()
        parent = os.path.dirname(os.path.abspath(self._selection_path))
        os.makedirs(parent, exist_ok=True)
        with open(self._selection_path, "w", encoding="utf-8") as fh:
            json.dump(sorted(self._monitored_ids), fh)

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
    all_channels: bool = False,
) -> bool:
    """True when the monitor is configured (at least one link plus the three
    credential fields present). Accepts a single link or an iterable."""
    if isinstance(links, str):
        links = (links,)
    has_link = any(str(link).strip() for link in links)
    return bool((has_link or all_channels) and api_id and api_hash.strip() and phone.strip())
