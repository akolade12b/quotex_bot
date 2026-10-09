"""Telegram command handlers: ``/mode`` and ``/balance``.

These let you operate the broker from Telegram. On the simulated broker only
``/balance`` shows anything useful; on the Quotex client ``/mode demo|live``
reconnects to the other account type. Both are gated by the same chat
allowlist as signal ingestion, so strangers can't flip your account type.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logger = logging.getLogger(__name__)

# switch_mode(demo: bool) -> (mode_label, balance); status_fn() -> (mode_label, balance)
ModeSwitch = Callable[[bool], Awaitable[tuple[str, float]]]
StatusFn = Callable[[], Awaitable[tuple[str, float]]]


def _authorized(chat_id: int, allowed: tuple[int, ...], enabled: bool) -> bool:
    if not enabled:
        return True
    return bool(allowed) and chat_id in allowed


async def _mode(update: Update, context: ContextTypes.DEFAULT_TYPE,
                *, switch_mode: ModeSwitch | None, status_fn: StatusFn,
                allowed: tuple[int, ...], enabled: bool) -> None:
    msg = update.effective_message
    if msg is None or not _authorized(msg.chat_id, allowed, enabled):
        return
    parts = (msg.text or "").split()
    if len(parts) >= 2:
        arg = parts[1].lower()
        if arg in ("demo", "live") and switch_mode is not None:
            try:
                label, balance = await switch_mode(arg == "live")
            except Exception as exc:  # surfaced to the user, not a crash
                await msg.reply_text(f"⚠️ Could not switch: {exc}")
                return
            await msg.reply_text(f"Quotex switched to **{label}** · balance **{balance:.2f}**")
            return
    try:
        label, balance = await status_fn()
    except Exception as exc:
        await msg.reply_text(f"⚠️ Status unavailable: {exc}")
        return
    await msg.reply_text(f"Mode: **{label}** · balance **{balance:.2f}**")


async def _balance(update: Update, context: ContextTypes.DEFAULT_TYPE,
                   *, status_fn: StatusFn, allowed: tuple[int, ...], enabled: bool) -> None:
    msg = update.effective_message
    if msg is None or not _authorized(msg.chat_id, allowed, enabled):
        return
    try:
        label, balance = await status_fn()
    except Exception as exc:
        await msg.reply_text(f"⚠️ Balance unavailable: {exc}")
        return
    await msg.reply_text(f"Mode: **{label}** · balance **{balance:.2f}**")


def register_commands(
    application: Application,
    *,
    switch_mode: ModeSwitch | None = None,
    status_fn: StatusFn | None = None,
    allowed_chat_ids: tuple[int, ...] = (),
    allowlist_enabled: bool = True,
) -> None:
    """Attach the command handlers to a built PTB application."""
    if status_fn is None:
        async def status_fn() -> tuple[str, float]:
            return "simulated", 0.0

    async def mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await _mode(update, context, switch_mode=switch_mode, status_fn=status_fn,
                    allowed=allowed_chat_ids, enabled=allowlist_enabled)

    async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await _balance(update, context, status_fn=status_fn,
                       allowed=allowed_chat_ids, enabled=allowlist_enabled)

    application.add_handler(CommandHandler("mode", mode))
    application.add_handler(CommandHandler("balance", balance))
