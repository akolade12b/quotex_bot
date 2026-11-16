"""Telegram command handlers and the inline control panel.

These let you operate the broker from Telegram. On the simulated broker only
``/balance`` shows anything useful; on the Quotex client ``/mode demo|live``
reconnects to the other account type. Both are gated by the same chat
allowlist as signal ingestion, so strangers can't flip your account type.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

logger = logging.getLogger(__name__)

# switch_mode(demo: bool) -> (mode_label, balance); status_fn() -> (mode_label, balance)
ModeSwitch = Callable[[bool], Awaitable[tuple[str, float]]]
StatusFn = Callable[[], Awaitable[tuple[str, float]]]
ToggleFn = Callable[[bool], Awaitable[bool]]
DetailsFn = Callable[[], Awaitable[str]]
ChannelChoicesFn = Callable[[], list[tuple[int, str, bool]]]
ToggleChannelFn = Callable[[int, bool], Awaitable[None]]


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


def _panel_text(label: str, balance: float | None, auto_on: bool, channels: str,
                details: str = "") -> str:
    balance_text = "unavailable" if balance is None else f"{balance:.2f}"
    return (
        "🤖 *Quotex Signal Bot*\n\n"
        f"Broker: `{label}`\n"
        f"Balance: `{balance_text}`\n"
        f"{details}"
        f"Auto trading: `{'ON' if auto_on else 'OFF'}`\n\n"
        "Commands: /start · /channels · /balance · /mode · /mode demo · /mode live\n\n"
        f"{channels}"
    )


def panel_markup(auto_on: bool, live: bool) -> InlineKeyboardMarkup:
    auto_label = "⏹ Turn auto trading OFF" if auto_on else "▶️ Turn auto trading ON"
    mode_label = "🟢 Switch to DEMO" if live else "🔴 Switch to LIVE"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="qsb:refresh"),
         InlineKeyboardButton("💰 Balance", callback_data="qsb:balance")],
        [InlineKeyboardButton(auto_label, callback_data=f"qsb:auto:{'off' if auto_on else 'on'}")],
        [InlineKeyboardButton(mode_label, callback_data=f"qsb:mode:{'demo' if live else 'live'}")],
        [InlineKeyboardButton("📡 Monitored channels", callback_data="qsb:channels")],
    ])


def channel_markup(choices: list[tuple[int, str, bool]]) -> InlineKeyboardMarkup:
    rows = []
    for channel_id, label, selected in choices:
        short_label = label.split(" (id ", 1)[0]
        rows.append([InlineKeyboardButton(
            f"{'✅' if selected else '⬜'} {short_label}",
            callback_data=f"qsb:channel:{channel_id}:{'off' if selected else 'on'}",
        )])
    return InlineKeyboardMarkup(
        rows or [[InlineKeyboardButton("No channels discovered", callback_data="qsb:refresh")]]
    )


def register_commands(
    application: Application,
    *,
    switch_mode: ModeSwitch | None = None,
    status_fn: StatusFn | None = None,
    allowed_chat_ids: tuple[int, ...] = (),
    allowlist_enabled: bool = True,
    auto_enabled: Callable[[], bool] | None = None,
    toggle_auto: ToggleFn | None = None,
    channels_fn: Callable[[], str] | None = None,
    connect_fn: Callable[[], str] | None = None,
    details_fn: DetailsFn | None = None,
    channel_choices_fn: ChannelChoicesFn | None = None,
    toggle_channel: ToggleChannelFn | None = None,
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

    async def channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = update.effective_message
        if msg is None or not _authorized(msg.chat_id, allowed_chat_ids, allowlist_enabled):
            return
        await msg.reply_text(channels_text(), reply_markup=channels_markup())

    def is_auto_on() -> bool:
        return auto_enabled() if auto_enabled is not None else True

    def channels_text() -> str:
        if channel_choices_fn is not None:
            choices = channel_choices_fn()
            if choices:
                return "📡 Monitored channels:\n" + "\n".join(
                    f"{'✅' if selected else '⬜'} {label}"
                    for _, label, selected in choices
                )
        return channels_fn() if channels_fn is not None else "📡 Monitored channels: not configured"

    def channels_markup() -> InlineKeyboardMarkup:
        return channel_markup(channel_choices_fn() if channel_choices_fn is not None else [])

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        msg = update.effective_message
        if msg is None or not _authorized(msg.chat_id, allowed_chat_ids, allowlist_enabled):
            return
        try:
            label, balance_value = await status_fn()
        except Exception:
            label, balance_value = "starting", None
        try:
            details = await details_fn() if details_fn is not None else ""
        except Exception as exc:
            details = f"Account balances: unavailable ({exc})\n"
        await msg.reply_text(
            _panel_text(label, balance_value, is_auto_on(), channels_text(), details),
            parse_mode="Markdown",
            reply_markup=panel_markup(is_auto_on(), label.endswith("-live")),
        )

    async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None or query.message is None:
            return
        if not _authorized(query.message.chat_id, allowed_chat_ids, allowlist_enabled):
            await query.answer("Not authorized", show_alert=True)
            return
        await query.answer()
        parts = (query.data or "").split(":")
        action = parts[1] if len(parts) > 1 else "refresh"
        value = parts[2] if len(parts) > 2 else ""
        if action == "auto" and toggle_auto is not None:
            await toggle_auto(value == "on")
        elif action == "mode" and switch_mode is not None:
            try:
                await switch_mode(value == "demo")
            except Exception as exc:
                await query.answer(f"Could not switch: {exc}", show_alert=True)
                return
        elif action == "mode":
            await query.answer(
                "Quotex demo/live switching requires QSB_BROKER_MODE=quotex.",
                show_alert=True,
            )
            return
        elif action == "channels":
            await query.message.reply_text(channels_text(), reply_markup=channels_markup())
            return
        elif action == "channel" and toggle_channel is not None and len(parts) >= 4:
            try:
                await toggle_channel(int(value), parts[3] == "on")
            except Exception as exc:
                await query.answer(f"Could not update channel: {exc}", show_alert=True)
                return
            await query.edit_message_text(
                channels_text(), reply_markup=channels_markup()
            )
            return
        try:
            label, balance_value = await status_fn()
        except Exception:
            label, balance_value = "starting", None
        try:
            details = await details_fn() if details_fn is not None else ""
        except Exception as exc:
            details = f"Account balances: unavailable ({exc})\n"
        await query.edit_message_text(
            _panel_text(label, balance_value, is_auto_on(), channels_text(), details),
            parse_mode="Markdown",
            reply_markup=panel_markup(is_auto_on(), label.endswith("-live")),
        )

    application.add_handler(CommandHandler("mode", mode))
    application.add_handler(CommandHandler("balance", balance))
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("channels", channels))
    application.add_handler(CallbackQueryHandler(callback, pattern=r"^qsb:"))
