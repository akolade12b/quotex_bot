"""Wire the python-telegram-bot Application to the signal handler."""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from telegram.ext import Application, ApplicationBuilder, MessageHandler, filters
from telegram.request import HTTPXRequest

from telegram_signals.commands import register_commands
from telegram_signals.handlers import SignalHandler

logger = logging.getLogger(__name__)


def build_bot(
    token: str,
    queue: asyncio.Queue,
    *,
    allowed_chat_ids: tuple[int, ...] = (),
    allowlist_enabled: bool = True,
    base_amount: float = 10.0,
    dedup_window_s: float = 600.0,
    signal_expiry_s: int = 0,
    handler: SignalHandler | None = None,
    otp_bridge: object | None = None,
    telegram_auth_bridge: object | None = None,
    switch_mode: Callable[[bool], Awaitable[tuple[str, float]]] | None = None,
    status_fn: Callable[[], Awaitable[tuple[str, float]]] | None = None,
    auto_enabled: Callable[[], bool] | None = None,
    toggle_auto=None,
    channels_fn=None,
    connect_fn=None,
    details_fn=None,
    channel_choices_fn=None,
    toggle_channel=None,
) -> Application:
    """Build a Telegram Application that enqueues parsed signals onto ``queue``.

    ``switch_mode`` (Quotex demo/live) and ``status_fn`` (mode + balance) power
    the ``/mode`` and ``/balance`` commands; leave them ``None`` on the
    simulated broker and the commands just report status.

    ``handler`` may be a pre-built ``SignalHandler`` to share dedup state with
    the MTProto group monitor (so a signal seen both ways isn't double-traded);
    otherwise one is created here.
    """
    if not token:
        raise ValueError("telegram token required to build bot")

    if handler is None:
        handler = SignalHandler(
            queue,
            allowed_chat_ids=allowed_chat_ids,
            allowlist_enabled=allowlist_enabled,
            base_amount=base_amount,
            dedup_window_s=dedup_window_s,
            signal_expiry_s=signal_expiry_s,
            otp_bridge=otp_bridge,
            telegram_auth_bridge=telegram_auth_bridge,
        )
    elif otp_bridge is not None:
        # A shared handler (group monitor) — attach the bridge the same way.
        handler.otp_bridge = otp_bridge
    # PTB's default 5s request timeouts are too tight on slow/VPN networks
    # (get_me()/get_updates will throw TimedOut). Give the API client room.
    request = HTTPXRequest(
        read_timeout=30.0,
        write_timeout=30.0,
        connect_timeout=30.0,
        pool_timeout=10.0,
    )
    application = (
        ApplicationBuilder().token(token).request(request).build()
    )
    # Any text message could be a forwarded signal; non-signal text is
    # silently ignored by the parser.
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handler)
    )
    register_commands(
        application,
        switch_mode=switch_mode,
        status_fn=status_fn,
        allowed_chat_ids=allowed_chat_ids,
        allowlist_enabled=allowlist_enabled,
        auto_enabled=auto_enabled,
        toggle_auto=toggle_auto,
        channels_fn=channels_fn,
        connect_fn=connect_fn,
        details_fn=details_fn,
        channel_choices_fn=channel_choices_fn,
        toggle_channel=toggle_channel,
    )
    return application
