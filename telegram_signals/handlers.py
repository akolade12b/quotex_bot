"""Telegram message handlers. The handler's only job is to parse a forwarded
signal, gate it by chat id, and enqueue it — it never touches the broker, so
Telegram latency can't block trading."""

from __future__ import annotations

import asyncio
import logging
import time as _time

from telegram import Update
from telegram.ext import ContextTypes

from telegram_signals.model import Signal, SignalParseError
from telegram_signals.parser import SignalParser

logger = logging.getLogger(__name__)


class SignalHandler:
    """Parses forwarded MagicTrader signals and queues them for the engine."""

    def __init__(
        self,
        queue: asyncio.Queue[Signal],
        *,
        allowed_chat_ids: tuple[int, ...] = (),
        allowlist_enabled: bool = True,
        base_amount: float = 10.0,
        dedup_window_s: float = 600.0,
        signal_expiry_s: int = 0,
        parser: SignalParser | None = None,
        otp_bridge: object | None = None,
        telegram_auth_bridge: object | None = None,
    ) -> None:
        self.queue = queue
        self.allowed_chat_ids = allowed_chat_ids
        self.allowlist_enabled = allowlist_enabled
        self.base_amount = base_amount
        self.dedup_window_s = dedup_window_s
        self.parser = parser or SignalParser(override_expiry_s=signal_expiry_s)
        # Optional Telegram OTP bridge: while Quotex login awaits a PIN, a
        # message is routed to submit_pin() instead of being parsed as a signal.
        self.otp_bridge = otp_bridge
        self.telegram_auth_bridge = telegram_auth_bridge
        # signal characteristics -> monotonic timestamp of last queue, used to
        # drop duplicate re-forwards of the same pending signal.
        self._seen: dict[tuple, float] = {}

    @staticmethod
    def _signal_key(signal: Signal) -> tuple:
        entry = signal.entry_time.strftime("%H:%M") if signal.entry_time else "now"
        return (signal.asset, signal.direction, signal.expiry_seconds, entry, signal.gale_count)

    def _prune_seen(self, now: float) -> None:
        """Drop entries older than the dedup window so _seen stays bounded."""
        stale = [k for k, ts in self._seen.items() if now - ts >= self.dedup_window_s]
        for k in stale:
            del self._seen[k]

    async def __call__(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self.handle_signal_message(update, context)

    async def handle_signal_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        message = update.effective_message
        if message is None or message.text is None:
            return
        chat_id = message.chat_id

        if self.allowlist_enabled:
            if not self.allowed_chat_ids:
                logger.info("allowlist enabled but no chat ids configured — denying all")
                return
            if chat_id not in self.allowed_chat_ids:
                logger.info("ignoring signal from unlisted chat %s", chat_id)
                return

        # An OTP reply (2FA PIN for Quotex login) is consumed here, before the
        # text is ever tried as a signal.
        if self.otp_bridge is not None and self.otp_bridge.submit_pin(message.text):
            logger.info("consumed OTP reply from chat %s (%.20r)", chat_id, message.text)
            return
        if (self.telegram_auth_bridge is not None
                and self.telegram_auth_bridge.submit_pin(message.text)):
            logger.info("consumed Telegram login reply from chat %s", chat_id)
            return

        await self.process_text(
            message.text, source=f"chat:{chat_id}/msg:{message.message_id}"
        )

    async def process_text(self, text: str, *, source: str) -> None:
        """Parse, dedup, and enqueue a signal message.

        Shared by the PTB message handler and the MTProto group monitor so both
        use the same parser and dedup window — a signal read twice (forwarded
        and by the monitor) is dropped, never double-traded.
        """
        try:
            signal = self.parser.parse(text, source=source)
        except SignalParseError as exc:
            logger.info("unparsable message from %s: %s", source, exc)
            return

        # For scheduled signals (entry_time set) skip identical re-forwards
        # within the dedup window — the same trade is already pending.
        # Immediate signals (entry_time=None) rely on the asset cooldown.
        if signal.entry_time is not None:
            key = self._signal_key(signal)
            now = _time.monotonic()
            last = self._seen.get(key)
            if last is not None and now - last < self.dedup_window_s:
                logger.info(
                    "duplicate scheduled signal for %s at %s within %.0fs window — skipping",
                    signal.asset, signal.entry_time.strftime("%H:%M"), self.dedup_window_s,
                )
                return
            self._seen[key] = now
            self._prune_seen(now)

        await self.queue.put(signal)
        logger.info(
            "queued signal for %s (L0 amount %.0f, gale_count %d)",
            signal.asset, self.base_amount, signal.gale_count,
        )
