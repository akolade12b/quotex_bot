"""Bridge Quotex's login OTP prompt to Telegram.

Quotex can ask for a one-time PIN during email/password login (2FA). pyquotex
handles this by calling an ``on_otp_callback`` that returns the code; in a
terminal bot the code comes from ``input()``. This bot has no terminal
session, so the code must come from the user over Telegram: the bridge sends a
prompt to the allowed chats and blocks on a ``Future``; the user's next message
is routed to :meth:`submit_pin` by the signal handler *before* it is tried as a
signal, resolving the Future and letting the login continue.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from broker.base import BrokerAuthError

logger = logging.getLogger(__name__)


class OtpBridge:
    """One-shot OTP wait: request a code over Telegram, resolve on reply."""

    def __init__(self, timeout_s: float = 180.0) -> None:
        self.timeout_s = timeout_s
        # Set by main.py once the Telegram bot is up: sends the OTP prompt.
        # Logged fallback keeps a terminal-session flow possible.
        self.send_prompt: Callable[[str], Awaitable[None]] | None = None
        self._pending: asyncio.Future[str] | None = None

    @property
    def awaiting(self) -> bool:
        return self._pending is not None and not self._pending.done()

    async def request_pin(self, prompt: str) -> str:
        """Send ``prompt`` over Telegram and wait for the user's PIN reply.

        Returns the submitted code. Raises ``BrokerAuthError`` on timeout or a
        non-numeric reply (the login fails and the caller may retry, which
        sends a fresh prompt).
        """
        code = await self.request_text(prompt)
        if not code.isdigit():
            raise BrokerAuthError(f"OTP reply was not a numeric PIN: {code!r}")
        return code

    async def request_text(self, prompt: str = "Enter the Telegram login code: ") -> str:
        """Send a Telegram prompt and wait for arbitrary text.

        Used for Telethon's Telegram login code and optional 2FA password.
        """
        if self.awaiting:
            raise BrokerAuthError("authentication input already pending — reply in Telegram")
        fut: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._pending = fut
        try:
            if self.send_prompt is not None:
                try:
                    await self.send_prompt(prompt)
                except Exception:
                    logger.exception("failed to send OTP prompt over Telegram")
            else:
                logger.warning(
                    "Quotex OTP required, but no Telegram prompt channel is "
                    "wired — the login will wait %ss for a reply nobody can send",
                    self.timeout_s,
                )
            code = await asyncio.wait_for(fut, timeout=self.timeout_s)
        except asyncio.TimeoutError:
            self._pending = None
            raise BrokerAuthError(
                "OTP prompt timed out — no reply within %.0fs", self.timeout_s
            ) from None
        finally:
            self._pending = None
        return str(code).strip()

    def submit_pin(self, text: str) -> bool:
        """Resolve a pending OTP wait with ``text``. Returns True if consumed
        (the message was an OTP reply, not a signal)."""
        if self._pending is not None and not self._pending.done():
            self._pending.set_result(text)
            return True
        return False
