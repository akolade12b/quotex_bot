"""Push trade-lifecycle notifications to Telegram (placement + settlement).

Kept out of the engine so the engine stays broker/transport-agnostic; the
notifier is wired in main.py only when a Telegram token and chat ids are
configured.
"""

from __future__ import annotations

import logging

from broker.base import TradeResult, TradeStatus
from telegram_signals.model import Signal

logger = logging.getLogger(__name__)

_STATUS_LABEL: dict[TradeStatus, tuple[str, str]] = {
    TradeStatus.WIN:       ("✅", "WON"),
    TradeStatus.LOSS:      ("❌", "LOST"),
    TradeStatus.DRAW:      ("⚪", "DRAW"),
    TradeStatus.CANCELLED: ("⚠️", "CANCELLED"),
}


def _money(value: float) -> str:
    return f"{value:,.2f}"


class TradeNotifier:
    """Sends a Telegram message for each placed / settled trade."""

    def __init__(self, bot, chat_ids: tuple[int, ...]) -> None:
        self._bot = bot          # anything with async send_message(chat_id, text)
        self._chat_ids = chat_ids

    async def on_placed(self, result: TradeResult, level: int) -> None:
        text = (
            f"📈 PLACED · L{level} {result.asset} {result.direction.value.upper()}\n"
            f"Stake: {_money(result.amount)}"
        )
        await self._send(text)

    async def on_settled(self, result: TradeResult, level: int, balance: float) -> None:
        emoji, label = _STATUS_LABEL.get(result.status, ("ℹ️", result.status.value.upper()))
        if result.pnl >= 0:
            pnl_line = f"Profit: +{_money(result.pnl)}"
        else:
            pnl_line = f"Loss: {_money(result.pnl)}"
        text = (
            f"{emoji} {label} · L{level} {result.asset} {result.direction.value.upper()}\n"
            f"{pnl_line}\n"
            f"Balance: {_money(balance)}"
        )
        await self._send(text)

    async def on_rejected(self, signal: Signal, reason: str) -> None:
        text = (
            f"⚠️ REJECTED · {signal.asset} {signal.direction.value.upper()}\n"
            f"{reason}"
        )
        await self._send(text)

    async def on_reconciled(self, row) -> None:
        text = (
            f"🧹 REMOVED STALE POSITION · {row.asset} L{row.level}\n"
            f"Not open on broker — marked cancelled"
        )
        await self._send(text)

    async def _send(self, text: str) -> None:
        if not self._chat_ids:
            return
        for chat_id in self._chat_ids:
            try:
                await self._bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                logger.exception("failed to send Telegram notification to %s", chat_id)
