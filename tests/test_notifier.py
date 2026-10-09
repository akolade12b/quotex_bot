"""Unit tests for TradeNotifier message formatting and delivery."""

from __future__ import annotations

from broker.base import Direction, TradeResult, TradeStatus, utcnow
from telegram_signals.model import Signal
from telegram_signals.notifier import TradeNotifier


class _FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, *, chat_id, text) -> None:
        self.messages.append((chat_id, text))


def _trade(status: TradeStatus = TradeStatus.WIN, pnl: float = 8.5) -> TradeResult:
    return TradeResult(
        trade_id="t1",
        asset="EUR/CAD",
        direction=Direction.PUT,
        amount=10.0,
        entry_price=1.1000,
        expiry_s=300,
        opened_at=utcnow(),
        status=status,
        pnl=pnl,
    )


async def test_placed_message():
    bot = _FakeBot()
    notifier = TradeNotifier(bot, (123,))
    await notifier.on_placed(_trade(), level=0)
    chat, text = bot.messages[0]
    assert chat == 123
    assert "PLACED" in text
    assert "L0" in text
    assert "EUR/CAD" in text
    assert "PUT" in text
    assert "10.00" in text


async def test_win_message_shows_profit_and_balance():
    bot = _FakeBot()
    notifier = TradeNotifier(bot, (123,))
    await notifier.on_settled(
        _trade(status=TradeStatus.WIN, pnl=8.5), level=0, balance=10008.5
    )
    text = bot.messages[0][1]
    assert "✅" in text
    assert "WON" in text
    assert "Profit: +8.50" in text
    assert "Balance: 10,008.50" in text


async def test_loss_message_shows_negative_amount():
    bot = _FakeBot()
    notifier = TradeNotifier(bot, (123,))
    await notifier.on_settled(
        _trade(status=TradeStatus.LOSS, pnl=-10.0), level=1, balance=9990.0
    )
    text = bot.messages[0][1]
    assert "❌" in text
    assert "LOST" in text
    assert "L1" in text
    assert "Loss: -10.00" in text


async def test_draw_message():
    bot = _FakeBot()
    notifier = TradeNotifier(bot, (123,))
    await notifier.on_settled(
        _trade(status=TradeStatus.DRAW, pnl=0.0), level=0, balance=10000.0
    )
    text = bot.messages[0][1]
    assert "DRAW" in text
    assert "Profit: +0.00" in text


async def test_no_chat_ids_sends_nothing():
    bot = _FakeBot()
    notifier = TradeNotifier(bot, ())
    await notifier.on_placed(_trade(), level=0)
    await notifier.on_settled(_trade(), level=0, balance=1.0)
    assert bot.messages == []


async def test_rejected_message():
    bot = _FakeBot()
    notifier = TradeNotifier(bot, (123,))
    signal = Signal(asset="EUR/CAD", direction=Direction.PUT, expiry_seconds=300)
    await notifier.on_rejected(signal, "1 open positions >= max_concurrent_positions 1")
    text = bot.messages[0][1]
    assert "REJECTED" in text
    assert "EUR/CAD" in text
    assert "max_concurrent_positions" in text


async def test_reconciled_message():
    class _Row:
        asset = "EUR/CAD"
        level = 0

    bot = _FakeBot()
    notifier = TradeNotifier(bot, (123,))
    await notifier.on_reconciled(_Row())
    text = bot.messages[0][1]
    assert "STALE" in text
    assert "EUR/CAD" in text


async def test_send_failure_is_suppressed():
    class _BrokenBot:
        async def send_message(self, **kwargs):
            raise RuntimeError("network down")

    notifier = TradeNotifier(_BrokenBot(), (123,))
    await notifier.on_placed(_trade(), level=0)  # must not raise
