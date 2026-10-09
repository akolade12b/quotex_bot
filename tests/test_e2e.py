"""End-to-end tests: raw forwarded message -> handler -> queue -> engine ->
simulated broker -> martingale ladder, plus rejection paths. No network."""

from __future__ import annotations

import asyncio

import pytest

from broker.base import Direction, TradeStatus
from execution.engine import ExecutionEngine
from execution.risk import RiskConfig, RiskManager
from persistence.repository import TradeRepository
from telegram_signals.handlers import SignalHandler

SAMPLE_TEXT = (
    "💰5-minute expiration\n"
    "USD/IDR; 08:55; PUT 🟥\n"
    "🕐 TIME TO 09:00\n"
    "1st GALE —> TIME TO 09:05\n"
    "2nd GALE —> TIME TO 09:10\n"
    "📲 Click to open the broker (https://t.me/Magictradereng_bot?startapp)"
)


class FakeMessage:
    def __init__(self, text: str, chat_id: int, message_id: int) -> None:
        self.text = text
        self.chat_id = chat_id
        self.message_id = message_id


class FakeUpdate:
    def __init__(self, message: FakeMessage) -> None:
        self.message = message
        self.effective_message = message


def _msg(text: str = SAMPLE_TEXT, chat_id: int = 111, msg_id: int = 1) -> FakeUpdate:
    return FakeUpdate(FakeMessage(text, chat_id, msg_id))


async def _pipeline(broker, repo, chat_id=111, **risk_kwargs):
    """Wire handler -> queue -> engine exactly like app.main, minus Telegram."""
    queue: asyncio.Queue = asyncio.Queue()
    handler = SignalHandler(queue, allowed_chat_ids=(chat_id,), base_amount=10.0)
    engine = ExecutionEngine(broker, repo, RiskManager(RiskConfig(**risk_kwargs), repo))
    return queue, handler, engine


async def test_signal_flows_chat_to_broker(broker, repo):
    queue, handler, engine = await _pipeline(broker, repo)
    await handler(_msg(), None)
    signal = await queue.get()
    assert signal.asset == "USD/IDR"
    assert signal.direction is Direction.PUT
    assert signal.gale_count == 2
    assert signal.source == "chat:111/msg:1"

    await engine.on_signal(signal, wait_for_entry=False)
    rows = await repo.list_trades()
    assert len(rows) == 1
    assert rows[0].level == 0
    assert rows[0].status is TradeStatus.OPEN


async def test_loss_drives_full_ladder_to_win(broker, repo):
    queue, handler, engine = await _pipeline(broker, repo)
    await handler(_msg(), None)
    await engine.on_signal(await queue.get(), wait_for_entry=False)

    entry = (await repo.list_trades())[0]
    # two losses -> gale1 (2x), gale2 (4x) -> win ends the ladder
    for expected_level, expected_amount in ((1, 20.0), (2, 40.0)):
        await broker.settle_open_now(exit_price=entry.entry_price + 1.0)
        await engine._on_settled(
            entry, await broker.wait_for_settlement(entry.broker_trade_id, 0.05)
        )
        entry = (await repo.list_trades())[0]
        assert entry.level == expected_level
        assert entry.amount == pytest.approx(expected_amount)
        assert entry.status is TradeStatus.OPEN

    await broker.settle_open_now(exit_price=entry.entry_price - 1.0)
    await engine._on_settled(
        entry, await broker.wait_for_settlement(entry.broker_trade_id, 0.05)
    )
    rows = await repo.list_trades()
    assert len(rows) == 3  # no 3rd gale after a win
    assert rows[0].status is TradeStatus.WIN
    assert rows[0].pnl == pytest.approx(40.0 * 0.85)

    state = await repo.get_risk_state()
    assert state.trade_count == 3
    assert state.realized_loss == pytest.approx(10.0 + 20.0)


async def test_duplicate_scheduled_signal_deduped(broker, repo):
    """Re-forwarding the same scheduled signal within the dedup window only
    queues it once — the scenario that would otherwise stack 4 ladders."""
    queue, handler, engine = await _pipeline(broker, repo)

    await handler(_msg(msg_id=1), None)
    await handler(_msg(msg_id=2), None)  # identical re-forward, different msg id
    assert queue.qsize() == 1

    # a genuinely different signal (different asset) is not a duplicate
    await handler(_msg(text=SAMPLE_TEXT.replace("USD/IDR", "EUR/USD"), msg_id=3), None)
    assert queue.qsize() == 2


async def test_back_to_back_distinct_signals_both_trade(broker, repo):
    """Two different signals given in sequence: each is queued and each gets
    traded. Dedup only drops identical re-forwards, never a new trade."""
    queue, handler, engine = await _pipeline(broker, repo)

    await handler(_msg(msg_id=1), None)
    await handler(_msg(text=SAMPLE_TEXT.replace("USD/IDR", "EUR/USD"), msg_id=2), None)
    assert queue.qsize() == 2

    await engine.on_signal(await queue.get(), wait_for_entry=False)
    await engine.on_signal(await queue.get(), wait_for_entry=False)

    rows = await repo.list_trades()
    assert len(rows) == 2
    assert {r.asset for r in rows} == {"USD/IDR", "EUR/USD"}
    assert all(r.status is TradeStatus.OPEN for r in rows)


async def test_dedup_disabled_with_zero_window(broker, repo):
    queue = asyncio.Queue()
    handler = SignalHandler(queue, allowed_chat_ids=(111,), dedup_window_s=0.0)
    await handler(_msg(msg_id=1), None)
    await handler(_msg(msg_id=2), None)
    assert queue.qsize() == 2


async def test_immediate_signal_not_deduped(broker, repo):
    """Signals without an entry time (immediate) bypass the dedup window — they
    rely on the asset cooldown instead, so two distinct immediate signals both
    queue."""
    queue = asyncio.Queue()
    handler = SignalHandler(queue, allowed_chat_ids=(111,))
    # no HH:MM anywhere -> parser yields entry_time=None (immediate)
    immediate = "5-minute expiration\nUSD/IDR; PUT 🟥\n"
    await handler(_msg(text=immediate, msg_id=1), None)
    await handler(_msg(text=immediate, msg_id=2), None)
    assert queue.qsize() == 2


async def test_rejection_flow_all_the_way(broker, repo):
    """Unlisted chat -> never queued. Allowlisted chat + disallowed asset ->
    rejected at the risk layer and recorded on the signal row."""
    queue, handler, engine = await _pipeline(broker, repo)

    await handler(_msg(chat_id=999), None)  # not allowlisted
    assert queue.empty()

    await handler(_msg(), None)
    assert not queue.empty()
    signal = await queue.get()
    assert signal is not None

    # replay with an asset-blocking risk config to exercise the reject path
    engine2 = ExecutionEngine(
        broker, repo, RiskManager(RiskConfig(asset_allowlist=("EUR/USD",)), repo)
    )
    await engine2.on_signal(signal, wait_for_entry=False)
    sig = await repo.get_signal(1)
    assert sig.parse_status == "rejected"
    assert "not allowlisted" in sig.reject_reason
    assert await repo.list_trades() == []


async def test_daily_loss_limit_stops_gale(broker, repo):
    """Once the daily loss cap is hit, even the auto-gale is denied."""
    # cap of 25: entry loss (10) still allows gale1; adding gale1's loss (20)
    # pushes realized_loss to 30 >= 25, so gale2 must be denied.
    queue, handler, engine = await _pipeline(broker, repo, max_daily_loss=25.0)
    await handler(_msg(), None)
    await engine.on_signal(await queue.get(), wait_for_entry=False)

    entry = (await repo.list_trades())[0]
    await broker.settle_open_now(exit_price=entry.entry_price + 1.0)
    await engine._on_settled(
        entry, await broker.wait_for_settlement(entry.broker_trade_id, 0.05)
    )
    gale1 = (await repo.list_trades())[0]
    assert gale1.level == 1  # first gale still allowed

    await broker.settle_open_now(exit_price=gale1.entry_price + 1.0)
    await engine._on_settled(
        gale1, await broker.wait_for_settlement(gale1.broker_trade_id, 0.05)
    )
    rows = await repo.list_trades()
    assert len(rows) == 2  # gale2 was denied — no new trade row
    assert rows[0].status is TradeStatus.LOSS
