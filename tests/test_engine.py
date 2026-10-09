import pytest
from datetime import datetime, timedelta, time as dtime, timezone

from broker.base import BrokerUnavailable, Direction, TradeResult, TradeStatus, utcnow
from execution.engine import ExecutionEngine
from execution.risk import RiskConfig, RiskManager
from telegram_signals.model import Signal


def _engine(broker, repo, **risk_kwargs) -> ExecutionEngine:
    return ExecutionEngine(broker, repo, RiskManager(RiskConfig(**risk_kwargs), repo))


async def test_win_ends_ladder(broker, repo):
    engine = _engine(broker, repo)
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=2),
        wait_for_entry=False,
    )
    rows = await repo.list_trades()
    assert len(rows) == 1
    assert rows[0].level == 0
    assert rows[0].amount == 10.0
    assert rows[0].status is TradeStatus.OPEN

    # PUT wins when price falls
    await broker.settle_open_now(exit_price=rows[0].entry_price - 1.0)
    result = await broker.wait_for_settlement(rows[0].broker_trade_id, timeout_s=0.05)
    await engine._on_settled(rows[0], result)

    rows = await repo.list_trades()
    assert len(rows) == 1  # no gale after a win
    assert rows[0].status is TradeStatus.WIN


async def test_loss_triggers_gale_then_win(broker, repo):
    engine = _engine(broker, repo)
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=2),
        wait_for_entry=False,
    )
    rows = await repo.list_trades()
    entry = rows[0]

    # PUT loses when price rises
    await broker.settle_open_now(exit_price=entry.entry_price + 1.0)
    result = await broker.wait_for_settlement(entry.broker_trade_id, timeout_s=0.05)
    await engine._on_settled(entry, result)

    rows = await repo.list_trades()
    assert len(rows) == 2  # entry + 1st gale
    gale = rows[0]  # newest first
    assert gale.level == 1
    assert gale.amount == pytest.approx(20.0)
    assert gale.status is TradeStatus.OPEN

    # win the gale → ladder ends
    await broker.settle_open_now(exit_price=gale.entry_price - 1.0)
    result2 = await broker.wait_for_settlement(gale.broker_trade_id, timeout_s=0.05)
    await engine._on_settled(gale, result2)

    rows = await repo.list_trades()
    assert len(rows) == 2  # no 2nd gale
    assert rows[0].status is TradeStatus.WIN


async def test_two_losses_then_win_uses_second_gale(broker, repo):
    engine = _engine(broker, repo)
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=2),
        wait_for_entry=False,
    )
    entry = (await repo.list_trades())[0]
    await broker.settle_open_now(exit_price=entry.entry_price + 1.0)
    await engine._on_settled(entry, await broker.wait_for_settlement(entry.broker_trade_id, 0.05))

    gale1 = (await repo.list_trades())[0]
    await broker.settle_open_now(exit_price=gale1.entry_price + 1.0)
    await engine._on_settled(gale1, await broker.wait_for_settlement(gale1.broker_trade_id, 0.05))

    gale2 = (await repo.list_trades())[0]
    assert gale2.level == 2
    assert gale2.amount == pytest.approx(40.0)

    await broker.settle_open_now(exit_price=gale2.entry_price - 1.0)
    await engine._on_settled(gale2, await broker.wait_for_settlement(gale2.broker_trade_id, 0.05))
    rows = await repo.list_trades()
    assert len(rows) == 3
    assert rows[0].status is TradeStatus.WIN


async def test_gale_count_zero_means_no_martingale(broker, repo):
    engine = _engine(broker, repo)
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    entry = (await repo.list_trades())[0]
    await broker.settle_open_now(exit_price=entry.entry_price + 1.0)
    await engine._on_settled(entry, await broker.wait_for_settlement(entry.broker_trade_id, 0.05))
    assert len(await repo.list_trades()) == 1  # loss but no gale configured


async def test_config_max_gales_caps_signal_ladder(broker, repo):
    engine = _engine(broker, repo, max_gales=1)
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=5),
        wait_for_entry=False,
    )
    entry = (await repo.list_trades())[0]
    await broker.settle_open_now(exit_price=entry.entry_price + 1.0)
    await engine._on_settled(entry, await broker.wait_for_settlement(entry.broker_trade_id, 0.05))

    gale1 = (await repo.list_trades())[0]
    assert gale1.level == 1
    await broker.settle_open_now(exit_price=gale1.entry_price + 1.0)
    await engine._on_settled(gale1, await broker.wait_for_settlement(gale1.broker_trade_id, 0.05))

    # config max_gales=1 → no 2nd gale even though signal allows 5
    assert len(await repo.list_trades()) == 2


async def test_settlement_loop_marks_ghost_trade_cancelled(broker, repo):
    """A trade row the broker has no record of (stale from a previous process,
    or an order the server never acknowledged) must be resolved locally once,
    not polled into a log-spam loop."""
    from broker.base import TradeResult, utcnow

    ghost = TradeResult(
        trade_id="ghost-1",
        asset="USD/IDR",
        direction=Direction.PUT,
        amount=10.0,
        entry_price=15400.0,
        expiry_s=60,
        opened_at=utcnow(),
    )
    await repo.insert_trade(ghost)  # status=open, but the broker never saw it

    engine = _engine(broker, repo)
    await engine.run_settlement_loop_once(poll_s=0.01)

    rows = await repo.list_trades()
    assert len(rows) == 1
    assert rows[0].status is TradeStatus.CANCELLED

    # A second pass must not re-process it — it's no longer open.
    await engine.run_settlement_loop_once(poll_s=0.01)
    assert len(await repo.list_trades()) == 1
    assert (await repo.list_trades())[0].status is TradeStatus.CANCELLED


def test_seconds_until_applies_entry_time_offset():
    now = datetime(2026, 8, 8, 12, 16, tzinfo=timezone.utc)
    # signal 08:50 + 4h offset -> 12:50, which is 34 min after "now" (12:16)
    d = ExecutionEngine._seconds_until(dtime(8, 50), 4 * 3600, now=now)
    assert d == pytest.approx(34 * 60)


def test_seconds_until_without_offset_past_time():
    now = datetime(2026, 8, 8, 12, 16, tzinfo=timezone.utc)
    d = ExecutionEngine._seconds_until(dtime(8, 50), 0.0, now=now)
    assert d < 0  # genuinely in the past -> late/skip path


def test_seconds_until_offset_crossing_midnight_trades_soon():
    """A 20:00+ signal on a +4h machine crosses midnight (20:52 -> 00:52).
    It must land ~now/minutes away, NOT be pushed to tomorrow."""
    lagos = timezone(timedelta(hours=1))
    now = datetime(2026, 8, 10, 0, 51, tzinfo=lagos)
    d = ExecutionEngine._seconds_until(dtime(20, 52), 4 * 3600, now=now)
    assert d == pytest.approx(60.0)  # trade in 1 minute


def test_seconds_until_long_lead_keeps_today_not_tomorrow():
    """A signal with a long lead (entry 08:53 provider = 12:53 machine, received
    at 00:52) is 12h out.  It must stay TODAY (12h), not be doubled to 36h."""
    lagos = timezone(timedelta(hours=1))
    now = datetime(2026, 8, 10, 0, 52, tzinfo=lagos)
    d = ExecutionEngine._seconds_until(dtime(8, 53), 4 * 3600, now=now)
    assert d == pytest.approx(12 * 3600 + 60, abs=1)


def test_seconds_until_offset_wraps_past_midnight_into_next_day():
    """entry 23:58 + 4h offset wraps to 03:58 the same day when it's still
    ahead of now — no spurious +1 day."""
    lagos = timezone(timedelta(hours=1))
    now = datetime(2026, 8, 10, 0, 48, tzinfo=lagos)
    d = ExecutionEngine._seconds_until(dtime(23, 58), 4 * 3600, now=now)
    assert d == pytest.approx(3 * 3600 + 600, abs=1)  # 03:58 - 00:48 = 3h10m


async def test_rejected_signal_recorded(broker, repo):
    engine = _engine(broker, repo, asset_allowlist=("EUR/USD",))
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    sig = await repo.get_signal(1)
    assert sig.parse_status == "rejected"
    assert "not allowlisted" in sig.reject_reason
    assert await repo.list_trades() == []


async def test_engine_calls_placed_and_settled_callbacks(broker, repo):
    calls = []

    async def on_placed(result, level):
        calls.append(("placed", level, result.asset))

    async def on_settled(result, level, balance):
        calls.append(("settled", level, result.status.value, balance))

    engine = ExecutionEngine(
        broker, repo, RiskManager(RiskConfig(), repo),
        on_trade_placed=on_placed, on_trade_settled=on_settled,
    )
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    assert calls == [("placed", 0, "USD/IDR")]

    entry = (await repo.list_trades())[0]
    await broker.settle_open_now(exit_price=entry.entry_price - 1.0)  # PUT wins
    await engine._on_settled(entry, await broker.wait_for_settlement(entry.broker_trade_id, 0.05))

    assert calls[-1][0] == "settled"
    assert calls[-1][1] == 0
    assert calls[-1][2] == "win"
    assert calls[-1][3] == pytest.approx(10008.5)  # 10000 - 10 stake + 18.5 payout


async def test_engine_calls_rejected_callback(broker, repo):
    calls = []

    async def on_rejected(signal, reason):
        calls.append((signal.asset, reason))

    engine = ExecutionEngine(
        broker, repo, RiskManager(RiskConfig(asset_allowlist=("EUR/USD",)), repo),
        on_trade_rejected=on_rejected,
    )
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    assert len(calls) == 1
    assert calls[0][0] == "USD/IDR"
    assert "not allowlisted" in calls[0][1]


async def test_notification_failure_does_not_break_trade(broker, repo):
    """A throwing notification callback must not stop the placement."""
    async def on_placed(result, level):
        raise RuntimeError("telegram down")

    engine = ExecutionEngine(
        broker, repo, RiskManager(RiskConfig(), repo), on_trade_placed=on_placed,
    )
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    assert len(await repo.list_trades()) == 1  # trade still placed


async def test_reconcile_cancels_stale_open_row_before_new_signal(broker, repo):
    """A DB-open row the broker no longer has (an order the server silently
    dropped) must be cancelled before the next signal is gated — otherwise the
    stale row blocks the new signal via max_concurrent_positions forever."""
    reconciled: list[tuple[str, int]] = []

    async def on_reconciled(row):
        reconciled.append((row.asset, row.level))

    stale = TradeResult(
        trade_id="stale-1",
        asset="USD/IDR",
        direction=Direction.PUT,
        amount=10.0,
        entry_price=15400.0,
        expiry_s=60,
        opened_at=utcnow() - timedelta(minutes=10),  # outside the freshness grace
    )
    await repo.insert_trade(stale)  # open in DB, but the broker has nothing

    engine = ExecutionEngine(
        broker, repo, RiskManager(RiskConfig(), repo),
        on_trade_reconciled=on_reconciled,
    )
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    rows = await repo.list_trades()
    assert len(rows) == 2
    assert rows[1].status is TradeStatus.CANCELLED  # stale row reconciled
    assert rows[0].status is TradeStatus.OPEN       # new signal placed anyway
    assert ("USD/IDR", 0) in reconciled             # user notified


async def test_reconcile_skips_fresh_rows(broker, repo):
    """A recently-placed DB row must not be cancelled even if the broker's
    pending/list hasn't reflected it yet (freshness grace)."""
    fresh = TradeResult(
        trade_id="fresh-1",
        asset="USD/IDR",
        direction=Direction.PUT,
        amount=10.0,
        entry_price=15400.0,
        expiry_s=60,
        opened_at=utcnow(),  # just placed
    )
    await repo.insert_trade(fresh)

    engine = _engine(broker, repo)
    await engine.on_signal(
        Signal(asset="EUR/USD", direction=Direction.CALL, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    rows = await repo.list_trades()
    statuses = {r.broker_trade_id: r.status for r in rows}
    assert statuses["fresh-1"] is TradeStatus.OPEN  # left alone by reconcile
    assert any(r.asset == "EUR/USD" and r.status is TradeStatus.OPEN for r in rows)


async def test_on_trade_recovered_loss_triggers_gale(broker, repo):
    """A trade recovered as LOSS after a disconnect must immediately place the
    next martingale level — the whole point of recovery."""
    engine = _engine(broker, repo)
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=2),
        wait_for_entry=False,
    )
    entry = (await repo.list_trades())[0]

    recovered = TradeResult(
        trade_id=entry.broker_trade_id,
        asset=entry.asset,
        direction=entry.direction,
        amount=entry.amount,
        entry_price=entry.entry_price,
        expiry_s=entry.expiry_s,
        opened_at=entry.opened_at,
        status=TradeStatus.LOSS,
        pnl=-entry.amount,
    )
    await engine.on_trade_recovered(recovered)

    rows = await repo.list_trades()
    assert len(rows) == 2                    # entry + 1st gale
    assert rows[0].level == 1
    assert rows[0].amount == pytest.approx(20.0)
    assert rows[0].status is TradeStatus.OPEN
    assert rows[1].status is TradeStatus.LOSS


async def test_on_trade_recovered_win_records_no_gale(broker, repo):
    """A recovered WIN updates the DB with the real outcome and does NOT gale."""
    engine = _engine(broker, repo)
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=2),
        wait_for_entry=False,
    )
    entry = (await repo.list_trades())[0]

    recovered = TradeResult(
        trade_id=entry.broker_trade_id,
        asset=entry.asset,
        direction=entry.direction,
        amount=entry.amount,
        entry_price=entry.entry_price,
        expiry_s=entry.expiry_s,
        opened_at=entry.opened_at,
        status=TradeStatus.WIN,
        pnl=8.5,
    )
    await engine.on_trade_recovered(recovered)

    rows = await repo.list_trades()
    assert len(rows) == 1
    assert rows[0].status is TradeStatus.WIN
    assert rows[0].pnl == pytest.approx(8.5)


async def test_cancelled_notification_withheld_then_recovery_sends_result(broker, repo):
    """With recovery enabled, the premature CANCELLED notification is withheld
    and the recovered result is sent instead."""
    notified: list[tuple[str, float]] = []

    async def on_settled(result, level, balance):
        notified.append((result.status.value, result.pnl))

    engine = ExecutionEngine(
        broker, repo, RiskManager(RiskConfig(), repo), on_trade_settled=on_settled,
    )
    engine.recover_cancelled_outcomes = True

    cancelled = TradeResult(
        trade_id="t-cancel", asset="USD/IDR", direction=Direction.PUT,
        amount=10.0, entry_price=15400.0, expiry_s=60, opened_at=utcnow(),
        status=TradeStatus.CANCELLED,
    )
    await repo.insert_trade(cancelled)
    row = (await repo.list_trades())[0]
    await engine._on_settled(row, cancelled)
    assert notified == []  # withheld

    won = TradeResult(
        trade_id="t-cancel", asset="USD/IDR", direction=Direction.PUT,
        amount=10.0, entry_price=15400.0, expiry_s=60, opened_at=utcnow(),
        status=TradeStatus.WIN, pnl=8.5,
    )
    await engine.on_trade_recovered(won)
    assert notified == [("win", 8.5)]


async def test_reconcile_ignores_broker_verification_failure(broker, repo):
    """If the broker can't answer pending/list, reconciliation must give up
    quietly (fall back to DB state) and still let the trade place."""
    class _FlakyBroker:
        def __init__(self, inner):
            self._inner = inner

        async def get_open_positions(self):
            raise BrokerUnavailable("pending/list timed out")

        def __getattr__(self, name):
            return getattr(self._inner, name)

    engine = ExecutionEngine(
        _FlakyBroker(broker), repo, RiskManager(RiskConfig(), repo)
    )
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )
    assert len(await repo.list_trades()) == 1  # still placed


async def test_place_retries_on_broker_unavailable(broker, repo):
    """A scheduled entry or gale that collides with a brief disconnect must not
    be lost — the engine waits for the reconnect and retries the placement."""

    call_count = {"place": 0}

    class _FlakyBroker:
        """Broker that fails place_trade once (BrokerUnavailable) then succeeds,
        and tracks wait_connected calls."""
        def __init__(self, inner):
            self._inner = inner
            self._waited = []

        async def wait_connected(self, timeout_s=10.0):
            self._waited.append(timeout_s)
            return True  # reconnects immediately

        async def place_trade(self, request):
            call_count["place"] += 1
            if call_count["place"] <= 1:
                raise BrokerUnavailable("Socket.IO close packet")
            return await self._inner.place_trade(request)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    flaky = _FlakyBroker(broker)
    engine = ExecutionEngine(flaky, repo, RiskManager(RiskConfig(), repo))
    await engine.on_signal(
        Signal(asset="USD/IDR", direction=Direction.PUT, expiry_seconds=60, gale_count=0),
        wait_for_entry=False,
    )

    # The trade should have been placed after one retry.
    rows = await repo.list_trades()
    assert len(rows) == 1  # NOT lost
    assert flaky._waited  # wait_connected was called to allow reconnect
    assert call_count["place"] == 2  # failed once, succeeded on retry


async def test_on_signal_prepares_asset_before_scheduled_entry(broker, repo):
    """For a time-of-day signal, the engine primes the asset's subscription
    during the wait so the order can fire instantly at entry time (not after a
    ~2.5 s priming recipe at the entry moment)."""

    prepared = []

    class _PreparingBroker:
        def __init__(self, inner):
            self._inner = inner

        async def prepare_asset(self, asset):
            prepared.append(asset)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    engine = ExecutionEngine(
        _PreparingBroker(broker), repo, RiskManager(RiskConfig(), repo)
    )
    # Entry at the next whole minute — _seconds_until drops seconds, so a
    # sub-minute target is treated as past.  Short enough for the test, long
    # enough to prove the prepare happens during the wait, not at placement.
    from datetime import time as dtime

    future = datetime.now() + timedelta(minutes=1)
    await engine.on_signal(
        Signal(
            asset="EUR/CAD", direction=Direction.PUT, expiry_seconds=60,
            entry_time=dtime(future.hour, future.minute, 0),
            gale_count=0,
        ),
        wait_for_entry=True,
    )
    assert prepared == ["EUR/CAD"]
    assert len(await repo.list_trades()) == 1
