from datetime import datetime, timezone

from broker.base import Direction, TradeResult, TradeStatus, utcnow


_trade_counter = 0


def _make_trade(asset="USD/IDR", direction=Direction.PUT, amount=10.0, signal_id=None, status=TradeStatus.OPEN):
    global _trade_counter
    _trade_counter += 1
    return TradeResult(
        trade_id=f"sim-test-{_trade_counter}",
        asset=asset,
        direction=direction,
        amount=amount,
        entry_price=15400.0,
        expiry_s=300,
        opened_at=utcnow(),
        status=status,
        payout_pct=0.85,
        expires_at=None,
        signal_id=signal_id,
    )


async def test_signal_roundtrip(repo, sample_signal):
    sig_id = await repo.insert_signal(sample_signal)
    row = await repo.get_signal(sig_id)
    assert row is not None
    assert row.id == sig_id
    assert row.asset == "USD/IDR"
    assert row.direction is Direction.PUT
    assert row.expiry_s == 300
    assert row.entry_time == "08:55"
    assert row.gale_count == 2
    assert row.parse_status == "parsed"
    assert row.reject_reason is None


async def test_mark_rejected(repo, sample_signal):
    sig_id = await repo.insert_signal(sample_signal)
    await repo.mark_rejected(sig_id, "asset not allowlisted")
    row = await repo.get_signal(sig_id)
    assert row.parse_status == "rejected"
    assert row.reject_reason == "asset not allowlisted"


async def test_trade_insert_and_update(repo, sample_signal):
    sig_id = await repo.insert_signal(sample_signal)
    trade = _make_trade(signal_id=sig_id)
    await repo.insert_trade(trade, level=1)
    open_rows = await repo.list_open_trades()
    assert len(open_rows) == 1
    assert open_rows[0].level == 1
    assert open_rows[0].signal_id == sig_id
    assert open_rows[0].status is TradeStatus.OPEN

    trade.status = TradeStatus.WIN
    trade.pnl = 8.5
    trade.closed_at = utcnow()
    trade.exit_price = 15399.0
    await repo.update_trade(trade)

    assert await repo.list_open_trades() == []
    rows = await repo.list_trades()
    assert len(rows) == 1
    assert rows[0].status is TradeStatus.WIN
    assert rows[0].pnl == 8.5
    assert rows[0].exit_price == 15399.0


async def test_list_trades_filters_by_asset(repo):
    t1 = _make_trade(asset="EUR/USD")
    await repo.insert_trade(t1)
    t2 = _make_trade(asset="GBP/USD")
    await repo.insert_trade(t2)
    assert len(await repo.list_trades(asset="EUR/USD")) == 1
    assert len(await repo.list_trades()) == 2


async def test_get_risk_state_returns_singleton(repo):
    state = await repo.get_risk_state()
    assert state.day == datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert state.realized_loss == 0.0
    assert state.trade_count == 0

    state.trade_count = 5
    state.realized_loss = 40.0
    await repo.save_risk_state(state)

    again = await repo.get_risk_state()
    assert again.trade_count == 5
    assert again.realized_loss == 40.0


async def test_connect_is_idempotent(repo):
    # calling connect() again must not raise or wipe existing tables
    await repo.connect()
    from telegram_signals.model import Signal

    sig_id = await repo.insert_signal(
        Signal(asset="EUR/USD", direction=Direction.CALL, expiry_seconds=60, raw_text="x")
    )
    assert await repo.get_signal(sig_id) is not None


async def test_balance_snapshot_roundtrip(repo):
    await repo.insert_balance_snapshot(1000.0, 1000.0)
    await repo.insert_balance_snapshot(990.0, 1005.0)
    rows = await repo.list_balance_snapshots()
    assert len(rows) == 2
    assert rows[0].balance == 990.0
    assert rows[0].equity == 1005.0
