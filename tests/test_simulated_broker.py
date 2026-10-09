import pytest

from broker.base import Direction, InsufficientBalance, PlaceTradeRequest, TradeStatus
from broker.simulated import SimulatedBroker, SimulatedConfig


async def test_connect_reports_starting_balance(broker):
    assert await broker.get_balance() == 10_000.0


async def test_place_trade_deducts_stake_and_returns_open(broker):
    req = PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=10.0, expiry_s=60)
    trade = await broker.place_trade(req)
    assert trade.status is TradeStatus.OPEN
    assert trade.entry_price > 0
    assert trade.expiry_s == 60
    assert await broker.get_balance() == pytest.approx(9_990.0)
    assert await broker.wait_for_settlement(trade.trade_id, timeout_s=0.05) is trade


async def test_call_win_credits_payout(broker):
    trade = await broker.place_trade(
        PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=10.0, expiry_s=60)
    )
    await broker.settle_open_now(exit_price=trade.entry_price + 1.0)
    resolved = await broker.wait_for_settlement(trade.trade_id, timeout_s=0.05)
    assert resolved.status is TradeStatus.WIN
    assert resolved.pnl == pytest.approx(10.0 * 0.85)
    # 10,000 - 10 stake + 10 * 1.85 payout
    assert await broker.get_balance() == pytest.approx(10_000.0 - 10.0 + 10.0 * 1.85)


async def test_put_win_on_lower_exit(broker):
    trade = await broker.place_trade(
        PlaceTradeRequest(asset="USD/IDR", direction=Direction.PUT, amount=20.0, expiry_s=60)
    )
    await broker.settle_open_now(exit_price=trade.entry_price - 1.0)
    resolved = await broker.wait_for_settlement(trade.trade_id, timeout_s=0.05)
    assert resolved.status is TradeStatus.WIN
    assert resolved.pnl == pytest.approx(20.0 * 0.85)


async def test_call_loss_on_lower_exit(broker):
    trade = await broker.place_trade(
        PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=10.0, expiry_s=60)
    )
    await broker.settle_open_now(exit_price=trade.entry_price - 1.0)
    resolved = await broker.wait_for_settlement(trade.trade_id, timeout_s=0.05)
    assert resolved.status is TradeStatus.LOSS
    assert resolved.pnl == pytest.approx(-10.0)
    # stake is lost
    assert await broker.get_balance() == pytest.approx(9_990.0)


async def test_equal_price_is_draw_refund(broker):
    trade = await broker.place_trade(
        PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=10.0, expiry_s=60)
    )
    await broker.settle_open_now(exit_price=trade.entry_price)
    resolved = await broker.wait_for_settlement(trade.trade_id, timeout_s=0.05)
    assert resolved.status is TradeStatus.DRAW
    assert resolved.pnl == 0.0
    # stake refunded in full
    assert await broker.get_balance() == pytest.approx(10_000.0)


async def test_insufficient_balance_raises(broker):
    with pytest.raises(InsufficientBalance):
        await broker.place_trade(
            PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=1_000_000.0, expiry_s=60)
        )


async def test_nonpositive_amount_raises(broker):
    with pytest.raises(InsufficientBalance):
        await broker.place_trade(
            PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=0.0, expiry_s=60)
        )


async def test_same_seed_produces_identical_price_path():
    b1 = SimulatedBroker(SimulatedConfig(seed=0))
    b2 = SimulatedBroker(SimulatedConfig(seed=0))
    await b1.connect()
    await b2.connect()
    try:
        seq1 = [b1.step_price("EUR/USD") for _ in range(25)]
        seq2 = [b2.step_price("EUR/USD") for _ in range(25)]
        assert seq1 == seq2
    finally:
        await b1.disconnect()
        await b2.disconnect()


async def test_fast_forward_settles_expired_trades():
    b = SimulatedBroker(SimulatedConfig(starting_balance=1_000.0, tick_s=0.01))
    await b.connect()
    try:
        trade = await b.place_trade(
            PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=10.0, expiry_s=60)
        )
        await b.fast_forward(60.0)
        resolved = await b.wait_for_settlement(trade.trade_id, timeout_s=0.05)
        assert resolved.status in (TradeStatus.WIN, TradeStatus.LOSS, TradeStatus.DRAW)
        assert resolved.closed_at is not None
    finally:
        await b.disconnect()


async def test_disconnect_marks_open_trades_cancelled():
    b = SimulatedBroker(SimulatedConfig(starting_balance=1_000.0))
    await b.connect()
    trade = await b.place_trade(
        PlaceTradeRequest(asset="EUR/USD", direction=Direction.CALL, amount=10.0, expiry_s=600)
    )
    await b.disconnect()
    assert trade.status is TradeStatus.CANCELLED
    resolved = await b.wait_for_settlement(trade.trade_id, timeout_s=0.05)
    assert resolved.status is TradeStatus.CANCELLED
