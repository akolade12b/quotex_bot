import pytest

from broker.base import Direction, TradeResult, TradeStatus, utcnow
from execution.risk import RiskConfig, RiskManager


def _trade(pnl: float):
    return TradeResult(
        trade_id=f"sim-r-{abs(hash(pnl))}",
        asset="USD/IDR",
        direction=Direction.PUT,
        amount=10.0,
        entry_price=15400.0,
        expiry_s=60,
        opened_at=utcnow(),
        status=TradeStatus.WIN if pnl >= 0 else TradeStatus.LOSS,
        pnl=pnl,
    )


async def test_happy_path_allows(risk):
    d = await risk.authorize(asset="USD/IDR", amount=10.0, level=0, balance=1000.0)
    assert d.allow is True
    assert d.amount == 10.0


async def test_asset_not_allowlisted(repo):
    rm = RiskManager(RiskConfig(asset_allowlist=("EUR/USD",)), repo)
    d = await rm.authorize(asset="USD/IDR", amount=10.0, level=0, balance=1000.0)
    assert d.allow is False
    assert "not allowlisted" in d.reason


async def test_amount_exceeds_max_stake(repo):
    rm = RiskManager(RiskConfig(max_stake=50.0), repo)
    d = await rm.authorize(asset="USD/IDR", amount=60.0, level=0, balance=1000.0)
    assert d.allow is False
    assert "max_stake" in d.reason


async def test_level_exceeds_max_gales(repo):
    rm = RiskManager(RiskConfig(max_gales=2), repo)
    d = await rm.authorize(asset="USD/IDR", amount=10.0, level=3, balance=1000.0)
    assert d.allow is False
    assert "max_gales" in d.reason


async def test_ladder_stake_cap(repo):
    rm = RiskManager(RiskConfig(base_amount=10.0, martingale_multiplier=2.0, max_ladder_stake=30.0), repo)
    # level 0 → cumulative 10, allowed; level 1 → cumulative 10+20=30, allowed; level 2 → 70, denied
    assert (await rm.authorize(asset="USD/IDR", amount=10.0, level=0, balance=1000.0)).allow
    assert (await rm.authorize(asset="USD/IDR", amount=20.0, level=1, balance=1000.0)).allow
    d = await rm.authorize(asset="USD/IDR", amount=40.0, level=2, balance=1000.0)
    assert d.allow is False
    assert "max_ladder_stake" in d.reason


async def test_cooldown_blocks_repeat(risk):
    assert (await risk.authorize(asset="USD/IDR", amount=10.0, level=0, balance=1000.0)).allow
    risk.note_placement("USD/IDR")
    d = await risk.authorize(asset="USD/IDR", amount=10.0, level=0, balance=1000.0)
    assert d.allow is False
    assert "cooldown" in d.reason


async def test_cooldown_passes_on_different_asset(risk):
    risk.note_placement("USD/IDR")
    d = await risk.authorize(asset="EUR/USD", amount=10.0, level=0, balance=1000.0)
    assert d.allow is True


async def test_max_concurrent_positions(repo):
    rm = RiskManager(RiskConfig(max_concurrent_positions=2), repo)
    for i in range(2):
        t = _trade(0.0)
        t.trade_id = f"sim-open-{i}"
        t.status = TradeStatus.OPEN  # must be open to count as a live position
        await repo.insert_trade(t)
    assert (await rm.authorize(asset="EUR/USD", amount=10.0, level=0, balance=1000.0)).allow is False


async def test_insufficient_balance(repo):
    rm = RiskManager(RiskConfig(), repo)
    d = await rm.authorize(asset="USD/IDR", amount=100.0, level=0, balance=50.0)
    assert d.allow is False
    assert "insufficient balance" in d.reason


async def test_daily_loss_limit(repo):
    rm = RiskManager(RiskConfig(max_daily_loss=100.0), repo)
    state = await repo.get_risk_state()
    state.realized_loss = 100.0
    await repo.save_risk_state(state)
    d = await rm.authorize(asset="USD/IDR", amount=10.0, level=0, balance=1000.0)
    assert d.allow is False
    assert "daily loss limit" in d.reason


async def test_record_result_accumulates_loss_and_count(risk):
    await risk.record_result(_trade(pnl=-10.0))
    await risk.record_result(_trade(pnl=8.5))
    state = await risk.repo.get_risk_state()
    assert state.trade_count == 2
    assert state.realized_loss == pytest.approx(10.0)


async def test_record_result_skips_cancelled(risk):
    """CANCELLED outcomes are not counted — a real WIN/LOSS may be recovered
    from the broker later and recorded instead (no double counting)."""
    cancelled = _trade(pnl=0.0)
    cancelled.status = TradeStatus.CANCELLED
    await risk.record_result(cancelled)
    state = await risk.repo.get_risk_state()
    assert state.trade_count == 0
    assert state.realized_loss == 0.0
