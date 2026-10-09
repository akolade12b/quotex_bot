"""Shared pytest fixtures."""

from datetime import time as dtime

import pytest

from broker.base import Direction
from broker.simulated import SimulatedBroker, SimulatedConfig
from execution.risk import RiskConfig, RiskManager
from persistence.repository import TradeRepository
from telegram_signals.model import Signal


@pytest.fixture
async def broker():
    b = SimulatedBroker(SimulatedConfig(seed=0, starting_balance=10_000.0))
    await b.connect()
    yield b
    await b.disconnect()


@pytest.fixture
async def repo():
    r = TradeRepository(":memory:")
    await r.connect()
    yield r
    await r.close()


@pytest.fixture
def sample_signal():
    """A fully-formed signal matching the user's MagicTrader gale format."""
    return Signal(
        asset="USD/IDR",
        direction=Direction.PUT,
        expiry_seconds=300,
        entry_time=dtime(8, 55),
        gale_count=2,
        raw_text="USD/IDR; 08:55; PUT",
        source="chat:123/msg:456",
    )


@pytest.fixture
def risk(repo):
    return RiskManager(RiskConfig(), repo)
