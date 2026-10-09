"""Broker interface and domain types.

Every subsystem (execution, app) depends on this module.  ``SimulatedBroker``
and ``LiveQuotexClient`` both implement :class:`Broker`, so the rest of the
application is broker-agnostic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class Direction(str, Enum):
    """Binary-option direction. ``CALL`` is Quotex "call" (price up), ``PUT``
    is Quotex "put" (price down)."""

    CALL = "call"
    PUT = "put"


class TradeStatus(str, Enum):
    OPEN = "open"
    WIN = "win"
    LOSS = "loss"
    DRAW = "draw"  # tie / refund
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class PlaceTradeRequest:
    asset: str
    direction: Direction
    amount: float
    expiry_s: int
    signal_id: int | None = None


@dataclass
class TradeResult:
    trade_id: str
    asset: str
    direction: Direction
    amount: float
    entry_price: float
    expiry_s: int
    opened_at: datetime
    status: TradeStatus = TradeStatus.OPEN
    payout_pct: float = 0.85
    expires_at: datetime | None = None
    closed_at: datetime | None = None
    exit_price: float | None = None
    pnl: float = 0.0
    signal_id: int | None = None
    # Balance at the moment of settlement, captured by the broker.  Lets the
    # notification show the real post-trade balance without a re-read that
    # could race a subsequent gale placement.
    balance_after: float | None = None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class BrokerError(Exception):
    """Base error for any broker-level failure."""


class BrokerAuthError(BrokerError):
    """Authentication with the broker failed."""


class BrokerUnavailable(BrokerError):
    """The broker connection is down or unusable."""


class TradeNotFound(BrokerError):
    """The broker has no record of a trade id it was asked to settle.

    Raised when a trade is not open, pending, or settled on this client — e.g.
    a row persisted by a previous process whose in-memory order state was lost,
    or an order the server never acknowledged. The outcome is unrecoverable
    from here, so callers should resolve it locally and stop polling.
    """


class InsufficientBalance(BrokerError):
    """The account balance cannot cover the requested stake."""


class Broker(ABC):
    """Async broker interface implemented by both the simulator and the live
    Quotex WebSocket client."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish the session and load account state."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the session; resolve or cancel in-flight trades."""

    @abstractmethod
    async def get_balance(self) -> float:
        """Return the current account balance."""

    @abstractmethod
    async def place_trade(self, request: PlaceTradeRequest) -> TradeResult:
        """Open a binary position and return immediately with status OPEN."""

    @abstractmethod
    async def get_open_positions(self) -> list[TradeResult]:
        """Return the positions that are actually open on the broker right now.

        This is the ground truth for concurrency gating: the local DB can hold
        stale ``open`` rows (orders the server silently dropped), so callers
        verify against this before deciding to reject a new signal.
        """

    @abstractmethod
    async def wait_for_settlement(self, trade_id: str, timeout_s: float) -> TradeResult:
        """Block up to ``timeout_s``; return the (possibly still OPEN)
        ``TradeResult`` once resolved."""

    async def wait_connected(self, timeout_s: float = 10.0) -> bool:
        """Wait until the broker connection is ready for a placement.

        Used by the engine to retry a trade whose scheduled entry collided
        with a brief disconnect (the client auto-reconnects in ~1s).  The
        simulator is always connected, so the default returns immediately."""
        return True

    async def prepare_asset(self, asset: str) -> None:
        """Prime any per-asset subscription needed for a fast placement.

        The engine calls this when a signal is received, ahead of its scheduled
        entry, so the order can be sent instantly at entry time.  The simulator
        has nothing to prime, so the default is a no-op."""
