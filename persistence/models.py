"""Row models mirroring the database tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from broker.base import Direction, TradeStatus


@dataclass
class SignalRow:
    id: int | None
    asset: str
    direction: Direction
    expiry_s: int
    entry_time: str | None  # "HH:MM" wall-clock or None
    gale_count: int
    raw_text: str
    source: str
    received_at: datetime
    parse_status: str = "parsed"  # "parsed" | "rejected"
    reject_reason: str | None = None


@dataclass
class TradeRow:
    id: int | None
    broker_trade_id: str
    signal_id: int | None
    asset: str
    direction: Direction
    amount: float
    entry_price: float
    expiry_s: int
    level: int
    status: TradeStatus
    opened_at: datetime
    expires_at: datetime | None
    closed_at: datetime | None
    exit_price: float | None
    payout_pct: float
    pnl: float


@dataclass
class BalanceSnapshotRow:
    id: int | None
    ts: datetime
    balance: float
    equity: float


@dataclass
class RiskStateRow:
    day: str  # UTC date "YYYY-MM-DD"; "" = not yet opened
    realized_loss: float = 0.0
    trade_count: int = 0
