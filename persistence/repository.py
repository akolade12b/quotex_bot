"""Async SQLite repository (aiosqlite).

All writes are serialized through a single asyncio lock so concurrent tasks
(settlement loop, risk updates, balance snapshots) never interleave a
read-modify-write sequence.  Reads are lock-free.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, time

import aiosqlite

from broker.base import Direction, TradeResult, TradeStatus, utcnow
from persistence.models import BalanceSnapshotRow, RiskStateRow, SignalRow, TradeRow
from persistence.schema import SCHEMA_SQL
from telegram_signals.model import Signal


def _time_to_str(t: time | None) -> str | None:
    return t.strftime("%H:%M") if t is not None else None


def _row_to_signal(row: aiosqlite.Row) -> SignalRow:
    return SignalRow(
        id=row["id"],
        asset=row["asset"],
        direction=Direction(row["direction"]),
        expiry_s=row["expiry_s"],
        entry_time=row["entry_time"],
        gale_count=row["gale_count"],
        raw_text=row["raw_text"],
        source=row["source"] or "",
        received_at=datetime.fromisoformat(row["received_at"]),
        parse_status=row["parse_status"],
        reject_reason=row["reject_reason"],
    )


def _row_to_trade(row: aiosqlite.Row) -> TradeRow:
    return TradeRow(
        id=row["id"],
        broker_trade_id=row["broker_trade_id"],
        signal_id=row["signal_id"],
        asset=row["asset"],
        direction=Direction(row["direction"]),
        amount=row["amount"],
        entry_price=row["entry_price"],
        expiry_s=row["expiry_s"],
        level=row["level"],
        status=TradeStatus(row["status"]),
        opened_at=datetime.fromisoformat(row["opened_at"]),
        expires_at=datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None,
        closed_at=datetime.fromisoformat(row["closed_at"]) if row["closed_at"] else None,
        exit_price=row["exit_price"],
        payout_pct=row["payout_pct"],
        pnl=row["pnl"],
    )


class TradeRepository:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path  # ":memory:" is supported for tests
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    # ------------------------------------------------------------- lifecycle
    async def connect(self) -> None:
        if self._db is not None:
            return  # idempotent
        if self._db_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(self._db_path))
            os.makedirs(parent, exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA_SQL)

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    # ------------------------------------------------------------- signals
    async def insert_signal(self, signal: Signal) -> int:
        assert self._db is not None
        async with self._write_lock:
            cur = await self._db.execute(
                "INSERT INTO signals "
                "(asset, direction, expiry_s, entry_time, gale_count, raw_text, source, received_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    signal.asset,
                    signal.direction.value,
                    signal.expiry_seconds,
                    _time_to_str(signal.entry_time),
                    signal.gale_count,
                    signal.raw_text,
                    signal.source,
                    signal.received_at.isoformat(),
                ),
            )
            await self._db.commit()
            return int(cur.lastrowid)

    async def mark_rejected(self, signal_id: int, reason: str) -> None:
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                "UPDATE signals SET parse_status='rejected', reject_reason=? WHERE id=?",
                (reason, signal_id),
            )
            await self._db.commit()

    async def get_signal(self, signal_id: int) -> SignalRow | None:
        assert self._db is not None
        cur = await self._db.execute("SELECT * FROM signals WHERE id=?", (signal_id,))
        row = await cur.fetchone()
        return _row_to_signal(row) if row else None

    # -------------------------------------------------------------- trades
    async def insert_trade(self, result: TradeResult, level: int = 0) -> int:
        assert self._db is not None
        async with self._write_lock:
            cur = await self._db.execute(
                "INSERT INTO trades "
                "(broker_trade_id, signal_id, asset, direction, amount, entry_price, "
                " expiry_s, level, status, opened_at, expires_at, payout_pct) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    result.trade_id,
                    result.signal_id,
                    result.asset,
                    result.direction.value,
                    result.amount,
                    result.entry_price,
                    result.expiry_s,
                    level,
                    result.status.value,
                    result.opened_at.isoformat(),
                    result.expires_at.isoformat() if result.expires_at else None,
                    result.payout_pct,
                ),
            )
            await self._db.commit()
            return int(cur.lastrowid)

    async def update_trade(self, result: TradeResult) -> None:
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                "UPDATE trades SET status=?, closed_at=?, exit_price=?, pnl=? "
                "WHERE broker_trade_id=?",
                (
                    result.status.value,
                    result.closed_at.isoformat() if result.closed_at else None,
                    result.exit_price,
                    result.pnl,
                    result.trade_id,
                ),
            )
            await self._db.commit()

    async def list_open_trades(self) -> list[TradeRow]:
        assert self._db is not None
        cur = await self._db.execute("SELECT * FROM trades WHERE status='open'")
        rows = await cur.fetchall()
        return [_row_to_trade(r) for r in rows]

    async def mark_trade_cancelled(self, broker_trade_id: str) -> None:
        """Resolve a trade the broker has no record of (stale/ghost row) so the
        settlement loop stops polling it. The outcome was unknowable."""
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                "UPDATE trades SET status='cancelled', closed_at=? WHERE "
                "broker_trade_id=? AND status='open'",
                (utcnow().isoformat(), broker_trade_id),
            )
            await self._db.commit()

    async def list_trades(self, asset: str | None = None, limit: int = 100) -> list[TradeRow]:
        assert self._db is not None
        if asset:
            cur = await self._db.execute(
                "SELECT * FROM trades WHERE asset=? ORDER BY id DESC LIMIT ?", (asset, limit)
            )
        else:
            cur = await self._db.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,))
        rows = await cur.fetchall()
        return [_row_to_trade(r) for r in rows]

    # ------------------------------------------------------------- balance
    async def insert_balance_snapshot(self, balance: float, equity: float) -> None:
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                "INSERT INTO balance_snapshots (ts, balance, equity) VALUES (?, ?, ?)",
                (utcnow().isoformat(), balance, equity),
            )
            await self._db.commit()

    async def list_balance_snapshots(self, limit: int = 100) -> list[BalanceSnapshotRow]:
        assert self._db is not None
        cur = await self._db.execute(
            "SELECT * FROM balance_snapshots ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
        return [
            BalanceSnapshotRow(
                id=r["id"], ts=datetime.fromisoformat(r["ts"]),
                balance=r["balance"], equity=r["equity"],
            )
            for r in rows
        ]

    # --------------------------------------------------------------- risk
    async def get_risk_state(self) -> RiskStateRow:
        """Return the singleton risk row, rolling the day when the UTC date
        changes (daily loss / trade-count limits reset)."""
        assert self._db is not None
        async with self._write_lock:
            cur = await self._db.execute(
                "SELECT day, realized_loss, trade_count FROM risk_state WHERE id=1"
            )
            row = await cur.fetchone()
            today = utcnow().strftime("%Y-%m-%d")
            if row is None or row["day"] != today:
                await self._db.execute(
                    "UPDATE risk_state SET day=?, realized_loss=0, trade_count=0 WHERE id=1",
                    (today,),
                )
                await self._db.commit()
                return RiskStateRow(day=today)
            return RiskStateRow(
                day=row["day"], realized_loss=row["realized_loss"], trade_count=row["trade_count"]
            )

    async def save_risk_state(self, row: RiskStateRow) -> None:
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                "UPDATE risk_state SET day=?, realized_loss=?, trade_count=? WHERE id=1",
                (row.day, row.realized_loss, row.trade_count),
            )
            await self._db.commit()
