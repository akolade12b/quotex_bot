"""Deterministic simulated broker.

A geometric-brownian-motion price engine with a virtual clock.  Same
``seed`` ⇒ identical price path ⇒ identical outcomes, so tests are
reproducible.  The deterministic test hooks (``settle_open_now``,
``fast_forward``) resolve trades instantly instead of waiting wall-clock.

Deliberately does NOT import ``broker.protocol``: this module knows nothing
about Quotex's wire format, so the whole app runs broker-agnostic.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from math import exp, sqrt

from broker.base import (
    Broker,
    BrokerUnavailable,
    Direction,
    InsufficientBalance,
    PlaceTradeRequest,
    TradeNotFound,
    TradeResult,
    TradeStatus,
    utcnow,
)

# Deterministic starting prices for well-known assets.  Anything not listed
# falls back to a hash-derived price.
_BASE_PRICES = {
    "EUR/USD": 1.0850,
    "GBP/USD": 1.2650,
    "USD/JPY": 147.50,
    "USD/IDR": 15400.0,
    "AUD/USD": 0.6650,
    "USD/CAD": 1.3550,
    "USD/CHF": 0.8850,
    "NZD/USD": 0.6120,
    "EUR/GBP": 0.8570,
    "EUR/JPY": 160.20,
    "GBP/JPY": 186.80,
    "XAU/USD": 2030.0,
    "XAG/USD": 23.10,
    "#BTC/USD": 64000.0,
    "#ETH/USD": 3200.0,
}


@dataclass(frozen=True)
class SimulatedConfig:
    seed: int = 0  # fixed ⇒ reproducible session; pass None for random
    starting_balance: float = 10_000.0
    payout_pct: float = 0.85
    tick_s: float = 0.5  # real seconds per price step
    time_scale: float = 1.0  # virtual-clock accelerator (60 ⇒ 1m expiry settles in 1s real)
    drift: float = 0.0  # per-step drift; keep 0 for a fair 50/50
    vol: float = 0.0002  # per-step volatility
    draw_epsilon: float = 1e-6  # |exit - entry| below this ⇒ DRAW / refund


def _decimals(asset: str) -> int:
    if asset.endswith(("JPY", "IDR")):
        return 3
    if asset.startswith("XAU") or asset.startswith("XAG"):
        return 2
    if asset.startswith("#") or "BTC" in asset or "ETH" in asset:
        return 1
    return 5


def _fallback_price(asset: str) -> float:
    digest = int(hashlib.sha1(asset.encode()).hexdigest()[:8], 16)
    return 100.0 + (digest % 900_000) / 100.0


class SimulatedBroker(Broker):
    def __init__(self, config: SimulatedConfig | None = None) -> None:
        self.config = config or SimulatedConfig()
        self._balance = 0.0
        self._prices: dict[str, float] = {}
        self._open: dict[str, TradeResult] = {}
        self._settled: dict[str, asyncio.Event] = {}
        self._settle_tasks: list[asyncio.Task] = []
        self._placed_virtual: dict[str, float] = {}
        self._rng = None
        self._connected = False
        self._t0 = 0.0
        self._clock_offset = 0.0

    # ------------------------------------------------------------------ clock
    def _now(self) -> float:
        """Virtual seconds since connect (accelerated by ``time_scale``)."""
        return self._clock_offset + (time.monotonic() - self._t0) * self.config.time_scale

    async def _sleep(self, dt: float) -> None:
        await asyncio.sleep(max(0.0, dt / self.config.time_scale))

    # ------------------------------------------------------------------ price
    def _ensure_price(self, asset: str) -> float:
        if asset not in self._prices:
            self._prices[asset] = _BASE_PRICES.get(asset, _fallback_price(asset))
        return self._prices[asset]

    def current_price(self, asset: str) -> float:
        """Test/diagnostic hook: latest simulated price for ``asset``."""
        return self._ensure_price(asset)

    def step_price(self, asset: str) -> float:
        """Test/determinism hook: advance ``asset`` one GBM step."""
        p = self._ensure_price(asset)
        dt = self.config.tick_s
        shock = self._rng.standard_normal()
        p *= exp(self.config.drift * dt + self.config.vol * sqrt(dt) * shock)
        p = round(p, _decimals(asset))
        self._prices[asset] = p
        return p

    # ------------------------------------------------------------- lifecycle
    async def connect(self) -> None:
        import numpy as np

        self._balance = self.config.starting_balance
        self._prices = {}
        self._open = {}
        self._settled = {}
        self._settle_tasks = []
        self._placed_virtual = {}
        self._rng = np.random.default_rng(self.config.seed)
        self._t0 = time.monotonic()
        self._clock_offset = 0.0
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False
        for task in self._settle_tasks:
            task.cancel()
        for trade in self._open.values():
            if trade.status is TradeStatus.OPEN:
                trade.status = TradeStatus.CANCELLED
                trade.closed_at = utcnow()
                self._settled.setdefault(trade.trade_id, asyncio.Event()).set()
        self._settle_tasks = []

    async def get_balance(self) -> float:
        if not self._connected:
            raise InsufficientBalance("broker not connected")
        return self._balance

    async def get_open_positions(self) -> list[TradeResult]:
        if not self._connected:
            raise InsufficientBalance("broker not connected")
        return [t for t in self._open.values() if t.status is TradeStatus.OPEN]

    # ------------------------------------------------------------- execution
    async def place_trade(self, request: PlaceTradeRequest) -> TradeResult:
        if not self._connected:
            raise InsufficientBalance("broker not connected")
        if request.amount <= 0:
            raise InsufficientBalance("amount must be positive")
        if request.amount > self._balance:
            raise InsufficientBalance(
                f"stake {request.amount:.2f} exceeds balance {self._balance:.2f}"
            )
        self._balance -= request.amount

        entry_price = self.current_price(request.asset)
        now = utcnow()
        trade = TradeResult(
            trade_id=f"sim-{uuid.uuid4().hex[:12]}",
            asset=request.asset,
            direction=request.direction,
            amount=request.amount,
            entry_price=entry_price,
            expiry_s=request.expiry_s,
            opened_at=now,
            status=TradeStatus.OPEN,
            payout_pct=self.config.payout_pct,
            expires_at=now + timedelta(seconds=request.expiry_s),
            signal_id=request.signal_id,
        )
        self._open[trade.trade_id] = trade
        self._settled.setdefault(trade.trade_id, asyncio.Event())
        self._placed_virtual[trade.trade_id] = self._now()

        expires_virtual = self._placed_virtual[trade.trade_id] + request.expiry_s
        task = asyncio.create_task(self._settle_loop(trade.trade_id, expires_virtual))
        self._settle_tasks.append(task)
        return trade

    async def wait_for_settlement(self, trade_id: str, timeout_s: float) -> TradeResult:
        trade = self._open.get(trade_id)
        if trade is None:
            raise TradeNotFound(f"unknown trade {trade_id!r}")
        if trade.status is not TradeStatus.OPEN:
            return trade
        try:
            await asyncio.wait_for(self._settled[trade_id].wait(), timeout_s)
        except asyncio.TimeoutError:
            pass
        return self._open[trade_id]

    async def _settle_loop(self, trade_id: str, expires_virtual: float) -> None:
        try:
            while (
                self._connected
                and self._now() < expires_virtual
                and self._open[trade_id].status is TradeStatus.OPEN
            ):
                self.step_price(self._open[trade_id].asset)
                await self._sleep(self.config.tick_s)
            if self._open[trade_id].status is TradeStatus.OPEN:
                self._resolve(trade_id, self.current_price(self._open[trade_id].asset))
        except asyncio.CancelledError:
            pass

    def _resolve(self, trade_id: str, exit_price: float) -> TradeResult:
        trade = self._open[trade_id]
        if trade.status is not TradeStatus.OPEN:
            return trade
        entry = trade.entry_price
        eps = self.config.draw_epsilon
        up = exit_price > entry + eps
        down = exit_price < entry - eps
        if not up and not down:
            status = TradeStatus.DRAW
            credit = trade.amount
            pnl = 0.0
        elif (trade.direction is Direction.CALL and up) or (
            trade.direction is Direction.PUT and down
        ):
            status = TradeStatus.WIN
            credit = trade.amount * (1.0 + self.config.payout_pct)
            pnl = trade.amount * self.config.payout_pct
        else:
            status = TradeStatus.LOSS
            credit = 0.0
            pnl = -trade.amount
        self._balance += credit
        trade.status = status
        trade.exit_price = exit_price
        trade.closed_at = utcnow()
        trade.pnl = round(pnl, 2)
        self._settled.setdefault(trade_id, asyncio.Event()).set()
        return trade

    # ---------------------------------------------------------- test hooks
    async def settle_open_now(self, exit_price: float | None = None) -> list[TradeResult]:
        """Resolve every open trade immediately — at a forced price when given
        (fully deterministic WIN/LOSS/DRAW), else at the current price."""
        resolved: list[TradeResult] = []
        for trade_id in list(self._open):
            if self._open[trade_id].status is TradeStatus.OPEN:
                px = exit_price if exit_price is not None else self.current_price(self._open[trade_id].asset)
                resolved.append(self._resolve(trade_id, px))
        return resolved

    async def fast_forward(self, seconds: float) -> None:
        """Advance the virtual clock by ``seconds`` of sim time, stepping the
        price path along the way and settling any trades that expire."""
        self._clock_offset += seconds
        for _ in range(max(0, int(seconds / self.config.tick_s))):
            for asset in list(self._prices):
                self.step_price(asset)
        now = self._now()
        for trade_id in list(self._open):
            trade = self._open[trade_id]
            if trade.status is TradeStatus.OPEN:
                placed = self._placed_virtual.get(trade_id, 0.0)
                if now >= placed + trade.expiry_s:
                    self._resolve(trade_id, self.current_price(trade.asset))
