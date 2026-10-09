"""Risk rules enforced before any trade placement, plus daily-loss tracking."""

from __future__ import annotations

import logging
import time as _time
from dataclasses import dataclass

from broker.base import TradeResult, TradeStatus
from persistence.repository import TradeRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RiskConfig:
    base_amount: float = 10.0
    martingale_multiplier: float = 2.0
    max_gales: int = 2
    max_stake: float = 100.0  # per trade
    max_ladder_stake: float = 0.0  # 0 = unlimited total across one ladder
    max_daily_loss: float = 200.0
    max_concurrent_positions: int = 3
    asset_allowlist: tuple[str, ...] = ()  # empty = allow all
    cooldown_seconds: float = 5.0
    entry_tolerance_seconds: int = 60


@dataclass(frozen=True)
class ExecutionDecision:
    allow: bool
    reason: str = ""
    amount: float = 0.0


class RiskManager:
    def __init__(self, config: RiskConfig, repo: TradeRepository) -> None:
        self.config = config
        self.repo = repo
        # per-asset monotonic timestamp of the last placement (in-memory hot path)
        self._last_trade_at: dict[str, float] = {}

    async def authorize(
        self, *, asset: str, amount: float, level: int, balance: float
    ) -> ExecutionDecision:
        """Ordered checks; the first failure wins."""
        if self.config.asset_allowlist and asset not in self.config.asset_allowlist:
            return self._deny(f"asset {asset} not allowlisted")
        if amount <= 0:
            return self._deny("amount must be positive")
        if amount > self.config.max_stake:
            return self._deny(f"amount {amount:.2f} exceeds max_stake {self.config.max_stake}")
        if level > self.config.max_gales:
            return self._deny(f"level {level} exceeds max_gales {self.config.max_gales}")
        if self.config.max_ladder_stake:
            cumulative = self.config.base_amount * sum(
                self.config.martingale_multiplier ** i for i in range(level + 1)
            )
            if cumulative > self.config.max_ladder_stake:
                return self._deny(
                    f"ladder stake {cumulative:.2f} exceeds max_ladder_stake {self.config.max_ladder_stake}"
                )
        now = _time.monotonic()
        last = self._last_trade_at.get(asset)
        # Cooldown throttles *new* signals on an asset, not the auto-gale
        # continuation of an in-flight ladder, so it only applies to level 0.
        if level == 0 and last is not None and now - last < self.config.cooldown_seconds:
            return self._deny("cooldown active for asset")
        open_positions = await self.repo.list_open_trades()
        if len(open_positions) >= self.config.max_concurrent_positions:
            return self._deny(
                f"{len(open_positions)} open positions >= max_concurrent_positions {self.config.max_concurrent_positions}"
            )
        if balance < amount:
            return self._deny(f"insufficient balance {balance:.2f} < {amount:.2f}")
        state = await self.repo.get_risk_state()
        if self.config.max_daily_loss and state.realized_loss >= self.config.max_daily_loss:
            return self._deny(f"daily loss limit {self.config.max_daily_loss} reached")
        return ExecutionDecision(allow=True, amount=amount)

    def note_placement(self, asset: str) -> None:
        self._last_trade_at[asset] = _time.monotonic()

    async def record_result(self, result: TradeResult) -> None:
        """Accumulate realized daily loss and trade count after a settlement.

        CANCELLED trades (unknown outcome) are skipped: they didn't really
        trade, and the broker may later recover a real WIN/LOSS/DRAW which is
        recorded instead (see engine.on_trade_recovered)."""
        if result.status is TradeStatus.CANCELLED:
            return
        state = await self.repo.get_risk_state()
        state.trade_count += 1
        if result.pnl < 0:
            state.realized_loss += -result.pnl
        await self.repo.save_risk_state(state)

    @staticmethod
    def _deny(reason: str) -> ExecutionDecision:
        return ExecutionDecision(allow=False, reason=reason)
