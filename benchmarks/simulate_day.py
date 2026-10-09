"""Replay a day's worth of signals through the full pipeline (parser -> engine
-> simulated broker -> settlement loop) and print session stats.

Usage:
    python -m benchmarks.simulate_day [--signals N] [--seed S] [--db PATH]

The broker runs at a high ``time_scale`` so a session completes in seconds.
Signals are generated in the MagicTrader gale format; risk limits are relaxed
so throughput (not rejection) is what's being measured.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
from datetime import datetime, timezone

from broker.base import Direction, TradeStatus
from broker.simulated import SimulatedBroker, SimulatedConfig
from execution.engine import ExecutionEngine
from execution.risk import RiskConfig, RiskManager
from persistence.repository import TradeRepository
from telegram_signals.model import Signal
from telegram_signals.parser import SignalParser

logger = logging.getLogger(__name__)

ASSETS = ("USD/IDR", "EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD")
EXPIRIES = (60, 180, 300)
GALE_COUNTS = (0, 1, 2)


def make_signal(i: int) -> str:
    """Synthesize a MagicTrader-format message (immediate entry)."""
    rng = random.Random(i)
    asset = ASSETS[i % len(ASSETS)]
    direction = "CALL" if rng.random() < 0.5 else "PUT"
    expiry = EXPIRIES[i % len(EXPIRIES)]
    gales = GALE_COUNTS[i % len(GALE_COUNTS)]
    minutes = expiry // 60
    text = f"💰{minutes}-minute expiration\n{asset}; {direction} 🟥\n"
    for g in range(1, gales + 1):
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(g, "th")
        text += f"{g}{suffix} GALE —> TIME TO 09:{g * minutes:02d}\n"
    return text


async def run(n_signals: int, seed: int, db_path: str) -> None:
    parser = SignalParser()
    repo = TradeRepository(db_path)
    await repo.connect()

    # Relax risk so the benchmark measures engine throughput, not rejects.
    risk_cfg = RiskConfig(
        max_concurrent_positions=999,
        cooldown_seconds=0.0,
        max_daily_loss=0.0,  # unlimited
        max_ladder_stake=0.0,
    )
    broker = SimulatedBroker(SimulatedConfig(seed=seed, time_scale=600.0))
    await broker.connect()
    engine = ExecutionEngine(broker, repo, RiskManager(risk_cfg, repo))

    start = datetime.now(timezone.utc)
    fed = 0
    for i in range(n_signals):
        text = make_signal(i)
        signal = parser.parse(text, source=f"bench:{i}")
        fed += 1
        await engine.on_signal(signal, wait_for_entry=False)

    # Let the settlement loop drain every open position.
    while await repo.list_open_trades():
        await engine.run_settlement_loop_once(0.5)
        await asyncio.sleep(0.5)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    await _report(repo, fed, elapsed, broker)
    await broker.disconnect()
    await repo.close()


async def _report(repo: TradeRepository, fed: int, elapsed: float, broker) -> None:
    trades = await repo.list_trades()
    state = await repo.get_risk_state()
    balance = await broker.get_balance()
    wins = sum(1 for t in trades if t.status is TradeStatus.WIN)
    losses = sum(1 for t in trades if t.status is TradeStatus.LOSS)
    draws = sum(1 for t in trades if t.status is TradeStatus.DRAW)
    pnl = sum(t.pnl for t in trades)
    by_level: dict[int, int] = {}
    for t in trades:
        by_level[t.level] = by_level.get(t.level, 0) + 1
    max_level = max((t.level for t in trades), default=0)

    print(f"\nsession: {fed} signals, {len(trades)} trades in {elapsed:.1f}s "
          f"(simulated, time_scale x{broker.config.time_scale})")
    print(f"wins {wins}  losses {losses}  draws {draws}  "
          f"win_rate {100 * wins / max(1, len(trades)):.1f}%")
    print(f"realized pnl {pnl:+.2f}   ending balance {balance:.2f}")
    print(f"max ladder depth reached: L{max_level}")
    print(f"ladder levels: " + ", ".join(f"L{l}:{c}" for l, c in sorted(by_level.items())))
    print(f"risk_state: trade_count={state.trade_count} "
          f"realized_loss={state.realized_loss:.2f}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--signals", type=int, default=30)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--db", default="data/benchmark.db")
    args = ap.parse_args()
    logging.basicConfig(level=logging.ERROR)
    asyncio.run(run(args.signals, args.seed, args.db))


if __name__ == "__main__":
    main()
