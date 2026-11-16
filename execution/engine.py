"""Execution engine: schedules the entry trade, runs the settlement loop, and
auto-places the martingale (gale) ladder on losses."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import datetime, time, timedelta

from broker.base import (
    Broker,
    BrokerError,
    BrokerUnavailable,
    PlaceTradeRequest,
    TradeNotFound,
    TradeResult,
    TradeStatus,
    utcnow,
)
from execution.risk import RiskManager
from persistence.models import TradeRow
from persistence.repository import TradeRepository
from telegram_signals.model import Signal

logger = logging.getLogger(__name__)

# A DB-open trade younger than this is left alone during reconciliation even
# if the broker doesn't report it yet — the server may not have reflected a
# just-placed order into pending/list. Older rows the broker confirms absent
# are stale (orders it silently dropped) and get cancelled so they can't keep
# blocking new signals via max_concurrent_positions.
_STALE_ROW_GRACE_S = 60.0


class ExecutionEngine:
    def __init__(
        self,
        broker: Broker,
        repo: TradeRepository,
        risk: RiskManager,
        entry_time_offset_s: float = 0.0,
        *,
        place_retries: int = 3,
        place_retry_wait_s: float = 5.0,
        on_trade_placed=None,
        on_trade_settled=None,
        on_trade_rejected=None,
        on_trade_reconciled=None,
    ) -> None:
        self.broker = broker
        self.repo = repo
        self.risk = risk
        # When a placement collides with a brief disconnect (the Quotex socket
        # auto-reconnects in ~1s), retry instead of losing the signal: wait for
        # the reconnect, then place. Bounded so a long outage still gives up.
        self.place_retries = place_retries
        self.place_retry_wait_s = place_retry_wait_s
        # Shifts parsed signal entry times before scheduling. E.g. +4h means a
        # signal saying 08:50 is treated as 12:50 on this machine's clock.
        self.entry_time_offset_s = entry_time_offset_s
        # Optional async callbacks for Telegram push notifications. Signature:
        #   on_trade_placed(result, level) -> None
        #   on_trade_settled(result, level, balance) -> None
        #   on_trade_rejected(signal, reason) -> None
        #   on_trade_reconciled(row) -> None
        # Wired in main.py when a Telegram token + chat ids are configured; the
        # engine treats them as best-effort and never lets a failure break a
        # trade. Settable after construction (main builds the bot later).
        self.on_trade_placed = on_trade_placed
        self.on_trade_settled = on_trade_settled
        self.on_trade_rejected = on_trade_rejected
        self.on_trade_reconciled = on_trade_reconciled
        # True when the broker (live client) can recover the real outcome of a
        # trade that got CANCELLED by a disconnection. While it is set, the
        # engine withholds the premature "CANCELLED" notification and lets the
        # recovery send the definitive result instead. Wired in main.py.
        self.recover_cancelled_outcomes = False
        # Can be toggled by the Telegram control panel. Existing positions
        # still settle; new entries and martingale follow-ups are skipped.
        self.auto_trading_enabled = True

    # ----------------------------------------------------------------- entry
    async def on_signal(self, signal: Signal, *, wait_for_entry: bool = True) -> None:
        """Start a ladder for ``signal``.

        With ``wait_for_entry=True`` the engine sleeps until the signal's
        entry time (wall-clock), skipping signals that arrived too late.
        Tests and CLI feeds pass ``wait_for_entry=False`` to go immediately.
        """
        if wait_for_entry and signal.entry_time is not None:
            delay = self._seconds_until(signal.entry_time, self.entry_time_offset_s)
            tolerance = self.risk.config.entry_tolerance_seconds
            if delay < -tolerance:
                logger.warning(
                    "signal %ds past entry time, skipping %s",
                    -int(delay), signal.raw_text[:60],
                )
                return
            if delay > 0:
                logger.info("scheduling %s entry in %.0fs", signal.asset, delay)
                # Prime the asset's depth subscription NOW (during the wait)
                # so the order fires instantly at entry time instead of the
                # ~2.5 s priming recipe running at the entry moment.
                try:
                    await self.broker.prepare_asset(signal.asset)
                except BrokerError as exc:
                    logger.warning("failed to pre-prime %s: %s", signal.asset, exc)
                # prepare_asset consumed some of the wait — recompute the
                # remaining time so the entry still fires on schedule.
                remaining = self._seconds_until(signal.entry_time, self.entry_time_offset_s)
                if remaining > 0:
                    await asyncio.sleep(remaining)
        await self._place_level(signal, 0, self.risk.config.base_amount)

    async def _place_level(
        self, signal: Signal, level: int, amount: float,
        known_balance: float | None = None,
    ) -> TradeResult | None:
        """Persist the signal (once), authorize, place, and record one trade.

        ``known_balance`` (used for gales) is the balance captured at the
        previous trade's settlement — skipping a fresh broker read saves
        ~200ms of gale latency."""
        if not self.auto_trading_enabled:
            logger.info("auto trading disabled — skipping L%d %s", level, signal.asset)
            return None
        if signal.signal_id is None:
            signal = replace(signal, signal_id=await self.repo.insert_signal(signal))

        request = PlaceTradeRequest(
            asset=signal.asset,
            direction=signal.direction,
            amount=amount,
            expiry_s=signal.expiry_seconds,
            signal_id=signal.signal_id,
        )
        result: TradeResult | None = None
        failure: str | None = None
        for attempt in range(1, self.place_retries + 1):
            try:
                balance = known_balance if known_balance is not None else await self.broker.get_balance()
                if level == 0:
                    # Verify against the broker's real open positions before
                    # gating: cancel stale DB rows (orders the server silently
                    # dropped) so they can't block this signal via
                    # max_concurrent_positions.
                    await self._reconcile_open_trades()
                decision = await self.risk.authorize(
                    asset=signal.asset, amount=amount, level=level, balance=balance
                )
                if not decision.allow:
                    if level == 0:
                        await self.repo.mark_rejected(signal.signal_id, decision.reason)
                    logger.warning("rejected L%d %s: %s", level, signal.asset, decision.reason)
                    await self._notify_rejected(signal, decision.reason)
                    return None
                result = await self.broker.place_trade(request)
                break
            except BrokerUnavailable as exc:
                # A scheduled entry/gale that collides with a brief disconnect
                # must not be lost — wait for the auto-reconnect and retry.
                if attempt >= self.place_retries:
                    failure = "broker unavailable"
                    break
                logger.warning(
                    "broker unavailable placing L%d %s (%s); waiting for reconnect "
                    "and retrying (%d/%d)",
                    level, signal.asset, exc, attempt, self.place_retries,
                )
                await self.broker.wait_connected(timeout_s=self.place_retry_wait_s)
            except BrokerError as exc:
                # Not a connection issue (e.g. insufficient balance) — not
                # transient, so don't retry.
                failure = f"broker error: {exc}"
                break

        if result is None:
            if level == 0:
                await self.repo.mark_rejected(signal.signal_id, failure)
            logger.error("failed to place L%d trade: %s", level, failure)
            await self._notify_rejected(signal, failure)
            return None

        await self.repo.insert_trade(result, level=level)
        self.risk.note_placement(signal.asset)
        logger.info(
            "PLACED L%d %s %s %.2f", level, signal.asset, signal.direction.value.upper(), amount
        )
        await self._notify_placed(result, level)
        return result

    # ------------------------------------------------------- notifications
    async def _notify_placed(self, result: TradeResult, level: int) -> None:
        if self.on_trade_placed is None:
            return
        try:
            await self.on_trade_placed(result, level)
        except Exception:
            logger.exception("placed notification failed")

    async def _notify_settled(self, result: TradeResult, level: int) -> None:
        if self.on_trade_settled is None:
            return
        try:
            # Prefer the balance captured at settlement (before any gale stake
            # deduction) so the notification shows the real post-trade balance.
            balance = result.balance_after
            if balance is None:
                balance = await self.broker.get_balance()
            await self.on_trade_settled(result, level, balance)
        except Exception:
            logger.exception("settled notification failed")

    async def _notify_rejected(self, signal: Signal, reason: str) -> None:
        if self.on_trade_rejected is None:
            return
        try:
            await self.on_trade_rejected(signal, reason)
        except Exception:
            logger.exception("rejected notification failed")

    # --------------------------------------------------------- reconciliation
    async def _reconcile_open_trades(self) -> None:
        """Verify real open positions against the broker before a new entry.

        The DB is the only record of orders this client placed, but the broker
        is the truth of what is actually open: an ``orders/open`` the server
        silently dropped leaves a stale ``open`` row that would otherwise block
        every later signal via ``max_concurrent_positions``. Cancel rows the
        broker confirms absent (older than the freshness grace). Best-effort —
        if the broker can't be queried, fall back to the DB as before.
        """
        try:
            broker_open = await self.broker.get_open_positions()
        except Exception as exc:
            logger.warning(
                "open-position verification failed (%s) — using DB state", exc
            )
            return
        broker_ids = {p.trade_id for p in broker_open}
        cutoff = utcnow() - timedelta(seconds=_STALE_ROW_GRACE_S)
        for row in await self.repo.list_open_trades():
            if row.broker_trade_id in broker_ids:
                continue
            if row.opened_at and row.opened_at > cutoff:
                continue  # too fresh — the broker may not have reflected it yet
            await self.repo.mark_trade_cancelled(row.broker_trade_id)
            logger.warning(
                "reconciled stale open trade %s (%s) — not on broker, marked cancelled",
                row.broker_trade_id, row.asset,
            )
            await self._notify_reconciled(row)

    async def _notify_reconciled(self, row: TradeRow) -> None:
        if self.on_trade_reconciled is None:
            return
        try:
            await self.on_trade_reconciled(row)
        except Exception:
            logger.exception("reconciled notification failed")

    # ------------------------------------------------ outcome recovery
    async def on_trade_recovered(self, trade: TradeResult) -> None:
        """Called by the live client after a reconnect when it recovers the
        real outcome (WIN / LOSS / DRAW) of a trade that was marked CANCELLED
        because the connection dropped at expiry.  Redoes the DB bookkeeping,
        records the result with the risk manager, and — when it turns out to
        be a LOSS — immediately places the next martingale level."""
        rows = await self.repo.list_trades(limit=1000)
        row = next((r for r in rows if r.broker_trade_id == trade.trade_id), None)
        if row is None:
            logger.warning("recovered trade %s not in DB — skipping", trade.trade_id)
            return
        if trade.status is TradeStatus.CANCELLED:
            # The broker had no record of it: send the withheld notification.
            await self._notify_settled(trade, row.level)
            logger.warning(
                "trade %s still cancelled after reconnect — no server record",
                trade.trade_id,
            )
            return
        await self.repo.update_trade(trade)
        await self.risk.record_result(trade)
        await self._notify_settled(trade, row.level)
        logger.info(
            "RECOVERED L%d %s %s pnl=%.2f (was cancelled by disconnect)",
            row.level, trade.asset, trade.status.value, trade.pnl,
        )
        if trade.status is TradeStatus.LOSS:
            signal = await self._signal_for_row(row)
            if signal is None:
                return
            next_level = row.level + 1
            if next_level <= signal.gale_count and next_level <= self.risk.config.max_gales:
                gale_amount = trade.amount * self.risk.config.martingale_multiplier
                logger.info(
                    "recovered loss — auto-gale L%d on %s (amount %.2f)",
                    next_level, trade.asset, gale_amount,
                )
                await self._place_level(
                    signal, next_level, gale_amount,
                    known_balance=trade.balance_after,
                )

    # ---------------------------------------------------------- settlement
    async def run_settlement_loop(self, poll_s: float = 0.5) -> None:
        """Poll open trades until resolved; drive gales on losses. Runs forever."""
        while True:
            await self.run_settlement_loop_once(poll_s)
            await asyncio.sleep(poll_s)

    async def run_settlement_loop_once(self, poll_s: float = 0.5) -> None:
        """One polling pass over open trades. The offline feed uses this in a
        bounded loop; the long-running service wraps it in ``run_settlement_loop``."""
        try:
            for row in await self.repo.list_open_trades():
                try:
                    result = await self.broker.wait_for_settlement(
                        row.broker_trade_id, timeout_s=poll_s
                    )
                except TradeNotFound:
                    # The broker never acknowledged this order (stale row from
                    # a previous process, or an order the server dropped). Its
                    # outcome is unknowable — resolve it locally once and stop
                    # polling instead of failing every cycle.
                    await self.repo.mark_trade_cancelled(row.broker_trade_id)
                    logger.warning(
                        "trade %s unknown to broker — marked cancelled", row.broker_trade_id
                    )
                    continue
                except BrokerError as exc:
                    logger.warning("settlement lookup failed for %s: %s", row.broker_trade_id, exc)
                    continue
                if result.status is not TradeStatus.OPEN:
                    await self._on_settled(row, result)
        except Exception:  # keep the loop alive across transient errors
            logger.exception("settlement loop error")

    async def _on_settled(self, row: TradeRow, result: TradeResult) -> None:
        await self.repo.update_trade(result)
        await self.risk.record_result(result)
        logger.info(
            "RESULT L%d %s %s pnl=%.2f",
            row.level, result.asset, result.status.value, result.pnl,
        )

        # Fire the gale BEFORE the notification so the notification reads the
        # post-trade balance while it's still the settlement balance (a re-read
        # after the gale would already include the gale's stake deduction).
        if result.status is TradeStatus.LOSS:
            signal = await self._signal_for_row(row)
            if signal is not None:
                next_level = row.level + 1
                if next_level <= signal.gale_count and next_level <= self.risk.config.max_gales:
                    gale_amount = result.amount * self.risk.config.martingale_multiplier
                    logger.info("auto-gale L%d on %s (amount %.2f)", next_level, result.asset, gale_amount)
                    await self._place_level(
                        signal, next_level, gale_amount,
                        known_balance=result.balance_after,
                    )

        if not (result.status is TradeStatus.CANCELLED and self.recover_cancelled_outcomes):
            # Withhold the premature CANCELLED notification while recovery may
            # find the real outcome; the recovered result is sent instead.
            await self._notify_settled(result, row.level)

    async def _signal_for_row(self, row: TradeRow) -> Signal | None:
        if row.signal_id is None:
            return None
        sig = await self.repo.get_signal(row.signal_id)
        if sig is None:
            return None
        return Signal(
            asset=sig.asset,
            direction=sig.direction,
            expiry_seconds=sig.expiry_s,
            gale_count=sig.gale_count,
            raw_text=sig.raw_text,
            source=sig.source,
            signal_id=sig.id,
        )

    # ---------------------------------------------------------------- timing
    @staticmethod
    def _seconds_until(
        entry_time: time, offset_s: float = 0.0, *, now: datetime | None = None
    ) -> float:
        """Seconds until ``entry_time`` (+ ``offset_s``) on the local wall clock.

        The parsed ``entry_time`` is the signal provider's wall clock (e.g.
        UTC-3), and ``offset_s`` shifts it to this machine's clock.  The target
        is built as a *time-of-day* (``entry + offset`` wrapped to 24h) placed
        on today's date — so an offset that crosses midnight (a 20:00 signal on
        a +4h machine → 00:00) lands ~minutes away, not 24h.  A target already
        past today returns a negative delay (late-signal → skip path).  ``now``
        is injectable for deterministic tests."""
        now = now if now is not None else datetime.now().astimezone()
        entry_tod = entry_time.hour * 3600 + entry_time.minute * 60
        target_tod = (entry_tod + offset_s) % 86400
        target = (
            now.replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(seconds=target_tod)
        )
        return (target - now).total_seconds()
