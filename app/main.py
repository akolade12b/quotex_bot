"""Application entrypoint.

Two modes:

* ``python -m app.main``                 — live bot. Starts the settlement and
  balance-snapshot loops, consumes the signal queue, and polls Telegram if a
  token is configured.
* ``python -m app.main --signal TEXT``    — offline feed. Parses one signal,
  runs the full ladder on the (simulated) broker, prints the trade table, exits.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal as _signal
from typing import AsyncIterator

from broker.base import Broker
from broker.live_client import LiveQuotexClient
from broker.protocol import SOCKETIO_ENDPOINT
from broker.simulated import SimulatedBroker
from execution.engine import ExecutionEngine
from execution.risk import RiskManager
from persistence.repository import TradeRepository
from telegram_signals.bot import build_bot
from telegram_signals.group_monitor import GroupMonitor, should_run
from telegram_signals.handlers import SignalHandler
from telegram_signals.model import Signal
from telegram_signals.notifier import TradeNotifier
from telegram_signals.otp_bridge import OtpBridge
from telegram_signals.parser import SignalParser

from app.config import BotConfig

logger = logging.getLogger(__name__)


def build_broker(config: BotConfig, *, otp_bridge: object | None = None) -> Broker:
    """Instantiate the configured broker. Live mode is gated: it only activates
    when ``broker_mode == quotex`` and a token or credentials are present.

    ``otp_bridge`` is the Telegram OTP bridge handed to the live client so a
    2FA PIN during email/password login can be answered over Telegram.
    """
    if config.app.is_live:
        return LiveQuotexClient(
            mode=config.app.quotex_mode,
            email=config.app.live_email,
            password=config.app.live_password,
            ssid=config.app.ssid,
            demo_ssid=config.app.demo_ssid,
            live_ssid=config.app.live_ssid,
            ws_url=config.app.live_ws_url or SOCKETIO_ENDPOINT,
            proxy=config.app.quotex_proxy,
            otp_bridge=otp_bridge,
        )
    return SimulatedBroker(config.simulated)


async def _consume(queue: asyncio.Queue[Signal], engine: ExecutionEngine) -> None:
    """Drain parsed signals from the queue into the engine. Runs forever.

    Each signal is handled in its own task so a scheduled signal (which awaits
    its entry time) never blocks later signals from being processed.
    """
    while True:
        signal = await queue.get()
        asyncio.create_task(
            _handle_signal(signal, engine),
            name=f"signal:{signal.source}",
        )


async def _handle_signal(signal: Signal, engine: ExecutionEngine) -> None:
    try:
        await engine.on_signal(signal)
    except Exception:  # keep the bot alive across a bad signal
        logger.exception("failed to process signal from %s", signal.source)


async def _snapshot_loop(broker: Broker, repo: TradeRepository, interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        try:
            balance = await broker.get_balance()
            # equity == balance for the simulated broker (no mark-to-market);
            # the live client can refine this later.
            await repo.insert_balance_snapshot(balance, balance)
        except Exception:
            logger.exception("balance snapshot failed")


async def run_until_quiet(
    engine: ExecutionEngine,
    repo: TradeRepository,
    *,
    poll_s: float = 0.5,
    timeout_s: float = 120.0,
) -> None:
    """Run the settlement loop until no trades are open (or timeout). Used by
    the offline feed so the process can exit once the ladder completes."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while await repo.list_open_trades():
        if loop.time() > deadline:
            logger.warning("timeout after %.0fs with trades still open", timeout_s)
            return
        await engine.run_settlement_loop_once(poll_s)
        await asyncio.sleep(poll_s)


async def _print_summary(repo: TradeRepository) -> None:
    rows = await repo.list_trades()
    if not rows:
        print("(no trades)")
        return
    print(f"{'level':>5} {'asset':<10} {'dir':<5} {'amount':>8} {'status':<10} {'pnl':>8}")
    print("-" * 52)
    for r in rows:
        print(f"{r.level:>5} {r.asset:<10} {r.direction.value.upper():<5} "
              f"{r.amount:>8.2f} {r.status.value:<10} {r.pnl:>8.2f}")
    total = sum(r.pnl for r in rows)
    print("-" * 52)
    print(f"{'TOTAL':>5} {'':10} {'':5} {'':8} {'':10} {total:>8.2f}")


async def _connect_and_log(broker: Broker, config: BotConfig) -> float:
    """Connect the broker, log the result, and surface the live-mode warning."""
    await broker.connect()
    balance = await broker.get_balance()
    logger.info("broker connected: %s (balance %.2f)", type(broker).__name__, balance)
    if config.app.is_live and config.app.quotex_mode == "live":
        logger.warning("LIVE REAL-MONEY MODE ACTIVE — only proceed if the demo "
                       "ladder has been validated first")
    return balance


async def run(args: argparse.Namespace) -> None:
    config = BotConfig.from_env()
    logging.basicConfig(
        level=getattr(logging, config.app.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    repo = TradeRepository(config.app.db_path)
    await repo.connect()

    # OTP bridge first, so a 2FA PIN during login can be answered over Telegram
    # (the server path starts the bot before broker.connect()).
    otp_bridge = OtpBridge()
    broker = build_broker(config, otp_bridge=otp_bridge)

    risk = RiskManager(config.risk, repo)
    engine = ExecutionEngine(
        broker, repo, risk, entry_time_offset_s=config.app.entry_time_offset_s
    )
    # Wire outcome recovery: when the connection drops at trade expiry and the
    # server later reveals the real result, the client calls back into the
    # engine to record it and trigger the martingale on a loss.
    if isinstance(broker, LiveQuotexClient):
        broker.on_trade_outcome_recovered = engine.on_trade_recovered
        engine.recover_cancelled_outcomes = True
    queue: asyncio.Queue[Signal] = asyncio.Queue()

    if args.signal:
        # CLI single-shot: connect the broker first (no Telegram to answer an
        # OTP here — use a pasted token if the account needs one).
        try:
            await _connect_and_log(broker, config)
        except Exception as exc:
            await repo.close()
            raise SystemExit(1) from exc
        signal = SignalParser(
            override_expiry_s=config.app.signal_expiry_s
        ).parse(args.signal, source="cli")
        logger.info("feeding CLI signal for %s (gale_count %d)", signal.asset,
                    signal.gale_count)
        await engine.on_signal(signal, wait_for_entry=args.wait_entry)
        await run_until_quiet(engine, repo, poll_s=config.app.settlement_poll_s,
                              timeout_s=args.timeout)
        await _print_summary(repo)
        await broker.disconnect()
        await repo.close()
        return

    tasks = [
        asyncio.create_task(engine.run_settlement_loop(config.app.settlement_poll_s)),
        asyncio.create_task(_snapshot_loop(broker, repo, config.app.balance_snapshot_interval_s)),
        asyncio.create_task(_consume(queue, engine)),
    ]

    async def _status() -> tuple[str, float]:
        """Mode label + current balance for the /balance and /mode commands."""
        label = "simulated"
        if isinstance(broker, LiveQuotexClient):
            label = f"quotex-{broker.mode_label}"
        return label, await broker.get_balance()

    async def _switch_mode(demo: bool) -> tuple[str, float]:
        """Switch the Quotex account type, refusing while trades are open."""
        if not isinstance(broker, LiveQuotexClient):
            raise RuntimeError("demo/live switching requires the Quotex broker")
        open_trades = await repo.list_open_trades()
        if open_trades:
            raise RuntimeError(
                f"refusing to switch while {len(open_trades)} trade(s) are open"
            )
        label, balance = await broker.switch_mode(demo=demo)
        return f"quotex-{label}", balance

    # One shared signal handler so the PTB bot and the group monitor parse with
    # the same dedup window — a signal seen twice is dropped, never double-traded.
    handler = None
    monitor_run = should_run(
        config.app.monitor_links,
        config.app.monitor_api_id,
        config.app.monitor_api_hash,
        config.app.monitor_phone,
    )
    if config.app.telegram_token or monitor_run:
        handler = SignalHandler(
            queue,
            allowed_chat_ids=config.app.allowed_chat_ids,
            allowlist_enabled=config.app.allowlist_enabled,
            base_amount=config.risk.base_amount,
            dedup_window_s=config.app.signal_dedup_window_s,
            signal_expiry_s=config.app.signal_expiry_s,
            otp_bridge=otp_bridge,
        )

    bot = None
    monitor = None
    if config.app.telegram_token:
        bot = build_bot(
            config.app.telegram_token,
            queue,
            allowed_chat_ids=config.app.allowed_chat_ids,
            allowlist_enabled=config.app.allowlist_enabled,
            base_amount=config.risk.base_amount,
            dedup_window_s=config.app.signal_dedup_window_s,
            signal_expiry_s=config.app.signal_expiry_s,
            handler=handler,
            switch_mode=_switch_mode if isinstance(broker, LiveQuotexClient) else None,
            status_fn=_status,
        )
        await bot.initialize()
        await bot.start()
        await bot.updater.start_polling()
        logger.info("Telegram bot polling started")
        if config.app.allowed_chat_ids:
            notifier = TradeNotifier(bot.bot, config.app.allowed_chat_ids)
            engine.on_trade_placed = notifier.on_placed
            engine.on_trade_settled = notifier.on_settled
            engine.on_trade_rejected = notifier.on_rejected
            engine.on_trade_reconciled = notifier.on_reconciled
            logger.info(
                "trade notifications enabled for %d chat(s)",
                len(config.app.allowed_chat_ids),
            )
        else:
            logger.info("no QSB_ALLOWED_CHAT_IDS — trade notifications disabled")
    else:
        logger.info("no QSB_TELEGRAM_TOKEN — running engine only (feed via --signal)")

    if monitor_run:
        if handler is None:
            # Shouldn't happen (monitor_run implies handler was built above),
            # but guard anyway — the monitor needs the shared handler.
            logger.error("group monitor configured but signal handler not built")
        else:
            monitor = GroupMonitor(
                handler,
                session_path=config.app.monitor_session,
                api_id=config.app.monitor_api_id,
                api_hash=config.app.monitor_api_hash,
                phone=config.app.monitor_phone,
                links=config.app.monitor_links,
            )
            tasks.append(asyncio.create_task(
                monitor.run_forever(), name="group-monitor"
            ))
            logger.info(
                "group monitor configured for %d link(s): %s",
                len(config.app.monitor_links),
                ", ".join(config.app.monitor_links),
            )
    elif config.app.monitor_links:
        logger.warning(
            "QSB_MONITOR_LINK set but userbot credentials incomplete "
            "(need QSB_MONITOR_API_ID, QSB_MONITOR_API_HASH, QSB_MONITOR_PHONE) "
            "— group monitor disabled"
        )

    # Wire the OTP prompt to Telegram (the bot is already polling) so a 2FA PIN
    # required during broker connect can be answered here. Also log it, so a
    # terminal watcher sees it too.
    if bot is not None:
        allowed = config.app.allowed_chat_ids

        async def _send_otp_prompt(prompt: str) -> None:
            logger.info("OTP required: %s", prompt.strip())
            for cid in allowed:
                try:
                    await bot.bot.send_message(chat_id=cid, text=prompt)
                except Exception:
                    logger.exception("failed to send OTP prompt to chat %s", cid)

        otp_bridge.send_prompt = _send_otp_prompt
        logger.info("OTP bridge wired to Telegram (%d chat(s))", len(allowed))
    else:
        logger.info("no Telegram bot — OTP login must use a pasted QSB_SSID")

    stop = asyncio.Event()

    def _on_sigint(*_) -> None:  # pragma: no cover - signal handling
        stop.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(_signal.SIGINT, _on_sigint)

    async def _shutdown(broker_disconnect: bool = False) -> None:
        """Cancel background tasks and stop bot/monitor/repo. Safe to call once."""
        loop.remove_signal_handler(_signal.SIGINT)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if monitor is not None:
            await monitor.stop()
        if bot is not None:
            await bot.updater.stop()
            await bot.stop()
            await bot.shutdown()
        if broker_disconnect:
            await broker.disconnect()
        await repo.close()

    # Connect the broker AFTER the bot is up, so a startup OTP prompt can be
    # answered over Telegram instead of hanging the process. The connect runs
    # in a task raced against Ctrl+C: a connect stuck in an OTP wait (or a
    # credential retry loop) must not make the bot un-stoppable.
    connect_task = asyncio.create_task(_connect_and_log(broker, config))
    stop_task = asyncio.create_task(stop.wait())
    done, _ = await asyncio.wait(
        {connect_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
    )
    if connect_task not in done:
        logger.info("interrupted during connect — shutting down")
        connect_task.cancel()
        stop_task.cancel()
        await asyncio.gather(connect_task, stop_task, return_exceptions=True)
        await _shutdown(broker_disconnect=False)
        return
    stop_task.cancel()
    await asyncio.gather(stop_task, return_exceptions=True)
    try:
        await connect_task  # surface connect errors with full cleanup
    except Exception as exc:
        logger.error("broker failed to connect: %s", exc)
        await _shutdown(broker_disconnect=False)
        raise SystemExit(1) from exc

    print("bot running — Ctrl+C to stop")
    try:
        await stop.wait()
    finally:
        await _shutdown(broker_disconnect=True)
        logger.info("shutdown complete")


def main() -> None:
    parser = argparse.ArgumentParser(prog="quotex-bot", description=__doc__)
    parser.add_argument("--signal", help="raw MagicTrader signal text to feed once, then exit")
    parser.add_argument("--wait-entry", action="store_true",
                        help="with --signal: respect the signal's entry time instead of going now")
    parser.add_argument("--timeout", type=float, default=120.0,
                        help="with --signal: seconds to wait for the ladder to resolve")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
