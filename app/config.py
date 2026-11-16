"""Environment-driven configuration. Every value reads a ``QSB_*`` env var
(with python-dotenv loading ``.env``), so the bot is configured purely by
deploying env vars — no code changes for a new risk profile or a live broker.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

from broker.simulated import SimulatedConfig
from execution.risk import RiskConfig

# Load .env once at import time; repeated loads are cheap no-ops.
load_dotenv()


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_tuple(name: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    raw = _env(name, "").strip()
    return tuple(x.strip() for x in raw.split(",") if x.strip()) if raw else default


@dataclass(frozen=True)
class AppConfig:
    broker_mode: str = "simulated"          # simulated | quotex (live, gated)
    db_path: str = "data/bot.db"
    log_level: str = "INFO"
    balance_snapshot_interval_s: float = 60.0
    settlement_poll_s: float = 0.25
    entry_time_offset_s: float = 0.0        # shift parsed signal times (e.g. +4h)
    signal_dedup_window_s: float = 600.0    # ignore re-forwards within this window
    signal_expiry_s: int = 0                # override parsed expiry (0 = use signal's own)
    allowlist_enabled: bool = True          # gate Telegram ingestion by chat id
    allowed_chat_ids: tuple[int, ...] = ()
    telegram_token: str = ""
    # --- Live Quotex (only used when broker_mode == quotex) ---
    quotex_mode: str = "demo"               # demo | live — account type
    live_email: str = ""
    live_password: str = ""
    ssid: str = ""                          # generic token (any mode)
    demo_ssid: str = ""                     # demo-account token (preferred)
    live_ssid: str = ""                     # live-account token (preferred)
    live_ws_url: str = ""                   # empty => protocol.py default
    quotex_proxy: str = ""                  # socks/http proxy for login (Cloudflare 403s)
    # --- Userbot group monitor (optional) ---------------------------------
    # Reads signals from a private group the bot can't be added to, by logging
    # in as the user's own Telegram account (MTProto). See
    # telegram_signals/group_monitor.py. Empty monitor_links disables it.
    monitor_links: tuple[str, ...] = ()     # comma-separated invite links/usernames
    monitor_all: bool = False               # monitor every broadcast channel in user session
    monitor_api_id: int = 0                 # from my.telegram.org (your account)
    monitor_api_hash: str = ""
    monitor_phone: str = ""                 # international format, e.g. +15551234567
    monitor_session: str = "data/userbot.session"
    monitor_selection: str = "data/monitor_selection.json"

    @property
    def is_live(self) -> bool:
        return self.broker_mode == "quotex"

    @classmethod
    def from_env(cls) -> "AppConfig":
        mode = _env("QSB_BROKER_MODE", "simulated")
        chat_ids = tuple(
            int(x) for x in _env_tuple("QSB_ALLOWED_CHAT_IDS")
        )
        return cls(
            broker_mode=mode,
            db_path=_env("QSB_DB_PATH", "data/bot.db"),
            log_level=_env("QSB_LOG_LEVEL", "INFO"),
            balance_snapshot_interval_s=_env_float("QSB_SNAPSHOT_INTERVAL_S", 60.0),
            settlement_poll_s=_env_float("QSB_SETTLEMENT_POLL_S", 0.25),
            # env is in hours (human-friendly), stored as seconds
            entry_time_offset_s=_env_float("QSB_ENTRY_TIME_OFFSET", 0.0) * 3600.0,
            signal_dedup_window_s=_env_float("QSB_SIGNAL_DEDUP_WINDOW_S", 600.0),
            signal_expiry_s=_env_int("QSB_SIGNAL_EXPIRY_S", 0),
            allowlist_enabled=_env_bool("QSB_ALLOWLIST_ENABLED", True),
            allowed_chat_ids=chat_ids,
            telegram_token=_env("QSB_TELEGRAM_TOKEN", ""),
            quotex_mode=_env("QSB_QUOTEX_MODE", "demo").strip().lower(),
            live_email=_env("QSB_EMAIL", ""),
            live_password=_env("QSB_PASSWORD", ""),
            ssid=_env("QSB_SSID", ""),
            demo_ssid=_env("QSB_DEMO_SSID", ""),
            live_ssid=_env("QSB_LIVE_SSID", ""),
            live_ws_url=_env("QSB_WS_URL", ""),
            quotex_proxy=_env("QSB_QUOTEX_PROXY", ""),
            monitor_links=_env_tuple("QSB_MONITOR_LINK"),
            monitor_all=_env_bool("QSB_MONITOR_ALL", False),
            monitor_api_id=_env_int("QSB_MONITOR_API_ID", 0),
            monitor_api_hash=_env("QSB_MONITOR_API_HASH", ""),
            monitor_phone=_env("QSB_MONITOR_PHONE", ""),
            monitor_session=_env("QSB_MONITOR_SESSION", "data/userbot.session"),
            monitor_selection=_env("QSB_MONITOR_SELECTION", "data/monitor_selection.json"),
        )


@dataclass(frozen=True)
class BotConfig:
    """Everything needed to construct the broker, engine, and bot."""
    app: AppConfig
    risk: RiskConfig
    simulated: SimulatedConfig = field(default_factory=SimulatedConfig)

    @classmethod
    def from_env(cls) -> "BotConfig":
        app = AppConfig.from_env()
        sim = SimulatedConfig(
            seed=_env_int("QSB_SEED", 0),
            starting_balance=_env_float("QSB_STARTING_BALANCE", 10_000.0),
            payout_pct=_env_float("QSB_PAYOUT_PCT", 0.85),
            time_scale=_env_float("QSB_TIME_SCALE", 1.0),
        )
        risk = RiskConfig(
            base_amount=_env_float("QSB_BASE_AMOUNT", 10.0),
            martingale_multiplier=_env_float("QSB_MARTINGALE_MULTIPLIER", 2.0),
            max_gales=_env_int("QSB_MAX_GALES", 2),
            max_stake=_env_float("QSB_MAX_STAKE", 100.0),
            max_ladder_stake=_env_float("QSB_MAX_LADDER_STAKE", 0.0),
            max_daily_loss=_env_float("QSB_MAX_DAILY_LOSS", 200.0),
            max_concurrent_positions=_env_int("QSB_MAX_CONCURRENT_POSITIONS", 3),
            asset_allowlist=_env_tuple("QSB_ASSET_ALLOWLIST"),
            cooldown_seconds=_env_float("QSB_COOLDOWN_SECONDS", 5.0),
            entry_tolerance_seconds=_env_int("QSB_ENTRY_TOLERANCE_SECONDS", 60),
        )
        return cls(app=app, risk=risk, simulated=sim)
