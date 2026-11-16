"""Live Quotex WebSocket client (gated).

Only constructed when ``broker_mode=quotex`` and a token or credentials are
present. Talks the **modern** Quotex protocol (EIO=3, ``orders/open``,
binary ``s_balance``/``s_orders/close``) — see :mod:`broker.protocol`.

Design notes
------------
* **Auth** is a token (not email/password): the token comes from
  ``QSB_DEMO_SSID`` / ``QSB_LIVE_SSID`` / ``QSB_SSID``, or is captured on the
  fly from email+password via :mod:`broker.login`.
* **Demo vs live** is selected by the ``isDemo`` flag on this connection and
  on every order. ``switch_mode()`` reconnects with the other account's token
  (or a fresh email login) — there is no runtime ``changeBalance`` message.
* A background task reads frames, answers Engine.IO pings, and dispatches
  application events (balance, order open/close) into a per-trade
  ``asyncio.Event`` + result cache that :meth:`wait_for_settlement` reads.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time as _time
from dataclasses import dataclass, field
from datetime import datetime as _dt, timedelta, timezone
from typing import Any

import websockets
from websockets.protocol import State

from broker.base import (
    Broker,
    BrokerAuthError,
    BrokerUnavailable,
    Direction,
    InsufficientBalance,
    PlaceTradeRequest,
    TradeNotFound,
    TradeResult,
    TradeStatus,
    utcnow,
)
from broker import login
from broker.protocol import (
    SOCKETIO_ENDPOINT,
    build_authorization_message,
    build_balance_request,
    build_depth_follow_message,
    build_instruments_update_message,
    build_order_message,
    build_orders_closed_list_message,
    build_pending_list_message,
    build_tick_message,
    compute_order_time_and_type,
    decode_frame,
    encode_pong,
    sanitize_asset,
)

logger = logging.getLogger(__name__)

# Post-reconnect outcome recovery: keep polling the server's closed-order
# history this often, for up to this long, until every trade that was marked
# CANCELLED by a disconnection is matched (or the window passes). The snapshot
# the server pushes at connect can lag a trade that settled during the drop.
_RECOVERY_POLL_INTERVAL_S = 5.0
_RECOVERY_POLL_WINDOW_S = 30.0
_CLOSED_LIST_RESPONSE_TIMEOUT_S = 3.0

# A still-deducted balance at the nominal expiry is ambiguous: ``expires_at``
# is the placement+expiry estimate, but the server opens the order a beat later
# (s_orders/open is not reliably pushed, so the true close isn't known) and
# credits a WIN ~0.5s after its own close. The win credit can therefore land up
# to this long after the local estimate. A loss must not be declared before the
# allowance elapses, or a won trade is misread as a loss and a spurious gale is
# placed (observed live: a +9200 win fired a 20000 gale). 2.5s covers the
# observed ~2s server open-latency plus the ~0.5s credit window; classification
# happens AT this boundary (single read + current-time check), so a loss is
# confirmed only ~0.5s after the server's real close.
_SETTLE_LATENCY_ALLOWANCE_S = 2.5

# The Quotex edge drops WebSocket upgrades that don't looklike a browser
# (observed as an abrupt close with no status — websockets reports code 1005).
# A known-good client sends these; mirror it so the upgrade is accepted.
_WS_ORIGIN = "https://quotex.com"
_WS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
_WS_EXTRA_HEADERS = {
    "Referer": "https://quotex.com/",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass(frozen=True)
class LiveConfig:
    mode: str = "demo"            # "demo" | "live" — account type to connect as
    email: str = ""
    password: str = ""
    ssid: str = ""                # generic fallback token
    demo_ssid: str = ""           # demo-account token
    live_ssid: str = ""           # live-account token
    ws_url: str = SOCKETIO_ENDPOINT
    auth_timeout_s: float = 15.0
    proxy: str = ""               # QUOTEX_PROXY — socks/http proxy for login + WS
    otp_bridge: Any = None        # Telegram OTP bridge for 2FA login (see OtpBridge)

    @property
    def is_demo(self) -> bool:
        return self.mode != "live"


def _find_closed_order(
    orders: list[dict], req_id: str, server_id: str | None,
    trade: TradeResult | None = None, wire_asset: str = "",
) -> dict | None:
    """Look up a server closed-order entry for a given trade.

    1. Exact id match — fastest: the server's ``id`` matches our ``server_id``
       (translated from ``_server_ids``) or ``req_id`` (server echoed it back).
    2. Fallback heuristic — the server doesn't include our ``requestId`` in the
       closed-order payload, and the ``s_orders/open`` ack is sometimes never
       received (no ``_server_ids`` entry).  In that case we match by wire
       asset (resolved by the caller so OTC/normal resolution is consistent),
       amount, and ``openTimestamp`` proximity (±120 s)."""
    # 1. Exact id match
    for order in orders:
        oid = str(order.get("id") or "")
        if oid == server_id or oid == req_id:
            return order
    if trade is None:
        return None
    # 2. Heuristic fallback — same asset, amount, and close timestamp
    for order in orders:
        if str(order.get("asset") or "") != wire_asset:
            continue
        if float(order.get("amount") or 0) != trade.amount:
            continue
        open_ts = order.get("openTimestamp")
        if open_ts is None:
            continue
        try:
            if abs(float(open_ts) - trade.opened_at.timestamp()) <= 120:
                return order
        except (TypeError, ValueError, AttributeError):
            continue
    return None


class LiveQuotexClient(Broker):
    def __init__(
        self,
        config: LiveConfig | None = None,
        *,
        mode: str = "demo",
        email: str = "",
        password: str = "",
        ssid: str = "",
        demo_ssid: str = "",
        live_ssid: str = "",
        ws_url: str = SOCKETIO_ENDPOINT,
        proxy: str = "",
        otp_bridge: Any = None,
    ) -> None:
        if config is None:
            config = LiveConfig(
                mode=mode, email=email, password=password, ssid=ssid,
                demo_ssid=demo_ssid, live_ssid=live_ssid, ws_url=ws_url,
                proxy=proxy, otp_bridge=otp_bridge,
            )
        self.config = config
        self.is_demo = config.is_demo
        self._ws: Any = None
        self._recv_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._closing = False
        self._session = ""
        self._auth_sent = False
        self._auth_ready = asyncio.Event()
        self._auth_error: BrokerAuthError | None = None
        # Session-token self-healing: the configured token can expire; when the
        # server rejects it during auth, mark it rejected and fall back to a
        # fresh email/password login. The refreshed token is cached in
        # Refreshed credential sessions are cached separately for demo/live;
        # never reuse a demo token when switching to the real account.
        self._token_rejected = False
        self._refreshed_sessions: dict[bool, str] = {}
        # Backward-compatible view used by older diagnostics/tests; the
        # authoritative cache above remains keyed by account mode.
        self._refreshed_session: str | None = None
        self._depth_ready = asyncio.Event()  # set when depth/change received
        # Warm-subscription tracking: monotonic time of the last depth/change
        # for the currently-subscribed asset.  A warm subscription lets
        # place_trade skip the priming recipe and send the order frame
        # immediately (critical for gale latency).
        self._depth_last_seen = 0.0
        self._primed_asset: str | None = None
        # Instruments the server advertises for this account, captured from the
        # ``instruments/list`` push at connect.  Used to prefer a market's
        # regular (non-OTC) symbol over its OTC variant when both exist — the
        # user wants EUR/CAD (normal) used before EURCAD_otc.
        self._instruments: dict[str, dict] = {}
        self._balance = 0.0
        self._have_balance = False
        # Set whenever a fresh balance arrives (s_balance push or response to a
        # balance request).  `_refresh_balance()` waits on it to guarantee a
        # read reflects the server's current balance — the cached `_balance`
        # can otherwise lag real settlement for seconds (s_balance pushes are
        # not reliably sent on every trade), which corrupts both balance-delta
        # settlement and the balance reported to Telegram.
        self._balance_ready = asyncio.Event()
        # Consecutive unanswered balance requests -> the server has gone
        # silent (half-dead socket); after this many, force a reconnect.
        self._balance_timeouts = 0
        self._balance_timeout_reconnect = 3
        self._pending_binary_event: str | None = None
        # request_id -> TradeResult / Event; request_id -> server order id
        self._pending: dict[str, TradeResult] = {}
        self._settled: dict[str, TradeResult] = {}
        self._settle_events: dict[str, asyncio.Event] = {}
        self._server_ids: dict[str, str] = {}
        # One in-flight ``pending/list`` query (request/response, never pushed).
        self._pending_list_ready: asyncio.Event | None = None
        self._pending_list_data: Any = None
        # Saved across a connection loss: the pending trades and the server-id
        # mapping, so the post-reconnect reconciliation can match our cancelled
        # trades against the server's closed-order history and recover outcomes.
        self._saved_cancelled: dict[str, TradeResult] = {}
        self._saved_server_ids: dict[str, str] = {}
        # True when exactly one trade was pending at the disconnect. The only
        # case where a balance-delta fallback can attribute a post-reconnect
        # balance movement to a single disconnection-cancelled trade (see
        # ``_try_recover_by_balance_delta``); with several trades the movement
        # is their combined result and can't be split.
        self._saved_cancelled_single = False
        # Server's closed-order history captured right after (re)connect.
        self._closed_orders_ready = asyncio.Event()
        self._closed_orders: list[dict] = []
        # Per-trade balance snapshot taken when the order is placed — used to
        # detect settlement via the account balance change (the modern Quotex
        # protocol never pushes s_orders/close, so balance-delta is the only
        # settlement signal during normal operation).
        self._baseline_balance: dict[str, float] = {}
        # Gap between balance samples while waiting for a settlement to become
        # stable.  Tests shrink this; production keeps it wide enough to catch
        # the win credit that lands a beat after the deducted state at expiry.
        self._settle_sample_gap_s = 0.2
        # How long past the local expiry estimate a still-deducted balance is
        # treated as "possibly a late win credit" instead of a LOSS (see
        # ``_SETTLE_LATENCY_ALLOWANCE_S``). Tests shrink this to classify fast.
        self._settle_latency_allowance_s = _SETTLE_LATENCY_ALLOWANCE_S
        # Optional async callback ``(TradeResult) -> None`` invoked after a
        # reconnect when a trade marked CANCELLED turns out to have a real
        # outcome on the server (so the engine can record it and fire the gale
        # on a loss). Wired by app/main.py for the live client only.
        self.on_trade_outcome_recovered = None
        # Background task that keeps polling the server's closed-order history
        # after a reconnect until every disconnection-cancelled trade is
        # resolved (or a deadline passes). None when no recovery is pending.
        self._recovery_task: asyncio.Task | None = None

    @property
    def mode_label(self) -> str:
        return "demo" if self.is_demo else "live"

    # -------------------------------------------------------------- lifecycle
    async def connect(self) -> None:
        """Open the connection, then start the receive/keepalive loops.

        The server occasionally closes the socket mid-handshake (a transient
        ``41`` close packet during auth), so the initial connect is retried
        with backoff instead of letting one dropped attempt kill the bot.
        CancelledError propagates, so Ctrl+C during the retry stops cleanly.
        """
        self._closing = False
        backoff = 1.0
        while not self._closing:
            try:
                await self._open_connection()
                break
            except asyncio.CancelledError:
                raise
            except (BrokerAuthError, BrokerUnavailable) as exc:
                # HTTP 429 (Too Many Requests) means the server is rate-limiting
                # us — hammering it makes the limit worse, so back off hard.
                wait = max(backoff, 15.0) if "429" in str(exc) else backoff
                logger.warning(
                    "Quotex connect failed (%s); retrying in %.0fs", exc, wait
                )
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 60.0)
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        await self._request_balance()

    async def _open_connection(self) -> None:
        """Fetch a session token and complete the Socket.IO handshake.

        Idempotent enough to be re-run by the reconnect loop; never spawns
        tasks and never sets ``_closing`` (a transient failure during a
        reconnect must not stop the retry loop).
        """
        self._session, is_demo = await self._obtain_session_token()
        self.is_demo = is_demo
        self._auth_sent = False
        self._auth_ready = asyncio.Event()
        self._auth_error = None
        self._depth_ready = asyncio.Event()
        self._pending_binary_event = None
        logger.info("connecting to %s (demo=%s)", self.config.ws_url, self.is_demo)
        try:
            self._ws = await websockets.connect(
                self.config.ws_url,
                origin=_WS_ORIGIN,
                user_agent_header=_WS_USER_AGENT,
                additional_headers=_WS_EXTRA_HEADERS,
                # Quotex keepalive is Engine.IO-level ("2"/"3" text frames)
                # driven by our _ping_loop; disable the library's WS pings so
                # it can't kill a healthy Socket.IO connection.
                ping_interval=None,
            )
        except Exception as exc:
            raise BrokerUnavailable(f"websocket connect failed: {exc}") from exc

        try:
            async with asyncio.timeout(self.config.auth_timeout_s):
                # Engine.IO open -> send "40" -> server Socket.IO connect -> we
                # send the authorization frame -> ack (s_authorization or the
                # pushed instruments/list header). Answer pings while waiting.
                while not self._auth_ready.is_set():
                    frame = await self._ws.recv()
                    await self._handle_frame(frame)
                    if self._auth_error:
                        self._token_rejected = True
                        raise self._auth_error
        except asyncio.TimeoutError as exc:
            await self._abort_connection()
            self._token_rejected = True
            raise BrokerAuthError("timed out waiting for Quotex auth ack") from exc
        except websockets.ConnectionClosed as exc:
            await self._abort_connection()
            self._token_rejected = True
            raise BrokerUnavailable(f"connection closed during auth: {exc}") from exc

        logger.info("Quotex connected and authenticated (mode=%s)", self.mode_label)

    async def _abort_connection(self) -> None:
        """Close the socket without touching the task/closing state."""
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    async def _obtain_session_token(self) -> tuple[str, bool]:
        """Return ``(session_token, is_demo)`` for the configured mode.

        Preference order:
        1. a session already refreshed from credentials (cached),
        2. the configured token, unless it was rejected by the server,
        3. a fresh email/password login (self-heals an expired token).
        """
        demo = self.is_demo
        if demo in self._refreshed_sessions:
            return self._refreshed_sessions[demo], demo
        token = (self.config.demo_ssid if demo else self.config.live_ssid) or self.config.ssid
        if token and not self._token_rejected:
            return self._parse_ssid(token, default_demo=demo)
        if self.config.email and self.config.password:
            logger.info("refreshing Quotex session from credentials")
            token = await login.http_login_async(
                self.config.email,
                self.config.password,
                demo,
                otp_bridge=self.config.otp_bridge,
                proxy=self.config.proxy,
            )
            self._refreshed_sessions[demo] = token
            self._refreshed_session = token
            self._token_rejected = False
            return token, demo
        raise BrokerAuthError(
            "quotex mode requires QSB_DEMO_SSID/QSB_LIVE_SSID (or QSB_SSID), "
            "or QSB_EMAIL + QSB_PASSWORD"
        )

    @staticmethod
    def _parse_ssid(ssid: str, default_demo: bool) -> tuple[str, bool]:
        """Accept either a bare token or a full ``42["authorization",{...}]``
        frame; the latter also carries the account type."""
        if ssid.startswith('42["authorization",'):
            try:
                js = ssid[ssid.find("{"): ssid.rfind("}") + 1]
                data = json.loads(js)
                session = data.get("session") or ssid
                demo = bool(int(data.get("isDemo", 1 if default_demo else 0)))
                return session, demo
            except (ValueError, KeyError):
                pass
        return ssid, default_demo

    async def switch_mode(self, demo: bool) -> tuple[str, float]:
        """Reconnect as the demo or live account and report the new balance."""
        if demo == self.is_demo:
            return self.mode_label, await self.get_balance()
        await self.disconnect()
        self.is_demo = demo
        await self.connect()
        return self.mode_label, await self.get_balance()

    async def disconnect(self) -> None:
        self._closing = True
        if self._recovery_task is not None:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
            self._recovery_task = None
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None
        if self._ping_task is not None:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            self._ping_task = None
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        for trade_id, trade in list(self._pending.items()):
            trade.status = TradeStatus.CANCELLED
            trade.closed_at = utcnow()
            self._settled[trade_id] = trade
            self._settle_events.setdefault(trade_id, asyncio.Event()).set()
        self._pending.clear()
        self._settle_events.clear()
        self._server_ids.clear()
        self._baseline_balance.clear()
        self._have_balance = False

    # ---------------------------------------------------------------- Broker
    async def get_balance(self) -> float:
        # Always fetch a fresh balance — the cached value can lag real
        # settlement (s_balance pushes are not reliable), and callers such as
        # the settlement/notification path depend on an accurate read.
        balance, _ = await self._refresh_balance()
        return balance

    async def get_open_positions(self) -> list[TradeResult]:
        """Ask the server for its real open positions (``pending/list``).

        Each result's ``trade_id`` is the bot's own request id when the order
        was placed by this client (translated via the server-id map), otherwise
        the raw server id (e.g. orders opened manually on the website). This is
        the ground truth for concurrency gating — the local DB can hold stale
        ``open`` rows for orders the server silently dropped.
        """
        if not self._ws_open():
            raise BrokerUnavailable("Quotex client not connected")
        self._pending_list_data = None
        self._pending_list_ready = asyncio.Event()
        await self._ws.send(build_pending_list_message())
        try:
            await asyncio.wait_for(self._pending_list_ready.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            raise BrokerUnavailable("pending/list request timed out")
        return self._parse_pending_list(self._pending_list_data)

    def _parse_pending_list(self, payload: Any) -> list[TradeResult]:
        entries = payload.get("list") if isinstance(payload, dict) else payload
        if not isinstance(entries, list):
            return []
        out: list[TradeResult] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            server_id = str(entry.get("id") or "")
            if not server_id:
                continue
            command = entry.get("command")
            direction = (
                Direction.PUT if command == 1
                else Direction.CALL if command == 2
                else Direction.CALL
            )
            out.append(TradeResult(
                # Orders placed by this client are keyed by our request id in
                # the DB; manual website orders keep the raw server id.
                trade_id=self._server_ids.get(server_id, server_id),
                asset=str(entry.get("asset") or ""),
                direction=direction,
                amount=float(entry.get("amount") or 0.0),
                entry_price=float(entry.get("openPrice") or 0.0),
                expiry_s=0,
                opened_at=utcnow(),
                status=TradeStatus.OPEN,
            ))
        return out

    def _ws_open(self) -> bool:
        """True if the WebSocket is established. websockets>=14 exposes
        ``.state`` (an enum); older versions exposed ``.open``."""
        ws = self._ws
        if ws is None:
            return False
        state = getattr(ws, "state", None)
        if state is not None:
            return state is State.OPEN
        return bool(getattr(ws, "open", False))

    async def wait_connected(self, timeout_s: float = 10.0) -> bool:
        """Wait until the WebSocket is open again (a reconnect may be in
        progress).  The engine calls this to retry a trade whose scheduled
        entry collided with a brief disconnect."""
        deadline = _time.monotonic() + timeout_s
        while not self._ws_open():
            if _time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.2)
        return True

    async def prepare_asset(self, asset: str) -> None:
        """Prime the depth subscription for ``asset`` without placing an order.

        The engine calls this as soon as a signal is received, so by the time
        the scheduled entry arrives the order can be sent instantly via the
        warm-subscription fast path (instead of the ~2.5 s priming recipe
        running at entry time).  ``place_trade`` still primes defensively if a
        signal is fed directly without ``prepare_asset``.
        """
        await self._prime_subscription(self.resolve_asset(asset))

    async def _prime_subscription(self, asset: str) -> bool:
        """Ensure the depth subscription for ``asset`` is active; return whether
        it was already warm (no recipe needed).

        Quotex requires a specific pre-order subscription recipe to accept an
        order.  The old recipe (tick + instruments/follow) was silently dropped
        by the server.  The validated recipe (from debug/recipe_matrix and
        debug/bisect_recipe) is:

          1. tick               — primes the tick stream
          2. instruments/update — subscribes to the asset's candle data
          3. depth/follow       — subscribes to the depth-of-market stream
          4. wait for depth/change — confirms the subscription is active
          5. (orders/open)      — the caller places the actual trade

        Without step 2+3+4 the server silently ignores the order.
        Inter-message delays (~0.5 s) are critical — sending the setup frames
        in a burst causes the server to silently drop the order.

        Fast path: if we already hold a live depth subscription for this asset,
        the recipe is unnecessary — the server accepts a bare orders/open frame
        (validated live; cuts gale placement from ~2.5 s to ~0.1 s).
        """
        warm = (
            self._primed_asset == asset
            and (_time.monotonic() - self._depth_last_seen) < 120.0
        )
        if warm:
            return True
        if self._ws is None:
            return False
        self._depth_ready.clear()
        try:
            await self._ws.send(build_tick_message())
            await asyncio.sleep(0.5)
            await self._ws.send(build_instruments_update_message(asset, period=60))
            await asyncio.sleep(0.5)
            await self._ws.send(build_depth_follow_message(asset))
            try:
                await asyncio.wait_for(self._depth_ready.wait(), timeout=8.0)
            except asyncio.TimeoutError:
                logger.warning("depth/change not received for %s — placing order anyway", asset)
            # Let the server finish processing the depth subscription before
            # we push the order — the server drops orders sent too quickly.
            await asyncio.sleep(1.0)
        except websockets.ConnectionClosed as exc:
            raise BrokerUnavailable(
                f"Quotex connection closed while priming {asset}: {exc}"
            ) from exc
        self._primed_asset = asset
        self._depth_last_seen = _time.monotonic()
        return False

    async def place_trade(self, request: PlaceTradeRequest) -> TradeResult:
        if not self._ws_open():
            raise BrokerUnavailable("Quotex client not connected")
        if request.amount <= 0:
            raise InsufficientBalance("amount must be positive")
        if self._have_balance and request.amount > self._balance:
            raise InsufficientBalance(
                f"stake {request.amount:.2f} exceeds balance {self._balance:.2f}"
            )
        request_id = str(int(_time.time() * 1000))
        # Resolve the user-facing asset to the wire symbol, preferring the
        # regular (non-OTC) market when the server lists it (EUR/CAD -> EURCAD,
        # not EURCAD_otc).
        asset = self.resolve_asset(request.asset)
        option_type, time_field = compute_order_time_and_type(
            asset, request.expiry_s, _time.time()
        )
        trade = TradeResult(
            trade_id=request_id,
            asset=request.asset,
            direction=request.direction,
            amount=request.amount,
            entry_price=0.0,  # filled by the order-opened event
            expiry_s=request.expiry_s,
            opened_at=utcnow(),
            status=TradeStatus.OPEN,
            expires_at=utcnow() + timedelta(seconds=request.expiry_s),
            signal_id=request.signal_id,
        )
        self._pending[request_id] = trade
        self._settle_events.setdefault(request_id, asyncio.Event())
        # Snapshot the current balance so wait_for_settlement can later detect
        # settlement via the balance change (the modern protocol never pushes
        # s_orders/close events; see _settle_by_balance_delta).
        self._baseline_balance[request_id] = self._balance
        # Quotex requires a specific pre-order subscription recipe to accept
        # an order — see ``_prime_subscription``.  The engine calls
        # ``prepare_asset`` when the signal is received (ahead of its entry
        # time), so the subscription is usually already warm here and the
        # order fires instantly.
        warm = await self._prime_subscription(asset)
        order_frame = build_order_message(
            asset=asset,
            amount=request.amount,
            time_field=time_field,
            action=request.direction.value,  # "call" | "put"
            is_demo=self.is_demo,
            request_id=int(request_id),
            option_type=option_type,
        )
        try:
            await self._ws.send(order_frame)
            if warm:
                logger.info("sending order (warm subscription): %s", order_frame[:120])
            else:
                logger.info("sending order (depth_ready=%s): %s", self._depth_ready.is_set(), order_frame[:120])
        except websockets.ConnectionClosed as exc:
            raise BrokerUnavailable(
                f"Quotex connection closed while placing trade: {exc}"
            ) from exc
        logger.info("orders/open sent for %s %.2f (req %s)", request.asset, request.amount, request_id)
        return trade

    async def wait_for_settlement(self, trade_id: str, timeout_s: float) -> TradeResult:
        event = self._settle_events.get(trade_id)
        if event is None:
            # Unknown to this client: either a stale row from a previous
            # process or an order the server never acknowledged. Distinct from
            # a down connection — polling it again will never resolve.
            raise TradeNotFound(f"unknown trade {trade_id!r}")
        trade = self._pending.get(trade_id)
        past_expiry = (
            trade is not None
            and trade.expires_at is not None
            and utcnow() >= trade.expires_at
        )
        if past_expiry:
            # Already past expiry — don't waste the poll on the event wait
            # (the modern protocol never pushes s_orders/close); settle via the
            # balance change immediately.
            await self._settle_by_balance_delta(trade)
        else:
            try:
                await asyncio.wait_for(event.wait(), timeout_s)
            except asyncio.TimeoutError:
                pass
            if (
                trade is not None
                and trade.expires_at is not None
                and utcnow() >= trade.expires_at
                and trade_id not in self._settled
            ):
                await self._settle_by_balance_delta(trade)
        # Settled by a pushed event (s_orders/close, disconnect recovery)?
        if trade_id in self._settled:
            self._baseline_balance.pop(trade_id, None)
            return self._settled[trade_id]
        trade = self._pending.get(trade_id)
        if trade is None:
            # Already removed from _pending by _apply_deal / disconnect.
            self._baseline_balance.pop(trade_id, None)
            return self._settled.get(trade_id, TradeResult(
                trade_id=trade_id, asset="", direction=Direction.PUT,
                amount=0.0, entry_price=0.0, expiry_s=0, opened_at=utcnow(),
                status=TradeStatus.CANCELLED,
            ))
        return self._settled.get(trade_id, trade)

    async def _settle_by_balance_delta(self, trade: TradeResult) -> None:
        """Detect settlement via the account balance change — FAST.

        Quotex deducts the stake at placement and credits the refund + profit
        at settlement, so at expiry a WIN briefly shows the *deducted* balance
        and is indistinguishable from a LOSS until the credit lands (~0.5s after
        the server's close). The server opens the order a beat after placement
        (up to ~2s), so the credit can land up to ~2.5s after our local expiry
        estimate.

        The settlement loop re-polls every poll_s (~0.25s), so this method does
        ONE fresh balance read per call:

        * balance above the deducted level -> WIN/DRAW resolving; one
          confirmation read for the final value, then classify;
        * balance at the deducted level    -> ambiguous: either a WIN whose
          credit hasn't landed yet, or a LOSS. Only once the settlement-latency
          allowance (``_settle_latency_allowance_s``) has elapsed past the
          local expiry estimate is it a certain LOSS; before then the trade
          stays OPEN and the next poll re-checks.

        A loss is confirmed at expires_at + allowance — only ~0.5s after the
        server's real close — so the gale fires within the user-visible 2s.
        If the balance never moves after a generous grace window the order was
        likely silently dropped by the server (never opened); mark it CANCELLED
        so the trade doesn't block subsequent signals forever.
        """
        baseline = self._baseline_balance.get(trade.trade_id, self._balance)
        grace = timedelta(seconds=max(60, trade.expiry_s * 2))
        deducted = baseline - trade.amount

        # One fresh read per call — the settlement loop re-polls every poll_s
        # (~0.25s), so a multi-read budget loop here would only add latency.  A
        # stale (timed-out) read is unusable: at expiry a WIN still shows the
        # deducted stake, so classifying from it would misread the win as a loss.
        balance, fresh = await self._refresh_balance()
        if not fresh:
            logger.warning(
                "settlement read for %s not fresh — deferring classification",
                trade.trade_id,
            )
            return

        # Checked AFTER the read so the latency-allowance boundary uses the
        # current time, not the cycle start — a loss is confirmed at
        # expires_at + allowance, not a full budget cycle later.
        now = utcnow()

        if balance > deducted + 0.5:
            # Win/draw credit landing — one confirmation read for the final
            # value before classifying.
            await asyncio.sleep(self._settle_sample_gap_s)
            balance, fresh = await self._refresh_balance()
            if not fresh:
                return

        delta = balance - baseline

        if abs(delta) < 0.5:
            # No balance movement — order may have been dropped (never opened)
            # or the credit hasn't arrived yet.  Retry next poll until the
            # grace window expires, then give up.
            if now > trade.expires_at + grace:
                trade.status = TradeStatus.CANCELLED
                trade.closed_at = now
                self._pending.pop(trade.trade_id, None)
                self._settled[trade.trade_id] = trade
                self._settle_events.setdefault(
                    trade.trade_id, asyncio.Event()
                ).set()
                self._baseline_balance.pop(trade.trade_id, None)
                logger.warning(
                    "trade %s: no balance movement after %s grace — marked "
                    "cancelled (dropped?)",
                    trade.trade_id, grace,
                )
            return

        # Balance moved — classify.  `baseline` is the pre-placement balance and
        # the stake was deducted at placement, so `delta` is already the net
        # result: -stake on a loss, +profit on a win, 0 on a draw.
        if delta < 0 and trade.expires_at is not None:
            # A still-deducted balance within the latency allowance is NOT a
            # certain loss: the win credit can land up to ``_settle_latency_allowance_s``
            # past our nominal expiry (the server opens a beat after placement,
            # then credits ~0.5s after its own close). Classifying here would
            # misread that win as a loss and fire a spurious gale. Keep the
            # trade OPEN and let the next settlement poll re-check the balance
            # until the allowance has elapsed.
            if now < trade.expires_at + timedelta(
                seconds=self._settle_latency_allowance_s
            ):
                logger.info(
                    "trade %s still at deducted balance %.2f — within %ss "
                    "settlement-latency allowance, deferring classification",
                    trade.trade_id, balance, self._settle_latency_allowance_s,
                )
                return

        trade.closed_at = now
        trade.balance_after = balance
        if delta > 0:
            trade.status = TradeStatus.WIN
            trade.pnl = delta
        elif delta < 0:
            trade.status = TradeStatus.LOSS
            trade.pnl = delta
        else:
            trade.status = TradeStatus.DRAW
            trade.pnl = 0.0

        self._pending.pop(trade.trade_id, None)
        self._settled[trade.trade_id] = trade
        self._settle_events.setdefault(trade.trade_id, asyncio.Event()).set()
        self._baseline_balance.pop(trade.trade_id, None)
        logger.info(
            "balance-delta settlement: %s %s pnl=%.2f "
            "(delta=%.2f from baseline %.2f)",
            trade.trade_id, trade.status.value, trade.pnl,
            delta, baseline,
        )

    # --------------------------------------------------------- outcome recovery
    async def _drain_until_closed_orders(self, timeout_s: float) -> None:
        """Process buffered frames after a reconnect until the server's
        ``orders/closed/list`` push arrives (or timeout).  The push is
        normally part of the initial data burst the server sends right after
        auth, but it sits in the socket buffer while ``_open_connection``
        consumes frames up to the auth ack — this drain reads the rest."""
        try:
            async with asyncio.timeout(timeout_s):
                while not self._closed_orders_ready.is_set() and self._ws is not None:
                    frame = await self._ws.recv()
                    await self._handle_frame(frame)
        except (asyncio.TimeoutError, websockets.ConnectionClosed):
            pass

    async def _recover_cancelled_trades(self) -> None:
        """Match trades that ``_on_connection_lost`` marked CANCELLED against
        the server's closed-order history and hand the final state back to the
        engine — real outcomes (WIN / LOSS / DRAW) when found, or the still-
        CANCELLED trade otherwise (so the engine can send the withheld
        notification and the user isn't left in the dark).

        The snapshot the server pushes at connect can lag a trade that settled
        during the disconnect, so unmatched trades are re-polled in a
        background task that actively re-requests the history until they show
        up (or a deadline passes)."""
        if not self._saved_cancelled:
            return
        await self._drain_until_closed_orders(timeout_s=3.0)
        if not self._closed_orders_ready.is_set():
            logger.warning(
                "no closed-order history after reconnect — "
                "%d cancelled trade(s) left as unknown",
                len(self._saved_cancelled),
            )
        await self._match_cancelled_trades()
        if self._saved_cancelled:
            # The just-settled trade may not be in the connect snapshot yet;
            # keep polling in the background so we don't block the recv loop.
            if self._recovery_task is None or self._recovery_task.done():
                self._recovery_task = asyncio.create_task(self._poll_recover_remaining())
            logger.info(
                "%d cancelled trade(s) still unmatched — retrying closed-order history",
                len(self._saved_cancelled),
            )

    async def _match_cancelled_trades(self) -> None:
        """One pass: match saved cancelled trades against the current closed-
        order history, recover the ones found, and hand them to the engine."""
        # request id -> server id (only populated when they differ)
        req_to_server = {v: k for k, v in self._saved_server_ids.items()}
        for req_id, trade in list(self._saved_cancelled.items()):
            server_id = req_to_server.get(req_id)
            order = _find_closed_order(
                self._closed_orders, req_id, server_id, trade,
                wire_asset=self.resolve_asset(trade.asset),
            )
            if order is None:
                continue
            profit = float(order.get("profit") or 0.0)
            trade.status = (
                TradeStatus.WIN if profit > 0
                else TradeStatus.LOSS if profit < 0
                else TradeStatus.DRAW
            )
            trade.pnl = profit
            trade.closed_at = utcnow()
            self._settled[req_id] = trade
            self._settle_events.setdefault(req_id, asyncio.Event()).set()
            del self._saved_cancelled[req_id]
            logger.info(
                "recovered %s %s pnl=%.2f for cancelled req %s",
                trade.asset, trade.status.value, profit, req_id,
            )
            if self.on_trade_outcome_recovered is not None:
                try:
                    await self.on_trade_outcome_recovered(trade)
                except Exception:
                    logger.exception(
                        "outcome-recovery callback failed for %s", req_id
                    )

        # Balance-delta fallback: the closed-order history can lag a trade that
        # settled during the disconnect (or never surface it before the window
        # ends). When exactly one trade was pending at the disconnect and it is
        # still unmatched, the post-settlement balance relative to its placement
        # baseline is that trade's outcome — use it so a loss still fires the
        # gale instead of being written off as CANCELLED.
        if self._saved_cancelled_single and len(self._saved_cancelled) == 1:
            req_id, trade = next(iter(self._saved_cancelled.items()))
            await self._try_recover_by_balance_delta(req_id, trade)

    async def _try_recover_by_balance_delta(
        self, req_id: str, trade: TradeResult,
    ) -> None:
        """Recover a disconnection-cancelled trade's outcome from the balance.

        The balance settles on the server even while we're disconnected, and
        the closed-order history can lag it for many seconds (or never show
        it). The account balance is authoritative: for a single unmatched
        trade, the post-settlement balance relative to its placement baseline
        is exactly its result (-stake = loss, +profit = win). The win credit
        can land a beat late, so the same settle-with-budget read as
        ``_settle_by_balance_delta`` is used, and the trade is only classified
        once past its expiry — before that the balance only shows the deducted
        stake, which would misread a still-open trade as a loss.
        """
        if trade.expires_at is None or utcnow() < trade.expires_at:
            return  # still open on the server — the balance only shows the stake
        baseline = self._baseline_balance.get(req_id)
        if baseline is None:
            return
        deducted = baseline - trade.amount
        budget = utcnow() + timedelta(seconds=self._settle_sample_gap_s * 2)
        balance, fresh = await self._refresh_balance()
        if not fresh:
            return
        while utcnow() < budget and balance <= deducted + 0.5:
            await asyncio.sleep(self._settle_sample_gap_s)
            balance, fresh = await self._refresh_balance()
            if not fresh:
                return
        if balance > deducted + 0.5:
            await asyncio.sleep(self._settle_sample_gap_s)
            balance, fresh = await self._refresh_balance()
            if not fresh:
                return
        delta = balance - baseline
        if abs(delta) < 0.5:
            # No balance movement — the order was likely dropped (never opened);
            # leave it CANCELLED rather than guess.
            return
        trade.closed_at = utcnow()
        trade.balance_after = balance
        if delta > 0:
            trade.status = TradeStatus.WIN
        else:
            trade.status = TradeStatus.LOSS
        trade.pnl = delta
        self._settled[req_id] = trade
        self._settle_events.setdefault(req_id, asyncio.Event()).set()
        del self._saved_cancelled[req_id]
        self._baseline_balance.pop(req_id, None)
        logger.info(
            "recovered %s %s pnl=%.2f for cancelled req %s "
            "(balance delta %.2f from baseline %.2f)",
            trade.asset, trade.status.value, trade.pnl, req_id, delta, baseline,
        )
        if self.on_trade_outcome_recovered is not None:
            try:
                await self.on_trade_outcome_recovered(trade)
            except Exception:
                logger.exception("outcome-recovery callback failed for %s", req_id)

    async def _poll_recover_remaining(self) -> None:
        """Background: actively re-request the closed-order history until every
        remaining cancelled trade is resolved or the deadline passes."""
        deadline = _time.monotonic() + _RECOVERY_POLL_WINDOW_S
        while self._saved_cancelled and not self._closing:
            await asyncio.sleep(_RECOVERY_POLL_INTERVAL_S)
            if not self._saved_cancelled or not self._ws_open() or self._closing:
                break
            self._closed_orders_ready = asyncio.Event()
            try:
                await self._ws.send(build_orders_closed_list_message())
                await asyncio.wait_for(
                    self._closed_orders_ready.wait(),
                    timeout=_CLOSED_LIST_RESPONSE_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                # One slow/ignored list request is not fatal — keep polling the
                # rest of the window (and still run the balance-delta fallback)
                # instead of abandoning recovery over a single timeout.
                logger.warning(
                    "closed-order history request timed out — continuing recovery poll"
                )
            except websockets.ConnectionClosed:
                break
            await self._match_cancelled_trades()
            if _time.monotonic() >= deadline:
                break
        # Final pass: notify the engine for anything still cancelled so the
        # withheld CANCELLED notification goes out.
        for req_id, trade in list(self._saved_cancelled.items()):
            if self.on_trade_outcome_recovered is not None:
                try:
                    await self.on_trade_outcome_recovered(trade)
                except Exception:
                    logger.exception(
                        "outcome-recovery callback failed for %s", req_id
                    )
        self._saved_cancelled.clear()
        self._saved_server_ids.clear()

    # --------------------------------------------------------------- receive
    async def _recv_loop(self) -> None:
        """Read frames and dispatch them; reconnect with backoff on loss."""
        backoff = 1.0
        while not self._closing:
            try:
                if self._ws is None:
                    await asyncio.sleep(backoff)
                    continue
                frame = await self._ws.recv()
                await self._handle_frame(frame)
                backoff = 1.0
            except websockets.ConnectionClosed:
                logger.warning("Quotex connection closed — reconnecting")
                self._on_connection_lost()
                if self._closing:
                    break
                while not self._closing:
                    try:
                        await self._open_connection()
                        await self._request_balance()
                        await self._recover_cancelled_trades()
                        backoff = 1.0
                        logger.info("Quotex reconnected (mode=%s)", self.mode_label)
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "reconnect attempt failed (%s); retrying in %.0fs",
                            exc, backoff,
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30.0)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("recv loop error")

    async def _ping_loop(self) -> None:
        """Send an Engine.IO ping ("2") every 25s.

        This server is client-driven: it drops the connection at ~30s if the
        client doesn't ping. (The server may also ping us; _handle_frame pongs.)
        """
        while not self._closing:
            await asyncio.sleep(25)
            if self._closing or self._ws is None:
                continue
            try:
                await self._ws.send("2")
            except websockets.ConnectionClosed:
                logger.debug("ping failed — connection already down")
            except Exception:
                logger.debug("ping send error", exc_info=True)

    def _on_connection_lost(self) -> None:
        """The socket is gone: drop our reference and resolve in-flight trades
        as CANCELLED so the settlement loop doesn't wait forever. Save enough
        state for the post-reconnect reconciliation to recover real outcomes
        from the server's closed-order history (so gales still fire on losses)."""
        self._ws = None
        self._have_balance = False
        self._depth_ready = asyncio.Event()
        # The depth subscription dies with the socket — a fresh socket has no
        # active subscription, so the warm-subscription fast path must not be
        # used until the next connect re-primes it.
        self._depth_last_seen = 0.0
        self._primed_asset = None
        self._pending_binary_event = None
        self._closed_orders_ready = asyncio.Event()
        self._closed_orders = []
        # A new disconnect supersedes any pending outcome recovery; the fresh
        # state below will be recovered by the next reconnect instead.
        if self._recovery_task is not None and not self._recovery_task.done():
            self._recovery_task.cancel()
        self._recovery_task = None
        # Snapshot before clearing: the reconciliation needs these after
        # reconnect to match our cancelled trades against the server's data.
        self._saved_cancelled = dict(self._pending)
        self._saved_server_ids = dict(self._server_ids)
        self._saved_cancelled_single = len(self._saved_cancelled) == 1
        for trade_id, trade in list(self._pending.items()):
            trade.status = TradeStatus.CANCELLED
            trade.closed_at = utcnow()
            self._settled[trade_id] = trade
            self._settle_events.setdefault(trade_id, asyncio.Event()).set()
        self._pending.clear()
        self._server_ids.clear()

    async def _handle_frame(self, frame: str | bytes) -> None:
        if isinstance(frame, bytes):
            self._dispatch_binary_payload(frame.decode("utf-8", "replace"))
            return
        if frame == "2":  # Engine.IO ping — answer or the server drops us.
            if self._ws is not None:
                await self._ws.send(encode_pong())
            return
        if frame.startswith("0") and '"sid"' in frame:  # Engine.IO open
            if self._ws is not None:
                await self._ws.send("40")
            return
        if frame.startswith("40"):  # Socket.IO connect — authenticate now
            if self._ws is not None and not self._auth_sent:
                self._auth_sent = True
                await self._ws.send(build_authorization_message(self._session, self.is_demo))
            return
        kind, data = decode_frame(frame)
        if kind == "event":
            self._on_event(*data)
        elif kind == "binary_header":
            self._pending_binary_event = data
            # depth/change confirms the depth subscription is active — the
            # server will now accept an orders/open for this asset.
            if data == "depth/change":
                logger.info("depth/change received — subscription active")
                self._depth_ready.set()
        elif kind == "binary_payload":
            self._dispatch_binary_payload(frame)
        elif kind == "connect_error":
            self._auth_error = BrokerAuthError(f"connect_error: {data!r}")
            self._auth_ready.set()
        elif kind == "close":
            # The server tears the Socket.IO session down with a 41 packet.
            # recv() does not raise on a clean Socket.IO close, so without this
            # the client would sit in a zombie state: no reconnect, and any
            # in-flight trades would never settle — their DB rows stay open
            # and block later signals via max_concurrent_positions. Raise so
            # the recv loop's loss handler resolves them and reconnects.
            logger.warning("server closed the connection (Socket.IO close packet)")
            raise websockets.ConnectionClosed(None, None)
        else:
            logger.debug("ignoring frame kind=%s", kind)

    def _dispatch_binary_payload(self, frame: str) -> None:
        if not frame.startswith("\x04"):
            return
        try:
            payload = json.loads(frame[1:])
        except ValueError:
            logger.warning("binary payload was not JSON")
            return
        name = self._pending_binary_event
        self._pending_binary_event = None
        if name in ("s_balance", "s_balance/list"):
            self._update_balance(payload)
        elif name == "s_orders/open":
            logger.info("s_orders/open ACK received: %s", json.dumps(payload)[:200])
            self._on_order_opened(payload)
        elif name == "s_orders/close":
            self._on_order_closed(payload)
        elif name == "pending/list":
            self._on_pending_list(payload)
        elif name == "orders/closed/list":
            self._on_closed_orders(payload)
        elif name == "depth/change":
            self._depth_last_seen = _time.monotonic()
            self._depth_ready.set()
            logger.debug("depth/change received — subscription active")
        elif name == "instruments/list":
            self._on_instruments_list(payload)
        else:
            logger.debug("binary event %s: %r", name, payload)

    # --------------------------------------------------------------- events
    def _on_event(self, name: str, args: list[Any]) -> None:
        logger.debug("event %s: %r", name, args)
        if name == "s_authorization":
            self._auth_ready.set()
        elif name == "instruments/list":
            # Pushed right after auth; also signals that auth succeeded.
            self._auth_ready.set()
        elif name in ("successupdateBalance",):
            if args and isinstance(args[0], dict):
                self._update_balance(args[0])
        elif name == "s_orders/open" or name == "successopenOrder":
            logger.info("text event %s received: %s", name, json.dumps(args)[:200])
            if args and isinstance(args[0], dict):
                self._on_order_opened(args[0])
        elif name == "s_orders/close" or name == "successcloseOrder":
            if args:
                self._on_order_closed(args[0])
        elif name == "error":
            logger.warning("Quotex error event: %r", args)
        else:
            logger.debug("unhandled Quotex event %s", name)

    def _on_order_opened(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        req_id = str(payload.get("requestId") or "")
        server_id = str(payload.get("id") or payload.get("orderId") or "")
        trade = self._pending.get(req_id)
        if trade is None:
            logger.debug("order_opened for unknown request %s", req_id)
            return
        if server_id and server_id != req_id:
            self._server_ids[server_id] = req_id
        price = payload.get("openPrice")
        if isinstance(price, (int, float)):
            trade.entry_price = float(price)
        balance = payload.get("accountBalance") or payload.get("balance")
        if isinstance(balance, (int, float)) and balance > 0:
            self._balance = float(balance)
            self._have_balance = True
        # The ACK carries the server's authoritative open/close timestamps.
        # Our local expires_at was set when place_trade was CALLED, but the
        # order isn't opened until the recipe + server processing finish
        # (seconds later) — settling on the local estimate would read the
        # balance BEFORE the server settles and misread a WIN as a LOSS.
        # Prefer the server's close timestamp (works for both OTC — open+60s —
        # and minute-anchored regular pairs); fall back to open + expiry.
        close_ts = payload.get("closeTimestamp")
        open_ts = payload.get("openTimestamp")
        if isinstance(close_ts, (int, float)) and close_ts > 0:
            trade.expires_at = _dt.fromtimestamp(close_ts, tz=timezone.utc)
        elif isinstance(open_ts, (int, float)) and open_ts > 0:
            trade.expires_at = _dt.fromtimestamp(open_ts, tz=timezone.utc) + timedelta(
                seconds=trade.expiry_s
            )
        logger.info("order opened: req=%s server=%s asset=%s amount=%.2f expires=%s",
                    req_id, server_id or "?", trade.asset, trade.amount, trade.expires_at)

    def _on_order_closed(self, payload: Any) -> None:
        for deal in _iter_deals(payload):
            try:
                self._apply_deal(deal)
            except Exception:
                logger.exception("failed to apply closed-order deal %r", deal)

    def _apply_deal(self, deal: dict[str, Any]) -> None:
        server_id = str(deal.get("id") or "")
        req_id = self._server_ids.get(server_id)
        if req_id is None:
            req_id = str(deal.get("requestId") or "")
        trade = self._pending.get(req_id)
        if trade is None:
            logger.debug("order_closed for unknown request %s", req_id)
            return
        profit = float(deal.get("profit") or 0.0)
        if profit > 0:
            trade.status = TradeStatus.WIN
        elif profit < 0:
            trade.status = TradeStatus.LOSS
        else:
            trade.status = TradeStatus.DRAW
        trade.pnl = profit
        price = deal.get("closePrice")
        if isinstance(price, (int, float)):
            trade.exit_price = float(price)
        trade.closed_at = utcnow()
        self._pending.pop(req_id, None)
        self._settled[req_id] = trade
        self._settle_events.setdefault(req_id, asyncio.Event()).set()
        self._baseline_balance.pop(req_id, None)
        logger.info("order closed: req=%s %s pnl=%.2f", req_id, trade.status.value, profit)

    def _on_pending_list(self, payload: Any) -> None:
        self._pending_list_data = payload
        if self._pending_list_ready is not None:
            self._pending_list_ready.set()

    def _on_closed_orders(self, payload: Any) -> None:
        if isinstance(payload, dict):
            payload = payload.get("list") or payload.get("orders") or payload
        if isinstance(payload, list):
            self._closed_orders = [o for o in payload if isinstance(o, dict)]
        self._closed_orders_ready.set()

    def _on_instruments_list(self, payload: Any) -> None:
        """Record the instruments the server advertises for this account.

        Each row is a list whose [1] is the wire symbol (``EURCAD`` /
        ``EURCAD_otc``) and [9] is the OTC flag.  The map keys are the wire
        symbols; ``resolve_asset`` consults it to prefer a market's regular
        symbol over its OTC variant."""
        if isinstance(payload, dict):
            payload = payload.get("list") or payload.get("instruments") or payload
        if not isinstance(payload, list):
            return
        instruments: dict[str, dict] = {}
        for row in payload:
            if not isinstance(row, list) or len(row) < 2:
                continue
            sym = str(row[1])
            if not sym:
                continue
            instruments[sym] = {
                "label": str(row[2]) if len(row) > 2 else "",
                "is_otc": bool(row[9]) if len(row) > 9 else "_otc" in sym.lower(),
            }
        if instruments:
            self._instruments = instruments
            logger.debug("captured %d instruments", len(instruments))

    def resolve_asset(self, asset: str) -> str:
        """Resolve a user-facing asset (``EUR/CAD``) to a wire symbol.

        Prefers the market's regular (non-OTC) symbol when the server lists it
        for this account — EUR/CAD -> ``EURCAD``, not ``EURCAD_otc`` — falling
        back to the OTC variant when only that is listed.  An explicit
        ``_otc`` request stays OTC.  If the instrument list hasn't been
        captured yet (first trade racing the connect burst), falls back to the
        24/7 OTC default so an order is never blocked on the map being empty."""
        if not asset:
            return asset
        cleaned = asset.replace("/", "").upper().replace("_OTC", "_otc")
        if cleaned.endswith("_otc"):
            return cleaned
        if self._instruments:
            if cleaned in self._instruments and not self._instruments[cleaned]["is_otc"]:
                return cleaned
            if cleaned + "_otc" in self._instruments:
                return cleaned + "_otc"
        # No map yet, or neither variant listed — keep the 24/7 OTC default
        # the placement recipe was built around.
        return cleaned + "_otc"

    def _update_balance(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return
        # s_balance/list carries per-account balances: pick ours by mode.
        account_key = "demoBalance" if self.is_demo else "liveBalance"
        if account_key in payload and isinstance(payload[account_key], (int, float)):
            self._balance = float(payload[account_key])
            self._have_balance = True
            self._balance_ready.set()
            return
        for key in ("balance", "amount", "value"):
            val = payload.get(key)
            # Ignore placeholder zero payloads once a real balance is known
            # (the server pushes degenerate balance:0 packets); a live account
            # can only legitimately read 0 on a fresh connect.
            if isinstance(val, (int, float)) and (val != 0 or not self._have_balance):
                self._balance = float(val)
                self._have_balance = True
                self._balance_ready.set()
                return

    async def _request_balance(self) -> None:
        if self._ws is not None:
            await self._ws.send(build_balance_request())

    async def _refresh_balance(self, timeout_s: float = 2.0) -> tuple[float, bool]:
        """Actively ask the server for the current balance and wait for it.

        Returns ``(balance, fresh)`` where ``fresh`` is False when the request
        timed out and the cached (possibly stale — pre-settlement) value was
        reused.  Settlement MUST NOT classify from a non-fresh read: at expiry a
        WIN briefly shows the deducted stake, and a timed-out read returns that
        stale deducted value, misreading the win as a loss.

        Consecutive timeouts mean the server has stopped answering (a
        half-dead socket — the recv loop never errors because the server stops
        sending rather than closing).  After ``_balance_timeout_reconnect``
        timeouts the socket is force-closed so the reconnect loop replaces it.
        """
        if self._ws is None:
            return self._balance, False
        self._balance_ready.clear()
        try:
            await self._ws.send(build_balance_request())
        except Exception:
            # Reuse the cached value rather than crashing the read.
            return self._balance, False
        try:
            await asyncio.wait_for(self._balance_ready.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            logger.warning("balance request timed out — using cached %.2f", self._balance)
            self._balance_timeouts += 1
            if self._balance_timeouts >= self._balance_timeout_reconnect:
                logger.warning(
                    "balance requests not being answered (%d consecutive) — "
                    "forcing reconnect", self._balance_timeouts,
                )
                self._balance_timeouts = 0
                try:
                    await self._ws.close()
                except Exception:
                    pass
            return self._balance, False
        self._balance_timeouts = 0
        return self._balance, True


def _iter_deals(payload: Any):
    """Normalize an ``s_orders/close`` payload into an iterable of deal dicts."""
    if isinstance(payload, dict):
        if isinstance(payload.get("deals"), list):
            return (d for d in payload["deals"] if isinstance(d, dict))
        if isinstance(payload.get("ticket"), dict):
            return (payload["ticket"],)
        return (payload,)
    if isinstance(payload, list):
        return (d for d in payload if isinstance(d, dict))
    return ()
