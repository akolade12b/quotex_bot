"""Quotex Socket.IO / Engine.IO wire protocol (modern, EIO=3).

This is the ONLY module that knows Quotex's wire format. ``SimulatedBroker``
and every test that exercises it never import this file, so if the live
protocol drifts you only ever edit here.

Two layers are involved:

* **Engine.IO v3** framing (transport): each WebSocket text frame starts with
  a single-digit packet type — ``0`` open, ``2`` ping (answer ``3`` pong),
  ``4`` message.
* **Socket.IO v5** packets nested inside an Engine.IO message: ``0`` connect,
  ``1`` disconnect, ``2`` event, ``3`` ack, ``4`` connect_error,
  ``51-`` binary-event header.

Modern Quotex specifics (verified against the current reference client,
Aug 2026 — the platform no longer speaks the old ``42["buy",...]`` format):

* Endpoint is ``wss://ws2.quotex.com/socket.io/?EIO=3&transport=websocket``.
* Authentication is ``42["authorization",{"session":...,"isDemo":1|0,"tournamentId":0}]``
  — there is no separate ``auth`` event, and demo/live is carried by
  ``isDemo``, not a ``changeBalance`` message.
* Opening a trade is ``42["orders/open",{...}]`` with ``action`` =
  ``"call"|"put"``, a per-order ``requestId``, and ``optionType`` 1 (regular,
  minute-anchored) or 100 (OTC, seconds).
* High-volume events (``s_balance``, ``s_orders/open``, ``s_orders/close``,
  …) use Socket.IO **binary** transport: a text header ``451-["<name>",{"_placeholder":true,"num":0}]``
  announces the event, then a separate Engine.IO binary frame ``\\x04<json>``
  carries the payload.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

# Quotex WebSocket endpoint (Engine.IO v3 over wss). Demo and live share it;
# the account type is selected by the ``isDemo`` field in the auth frame.
SOCKETIO_ENDPOINT = "wss://ws2.quotex.com/socket.io/?EIO=3&transport=websocket"
# Login pages used by the cloudscraper email/password fast path (see login.py).
LOGIN_PAGE_URL = "https://quotex.com/en/sign-in/"
TRADE_PAGE_URL = "https://quotex.com/en/trade"          # live
DEMO_TRADE_PAGE_URL = "https://quotex.com/en/demo-trade"  # demo

# Engine.IO v3 packet types.
_EIO_OPEN = "0"
_EIO_CLOSE = "1"
_EIO_PING = "2"
_EIO_PONG = "3"
_EIO_MESSAGE = "4"
_EIO_UPGRADE = "5"

# Socket.IO v5 packet types.
_SIO_CONNECT = "0"
_SIO_DISCONNECT = "1"
_SIO_EVENT = "2"
_SIO_ACK = "3"
_SIO_CONNECT_ERROR = "4"
_SIO_BINARY_EVENT = "5"


def encode_socketio(event: str, payload: Any = None) -> str:
    """Encode a Socket.IO event frame: ``42["event", {...}]``."""
    body = [event]
    if payload is not None:
        body.append(payload)
    return _EIO_MESSAGE + _SIO_EVENT + json.dumps(body, separators=(",", ":"))


def encode_pong() -> str:
    """Reply to a server ping with an Engine.IO pong frame."""
    return _EIO_PONG


def is_pong(frame: str) -> bool:
    return frame == _EIO_PONG


def is_ping(frame: str) -> bool:
    return frame == _EIO_PING


def _maybe_json(s: str) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except ValueError:
        return None


def decode_frame(frame: str) -> tuple[str, Any]:
    """Parse an inbound Engine.IO/Socket.IO frame.

    Returns ``(kind, data)`` where ``kind`` is one of
    ``"connect" | "ping" | "pong" | "event" | "binary_header" |
    "binary_payload" | "close" | "connect_error" | "unknown"``.

    * ``event`` → ``data = (name, args)``
    * ``binary_header`` → ``data = event name`` (the payload arrives next as a
      ``"binary_payload"`` frame)
    * ``binary_payload`` → ``data = parsed JSON``
    """
    if not frame:
        return "unknown", None
    if frame == _EIO_PING:
        return "ping", None
    if frame == _EIO_PONG:
        return "pong", None
    if frame.startswith(_EIO_OPEN):  # Engine.IO open: 0{"sid":...}
        return "connect", _maybe_json(frame[1:])
    if frame.startswith("40"):  # Socket.IO connect
        return "connect", _maybe_json(frame[2:])
    if frame.startswith("41"):
        return "close", None
    if frame.startswith("44"):
        return "connect_error", _maybe_json(frame[2:])
    if frame.startswith(_EIO_MESSAGE + "51-["):  # binary-event header: 451-[
        return "binary_header", parse_binary_header(frame)
    if frame.startswith("42"):
        parts = json.loads(frame[2:])
        return "event", (parts[0], parts[1:])
    if frame.startswith("\x04"):  # Engine.IO binary message carrying JSON
        return "binary_payload", json.loads(frame[1:])
    return "unknown", None


def parse_binary_header(frame: str) -> str:
    """Extract the event name from a ``451-["<name>",{"_placeholder":...}]``
    header that announces a following binary payload."""
    head = frame.split("451-", 1)[1]  # keeps the leading '["...'
    return json.loads(head)[0]


# --------------------------------------------------------------------------
# Outbound application payloads.
# --------------------------------------------------------------------------

def build_authorization_message(session: str, is_demo: bool) -> str:
    """Frame that authenticates a WebSocket session.

    ``session`` is the Quotex token (from a captured browser session or the
    email/password login). ``is_demo`` selects the demo or real account for
    this connection — the modern equivalent of the old ``changeBalance``.
    """
    demo = 1 if is_demo else 0
    return f'42["authorization",{{"session":"{session}","isDemo":{demo},"tournamentId":0}}]'


def build_balance_request() -> str:
    """Frame that asks the server to push the current balance.

    ``42["balance/list"]`` (not ``42["balance",{}]``) — the server reliably
    answers ``balance/list`` with an ``s_balance/list`` payload carrying the
    per-account balance, while ``balance`` is answered unreliably (only the
    first request on a connection gets a response)."""
    return '42["balance/list"]'


def build_pending_list_message() -> str:
    """Frame that asks the server for the currently-open positions
    (``pending/list``). The response is a ``pending/list`` binary event whose
    payload is a list of open order dicts — the ground truth for "how many
    positions are really open" on the account, used to verify before gating."""
    return '42["pending/list"]'


def build_orders_closed_list_message() -> str:
    """Frame that asks the server for the account's closed-order history
    (``orders/closed/list``). The response is an ``orders/closed/list`` binary
    event. The server also pushes this once at connect, but the connect-time
    snapshot can lag a trade that settled during a disconnect, so recovery
    re-requests it after a short delay to get a fresh snapshot."""
    return '42["orders/closed/list"]'


def sanitize_asset(asset: str) -> str:
    """Map a user-facing symbol ('EUR/USD') to Quotex's no-slash wire symbol
    ('EURUSD').

    Quotex instruments are keyed without a slash ('AUDCAD', 'EURUSD'), and OTC
    variants carry a lowercase ``_otc`` suffix ('EURCAD_otc').  This function
    only *cleans* the symbol — it deliberately does **not** force the OTC
    variant.  Choosing OTC vs the regular market is the caller's job
    (:meth:`LiveQuotexClient.resolve_asset` prefers the regular pair when the
    server lists it).  Returns ``""`` unchanged so a missing asset stays
    obviously wrong.
    """
    if not asset:
        return asset
    return asset.replace("/", "").upper().replace("_OTC", "_otc")


def build_tick_message() -> str:
    """Frame that primes the tick stream. The current reference client sends
    this immediately before every ``orders/open``; without it the server may
    not process the order."""
    return '42["tick"]'


def build_follow_message(asset: str) -> str:
    """Frame that subscribes to an instrument's live stream. Also sent before
    ``orders/open`` (asset is sanitized to the wire symbol).

    .. note:: The modern Quotex server **silently ignores** ``orders/open``
       frames when only ``instruments/follow`` has been sent.  Use
       :func:`build_instruments_update_message` + :func:`build_depth_follow`
       instead — see ``debug/recipe_matrix.py`` for the validated recipe.
    """
    return '42["instruments/follow","' + sanitize_asset(asset) + '"]'


def build_instruments_update_message(asset: str, period: int = 60) -> str:
    """Frame that opens a live candle/data subscription for *asset*.

    This is the **correct** pre-order subscription frame — the server
    requires it (or a ``depth/follow``) before it will accept an
    ``orders/open``.  ``instruments/follow`` alone is silently dropped.

    *period* is the candle period in seconds (default 60 = 1-minute).
    """
    wire = sanitize_asset(asset)
    return (
        '42["instruments/update",'
        + json.dumps({"asset": wire, "period": int(period)}, separators=(",", ":"))
        + "]"
    )


def build_depth_follow_message(asset: str) -> str:
    """Frame that subscribes to the live depth-of-market stream for *asset*.

    The server pushes a ``depth/change`` binary event once the subscription
    is active — the client must wait for this before placing an order to
    guarantee the server will process it.
    """
    return '42["depth/follow","' + sanitize_asset(asset) + '"]'


def build_order_message(
    *,
    asset: str,
    amount: float,
    time_field: int,
    action: str,  # "call" | "put"
    is_demo: bool,
    request_id: int,
    option_type: int,
) -> str:
    """Frame that opens a binary position (``orders/open``).

    The asset is sanitized to Quotex's no-slash wire symbol ('EUR/USD' ->
    'EURUSD') — the server silently ignores orders whose asset it can't match.
    """
    payload = {
        "asset": sanitize_asset(asset),
        "amount": float(amount),
        "time": int(time_field),
        "action": action,
        "isDemo": 1 if is_demo else 0,
        "tournamentId": 0,
        "requestId": int(request_id),
        "optionType": int(option_type),
    }
    return '42["orders/open",' + json.dumps(payload, separators=(",", ":")) + "]"


def compute_order_time_and_type(asset: str, expiry_s: int, now_ts: float) -> tuple[int, int]:
    """Map a duration to Quotex's ``(optionType, time)`` pair.

    OTC assets (``"..._otc"``) use ``time = seconds`` and ``optionType 100``.
    Regular assets anchor expiries to minute boundaries: ``optionType 1`` and
    ``time`` = the epoch at the next platform expiry mark for
    ``ceil(duration / 60)`` minutes.

    The asset is sanitized first so a user-facing 'EUR/USD_otc' is recognized
    as OTC.
    """
    secs = max(int(expiry_s), 1)
    if "_otc" in sanitize_asset(asset).lower():
        return 100, secs
    minutes = max(int((secs + 59) // 60), 1)
    return 1, _qx_expiration_epoch(int(now_ts), minutes)


def _qx_expiration_epoch(now_ts: int, minutes: int) -> int:
    """Next minute-anchored expiry ``minutes`` minutes out, matching Quotex:
    a position opened at or past :30 of a minute closes on the following whole
    minute boundary."""
    now = datetime.fromtimestamp(now_ts)
    shift = 1 if now.second >= 30 else 0
    base = now.replace(second=0, microsecond=0) + timedelta(minutes=shift + minutes)
    return int(base.timestamp())
