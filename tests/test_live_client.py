"""Unit tests for the live client's offline logic (no network): ssid parsing,
order-open/close correlation, and result classification."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from broker.base import (
    BrokerAuthError,
    BrokerUnavailable,
    Direction,
    PlaceTradeRequest,
    TradeResult,
    TradeStatus,
    utcnow,
)
from broker.protocol import sanitize_asset
from broker.live_client import _find_closed_order
from broker.live_client import LiveQuotexClient, _iter_deals

try:
    from websockets.protocol import State
except ImportError:  # websockets < 14 used .open instead of .state
    State = None


class _FakeWs:
    def __init__(self) -> None:
        self.state = None
        self.open = False


class _SendingWs(_FakeWs):
    def __init__(self) -> None:
        super().__init__()
        self.sent: list[str] = []

    async def send(self, frame: str) -> None:
        self.sent.append(frame)


def _client(mode: str = "demo", **kwargs) -> LiveQuotexClient:
    client = LiveQuotexClient(mode=mode, ws_url="wss://example.invalid", **kwargs)
    # Keep the balance-delta settlement sampling fast in tests.
    client._settle_sample_gap_s = 0.01
    # Don't defer LOSS classification in the latency-allowance window unless a
    # test deliberately enables it (see test_settle_by_balance_delta_loss_deferred...).
    client._settle_latency_allowance_s = 0.0
    return client


def _trade(trade_id: str, amount: float = 10.0) -> TradeResult:
    return TradeResult(
        trade_id=trade_id,
        asset="EUR/USD",
        direction=Direction.PUT,
        amount=amount,
        entry_price=1.1000,
        expiry_s=60,
        opened_at=utcnow(),
        status=TradeStatus.OPEN,
    )


def test_parse_ssid_bare_token():
    client = _client()
    session, demo = client._parse_ssid("raw-token-123", default_demo=True)
    assert session == "raw-token-123"
    assert demo is True


def test_parse_ssid_full_authorization_frame():
    client = _client()
    frame = '42["authorization",{"session":"abc","isDemo":0,"tournamentId":0}]'
    session, demo = client._parse_ssid(frame, default_demo=True)
    assert session == "abc"
    assert demo is False  # frame's isDemo wins over the default


def test_order_open_correlates_and_sets_entry_price():
    client = _client()
    trade = _trade("req1")
    client._pending["req1"] = trade
    client._on_order_opened(
        {"requestId": "req1", "id": "srv9", "openPrice": 1.1010,
         "accountBalance": 9990.0}
    )
    assert trade.entry_price == pytest.approx(1.1010)
    assert client._server_ids["srv9"] == "req1"
    assert client._balance == pytest.approx(9990.0)
    assert client._have_balance is True


def test_order_close_win_via_server_id():
    client = _client()
    trade = _trade("req1")
    client._pending["req1"] = trade
    client._server_ids["srv9"] = "req1"
    client._on_order_closed({"id": "srv9", "profit": 8.5, "closePrice": 1.1050})
    assert trade.status is TradeStatus.WIN
    assert trade.pnl == pytest.approx(8.5)
    assert trade.exit_price == pytest.approx(1.1050)
    assert client._settled["req1"] is trade
    assert client._settle_events["req1"].is_set()
    assert "req1" not in client._pending


def test_order_close_loss_via_request_id():
    client = _client()
    trade = _trade("req2")
    client._pending["req2"] = trade
    client._on_order_closed({"requestId": "req2", "profit": -10.0})
    assert trade.status is TradeStatus.LOSS
    assert trade.pnl == pytest.approx(-10.0)


def test_order_close_draw():
    client = _client()
    trade = _trade("req3")
    client._pending["req3"] = trade
    client._on_order_closed({"requestId": "req3", "profit": 0.0})
    assert trade.status is TradeStatus.DRAW
    assert trade.pnl == 0.0


def test_order_close_unknown_request_ignored():
    client = _client()
    client._on_order_closed({"id": "srv-x", "profit": 5.0})
    assert client._settled == {}


def test_deals_normalization():
    assert list(_iter_deals({"deals": [{"id": "1"}, {"id": "2"}]})) == [{"id": "1"}, {"id": "2"}]
    assert list(_iter_deals({"ticket": {"id": "3"}})) == [{"id": "3"}]
    assert list(_iter_deals({"id": "4"})) == [{"id": "4"}]
    assert list(_iter_deals([{"id": "5"}])) == [{"id": "5"}]
    assert list(_iter_deals("OPEN")) == []


def test_balance_from_demo_balance_list():
    client = _client()  # demo mode
    client._update_balance(
        {"liveBalance": 0, "demoBalance": 15000000,
         "tournamentsBalances": {}, "dayLimit": 0, "dayBalance": 0}
    )
    assert client._balance == pytest.approx(15000000)
    assert client._have_balance is True


def test_balance_live_prefers_live_balance():
    client = _client(mode="live")
    client._update_balance(
        {"liveBalance": 4321.5, "demoBalance": 15000000, "dayLimit": 0}
    )
    assert client._balance == pytest.approx(4321.5)


def test_balance_generic_keys_fallback():
    client = _client()
    client._update_balance({"balance": 99.5})
    assert client._balance == pytest.approx(99.5)
    assert client._have_balance is True


def test_balance_placeholder_zero_does_not_clobber():
    client = _client()
    client._update_balance({"demoBalance": 15000000})
    assert client._balance == pytest.approx(15000000)
    # A degenerate generic zero payload must not wipe the known balance.
    client._update_balance({"balance": 0})
    assert client._balance == pytest.approx(15000000)
    assert client._have_balance is True


def test_balance_zero_accepted_on_fresh_connect():
    client = _client()
    assert client._have_balance is False
    client._update_balance({"balance": 0})
    assert client._have_balance is True
    assert client._balance == 0.0


async def test_close_packet_treated_as_connection_loss_and_reconnects():
    """A Socket.IO 41 close packet must follow the same path as a dropped
    transport: resolve in-flight trades as cancelled, then reconnect. Before
    the fix it only logged, leaving the client zombie — open trades never
    settled, so their DB rows blocked later signals via max_concurrent_positions."""
    client = _client()
    trade = _trade("req1")
    client._pending["req1"] = trade
    client._have_balance = True

    state = State.OPEN if State is not None else None
    open_flag = True if State is None else False

    class _Ws:
        def __init__(self) -> None:
            self.state = state
            self.open = open_flag
            self.recv_count = 0

        async def recv(self):
            self.recv_count += 1
            if self.recv_count == 1:
                return "41"  # server-initiated Socket.IO close packet
            raise asyncio.CancelledError  # stop the recv loop after reconnect

    fake_ws = _Ws()
    client._ws = fake_ws
    calls: list[str] = []

    async def _fake_open_connection() -> None:
        client._ws = fake_ws  # a fresh socket replaces the lost one
        client._closed_orders_ready.set()  # server pushes closed list during auth
        calls.append("reconnect")

    async def _fake_request_balance() -> None:
        calls.append("balance")

    client._open_connection = _fake_open_connection
    client._request_balance = _fake_request_balance

    await client._recv_loop()

    # The reconnect spawned a background recovery poller (the fake ws never
    # answers it) — cancel it so the test doesn't leak a pending task.
    if client._recovery_task is not None:
        client._recovery_task.cancel()
        try:
            await client._recovery_task
        except asyncio.CancelledError:
            pass

    assert calls == ["reconnect", "balance"]
    assert trade.status is TradeStatus.CANCELLED
    assert "req1" in client._settled
    assert client._pending == {}
    assert client._have_balance is False
    assert fake_ws.recv_count == 2  # "41", then loop stopped


async def test_poll_recover_remaining_recovers_late_trade(monkeypatch):
    """A trade that settled during the disconnect may not be in the connect-
    time snapshot; the background poller actively re-requests the closed-order
    history and recovers it as soon as it appears."""
    monkeypatch.setattr("broker.live_client._RECOVERY_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("broker.live_client._RECOVERY_POLL_WINDOW_S", 1.0)
    client = _client()
    ws = _SendingWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True

    opened = utcnow()
    trade = TradeResult(
        trade_id="req-late", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=300, opened_at=opened,
    )
    client._saved_cancelled["req-late"] = trade
    recovered: list[tuple[str, str, float]] = []

    async def on_recovered(t):
        recovered.append((t.trade_id, t.status.value, t.pnl))

    client.on_trade_outcome_recovered = on_recovered

    async def _feed_late_response():
        await asyncio.sleep(0.02)
        client._on_closed_orders([
            {"id": "srv-late", "asset": "EURCAD_otc", "amount": 10000,
             "openTimestamp": opened.timestamp(), "profit": 9300, "command": 1},
        ])

    feeder = asyncio.create_task(_feed_late_response())
    await client._poll_recover_remaining()
    await feeder

    assert recovered == [("req-late", "win", 9300.0)]
    assert "req-late" not in client._saved_cancelled
    assert '42["orders/closed/list"]' in ws.sent


async def test_poll_recover_gives_up_and_notifies_cancelled(monkeypatch):
    """If the server never reveals the outcome, the poller gives up after the
    window and still notifies the engine (CANCELLED) so the user isn't left
    in the dark."""
    monkeypatch.setattr("broker.live_client._RECOVERY_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("broker.live_client._RECOVERY_POLL_WINDOW_S", 0.02)
    monkeypatch.setattr("broker.live_client._CLOSED_LIST_RESPONSE_TIMEOUT_S", 0.01)
    client = _client()
    ws = _SendingWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True

    trade = TradeResult(
        trade_id="req-ghost", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=300, opened_at=utcnow(),
        status=TradeStatus.CANCELLED, closed_at=utcnow(),
    )
    client._saved_cancelled["req-ghost"] = trade
    notified: list[str] = []

    async def on_recovered(t):
        notified.append(t.trade_id)

    client.on_trade_outcome_recovered = on_recovered

    await client._poll_recover_remaining()

    assert notified == ["req-ghost"]          # final CANCELLED notification sent
    assert client._saved_cancelled == {}
    assert trade.status is TradeStatus.CANCELLED


async def test_poll_recover_recovers_loss_via_balance_delta(monkeypatch):
    """A trade that lost during the disconnect but never appears in the closed-
    order history (e.g. the history request times out) is still recovered from
    the balance movement — placement baseline -> post-settlement balance — so
    the engine fires the gale instead of writing the loss off as CANCELLED."""
    monkeypatch.setattr("broker.live_client._RECOVERY_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("broker.live_client._RECOVERY_POLL_WINDOW_S", 0.5)
    monkeypatch.setattr("broker.live_client._CLOSED_LIST_RESPONSE_TIMEOUT_S", 0.01)
    client = _client()
    ws = _SendingWs()
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True

    async def _send(frame: str) -> None:
        ws.sent.append(frame)
        # Answer balance requests so the fallback gets a fresh, settled read.
        if '"balance/list"' in frame:
            client._balance_ready.set()

    ws.send = _send  # type: ignore[method-assign]
    client._ws = ws

    trade = TradeResult(
        trade_id="req-lost", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60, opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=1),  # already expired
    )
    client._saved_cancelled["req-lost"] = trade
    client._saved_cancelled_single = True
    client._baseline_balance["req-lost"] = 50000.0
    client._balance = 40000.0  # stake deducted and never refunded -> LOSS

    recovered: list[tuple[str, str, float]] = []

    async def on_recovered(t):
        recovered.append((t.trade_id, t.status.value, t.pnl))

    client.on_trade_outcome_recovered = on_recovered

    await client._poll_recover_remaining()

    assert recovered == [("req-lost", "loss", -10000.0)]
    assert "req-lost" not in client._saved_cancelled
    assert trade.balance_after == pytest.approx(40000.0)
    assert client._baseline_balance == {}


async def test_connect_retries_transient_auth_close():
    """A server close during the auth handshake must not kill the bot: the
    initial connect retries with backoff and succeeds on the next attempt."""
    client = _client()
    attempts: list[int] = []

    async def _flaky_open():
        attempts.append(1)
        if len(attempts) == 1:
            raise BrokerUnavailable("connection closed during auth")

    client._open_connection = _flaky_open
    try:
        await client.connect()
        assert len(attempts) == 2  # first dropped, second succeeded
    finally:
        await client.disconnect()


async def test_obtain_session_refreshes_rejected_token(monkeypatch):
    """When the configured token is rejected by the server, the next connect
    logs in from credentials to get a fresh session, then reuses it."""
    client = _client(mode="demo", demo_ssid="dead-token", email="u@x.com", password="p")

    async def _fake_login(email, password, demo, *, otp_bridge=None, proxy=""):
        assert demo is True
        return "fresh-token"

    monkeypatch.setattr("broker.live_client.login.http_login_async", _fake_login)

    tok, demo = await client._obtain_session_token()
    assert tok == "dead-token"  # configured token preferred while not rejected

    client._token_rejected = True
    tok, demo = await client._obtain_session_token()
    assert tok == "fresh-token"  # refreshed from credentials

    # The refreshed session is cached and reused on later calls
    tok, demo = await client._obtain_session_token()
    assert tok == "fresh-token"
    assert client._refreshed_session == "fresh-token"


async def test_obtain_session_rejected_token_without_credentials_raises():
    """Without credentials there is nothing to refresh with — surface the error
    instead of replaying the dead token forever."""
    client = _client(mode="demo", demo_ssid="dead-token")  # no email/password
    client._token_rejected = True
    with pytest.raises(BrokerAuthError):
        await client._obtain_session_token()


async def test_get_open_positions_translates_server_ids():
    """pending/list responses map bot orders back to their request ids (the
    id space the DB uses) while manual website orders keep the raw server id."""
    client = _client()
    ws = _SendingWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True
    client._server_ids["srv-open-1"] = "req-1"

    async def _feed():
        await asyncio.sleep(0.01)
        client._on_pending_list([
            {"id": "srv-open-1", "asset": "EURCAD_otc", "amount": 10000, "command": 1},
            {"id": "manual-uuid", "asset": "GBPUSD_otc", "amount": 5000, "command": 2},
        ])

    task = asyncio.create_task(_feed())
    positions = await client.get_open_positions()
    await task

    ids = {p.trade_id for p in positions}
    assert "req-1" in ids        # bot order translated to its request id
    assert "manual-uuid" in ids  # website order keeps the raw server id
    assert ws.sent == ['42["pending/list"]']


def test_parse_pending_list_maps_direction_and_skips_junk():
    client = _client()
    parsed = client._parse_pending_list([
        {"id": "a", "command": 1, "amount": 100, "asset": "EURCAD_otc"},
        {"id": "b", "command": 2, "amount": 50, "asset": "GBPUSD_otc"},
        {"id": ""},   # no server id -> skipped
        "junk",       # not a dict -> skipped
    ])
    by_id = {p.trade_id: p for p in parsed}
    assert by_id["a"].direction is Direction.PUT
    assert by_id["b"].direction is Direction.CALL
    assert "req-1" not in by_id


def test_parse_pending_list_accepts_list_and_dict_shapes():
    client = _client()
    assert client._parse_pending_list([]) == []
    assert client._parse_pending_list({"list": []}) == []
    assert client._parse_pending_list(None) == []


def test_find_closed_order_fallback_heuristic():
    """When the server never sends an s_orders/open ack (no _server_ids),
    the fallback matches by wire asset, amount, and openTimestamp proximity."""
    trade = TradeResult(
        trade_id="1786269840007", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=300,
        opened_at=utcnow(),
    )
    orders = [
        {"id": "some-uuid", "asset": "EURCAD_otc", "amount": 10000,
         "openTimestamp": trade.opened_at.timestamp(), "profit": 9300, "command": 1},
    ]
    assert _find_closed_order(orders, "req-id", None, trade, wire_asset="EURCAD_otc") is orders[0]


def test_find_closed_order_fallback_skips_wrong_asset_or_amount():
    trade = TradeResult(
        trade_id="x", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=300,
        opened_at=utcnow(),
    )
    orders = [
        {"id": "a", "asset": "GBPUSD_otc", "amount": 10000,
         "openTimestamp": trade.opened_at.timestamp(), "profit": 500},
        {"id": "b", "asset": "EURCAD_otc", "amount": 5000,
         "openTimestamp": trade.opened_at.timestamp(), "profit": 300},
        {"id": "c", "asset": "EURCAD_otc", "amount": 10000,
         "openTimestamp": trade.opened_at.timestamp() - 300, "profit": 200},  # too old
    ]
    assert _find_closed_order(orders, "req", None, trade, wire_asset="EURCAD_otc") is None


def test_find_closed_order_exact_id_beats_heuristic():
    trade = TradeResult(
        trade_id="req1", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=300,
        opened_at=utcnow(),
    )
    orders = [
        {"id": "exact-match", "asset": "EURCAD_otc", "amount": 10000,
         "openTimestamp": trade.opened_at.timestamp() - 200, "profit": 9300},
    ]
    assert _find_closed_order(orders, "req1", "exact-match", trade) is orders[0]


def test_on_connection_lost_cancels_pending():
    client = _client()
    trade = _trade("req1")
    client._pending["req1"] = trade
    client._have_balance = True
    client._on_connection_lost()
    assert trade.status is TradeStatus.CANCELLED
    assert "req1" in client._settled
    assert client._pending == {}
    assert client._have_balance is False


def test_on_connection_lost_resets_warm_subscription():
    """The depth subscription dies with the socket — after a connection loss
    the warm-subscription fast path must not be used (a fresh socket needs
    re-priming or the order would be silently dropped)."""
    client = _client()
    client._primed_asset = "EURCAD"
    client._depth_last_seen = 5.0
    client._depth_ready.set()
    client._on_connection_lost()
    assert client._primed_asset is None
    assert client._depth_last_seen == 0.0
    assert client._depth_ready.is_set() is False


def test_resolve_asset_prefers_normal_over_otc():
    """When the server lists both EURCAD and EURCAD_otc, the regular market
    must win — the user wants EUR/CAD (normal) before the OTC variant."""
    client = _client()
    client._instruments = {
        "EURCAD": {"label": "EUR/CAD", "is_otc": False},
        "EURCAD_otc": {"label": "EUR/CAD (OTC)", "is_otc": True},
    }
    assert client.resolve_asset("EUR/CAD") == "EURCAD"


def test_resolve_asset_falls_back_to_otc_when_normal_absent():
    """If the server only lists the OTC variant (e.g. normal market not
    offered this session), resolve to OTC rather than a non-listed symbol."""
    client = _client()
    client._instruments = {
        "EURCAD_otc": {"label": "EUR/CAD (OTC)", "is_otc": True},
    }
    assert client.resolve_asset("EUR/CAD") == "EURCAD_otc"


def test_resolve_asset_empty_instruments_falls_back_to_sanitize():
    """Before the connect-burst instruments/list is captured, resolve must not
    block an order — fall back to the OTC default."""
    client = _client()
    assert client._instruments == {}
    assert client.resolve_asset("EUR/CAD") == "EURCAD_otc"


def test_resolve_asset_explicit_otc_request_stays_otc():
    client = _client()
    client._instruments = {
        "EURCAD": {"label": "EUR/CAD", "is_otc": False},
        "EURCAD_otc": {"label": "EUR/CAD (OTC)", "is_otc": True},
    }
    assert client.resolve_asset("EUR/CAD_otc") == "EURCAD_otc"


def test_on_instruments_list_captures_map():
    """The connect-time instruments/list payload populates _instruments so
    resolve_asset can prefer normal markets."""
    client = _client()
    client._on_instruments_list([
        [45, "EURCAD", "EUR/CAD", "currency", 5, 76, 60, 30, 3, 0, 76, 0, [], 1, True],
        [76, "EURCAD_otc", "EUR/CAD (OTC)", "currency", 5, 93, 60, 30, 3, 1, 0, 45, [], 1, False],
    ])
    assert client._instruments["EURCAD"]["is_otc"] is False
    assert client._instruments["EURCAD_otc"]["is_otc"] is True
    assert client.resolve_asset("EUR/CAD") == "EURCAD"


def test_ws_open_detects_state_enum():
    client = _client()
    assert client._ws_open() is False  # no socket
    ws = _FakeWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
        assert client._ws_open() is True
        ws.state = State.CLOSED
        assert client._ws_open() is False
    else:  # legacy fallback path
        ws.open = True
        assert client._ws_open() is True
        ws.open = False
        assert client._ws_open() is False


async def test_place_trade_primes_tick_follow_then_orders_sanitized_asset():
    client = _client()
    ws = _SendingWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True
    client._have_balance = True
    client._balance = 1000.0
    # Simulate that depth/change was already received (real flow: the server
    # pushes depth/change after instruments/update + depth/follow, before we
    # send the order).  Without this the test would wait the full 10 s timeout.
    client._depth_ready.set()
    req = PlaceTradeRequest(
        asset="EUR/CAD", direction=Direction.PUT, amount=10.0, expiry_s=300
    )
    trade = await client.place_trade(req)
    assert trade.asset == "EUR/CAD"  # user-facing format kept for logs/persistence
    assert ws.sent[0] == '42["tick"]'
    assert '"instruments/update"' in ws.sent[1]
    assert '"EURCAD_otc"' in ws.sent[1]
    assert ws.sent[2] == '42["depth/follow","EURCAD_otc"]'
    assert ws.sent[3].startswith('42["orders/open",')
    assert '"asset":"EURCAD_otc"' in ws.sent[3]
    assert '"action":"put"' in ws.sent[3]
    assert trade.trade_id in client._pending


# -------------------------------------------------- balance-delta settlement


async def test_settle_by_balance_delta_win():
    """A past-expiry trade whose balance rose — the server settled it as a WIN.
    The balance delta equals stake + payout; net profit = delta − amount."""
    from datetime import timedelta

    client = _client()
    client._have_balance = True
    client._balance = 15000.0

    trade = TradeResult(
        trade_id="req-win", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=1),  # past expiry
    )
    client._pending["req-win"] = trade
    client._baseline_balance["req-win"] = 15000.0
    client._settle_events["req-win"] = asyncio.Event()

    # Quotex deducts the stake at placement, so a WIN settles at baseline +
    # profit (not baseline + stake + profit).  delta = +9000 = pnl.
    client._balance = 15000.0 + 9000.0

    async def fake_refresh(timeout_s=2.0):
        return client._balance, True

    client._refresh_balance = fake_refresh
    result = await client.wait_for_settlement("req-win", timeout_s=0.05)
    assert result.status is TradeStatus.WIN
    assert result.pnl == pytest.approx(9000.0)
    assert "req-win" not in client._pending
    assert "req-win" in client._settled


async def test_settle_by_balance_delta_waits_for_win_credit():
    """The core gale bug: at expiry a WIN briefly shows the deducted (loss-like)
    balance before the credit lands.  Settlement must wait (within the latency
    allowance) instead of classifying the first (deducted) reading as a loss.
    The single-read design defers on the deducted reading and the next poll —
    re-reads — catches the credit as a WIN."""
    from datetime import timedelta

    client = _client()
    client._settle_latency_allowance_s = 1.0
    client._have_balance = True
    client._balance = 14000.0  # baseline; stake deducted later in sampling

    trade = TradeResult(
        trade_id="req-credit", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=0.5),
    )
    client._pending["req-credit"] = trade
    client._baseline_balance["req-credit"] = 15000.0
    client._settle_events["req-credit"] = asyncio.Event()

    calls = {"n": 0}
    async def fake_refresh(timeout_s=2.0):
        calls["n"] += 1
        # First poll: stake deducted, win credit not yet landed (loss-like).
        # The next poll sees the credit land and the balance rise to baseline +
        # profit — the fast settlement must catch it and classify WIN, not the
        # deducted-state LOSS.
        if calls["n"] <= 1:
            client._balance = 5000.0  # 15000 - 10000 (deducted, pre-credit)
        else:
            client._balance = 15900.0  # 15000 + 900 (win at 9%, credit landed)
        return client._balance, True

    client._refresh_balance = fake_refresh
    # Mimic the settlement loop: keep polling until the trade settles.
    result = None
    for _ in range(20):
        result = await client.wait_for_settlement("req-credit", timeout_s=0.01)
        if "req-credit" not in client._pending:
            break
    assert result is not None and result.status is TradeStatus.WIN, \
        "win must not be misread as a loss"
    assert result.pnl == pytest.approx(900.0)
    assert "req-credit" not in client._pending


async def test_settle_by_balance_delta_loss_triggers_gale():
    """A past-expiry trade whose balance dropped by the stake — LOSS.
    The engine should place the next gale level."""
    from datetime import timedelta

    client = _client()
    client._have_balance = True
    client._balance = 20000.0

    trade = TradeResult(
        trade_id="req-loss", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=1),
    )
    client._pending["req-loss"] = trade
    client._baseline_balance["req-loss"] = 20000.0
    client._settle_events["req-loss"] = asyncio.Event()

    # Simulate the balance dropping by the stake (loss) — fresh reads.
    client._balance = 10000.0  # 20000 − 10000

    async def fake_refresh(timeout_s=2.0):
        return client._balance, True

    client._refresh_balance = fake_refresh
    result = await client.wait_for_settlement("req-loss", timeout_s=0.05)
    assert result.status is TradeStatus.LOSS
    assert result.pnl == pytest.approx(-10000.0)
    assert "req-loss" not in client._pending
    assert "req-loss" in client._settled


async def test_settle_by_balance_delta_loss_deferred_within_latency_allowance():
    """The server opens the order a beat after placement and credits a WIN up to
    ~2s past our local expiry estimate. A still-deducted balance within the
    settlement-latency allowance must NOT be classified LOSS (which would fire
    a spurious gale) — it stays OPEN and is re-checked by the next settlement
    poll."""
    from datetime import timedelta

    client = _client()
    client._settle_latency_allowance_s = 1.0  # enable allowance for this test
    client._have_balance = True
    client._balance = 15000.0

    trade = TradeResult(
        trade_id="req-defer", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=0.5),  # within the allowance
    )
    client._pending["req-defer"] = trade
    client._baseline_balance["req-defer"] = 15000.0
    client._settle_events["req-defer"] = asyncio.Event()

    # Balance stays deducted (win credit hasn't landed) — a loss-looking
    # reading, but inside the allowance so it must be deferred.
    async def fake_refresh(timeout_s=2.0):
        return 5000.0, True  # 15000 - 10000 (deducted, pre-credit)

    client._refresh_balance = fake_refresh
    result = await client.wait_for_settlement("req-defer", timeout_s=0.05)
    assert result.status is TradeStatus.OPEN
    assert "req-defer" in client._pending


async def test_settle_by_balance_delta_win_credit_caught_inside_allowance():
    """With a positive latency allowance, a win credit that arrives within
    the allowance window is still detected as a WIN (no spurious gale)."""
    from datetime import timedelta

    client = _client()
    client._settle_latency_allowance_s = 1.0
    client._have_balance = True
    client._balance = 15000.0

    trade = TradeResult(
        trade_id="req-win2", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=0.5),
    )
    client._pending["req-win2"] = trade
    client._baseline_balance["req-win2"] = 15000.0
    client._settle_events["req-win2"] = asyncio.Event()

    calls = {"n": 0}
    async def fake_refresh(timeout_s=2.0):
        calls["n"] += 1
        # First reads: still at deducted (win credit late). After the allowance
        # passes the credit lands and the balance rises — the re-check catches
        # it as a WIN, not a LOSS.
        if calls["n"] <= 2:
            return 5000.0, True   # deducted, pre-credit
        return 15900.0, True     # 15000 + 900 (win at 9%)

    client._refresh_balance = fake_refresh
    result = None
    for _ in range(20):
        result = await client.wait_for_settlement("req-win2", timeout_s=0.01)
        if "req-win2" not in client._pending:
            break
    assert result is not None and result.status is TradeStatus.WIN
    assert result.pnl == pytest.approx(900.0)


async def test_settle_by_balance_delta_still_open_before_expiry():
    """A trade whose expiry hasn't passed yet must stay OPEN regardless of balance."""
    from datetime import timedelta

    client = _client()
    client._have_balance = True
    client._balance = 15000.0

    trade = TradeResult(
        trade_id="req-open", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=300,
        opened_at=utcnow(),
        expires_at=utcnow() + timedelta(seconds=100),  # still in the future
    )
    client._pending["req-open"] = trade
    client._baseline_balance["req-open"] = 15000.0
    client._settle_events["req-open"] = asyncio.Event()

    result = await client.wait_for_settlement("req-open", timeout_s=0.05)
    assert result.status is TradeStatus.OPEN
    assert "req-open" in client._pending


async def test_settle_by_balance_delta_no_movement_marks_cancelled_after_grace():
    """An order the server silently dropped: balance never moves.  After the
    grace window the trade is marked CANCELLED so it doesn't block signals."""
    from datetime import timedelta

    client = _client()
    client._have_balance = True
    client._balance = 20000.0

    grace = timedelta(seconds=120)
    trade = TradeResult(
        trade_id="req-ghost", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow() - timedelta(seconds=200),
        expires_at=utcnow() - timedelta(seconds=140),  # past expiry + grace
    )
    client._pending["req-ghost"] = trade
    client._baseline_balance["req-ghost"] = 20000.0
    client._settle_events["req-ghost"] = asyncio.Event()

    # Balance unchanged — no settlement happened (fresh reads, no movement).
    async def fake_refresh(timeout_s=2.0):
        return client._balance, True

    client._refresh_balance = fake_refresh
    result = await client.wait_for_settlement("req-ghost", timeout_s=0.05)
    assert result.status is TradeStatus.CANCELLED
    assert "req-ghost" not in client._pending
    assert "req-ghost" in client._settled


async def test_settle_refreshes_balance_before_delta():
    """The cached _balance can lag the server (s_balance pushes are not
    reliable).  _settle_by_balance_delta must read the FRESH balance, otherwise
    a winning trade whose cached balance is stale is misclassified as a loss —
    which fires a spurious gale."""
    from datetime import timedelta

    client = _client()
    client._have_balance = True
    client._balance = 14972000.0  # stale cached balance (looks like a loss)

    trade = TradeResult(
        trade_id="req-stale", asset="EUR/CAD", direction=Direction.PUT,
        amount=40000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=1),
    )
    client._pending["req-stale"] = trade
    client._baseline_balance["req-stale"] = 15000000.0
    client._settle_events["req-stale"] = asyncio.Event()

    async def fake_refresh(timeout_s=2.0):
        # The server's real balance after the trade WON (baseline + profit).
        client._balance = 15036000.0  # 15000000 + 36000 (90% of 40000)
        return client._balance, True

    client._refresh_balance = fake_refresh
    result = await client.wait_for_settlement("req-stale", timeout_s=0.05)
    assert result.status is TradeStatus.WIN
    assert result.pnl == pytest.approx(36000.0)


async def test_get_balance_always_refreshes():
    """get_balance() must actively request the current balance rather than
    return the possibly-stale cached value (the Telegram notification balance
    depends on this)."""
    client = _client()
    client._have_balance = True
    client._balance = 1000.0  # stale

    async def fake_refresh(timeout_s=2.0):
        client._balance = 12345.0
        return client._balance, True

    client._refresh_balance = fake_refresh
    bal = await client.get_balance()
    assert bal == pytest.approx(12345.0)


async def test_refresh_balance_requests_and_waits_for_response():
    """_refresh_balance sends 42["balance",{}] and returns the fresh value once
    the s_balance response lands (via _update_balance setting _balance_ready)."""
    from broker.protocol import build_balance_request

    client = _client()
    ws = _SendingWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True
    client._have_balance = True
    client._balance = 5000.0

    # Start the refresh and let it send the request.
    task = asyncio.create_task(client._refresh_balance(timeout_s=1.0))
    await asyncio.sleep(0)
    assert ws.sent and ws.sent[0] == build_balance_request()

    # Simulate the server's s_balance response arriving in the recv loop.
    client._update_balance({"demoBalance": 7777.0})
    bal, fresh = await asyncio.wait_for(task, timeout=1.0)
    assert fresh is True
    assert bal == pytest.approx(7777.0)


async def test_refresh_balance_timeout_reports_not_fresh():
    """A timed-out balance read must report ``fresh=False`` so settlement
    doesn't classify a WIN from the stale deducted value."""
    client = _client()
    ws = _SendingWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True
    client._have_balance = True
    client._balance = 5000.0  # stale pre-settlement value

    bal, fresh = await client._refresh_balance(timeout_s=0.05)
    assert fresh is False
    assert bal == pytest.approx(5000.0)


async def test_settle_defers_classification_on_stale_read():
    """If the only settlement reads are stale (timed out -> cached), the trade
    must stay OPEN for the next poll, not be misread as a LOSS."""
    from datetime import timedelta

    client = _client()
    client._have_balance = True
    client._balance = 5000.0  # deducted state cached

    trade = TradeResult(
        trade_id="req-stale-read", asset="EUR/CAD", direction=Direction.PUT,
        amount=10000.0, entry_price=1.614, expiry_s=60,
        opened_at=utcnow(),
        expires_at=utcnow() - timedelta(seconds=1),  # past expiry
    )
    client._pending["req-stale-read"] = trade
    client._baseline_balance["req-stale-read"] = 15000.0
    client._settle_events["req-stale-read"] = asyncio.Event()

    async def fake_refresh(timeout_s=2.0):
        return client._balance, False  # always stale (server silent)

    client._refresh_balance = fake_refresh
    result = await client.wait_for_settlement("req-stale-read", timeout_s=0.05)
    assert result.status is TradeStatus.OPEN  # NOT classified from stale data
    assert "req-stale-read" in client._pending  # still being polled


async def test_baseline_captured_in_place_trade():
    """place_trade should snapshot the current balance as the trade's baseline."""
    client = _client()
    ws = _SendingWs()
    client._ws = ws
    if State is not None:
        ws.state = State.OPEN
    else:
        ws.open = True
    client._have_balance = True
    client._balance = 42000.0
    client._depth_ready.set()
    req = PlaceTradeRequest(
        asset="EUR/CAD", direction=Direction.PUT, amount=10.0, expiry_s=300
    )
    trade = await client.place_trade(req)
    assert client._baseline_balance[trade.trade_id] == pytest.approx(42000.0)
