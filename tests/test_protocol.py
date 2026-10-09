"""Pure-framing tests for the modern Quotex wire protocol. No network — these
exercise encode/decode and payload builders against known-good frames."""

import json
from datetime import datetime

from broker.protocol import (
    build_authorization_message,
    build_balance_request,
    build_follow_message,
    build_order_message,
    build_pending_list_message,
    build_tick_message,
    compute_order_time_and_type,
    decode_frame,
    encode_pong,
    encode_socketio,
    is_pong,
    parse_binary_header,
    sanitize_asset,
)


def test_encode_event_frame():
    frame = encode_socketio("auth", {"session": "abc123"})
    assert frame.startswith("42[")
    name, payload = json.loads(frame[2:])
    assert name == "auth"
    assert payload == {"session": "abc123"}


def test_pong_roundtrip():
    assert is_pong(encode_pong())
    assert not is_pong("42[..]")


def test_decode_server_ping_and_pong():
    assert decode_frame("2") == ("ping", None)
    assert decode_frame("3") == ("pong", None)


def test_decode_connect_engineio_open():
    kind, data = decode_frame('0{"sid":"x","pingInterval":25000}')
    assert kind == "connect"
    assert data == {"sid": "x", "pingInterval": 25000}


def test_decode_connect_socketio():
    kind, data = decode_frame("40")
    assert kind == "connect"
    assert data is None


def test_decode_inbound_event():
    kind, (name, args) = decode_frame('42["s_authorization",{}]')
    assert kind == "event"
    assert name == "s_authorization"
    assert args == [{}]


def test_decode_binary_header():
    kind, name = decode_frame('451-["s_balance",{"_placeholder":true,"num":0}]')
    assert kind == "binary_header"
    assert name == "s_balance"


def test_decode_binary_payload():
    kind, data = decode_frame('\x04{"uid":1,"balance":12345.6}')
    assert kind == "binary_payload"
    assert data == {"uid": 1, "balance": 12345.6}


def test_decode_close_and_error():
    assert decode_frame("41") == ("close", None)
    kind, data = decode_frame('44{"message":"bad token"}')
    assert kind == "connect_error"
    assert data == {"message": "bad token"}


def test_decode_unknown_garbage():
    assert decode_frame("hello") == ("unknown", None)
    assert decode_frame("") == ("unknown", None)


def test_parse_binary_header_event_name():
    frame = '451-["s_orders/close",{"_placeholder":true,"num":0}]'
    assert parse_binary_header(frame) == "s_orders/close"


def test_build_authorization_demo_and_live():
    assert build_authorization_message("tok123", True) == (
        '42["authorization",{"session":"tok123","isDemo":1,"tournamentId":0}]'
    )
    assert build_authorization_message("tok123", False) == (
        '42["authorization",{"session":"tok123","isDemo":0,"tournamentId":0}]'
    )


def test_build_balance_request():
    assert build_balance_request() == '42["balance/list"]'


def test_build_pending_list_message():
    assert build_pending_list_message() == '42["pending/list"]'


def test_build_order_message_payload_sanitizes_asset():
    """build_order_message cleans the symbol but does NOT force OTC — the
    caller (resolve_asset) decides regular vs OTC."""
    frame = build_order_message(
        asset="EUR/USD",
        amount=10.0,
        time_field=1750000000,
        action="put",
        is_demo=True,
        request_id=1750000000123,
        option_type=1,
    )
    assert frame.startswith('42["orders/open",')
    payload = json.loads(frame[len('42["orders/open",'):-1])
    assert payload["asset"] == "EURUSD"  # regular, not EURUSD_otc
    assert payload["amount"] == 10.0
    assert payload["time"] == 1750000000
    assert payload["action"] == "put"
    assert payload["isDemo"] == 1
    assert payload["requestId"] == 1750000000123
    assert payload["optionType"] == 1


def test_build_order_message_live_sends_isDemo_zero():
    frame = build_order_message(
        asset="USDIDR_otc", amount=5.0, time_field=1, action="call",
        is_demo=False, request_id=7, option_type=100,
    )
    assert '"isDemo":0' in frame
    assert '"asset":"USDIDR_otc"' in frame


def test_sanitize_asset():
    """sanitize_asset only CLEANS the symbol; it no longer forces OTC."""
    assert sanitize_asset("EUR/USD") == "EURUSD"       # clean only, no _otc
    assert sanitize_asset("eur/usd") == "EURUSD"       # case normalized
    assert sanitize_asset("EURUSD") == "EURUSD"        # already clean
    assert sanitize_asset("EUR/USD_otc") == "EURUSD_otc"  # OTC suffix preserved
    assert sanitize_asset("EURUSD_otc") == "EURUSD_otc"   # already OTC
    assert sanitize_asset("") == ""


def test_build_tick_and_follow_messages():
    assert build_tick_message() == '42["tick"]'
    assert build_follow_message("EUR/USD") == '42["instruments/follow","EURUSD"]'
    assert build_follow_message("EURUSD") == '42["instruments/follow","EURUSD"]'


def test_compute_time_otc_uses_seconds():
    assert compute_order_time_and_type("EUR/USD_otc", 300, 1750000000.0) == (100, 300)


def test_compute_time_regular_anchors_to_minutes():
    """Regular (non-OTC) assets use optionType=1 and a minute-anchored time —
    resolve_asset picks the regular pair, so this is the shape normal orders
    take."""
    ts = datetime(2026, 8, 8, 12, 0, 0).timestamp()  # exactly on the minute
    option_type, time_field = compute_order_time_and_type("EURUSD", 60, ts)
    assert option_type == 1
    assert time_field == int(ts) + 60  # anchor to next minute boundary

    option_type, time_field = compute_order_time_and_type("EURUSD", 300, ts)
    assert option_type == 1
    assert time_field == int(ts) + 300


def test_compute_time_regular_rounds_up_half_minute():
    """Regular assets anchor to the next whole minute: past the :30 mark the
    expiry moves to the following minute boundary."""
    ts = datetime(2026, 8, 8, 12, 0, 45).timestamp()  # past the :30 mark
    option_type, time_field = compute_order_time_and_type("EURUSD", 60, ts)
    assert option_type == 1
    # 12:00:45 -> shift 1 -> 12:01:00 -> +1min -> 12:02:00 = +75s
    assert time_field == int(ts) + 75
