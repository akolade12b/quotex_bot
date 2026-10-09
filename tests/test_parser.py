from datetime import time

import pytest

from broker.base import Direction
from telegram_signals.model import Signal, SignalParseError
from telegram_signals.parser import SignalParser

SAMPLE = """💰5-minute expiration
USD/IDR; 08:55; PUT 🟥

🕐 TIME TO 09:00

1st GALE —> TIME TO 09:05
2nd GALE —> TIME TO 09:10"""

parser = SignalParser()


def test_exact_sample_parses():
    sig = parser.parse(SAMPLE, source="chat:1/msg:2")
    assert sig.asset == "USD/IDR"
    assert sig.direction is Direction.PUT
    assert sig.expiry_seconds == 300
    assert sig.entry_time == time(8, 55)
    assert sig.gale_count == 2
    assert sig.raw_text == SAMPLE
    assert sig.source == "chat:1/msg:2"


def test_call_variant_with_green_emoji():
    text = "💰5-minute expiration\nEUR/USD; 14:30; CALL 🟩\n🕐 TIME TO 14:35\n1st GALE —> TIME TO 14:40"
    sig = parser.parse(text)
    assert sig.asset == "EUR/USD"
    assert sig.direction is Direction.CALL
    assert sig.entry_time == time(14, 30)
    assert sig.gale_count == 1


def test_no_gale_signal():
    text = "💰1-minute expiration\nGBP/USD; 09:00; PUT"
    sig = parser.parse(text)
    assert sig.gale_count == 0
    assert sig.expiry_seconds == 60
    assert sig.entry_time == time(9, 0)


def test_case_insensitive_direction_and_asset():
    sig = parser.parse("💰2-minute expiration\nusd/idr; 08:55; put")
    assert sig.asset == "USD/IDR"
    assert sig.direction is Direction.PUT


def test_emoji_only_direction():
    sig = parser.parse("💰5-minute expiration\nUSD/IDR; 08:55; 🟥")
    assert sig.direction is Direction.PUT
    sig = parser.parse("💰5-minute expiration\nEUR/USD; 08:55; 🟩")
    assert sig.direction is Direction.CALL


def test_buy_sell_map_to_call_put():
    assert parser.parse("💰5-minute expiration\nUSD/IDR; 08:55; SELL").direction is Direction.PUT
    assert parser.parse("💰5-minute expiration\nUSD/IDR; 08:55; BUY").direction is Direction.CALL


def test_expiry_variants():
    assert parser.parse("💰1 minute expiration\nUSD/IDR; 08:55; PUT").expiry_seconds == 60
    assert parser.parse("💰15-minute expiration\nUSD/IDR; 08:55; PUT").expiry_seconds == 900


def test_expiry_from_signal_times():
    # 16:40 -> 16:41 = 1 minute
    sig = parser.parse("USD/IDR; 16:40; PUT\n🕐 TIME TO 16:41")
    assert sig.expiry_seconds == 60
    assert sig.entry_time == time(16, 40)
    # 16:40 -> 16:42 = 2 minutes
    sig = parser.parse("USD/IDR; 16:40; PUT\n🕐 TIME TO 16:42")
    assert sig.expiry_seconds == 120
    # Signal text saying 5-min is ignored when the times disagree.
    sig = parser.parse("💰5-minute expiration\nUSD/IDR; 16:40; PUT\n🕐 TIME TO 16:41")
    assert sig.expiry_seconds == 60


def test_expiry_from_times_beats_override():
    forced = SignalParser(override_expiry_s=60)
    # The times on the signal win over the config override.
    sig = forced.parse("USD/IDR; 16:40; PUT\n🕐 TIME TO 16:42")
    assert sig.expiry_seconds == 120


def test_expiry_crosses_midnight():
    sig = parser.parse("USD/IDR; 23:58; PUT\n🕐 TIME TO 00:00")
    assert sig.expiry_seconds == 120


def test_expiry_from_time_until_format():
    # Real MagicTrader signals use "TIME UNTIL", not "TIME TO".
    text = """💰5-minute expiration
EUR/CAD;09:30;PUT 🟥

🕐TIME UNTIL 09:32

1st GALE —>TIME UNTIL 09:34
2nd GALE —>TIME UNTIL 09:36"""
    sig = parser.parse(text)
    assert sig.asset == "EUR/CAD"
    assert sig.direction is Direction.PUT
    assert sig.entry_time == time(9, 30)
    assert sig.expiry_seconds == 120  # 09:30 -> 09:32 = 2 minutes
    assert sig.gale_count == 2


def test_override_expiry_forces_1_minute():
    forced = SignalParser(override_expiry_s=60)
    sig = forced.parse("💰5-minute expiration\nUSD/IDR; 08:55; PUT")
    assert sig.expiry_seconds == 60  # signal said 5-min, config forces 1-min

    # Even a signal with no expiry works when an override is set.
    sig = forced.parse("USD/IDR; 08:55; PUT")
    assert sig.expiry_seconds == 60

    # No override -> normal behaviour preserved.
    assert parser.parse("💰5-minute expiration\nUSD/IDR; 08:55; PUT").expiry_seconds == 300


def test_missing_direction_raises():
    with pytest.raises(SignalParseError):
        parser.parse("💰5-minute expiration\nUSD/IDR; 08:55")


def test_missing_asset_raises():
    with pytest.raises(SignalParseError):
        parser.parse("💰5-minute expiration\n08:55; PUT")


def test_missing_expiry_raises():
    with pytest.raises(SignalParseError):
        parser.parse("USD/IDR; 08:55; PUT")


def test_try_parse_returns_none_for_gibberish():
    assert parser.try_parse("hello world, nothing to see here") is None
    assert parser.try_parse("") is None


def test_try_parse_ignores_links_and_boilerplate():
    text = """💰5-minute expiration
USD/IDR; 08:55; PUT 🟥

📲 Click to open the broker (https://t.me/Magictradereng_bot?startapp)
🙋‍♂️ Don't know how to trade yet? Click here (https://t.me/c/3559202666/5)"""
    sig = parser.try_parse(text)
    assert sig is not None
    assert sig.asset == "USD/IDR"
    assert sig.direction is Direction.PUT
    assert sig.entry_time == time(8, 55)
