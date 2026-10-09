"""SignalParser tailored to the MagicTrader gale signal format.

Expected layout (from the user's sample)::

    💰5-minute expiration
    USD/IDR; 08:55; PUT 🟥
    🕐 TIME TO 09:00
    1st GALE —> TIME TO 09:05
    2nd GALE —> TIME TO 09:10

The regexes are all local to this module — tuning them to match new message
layouts changes nothing else in the codebase.
"""

from __future__ import annotations

import re
from datetime import time

from broker.base import Direction
from telegram_signals.model import Signal, SignalParseError

# "5-minute" / "5 minutes" / "5 min" / "1 minute"
_EXPIRY_RE = re.compile(r"(\d+)[-\s]*(?:minute|min)s?\b", re.I)
# "USD/IDR"
_ASSET_RE = re.compile(r"\b([A-Z]{3}/[A-Z]{3})\b")
# words first, emoji as a fallback signal
_DIRECTION_WORD_RE = re.compile(r"\b(PUT|SELL|DOWN|CALL|BUY|UP)\b", re.I)
_TIME_RE = re.compile(r"\b(\d{1,2}):(\d{2})\b")
# the trade's expiry time, e.g. "🕐 TIME TO 16:41" or "🕐 TIME UNTIL 09:32"
_TIME_TO_RE = re.compile(r"TIME\s+(?:TO|UNTIL)\s*(\d{1,2}):(\d{2})", re.I)
_GALE_RE = re.compile(r"\b(\d+)\s*(?:st|nd|rd|th)?\s*GALE\b", re.I)

_WORD_TO_DIRECTION = {
    "PUT": Direction.PUT,
    "SELL": Direction.PUT,
    "DOWN": Direction.PUT,
    "CALL": Direction.CALL,
    "BUY": Direction.CALL,
    "UP": Direction.CALL,
}
_EMOJI_TO_DIRECTION = {
    "🟥": Direction.PUT,
    "🔴": Direction.PUT,
    "🟩": Direction.CALL,
    "🟢": Direction.CALL,
}


def _seconds_between(start: time, end: time) -> int:
    """Seconds from ``start`` to ``end`` on the clock; crosses midnight when
    ``end`` is earlier (e.g. 23:58 -> 00:00 = 2 minutes)."""
    a = start.hour * 3600 + start.minute * 60 + start.second
    b = end.hour * 3600 + end.minute * 60 + end.second
    diff = b - a
    if diff <= 0:
        diff += 24 * 3600
    return diff


class SignalParser:
    """Parses MagicTrader gale signal text.

    Expiry is taken from the signal's own times when present (``entry`` vs the
    ``TIME TO HH:MM`` line — e.g. 16:40 to 16:41 ⇒ 1 minute), then from
    ``override_expiry_s`` (>0) if set, then from the ``N-minute expiration``
    text. So a manually-written signal always gets the expiry the author set.
    """

    def __init__(self, override_expiry_s: int = 0) -> None:
        self.override_expiry_s = override_expiry_s

    def parse(self, text: str, *, source: str = "") -> Signal:
        """Parse a signal message.  Raises :class:`SignalParseError` when any
        of the mandatory fields (asset, direction, expiry) is missing.

        Expiry resolution order:
        1. the signal's own times — ``entry`` vs the ``TIME TO HH:MM`` line
           (e.g. ``16:40`` to ``16:41`` ⇒ 1 minute),
        2. ``override_expiry_s`` (config), when set,
        3. the ``N-minute expiration`` text.
        """
        if not text or not text.strip():
            raise SignalParseError("empty message")

        upper = text.upper()
        asset_m = _ASSET_RE.search(upper)
        if asset_m is None:
            raise SignalParseError("asset not found (expected XXX/YYY)")
        asset = asset_m.group(1)

        direction = self._find_direction(text)
        if direction is None:
            raise SignalParseError("direction not found (expected CALL or PUT)")

        entry_time = self._find_entry_time(text, asset)
        expiry_time = self._find_expiry_time(text)
        time_expiry_s = (
            _seconds_between(entry_time, expiry_time)
            if entry_time is not None and expiry_time is not None
            else 0
        )
        if time_expiry_s > 0:
            expiry_s = time_expiry_s
        elif self.override_expiry_s > 0:
            expiry_s = self.override_expiry_s
        else:
            expiry = _EXPIRY_RE.search(text)
            if expiry is None:
                raise SignalParseError(
                    "expiry not found (expected 'N-minute expiration' or a TIME TO time)"
                )
            expiry_s = int(expiry.group(1)) * 60
            if expiry_s <= 0:
                raise SignalParseError("invalid expiry")

        return Signal(
            asset=asset,
            direction=direction,
            expiry_seconds=expiry_s,
            entry_time=entry_time,
            gale_count=self._find_gale_count(text),
            raw_text=text,
            source=source,
        )

    def try_parse(self, text: str, *, source: str = "") -> Signal | None:
        """``parse`` that returns ``None`` instead of raising."""
        try:
            return self.parse(text, source=source)
        except SignalParseError:
            return None

    @staticmethod
    def _find_direction(text: str) -> Direction | None:
        m = _DIRECTION_WORD_RE.search(text)
        if m:
            return _WORD_TO_DIRECTION[m.group(1).upper()]
        for ch in text:
            if ch in _EMOJI_TO_DIRECTION:
                return _EMOJI_TO_DIRECTION[ch]
        return None

    @staticmethod
    def _find_entry_time(text: str, asset: str) -> time | None:
        # Prefer a time on the same line as the asset (the entry line).
        for line in text.splitlines():
            if _ASSET_RE.search(line.upper()):
                m = _TIME_RE.search(line)
                if m:
                    return SignalParser._make_time(m)
        # Fallback: first time that is not one of the "TIME TO" expiry times.
        expiry_at = {m.start() for m in _TIME_TO_RE.finditer(text)}
        for m in _TIME_RE.finditer(text):
            if m.start() in expiry_at:
                continue
            return SignalParser._make_time(m)
        return None

    @staticmethod
    def _find_expiry_time(text: str) -> time | None:
        # The trade's expiry is the first "TIME TO HH:MM" (gale lines also use
        # "TIME TO", so the first match is the level-0 expiry).
        m = _TIME_TO_RE.search(text)
        return SignalParser._make_time(m) if m else None

    @staticmethod
    def _make_time(m: re.Match) -> time | None:
        hour, minute = int(m.group(1)), int(m.group(2))
        if hour > 23 or minute > 59:
            return None
        return time(hour, minute)

    @staticmethod
    def _find_gale_count(text: str) -> int:
        counts = [int(m.group(1)) for m in _GALE_RE.finditer(text)]
        return max(counts) if counts else 0
