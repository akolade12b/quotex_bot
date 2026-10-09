"""The parsed Signal value object shared by the parser, handlers, and engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timezone

from broker.base import Direction


@dataclass(frozen=True)
class Signal:
    asset: str
    direction: Direction
    expiry_seconds: int
    entry_time: time | None = None  # wall-clock HH:MM; None ⇒ execute immediately
    gale_count: int = 0  # martingale ladder length after the initial entry
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_text: str = ""
    source: str = ""  # e.g. "chat:123/msg:45"
    signal_id: int | None = None  # assigned once persisted


class SignalParseError(ValueError):
    """Raised when a message cannot be parsed into a Signal."""
