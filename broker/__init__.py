"""Broker package: the domain interface, the deterministic simulator, and the
gated live Quotex WebSocket client."""

from broker.base import (
    Broker,
    BrokerAuthError,
    BrokerError,
    BrokerUnavailable,
    Direction,
    InsufficientBalance,
    PlaceTradeRequest,
    TradeResult,
    TradeStatus,
    utcnow,
)

__all__ = [
    "Broker",
    "BrokerAuthError",
    "BrokerError",
    "BrokerUnavailable",
    "Direction",
    "InsufficientBalance",
    "PlaceTradeRequest",
    "TradeResult",
    "TradeStatus",
    "utcnow",
]
