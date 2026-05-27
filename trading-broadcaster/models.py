from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


TradeMessage = dict[str, Any]


@dataclass
class TradeEvent:
    symbol: str
    price: float
    quantity: int
    timestamp: int

    @classmethod
    def from_message(cls, data: Mapping[str, Any]) -> TradeEvent:
        return cls(
            symbol=data["symbol"],
            price=data["price"],
            quantity=data["quantity"],
            timestamp=data["timestamp"],
        )

    def to_message(self) -> TradeMessage:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "timestamp": self.timestamp,
        }


@dataclass
class ProviderTradeSnapshot:
    host: str
    trade: TradeEvent
    arrival_order: int
