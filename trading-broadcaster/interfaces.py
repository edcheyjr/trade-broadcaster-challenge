from __future__ import annotations

from typing import Any, Protocol, TypeAlias


JsonMessage: TypeAlias = dict[str, Any]


class ProviderClient(Protocol):
    host: str
    symbols: set[str]

    async def connect(self) -> None:
        ...

    def update_symbols(self, symbols: set[str]) -> None:
        ...

    async def close(self) -> None:
        ...

    async def send_ack(self, processed: bool) -> None:
        ...

    async def request_reservation(self, symbol: str) -> JsonMessage:
        ...
