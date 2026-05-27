from __future__ import annotations

import asyncio
from typing import Any

import requests


class SymbolCache:
    def __init__(self, url: str) -> None:
        self.url = url
        self._symbols: set[str] = set()

    @property
    def symbols(self) -> set[str]:
        return set(self._symbols)

    def is_valid(self, symbol: str) -> bool:
        return symbol in self._symbols

    async def load(self) -> set[str]:
        # Offloads the blocking synchronous function to a separate thread
        self._symbols = await asyncio.to_thread(self._fetch_symbols)
        return self.symbols

    def _fetch_symbols(self) -> set[str]:
        response = requests.get(self.url, timeout=5)
        response.raise_for_status()
        payload: Any = response.json()

        if not isinstance(payload, list):
            raise ValueError("invalid symbol API response")

        symbols: set[str] = set()
        for item in payload:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                raise ValueError("invalid symbol API response")
            symbols.add(item["id"])

        return symbols
