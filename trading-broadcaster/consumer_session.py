from __future__ import annotations

import asyncio
import json
from typing import Any, TypeAlias

from models import ProviderTradeSnapshot, TradeEvent
from provider_connection import ProviderConnection

JsonMessage: TypeAlias = dict[str, Any]


class ConsumerSession:
    def __init__(
        self,
        websocket: Any,
        symbol_cache: Any,
        provider_connection_cls: type[ProviderConnection] = ProviderConnection,
    ) -> None:
        self.websocket = websocket
        self.symbol_cache = symbol_cache
        self.provider_connection_cls = provider_connection_cls
        self.providers_by_host: dict[str, ProviderConnection] = {}
        self.latest_forwarded_by_symbol: dict[str, int] = {}
        self.seen_trades_by_symbol: dict[str, dict[str, ProviderTradeSnapshot]] = {}
        self._send_lock = asyncio.Lock()
        self._reserve_lock = asyncio.Lock()
        self._arrival_counter = 0
        self._closed = False

    async def run(self) -> None:
        try:
            print("client connected")
            async for raw_message in self.websocket:
                await self.handle_consumer_message(raw_message)
        except Exception as error:
            print(error)
        finally:
            await self.close()
            print("client disconnected")

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        await self._close_all_providers()

        try:
            await self.websocket.close()
        except Exception:
            pass

    async def handle_consumer_message(self, raw_message: str) -> None:
        try:
            data: Any = json.loads(raw_message)
        except json.JSONDecodeError:
            await self.send_json({"status": "not processed", "message": "invalid JSON"})
            return

        if not isinstance(data, dict):
            await self.send_json({"status": "not processed", "message": "invalid message"})
            return

        action = data.get("action")

        try:
            if action == "add-provider":
                await self._handle_add_provider(data)
            elif action == "clear-providers":
                await self._handle_clear_providers()
            elif action == "clear-prices":
                await self._handle_clear_prices()
            elif action == "reserve-symbol":
                await self._handle_reserve_symbol(data)
            else:
                await self.send_json({"status": "not processed", "message": "unknown action"})
        except Exception as error:
            await self.send_json({"status": "not processed", "message": str(error)})

    async def handle_provider_trade(
        self, provider: ProviderConnection, message: Any
    ) -> None:
        required_fields = {"symbol", "price", "quantity", "timestamp"}
        if not isinstance(message, dict) or not required_fields.issubset(message):
            return

        symbol = message["symbol"]
        if not isinstance(symbol, str):
            return

        if not self.symbol_cache.is_valid(symbol):
            await provider.send_ack(False)
            return

        await provider.send_ack(True)

        if symbol not in provider.symbols:
            return

        trade = TradeEvent.from_message(message)
        self._record_seen_trade(provider.host, trade)

        latest_timestamp = self.latest_forwarded_by_symbol.get(symbol)
        if latest_timestamp is not None and trade.timestamp <= latest_timestamp:
            return

        self.latest_forwarded_by_symbol[symbol] = trade.timestamp
        await self.send_json(trade.to_message())

    async def handle_provider_disconnect(
        self, host: str, provider: ProviderConnection
    ) -> None:
        current_provider = self.providers_by_host.get(host)
        if current_provider is provider:
            self.providers_by_host.pop(host, None)

    async def send_json(self, payload: JsonMessage) -> None:
        async with self._send_lock:
            await self.websocket.send(json.dumps(payload))

    async def _handle_add_provider(self, data: JsonMessage) -> None:
        host = data.get("host")
        symbols = data.get("symbols")

        if not isinstance(host, str) or host == "":
            raise ValueError("host is required")
        if not isinstance(symbols, list):
            raise ValueError("symbols must be a list")

        filtered_symbols = {
            symbol
            for symbol in symbols
            if isinstance(symbol, str) and self.symbol_cache.is_valid(symbol)
        }

        provider = self.providers_by_host.get(host)
        if provider is None:
            provider = self.provider_connection_cls(host, self)
            try:
                await provider.connect()
            except Exception:
                await provider.close()
                await self.send_json(
                    {"status": "not processed", "message": f"error connecting to {host}"}
                )
                return

            self.providers_by_host[host] = provider

        provider.update_symbols(filtered_symbols)
        await self.send_json({"status": "processed", "message": f"connected to {host}"})

    async def _handle_clear_providers(self) -> None:
        await self._close_all_providers()
        await self.send_json({"status": "processed"})

    async def _handle_clear_prices(self) -> None:
        self.latest_forwarded_by_symbol.clear()
        self.seen_trades_by_symbol.clear()
        await self.send_json({"status": "processed"})

    async def _handle_reserve_symbol(self, data: JsonMessage) -> None:
        symbol = data.get("symbol")
        if not isinstance(symbol, str) or symbol == "":
            raise ValueError("symbol is required")

        async with self._reserve_lock:
            snapshots = list(self.seen_trades_by_symbol.get(symbol, {}).values())
            if not snapshots:
                await self.send_json({"status": "not processed", "message": "symbol not available"})
                return

            snapshots.sort(
                key=lambda snapshot: (snapshot.trade.timestamp, snapshot.arrival_order),
                reverse=True,
            )

            for snapshot in snapshots:
                provider = self.providers_by_host.get(snapshot.host)
                if provider is None:
                    continue

                try:
                    response = await provider.request_reservation(symbol)
                except Exception:
                    continue

                if response.get("reserved") is True:
                    await self.send_json(response)
                    return

            await self.send_json(
                {
                    "status": "processed",
                    "reserved": False,
                    "reservationId": None,
                    "replyTo": {"action": "reserve-symbol", "symbol": symbol},
                }
            )

    async def _close_all_providers(self) -> None:
        providers = list(self.providers_by_host.values())
        self.providers_by_host.clear()

        for provider in providers:
            await provider.close()

    def _record_seen_trade(self, host: str, trade: TradeEvent) -> None:
        symbol_snapshots = self.seen_trades_by_symbol.setdefault(trade.symbol, {})
        self._arrival_counter += 1
        symbol_snapshots[host] = ProviderTradeSnapshot(
            host=host,
            trade=trade,
            arrival_order=self._arrival_counter,
        )
