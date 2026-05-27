from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

from interfaces import JsonMessage
from websockets.legacy.client import connect

if TYPE_CHECKING:
    from consumer_session import ConsumerSession


class ProviderConnection:
    def __init__(self, host: str, session: ConsumerSession) -> None:
        self.host = host
        self.session = session
        self.websocket: Any | None = None
        self.reader_task: asyncio.Task[None] | None = None
        self.symbols: set[str] = set()
        self._closed = False
        self._send_lock = asyncio.Lock()
        self._reservation_response_future: asyncio.Future[JsonMessage] | None = None

    async def connect(self) -> None:
        self.websocket = await connect(self.host)
        self.reader_task = asyncio.create_task(self._read_messages())

    def update_symbols(self, symbols: set[str]) -> None:
        self.symbols.update(symbols)

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True

        if self.websocket is not None:
            await self.websocket.close()

        if self.reader_task is not None:
            try:
                await self.reader_task
            except Exception:
                pass

    async def send_ack(self, processed: bool) -> None:
        payload = {"status": "processed" if processed else "not processed"}
        await self.send_json(payload)

    async def send_json(self, payload: JsonMessage) -> None:
        if self.websocket is None:
            raise RuntimeError("provider websocket is not connected")

        async with self._send_lock:
            await self.websocket.send(json.dumps(payload))

    async def request_reservation(self, symbol: str) -> JsonMessage:
        if self.websocket is None:
            raise RuntimeError("provider websocket is not connected")

        if (
            self._reservation_response_future is not None
            and not self._reservation_response_future.done()
        ):
            raise RuntimeError("reservation already in progress")

        self._reservation_response_future = asyncio.get_running_loop().create_future()
        request = {"action": "reserve-symbol", "symbol": symbol}

        try:
            await self.send_json(request)
            return await self._reservation_response_future
        finally:
            self._reservation_response_future = None

    async def _read_messages(self) -> None:
        if self.websocket is None:
            raise RuntimeError("provider websocket is not connected")
        try:
            async for raw_message in self.websocket:
                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError:
                    continue

                if self._is_reservation_reply(message):
                    future = self._reservation_response_future
                    if future is not None and not future.done():
                        future.set_result(message)
                    continue

                await self.session.handle_provider_trade(self, message)
        except Exception:
            pass
        finally:
            future = self._reservation_response_future
            if future is not None and not future.done():
                future.set_exception(
                    ConnectionError(f"provider disconnected: {self.host}")
                )

            await self.session.handle_provider_disconnect(self.host, self)

    @staticmethod
    def _is_reservation_reply(message: Any) -> bool:
        return (
            isinstance(message, dict)
            and "replyTo" in message
            and isinstance(message["replyTo"], dict)
            and message["replyTo"].get("action") == "reserve-symbol"
            and "reserved" in message
        )
