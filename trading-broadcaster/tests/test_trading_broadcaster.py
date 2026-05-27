from __future__ import annotations

import json
import pathlib
import sys
import types
import unittest
from typing import Any, ClassVar
from unittest.mock import MagicMock, Mock, patch

PROJECT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

if "websockets" not in sys.modules:
    websockets_stub = types.ModuleType("websockets")
    legacy_stub = types.ModuleType("websockets.legacy")
    client_stub = types.ModuleType("websockets.legacy.client")

    async def _unexpected_connect(*args: object, **kwargs: object) -> None:
        raise RuntimeError("websockets connect should not be used in unit tests")

    async def _unexpected_serve(*args: object, **kwargs: object) -> None:
        raise RuntimeError("websockets serve should not be used in unit tests")

    setattr(websockets_stub, "connect", _unexpected_connect)
    setattr(websockets_stub, "serve", _unexpected_serve)
    setattr(client_stub, "connect", _unexpected_connect)
    setattr(legacy_stub, "client", client_stub)
    setattr(websockets_stub, "legacy", legacy_stub)
    sys.modules["websockets"] = websockets_stub
    sys.modules["websockets.legacy"] = legacy_stub
    sys.modules["websockets.legacy.client"] = client_stub

from consumer_session import ConsumerSession
from interfaces import JsonMessage
from symbol_cache import SymbolCache


class FakeConsumerWebSocket:
    def __init__(self) -> None:
        self.sent_messages: list[JsonMessage] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent_messages.append(json.loads(message))

    async def close(self) -> None:
        self.closed = True


class FakeProviderConnection:
    fail_hosts: ClassVar[set[str]] = set()
    reserve_responses: ClassVar[dict[str, list[JsonMessage]]] = {}
    instances: ClassVar[dict[str, FakeProviderConnection]] = {}

    def __init__(self, host: str, session: Any) -> None:
        self.host = host
        self.session = session
        self.symbols: set[str] = set()
        self.connected = False
        self.closed = False
        self.acks: list[bool] = []
        self.reserve_requests: list[str] = []
        FakeProviderConnection.instances[host] = self

    @classmethod
    def reset(cls) -> None:
        cls.fail_hosts = set()
        cls.reserve_responses = {}
        cls.instances = {}

    async def connect(self) -> None:
        if self.host in self.fail_hosts:
            raise ConnectionError("failed")
        self.connected = True

    def update_symbols(self, symbols: set[str]) -> None:
        self.symbols.update(symbols)

    async def close(self) -> None:
        self.closed = True

    async def send_ack(self, processed: bool) -> None:
        self.acks.append(processed)

    async def request_reservation(self, symbol: str) -> JsonMessage:
        self.reserve_requests.append(symbol)
        responses = self.reserve_responses.get(self.host, [])
        if responses:
            return responses.pop(0)
        return {
            "status": "processed",
            "reserved": False,
            "reservationId": None,
            "replyTo": {"action": "reserve-symbol", "symbol": symbol},
        }


class SymbolCacheTests(unittest.TestCase):
    def test_loads_valid_symbol_ids(self) -> None:
        response = Mock()
        response.json.return_value = [{"id": "SYM-1"}, {"id": "SYM-2"}]
        response.raise_for_status.return_value = None

        cache = SymbolCache("http://127.0.0.1:3000/api/symbols")

        with patch("symbol_cache.requests.get", return_value=response) as mock_get:
            symbols = cache._fetch_symbols()

        self.assertEqual(symbols, {"SYM-1", "SYM-2"})
        mock_get.assert_called_once_with("http://127.0.0.1:3000/api/symbols", timeout=5)


class ConsumerSessionTests(unittest.IsolatedAsyncioTestCase):
    symbol_cache: MagicMock
    websocket: FakeConsumerWebSocket
    session: ConsumerSession

    async def asyncSetUp(self) -> None:
        FakeProviderConnection.reset()
        self.symbol_cache = MagicMock()
        self.symbol_cache.is_valid.side_effect = lambda symbol: (
            symbol
            in {
                "SYM-1",
                "SYM-2",
                "SYM-3",
            }
        )
        self.websocket = FakeConsumerWebSocket()
        self.session = ConsumerSession(
            self.websocket,
            self.symbol_cache,
            provider_connection_cls=FakeProviderConnection,
        )

    async def asyncTearDown(self) -> None:
        await self.session.close()

    async def test_repeated_add_provider_merges_symbols_for_same_host(self) -> None:
        await self.session.handle_consumer_message(
            json.dumps(
                {
                    "action": "add-provider",
                    "host": "ws://localhost:9001",
                    "symbols": ["SYM-1", "SYM-2"],
                }
            )
        )
        await self.session.handle_consumer_message(
            json.dumps(
                {
                    "action": "add-provider",
                    "host": "ws://localhost:9001",
                    "symbols": ["SYM-1", "SYM-3"],
                }
            )
        )

        provider = self.session.providers_by_host["ws://localhost:9001"]
        self.assertEqual(provider.symbols, {"SYM-1", "SYM-2", "SYM-3"})
        self.assertEqual(self.websocket.sent_messages[-1]["status"], "processed")

    async def test_different_hosts_remain_independent(self) -> None:
        await self.session.handle_consumer_message(
            json.dumps(
                {
                    "action": "add-provider",
                    "host": "ws://localhost:9001",
                    "symbols": ["SYM-1", "SYM-2"],
                }
            )
        )
        await self.session.handle_consumer_message(
            json.dumps(
                {
                    "action": "add-provider",
                    "host": "ws://localhost:9002",
                    "symbols": ["SYM-1"],
                }
            )
        )

        self.assertEqual(
            self.session.providers_by_host["ws://localhost:9001"].symbols,
            {"SYM-1", "SYM-2"},
        )
        self.assertEqual(
            self.session.providers_by_host["ws://localhost:9002"].symbols,
            {"SYM-1"},
        )

    async def test_invalid_provider_symbol_is_not_processed_and_ignored(self) -> None:
        provider = FakeProviderConnection("ws://localhost:9001", self.session)
        provider.update_symbols({"INVALID"})

        await self.session.handle_provider_trade(
            provider,
            {"symbol": "INVALID", "price": 10.5, "quantity": 3, "timestamp": 100},
        )

        self.assertEqual(provider.acks, [False])
        self.assertEqual(self.websocket.sent_messages, [])
        self.assertEqual(self.session.seen_trades_by_symbol, {})

    async def test_valid_but_unsubscribed_symbol_is_acked_and_not_forwarded(
        self,
    ) -> None:
        provider = FakeProviderConnection("ws://localhost:9001", self.session)
        provider.update_symbols({"SYM-2"})

        await self.session.handle_provider_trade(
            provider,
            {"symbol": "SYM-1", "price": 10.5, "quantity": 3, "timestamp": 100},
        )

        self.assertEqual(provider.acks, [True])
        self.assertEqual(self.websocket.sent_messages, [])

    async def test_only_newer_timestamps_are_forwarded(self) -> None:
        provider = FakeProviderConnection("ws://localhost:9001", self.session)
        provider.update_symbols({"SYM-1"})

        await self.session.handle_provider_trade(
            provider,
            {"symbol": "SYM-1", "price": 10.5, "quantity": 3, "timestamp": 100},
        )
        await self.session.handle_provider_trade(
            provider,
            {"symbol": "SYM-1", "price": 11.0, "quantity": 4, "timestamp": 99},
        )
        await self.session.handle_provider_trade(
            provider,
            {"symbol": "SYM-1", "price": 12.0, "quantity": 5, "timestamp": 100},
        )
        await self.session.handle_provider_trade(
            provider,
            {"symbol": "SYM-1", "price": 13.0, "quantity": 6, "timestamp": 101},
        )

        self.assertEqual(len(self.websocket.sent_messages), 2)
        self.assertEqual(self.websocket.sent_messages[0]["timestamp"], 100)
        self.assertEqual(self.websocket.sent_messages[1]["timestamp"], 101)

    async def test_clear_prices_resets_latest_price_filtering(self) -> None:
        provider = FakeProviderConnection("ws://localhost:9001", self.session)
        provider.update_symbols({"SYM-1"})

        await self.session.handle_provider_trade(
            provider,
            {"symbol": "SYM-1", "price": 10.5, "quantity": 3, "timestamp": 100},
        )
        await self.session.handle_consumer_message(
            json.dumps({"action": "clear-prices"})
        )
        await self.session.handle_provider_trade(
            provider,
            {"symbol": "SYM-1", "price": 11.5, "quantity": 4, "timestamp": 100},
        )

        trade_messages = [
            message for message in self.websocket.sent_messages if "symbol" in message
        ]
        self.assertEqual(len(trade_messages), 2)
        self.assertEqual(trade_messages[1]["timestamp"], 100)

    async def test_clear_providers_only_tears_down_requesting_consumer(self) -> None:
        other_websocket = FakeConsumerWebSocket()
        other_session = ConsumerSession(
            other_websocket,
            self.symbol_cache,
            provider_connection_cls=FakeProviderConnection,
        )

        await self.session.handle_consumer_message(
            json.dumps(
                {
                    "action": "add-provider",
                    "host": "ws://localhost:9001",
                    "symbols": ["SYM-1"],
                }
            )
        )
        await other_session.handle_consumer_message(
            json.dumps(
                {
                    "action": "add-provider",
                    "host": "ws://localhost:9002",
                    "symbols": ["SYM-1"],
                }
            )
        )

        await self.session.handle_consumer_message(
            json.dumps({"action": "clear-providers"})
        )

        self.assertEqual(self.session.providers_by_host, {})
        self.assertIn("ws://localhost:9002", other_session.providers_by_host)
        await other_session.close()

    async def test_two_consumers_keep_isolated_latest_state(self) -> None:
        other_websocket = FakeConsumerWebSocket()
        other_session = ConsumerSession(
            other_websocket,
            self.symbol_cache,
            provider_connection_cls=FakeProviderConnection,
        )
        provider_a = FakeProviderConnection("ws://localhost:9001", self.session)
        provider_b = FakeProviderConnection("ws://localhost:9001", other_session)
        provider_a.update_symbols({"SYM-1"})
        provider_b.update_symbols({"SYM-1"})

        await self.session.handle_provider_trade(
            provider_a,
            {"symbol": "SYM-1", "price": 10.0, "quantity": 2, "timestamp": 100},
        )
        await self.session.handle_provider_trade(
            provider_a,
            {"symbol": "SYM-1", "price": 11.0, "quantity": 2, "timestamp": 101},
        )
        await other_session.handle_provider_trade(
            provider_b,
            {"symbol": "SYM-1", "price": 9.0, "quantity": 1, "timestamp": 100},
        )

        self.assertEqual(len(self.websocket.sent_messages), 2)
        self.assertEqual(len(other_websocket.sent_messages), 1)
        self.assertEqual(other_websocket.sent_messages[0]["timestamp"], 100)
        await other_session.close()

    async def test_reserve_symbol_not_available_when_never_seen(self) -> None:
        await self.session.handle_consumer_message(
            json.dumps({"action": "reserve-symbol", "symbol": "SYM-1"})
        )

        self.assertEqual(
            self.websocket.sent_messages[-1],
            {"status": "not processed", "message": "symbol not available"},
        )

    async def test_reserve_symbol_falls_back_until_success(self) -> None:
        first_provider = FakeProviderConnection("ws://localhost:9001", self.session)
        second_provider = FakeProviderConnection("ws://localhost:9002", self.session)
        self.session.providers_by_host[first_provider.host] = first_provider
        self.session.providers_by_host[second_provider.host] = second_provider
        first_provider.update_symbols({"SYM-1"})
        second_provider.update_symbols({"SYM-1"})

        await self.session.handle_provider_trade(
            first_provider,
            {"symbol": "SYM-1", "price": 10.0, "quantity": 2, "timestamp": 100},
        )
        await self.session.handle_provider_trade(
            second_provider,
            {"symbol": "SYM-1", "price": 11.0, "quantity": 2, "timestamp": 101},
        )

        FakeProviderConnection.reserve_responses = {
            "ws://localhost:9002": [
                {
                    "status": "processed",
                    "reserved": False,
                    "reservationId": None,
                    "replyTo": {"action": "reserve-symbol", "symbol": "SYM-1"},
                }
            ],
            "ws://localhost:9001": [
                {
                    "status": "processed",
                    "reserved": True,
                    "reservationId": "abc12",
                    "replyTo": {"action": "reserve-symbol", "symbol": "SYM-1"},
                }
            ],
        }

        await self.session.handle_consumer_message(
            json.dumps({"action": "reserve-symbol", "symbol": "SYM-1"})
        )

        self.assertEqual(second_provider.reserve_requests, ["SYM-1"])
        self.assertEqual(first_provider.reserve_requests, ["SYM-1"])
        self.assertEqual(self.websocket.sent_messages[-1]["reserved"], True)

    async def test_stale_trade_does_not_overwrite_reserve_candidate(self) -> None:
        first_provider = FakeProviderConnection("ws://localhost:9001", self.session)
        second_provider = FakeProviderConnection("ws://localhost:9002", self.session)
        self.session.providers_by_host[first_provider.host] = first_provider
        self.session.providers_by_host[second_provider.host] = second_provider
        first_provider.update_symbols({"SYM-1"})
        second_provider.update_symbols({"SYM-1"})

        await self.session.handle_provider_trade(
            first_provider,
            {"symbol": "SYM-1", "price": 10.0, "quantity": 2, "timestamp": 101},
        )
        await self.session.handle_provider_trade(
            first_provider,
            {"symbol": "SYM-1", "price": 9.0, "quantity": 2, "timestamp": 99},
        )
        await self.session.handle_provider_trade(
            second_provider,
            {"symbol": "SYM-1", "price": 10.5, "quantity": 2, "timestamp": 100},
        )

        FakeProviderConnection.reserve_responses = {
            "ws://localhost:9001": [
                {
                    "status": "processed",
                    "reserved": True,
                    "reservationId": "latest",
                    "replyTo": {"action": "reserve-symbol", "symbol": "SYM-1"},
                }
            ],
            "ws://localhost:9002": [
                {
                    "status": "processed",
                    "reserved": True,
                    "reservationId": "older",
                    "replyTo": {"action": "reserve-symbol", "symbol": "SYM-1"},
                }
            ],
        }

        await self.session.handle_consumer_message(
            json.dumps({"action": "reserve-symbol", "symbol": "SYM-1"})
        )

        self.assertEqual(first_provider.reserve_requests, ["SYM-1"])
        self.assertEqual(second_provider.reserve_requests, [])
        self.assertEqual(self.websocket.sent_messages[-1]["reservationId"], "latest")

    async def test_add_provider_connection_failure_returns_not_processed(self) -> None:
        FakeProviderConnection.fail_hosts.add("ws://localhost:9009")

        await self.session.handle_consumer_message(
            json.dumps(
                {
                    "action": "add-provider",
                    "host": "ws://localhost:9009",
                    "symbols": ["SYM-1"],
                }
            )
        )

        self.assertEqual(
            self.websocket.sent_messages[-1],
            {
                "status": "not processed",
                "message": "error connecting to ws://localhost:9009",
            },
        )


if __name__ == "__main__":
    unittest.main()
