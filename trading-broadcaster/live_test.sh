#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

poetry run python - <<'PY'
import asyncio
import json
from typing import Any

import websockets


TRADING_BROADCASTER_URL = "ws://127.0.0.1:9000"

TSLA = "a631dc6c-ee85-458d-80d7-50018aedfbad"
INVALID_SYMBOL = "not-in-symbol-api"


class ControlledProvider:
    def __init__(
        self,
        name: str,
        port: int,
        trades: list[dict[str, Any]],
        reservation_id: str,
    ) -> None:
        self.name = name
        self.port = port
        self.host = f"ws://127.0.0.1:{port}"
        self.trades = trades
        self.reservation_id = reservation_id
        self.server = None
        self.connections = 0

    async def start(self) -> None:
        self.server = await websockets.serve(self.handle_connection, "127.0.0.1", self.port)
        print(f"{self.name}: provider listening on {self.host}")

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def handle_connection(self, ws, path: str) -> None:
        self.connections += 1
        connection_id = self.connections
        print(f"{self.name}: TB connected as provider client #{connection_id}")

        # Give TB enough time to merge this provider's subscribed symbol list.
        await asyncio.sleep(0.25)

        for trade in self.trades:
            await ws.send(json.dumps(trade))
            ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            print(f"{self.name}: sent {trade['symbol']}@{trade['timestamp']}, ack={ack}")

        async for raw_message in ws:
            message = json.loads(raw_message)
            print(f"{self.name}: received {message}")

            if message.get("action") == "reserve-symbol":
                await ws.send(
                    json.dumps(
                        {
                            "status": "processed",
                            "reserved": True,
                            "reservationId": self.reservation_id,
                            "replyTo": message,
                        }
                    )
                )


async def receive_json(ws, label: str, timeout: float = 5) -> dict[str, Any]:
    raw_message = await asyncio.wait_for(ws.recv(), timeout=timeout)
    print(f"{label}: {raw_message}")
    return json.loads(raw_message)


async def expect_no_message(ws, label: str, timeout: float = 1) -> None:
    try:
        raw_message = await asyncio.wait_for(ws.recv(), timeout=timeout)
    except asyncio.TimeoutError:
        print(f"{label}: no message, as expected")
        return

    raise AssertionError(f"{label}: expected no message, got {raw_message}")


def assert_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


async def add_provider(ws, host: str) -> None:
    await ws.send(
        json.dumps(
            {
                "action": "add-provider",
                "host": host,
                "symbols": [TSLA],
            }
        )
    )
    response = await receive_json(ws, f"add-provider {host}")
    assert_equal(response["status"], "processed", f"add-provider {host} status")


async def consumer_one(provider_a: ControlledProvider, provider_b: ControlledProvider) -> None:
    print("\nconsumer 1: validates latest-trade filtering and reserve ordering")

    async with websockets.connect(TRADING_BROADCASTER_URL) as ws:
        await add_provider(ws, provider_a.host)

        trade = await receive_json(ws, "consumer 1 trade from provider A")
        assert_equal(trade["symbol"], TSLA, "consumer 1 provider A symbol")
        assert_equal(trade["timestamp"], 101, "consumer 1 provider A latest timestamp")

        await expect_no_message(ws, "consumer 1 stale provider A trade")

        await add_provider(ws, provider_b.host)
        await expect_no_message(ws, "consumer 1 provider B older trade")

        await ws.send(json.dumps({"action": "reserve-symbol", "symbol": TSLA}))
        reserve_response = await receive_json(ws, "consumer 1 reserve response")
        assert_equal(
            reserve_response["reservationId"],
            "provider-a-latest",
            "consumer 1 reserve provider",
        )


async def consumer_two(provider_b: ControlledProvider) -> None:
    print("\nconsumer 2: validates independent latest-price state")

    async with websockets.connect(TRADING_BROADCASTER_URL) as ws:
        await add_provider(ws, provider_b.host)

        trade = await receive_json(ws, "consumer 2 trade from provider B")
        assert_equal(trade["symbol"], TSLA, "consumer 2 provider B symbol")
        assert_equal(trade["timestamp"], 100, "consumer 2 provider B timestamp")


async def main() -> None:
    provider_a = ControlledProvider(
        name="provider A",
        port=9101,
        reservation_id="provider-a-latest",
        trades=[
            {"symbol": TSLA, "price": 101.0, "quantity": 10, "timestamp": 101},
            {"symbol": TSLA, "price": 99.0, "quantity": 10, "timestamp": 99},
            {"symbol": INVALID_SYMBOL, "price": 1.0, "quantity": 1, "timestamp": 102},
        ],
    )
    provider_b = ControlledProvider(
        name="provider B",
        port=9102,
        reservation_id="provider-b-older",
        trades=[
            {"symbol": TSLA, "price": 100.0, "quantity": 20, "timestamp": 100},
        ],
    )

    await provider_a.start()
    await provider_b.start()

    try:
        await consumer_one(provider_a, provider_b)
        await consumer_two(provider_b)
    finally:
        await provider_a.close()
        await provider_b.close()

    print("\nlive test passed")


asyncio.run(main())
PY
