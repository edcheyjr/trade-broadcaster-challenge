from __future__ import annotations

import asyncio
from typing import Any

import websockets
from consumer_session import ConsumerSession
from symbol_cache import SymbolCache

HOST = "127.0.0.1"
PORT = 9000
SYMBOLS_URL = "http://127.0.0.1:3000/api/symbols"


async def main() -> None:
    trading_broadcaster_server: Any | None = None

    try:
        # load symbols before accepting connections
        symbol_cache = SymbolCache(SYMBOLS_URL)
        await symbol_cache.load()

        # start server
        trading_broadcaster_server = await create_trading_broadcaster_server(
            symbol_cache
        )
        await asyncio.Future()  # run forever
    except Exception as error:
        print(error)
        if trading_broadcaster_server is not None:
            trading_broadcaster_server.close()
            await trading_broadcaster_server.wait_closed()
        raise


async def create_trading_broadcaster_server(symbol_cache: SymbolCache) -> Any:
    async def handler(ws: Any, path: str) -> None:
        session = ConsumerSession(ws, symbol_cache)
        await session.run()

    trading_broadcaster_server = await websockets.serve(handler, HOST, PORT)
    print(f"server listening on port {PORT}")
    return trading_broadcaster_server


if __name__ == "__main__":
    asyncio.run(main())
