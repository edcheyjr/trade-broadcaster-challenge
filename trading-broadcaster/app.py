import asyncio
import json
import requests
import websockets

async def main():
    try:
        trading_broadcaster_server = await create_trading_broadcaster_server()
        await asyncio.Future()  # run forever
    except Exception as e:
        if trading_broadcaster_server:
            await trading_broadcaster_server.close()
        print(e)


async def create_trading_broadcaster_server():
    port = 9000
    trading_broadcaster_server = await websockets.serve(handler, "127.0.0.1", port)
    print(f"server listening on port {port}")
    return trading_broadcaster_server


async def handler(ws, path):
    try:
        print("client connected")

        async for message in ws:
            try:
                data = json.loads(message)
                print("message received: ", data)
            except Exception as e:
                print(e)

    except Exception as e:
        print(e)

    finally:
        await ws.close()
        print("client disconnected")


if __name__ == "__main__":
    asyncio.run(main())
