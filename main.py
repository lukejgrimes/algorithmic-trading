from ZC_ARIMA import ZCArima
import asyncio
import os
import json
import api
import websockets
import datastream

DATA_URL = "wss://tasty-openapi-ws.dxfeed.com/realtime"
TASTY_API = "https://api.tastyworks.com"
ACCOUNT_NUMBER = os.getenv("ACCOUNT_NUMBER")

async def main():
    strategies = {1: ZCArima()}
    ws = None
    while True:
        if ws is None or ws.closed:
            strategies[1].save_data()
            api.login()
            api.get_quote_token()
            ws = await connect_to_data_stream(strategies)

        try:
            message = await ws.recv()
            channel, bid, ask = await datastream.on_message(ws, json.loads(message))
            if channel != 0:
                api.login()
                algo = strategies[channel]
                algo.run(bid, ask)
                strategies[1].save_data()

        except websockets.ConnectionClosed:
            print("Connection was closed, reconnecting...")
            ws = None 

async def connect_to_data_stream(strategies):
    ws = await websockets.connect(DATA_URL)
    for channel, algo in strategies.items():
        if channel == 1:
            await datastream.set_up_data_stream(ws, algo.ticker, "QUOTE")
        else:
             await datastream.add_channel(ws, algo.ticker, "QUOTE", channel)

        return ws

if __name__ == "__main__":
    asyncio.run(main())
