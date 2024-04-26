import os
import json
import api

DATA_URL = "wss://tasty-openapi-ws.dxfeed.com/realtime"

async def set_up_data_stream(ws, ticker, feed_type, start_time=None):
    set_up = {
        "type": "SETUP", 
        "channel": 0, 
        "keepaliveTimeout": 60, 
        "acceptKeepaliveTimeout": 60, 
        "version": "0.1" 
    }
    
    auth = {
        "type": "AUTH",
        "token": os.getenv("QUOTE_TOKEN"),
        "channel": 0
    }

    channel_req = {
        "type": "CHANNEL_REQUEST",
        "channel": 1,
        "service": "FEED",
        "parameters": { "contract": "AUTO" }
    }

    await ws.send(json.dumps(set_up))
    res = await ws.recv()
    print(res)
    await ws.send(json.dumps(auth))
    await ws.recv()
    res = await ws.recv()
    print(res)
    if json.loads(res)["state"] == "UNAUTHORIZED":
        api.get_quote_token()
        await ws.send(json.dumps(auth))
        await ws.recv()
        res = await ws.recv()
        print(res)

    await ws.send(json.dumps(channel_req))
    res = await ws.recv()
    print(res)

    if feed_type == "QUOTE":
        quote_feed_req = {
            "type": "FEED_SUBSCRIPTION",
            "channel": 1,
            "add": [{ "symbol": ticker, "type": "Quote" }]
        }
        await ws.send(json.dumps(quote_feed_req))

    elif feed_type == "CANDLE":
        price_feed_req = {
            "type": "FEED_SUBSCRIPTION",
            "channel": 1,
            "add": [{ "symbol": ticker, "type": "Candle", "fromTime": start_time}]
        }
        await ws.send(json.dumps(price_feed_req))

    res = await ws.recv()
    print(res)

async def on_message(ws, message):
    keep_alive = {
        "type": "KEEPALIVE",
        "channel": 0
    }
    
    channel = 0
    bid = None
    ask = None

    if message["type"] == "KEEPALIVE":
        await ws.send(json.dumps(keep_alive))
    
    elif message["type"] == "FEED_DATA":
        print(message)
        channel = message["channel"]
        bid = message["data"][0]["bidPrice"]
        ask = message["data"][0]["askPrice"]

    return channel, bid, ask

async def add_channel(ws, ticker, feed_type, channel, start_time=None):
    channel_req = {
        "type": "CHANNEL_REQUEST",
        "channel": channel,
        "service": "FEED",
        "parameters": { "contract": "AUTO" }
    }

    await ws.send(json.dumps(channel_req))
    res = await ws.recv()
    print(res)

    if feed_type == "QUOTE":
        quote_feed_req = {
            "type": "FEED_SUBSCRIPTION",
            "channel": 1,
            "add": [{ "symbol": ticker, "type": "Quote" }]
        }
        await ws.send(json.dumps(quote_feed_req))

    elif feed_type == "CANDLE":
        price_feed_req = {
            "type": "FEED_SUBSCRIPTION",
            "channel": 1,
            "add": [{ "symbol": ticker, "type": "Candle", "fromTime": start_time}]
        }
        await ws.send(json.dumps(price_feed_req))

    res = await ws.recv()
    print(res)
