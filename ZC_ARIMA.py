import pandas as pd
import numpy as np
from collections import deque
import datetime
import api
import requests
import os
import pytz
import yfinance as yf
import time

DATA_URL = "wss://tasty-openapi-ws.dxfeed.com/realtime"
TASTY_API = "https://api.tastyworks.com"
ZC_TICKER = "/ZCN4"
UNDERLYING_SYMBOL = "/ZC"
ACCOUNT_NUMBER = os.getenv("ACCOUNT_NUMBER")

class ZCArima:
    def __init__(self):
        self.ticker = "/ZCN24:XCBT"
        self.market_tz = pytz.timezone('America/Chicago')

        zc = yf.Ticker("ZC=F")
        price_history = zc.history(period="1mo", interval="1h")
        if self.is_trading_hour():
            self.price_window = deque(list(price_history["Close"])[-2:-13:-1][::-1])
        else: 
            self.price_window = deque(list(price_history["Close"])[-1:-12:-1][::-1])

        self.returns_window = deque(list(pd.Series(self.price_window).diff())[1:])
        self.preds_window = []
        self.cur_bid = self.price_window[-1]
        self.cur_ask = self.price_window[-1]
        self.position = 0

        now = datetime.datetime.now(datetime.timezone.utc)
        self.next_trade_hour = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)

        self.prices_df = pd.DataFrame({"timestamp": [], "bid": [], "ask": [], "mid": []})


    def run(self, bid, ask):
        if self.is_trading_hour():
            self.bid = bid
            self.ask = ask
            mid = bid + (ask - bid) / 2

            self.prices_df.loc[len(self.prices_df)] = {
                "timestamp": datetime.datetime.now(datetime.timezone.utc), 
                "bid": bid,
                "ask": ask,
                "mid": mid
            }
            
        
        else:
            return

        if datetime.datetime.now(datetime.timezone.utc) >= self.next_trade_hour:
            api.login()
            print(datetime.datetime.now().astimezone(self.market_tz))
            self.cancel_working_orders()
            now = datetime.datetime.now(datetime.timezone.utc)
            self.next_trade_hour = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
            self.returns_window.append(mid - self.price_window[-1])
            self.returns_window.popleft()
            self.price_window.append(mid)
            self.price_window.popleft()

            positions = api.get_positions()
            self.position = positions.get(ZC_TICKER, 0)

            next_return = self.predict(self.returns_window)
            print(f"NEXT RETURN: {next_return}")

            order_type = "Market" if 8 <= now.astimezone(self.market_tz).hour <= 10 else "Limit"

            orders = []

            if next_return > 1.5:
                if self.position < 0:
                    orders.append({
                        "time-in-force": "Day",
                        "order-type": "Market",
                        "legs": [
                            {
                                "instrument-type": "Future",
                                "symbol": ZC_TICKER,
                                "quantity": 1,
                                "action": "Buy to Close"
                            }
                        ]
                    })

                    if order_type == "Limit":

                        orders.append({
                            "time-in-force": "Day",
                            "price": self.bid,
                            "price-effect": "Debit",
                            "order-type": "Limit",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Buy to Open"
                                }
                            ]
                        })
                    else:

                        orders.append({
                            "time-in-force": "Day",
                            "order-type": "Market",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Buy to Open"
                                }
                            ]
                        })

                elif self.position == 0:
                    if order_type == "Limit":

                        orders.append({
                            "time-in-force": "Day",
                            "price": self.bid,
                            "price-effect": "Debit",
                            "order-type": "Limit",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Buy to Open"
                                }
                            ]
                        })
                    else:

                        orders.append({
                            "time-in-force": "Day",
                            "order-type": "Market",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Buy to Open"
                                }
                            ]
                        })

        
            elif next_return < -1.5:
                if self.position > 0:
                    orders.append({
                        "time-in-force": "Day",
                        "order-type": "Market",
                        "legs": [
                            {
                                "instrument-type": "Future",
                                "symbol": ZC_TICKER,
                                "quantity": 1,
                                "action": "Sell to Close"
                            }
                        ]
                    })

                    if order_type == "Limit":

                        orders.append({
                            "time-in-force": "Day",
                            "price": self.ask,
                            "price-effect": "Credit",
                            "order-type": "Limit",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Sell to Open"
                                }
                            ]
                        })
                    else:

                        orders.append({
                            "time-in-force": "Day",
                            "order-type": "Market",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Sell to Open"
                                }
                            ]
                        })



                elif self.position == 0:
                    if order_type == "Limit":
                        orders.append({
                            "time-in-force": "Day",
                            "price": self.ask,
                            "price-effect": "Credit",
                            "order-type": "Limit",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Sell to Open"
                                }
                            ]
                        })
                    
                    else:
                        orders.append({
                            "time-in-force": "Day",
                            "order-type": "Market",
                            "legs": [
                                {
                                    "instrument-type": "Future",
                                    "symbol": ZC_TICKER,
                                    "quantity": 1,
                                    "action": "Sell to Open"
                                }
                            ]
                        })
            else:
                if self.position < 0:
                    orders.append({
                        "time-in-force": "Day",
                        "order-type": "Market",
                        "legs": [
                            {
                                "instrument-type": "Future",
                                "symbol": ZC_TICKER,
                                "quantity": 1,
                                "action": "Buy to Close"
                            }
                        ]
                    })
                
                elif self.position > 0:
                    orders.append({
                        "time-in-force": "Day",
                        "order-type": "Market",
                        "legs": [
                            {
                                "instrument-type": "Future",
                                "symbol": ZC_TICKER,
                                "quantity": 1,
                                "action": "Sell to Close"
                            }
                        ]
                    })


            self.trade(orders)

        return
        

    def predict(self, prev_returns):
        next_return = -0.028981497326160434
        ar_coef = [0.15494185346861913, 0.20442032212528188, 0.10899240667718338, 0.04594915455301834, -0.08022025458277479, -0.029929707039411384, -0.09731107859531843, -0.27025322094876075, -0.48674478620344525, -0.6472852846507117]

        for i in range(len(ar_coef)):
            next_return += ar_coef[i] * prev_returns[i] 

        return next_return
    
    def trade(self, orders):
        headers = {"Authorization": os.getenv("SESSION_TOKEN")}
        for order in orders:
            res = requests.post(f"{TASTY_API}/accounts/{ACCOUNT_NUMBER}/orders", headers=headers, json=order)
            print(res.json())
            print(order)
            time.sleep(1)

    def cancel_working_orders(self):
        headers = {"Authorization": os.getenv("SESSION_TOKEN")}
        res = requests.get(f"{TASTY_API}/accounts/{ACCOUNT_NUMBER}/orders/live", headers=headers)
        live_orders = res.json()["data"]["items"]
        for order in live_orders:
            if order["underlying-symbol"] != UNDERLYING_SYMBOL:
                continue

            status = order["status"]
            if status == "Received" or status == "Routed" or status == "In Flight" or status == "Live":
                res = requests.delete(f"{TASTY_API}/accounts/{ACCOUNT_NUMBER}/orders/{order['id']}", headers=headers) 
                print(res.json())

    def is_trading_hour(self):
        self.market_tz = pytz.timezone('America/Chicago')
        night_start = datetime.time(hour=19, minute=0)
        night_end = datetime.time(hour=7, minute=45)
        day_start = datetime.time(hour=8, minute=30)
        day_end = datetime.time(hour=13, minute=20)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_local = now_utc.astimezone(self.market_tz)
        today = now_local.weekday()

        is_weekday = today < 5  
        is_sunday = today == 6 

        # Check night session (Sunday 7:00 PM - Friday 7:45 AM)
        if (is_sunday or is_weekday) and (night_start <= now_local.time() or now_local.time() < night_end):
            return True

        # Check day session (Monday - Friday 8:30 AM - 1:20 PM)
        if is_weekday and day_start <= now_local.time() < day_end:
            return True

        return False
    
    def save_data(self):
        self.prices_df.to_csv(f"ZC_price_history{str(datetime.date.today())}.csv", index=False)
