import pandas as pd
import numpy as np
from collections import deque
import datetime
import api
import requests
import os
import json
import pytz

DATA_URL = "wss://tasty-openapi-ws.dxfeed.com/realtime"
TASTY_API = "https://api.tastyworks.com"
ZC_TICKER = "/ZCN4"
ACCOUNT_NUMBER = os.getenv("ACCOUNT_NUMBER")

class ZCArima:
    def __init__(self):
        self.ticker = "/ZCN24:XCBT"
        self.price_window = deque([450.25, 450.0, 451.25, 452.75, 452.0, 452.75, 453.0, 452.5, 452.5, 452.25, 452.5, 452.25, 452.25, 451.5, 451.5, 451.0, 451.5, 451.75])
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

            new_row = [{
                "timestamp": datetime.datetime.now(datetime.timezone.utc), 
                "bid": bid,
                "ask": ask,
                "mid": mid
                }]
            
            self.prices_df = pd.concat([self.prices_df, pd.DataFrame(new_row)], ignore_index=True)

        
        else:
            return

        if datetime.datetime.now(datetime.timezone.utc) >= self.next_trade_hour:
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

            orders = []

            if next_return > 0:
                if self.position < 0:
                    orders.append({
                        "time-in-force": "Day",
                        "price": self.ask,
                        "price-effect": "Debit",
                        "order-type": "Limit",
                        "legs": [
                            {
                                "instrument-type": "Future",
                                "symbol": ZC_TICKER,
                                "quantity": self.position,
                                "action": "Buy to Close"
                            }
                        ]
                    })
                
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

                elif self.position == 0:
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

        
            elif next_return < 0:
                if self.position > 0:
                    orders.append({
                        "time-in-force": "Day",
                        "price": self.ask,
                        "price-effect": "Credit",
                        "order-type": "Limit",
                        "legs": [
                            {
                                "instrument-type": "Future",
                                "symbol": ZC_TICKER,
                                "quantity": self.position,
                                "action": "Sell to Close"
                            }
                        ]
                    })

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
                elif self.position == 0:
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

            self.trade(orders)

        return
        

    def predict(self, prev_returns):
        next_return = -0.027017222271711123
        ar_coef = [-0.10433543565158056, -0.006098515054023878, -0.10823116461351842, -0.13969271500432726, 0.016371819883228406, -0.007764408643629614, -0.042714924767569065, -0.03182387574302226, 0.04321009401969834, -0.021595934551837313, 0.04475933684340608, -0.09544543833122922, 0.036680877107984185, 0.0014720893197936267, -0.013495873774031721, 0.050600988995831056, -0.028509802057009412]

        for i in range(len(ar_coef)):
            next_return += ar_coef[i] * prev_returns[i] 

        return next_return
    
    def trade(self, orders):
        api.login()
        headers = {"Authorization": os.getenv("SESSION_TOKEN")}
        for order in orders:
            res = requests.post(f"{TASTY_API}/accounts/{ACCOUNT_NUMBER}/orders", headers=headers, json=order)
            print(res.json())
            print(order)

    def cancel_working_orders(self):
        api.login()
        headers = {"Authorization": os.getenv("SESSION_TOKEN")}
        res = requests.get(f"{TASTY_API}/accounts/{ACCOUNT_NUMBER}/orders/live", headers=headers)
        live_orders = res.json()["data"]["items"]
        for order in live_orders:
            if order["underlying-symbol"] != self.ticker:
                continue

            status = order["status"]
            if status == "Received" or status == "Routed" or status == "In Flight" or status == "Live":
                res = requests.put(f"{TASTY_API}/accounts/{ACCOUNT_NUMBER}/orders/{order['id']}") 
                print(res)

    def is_trading_hour(self):
        market_tz = pytz.timezone('America/Chicago')
        night_start = datetime.time(hour=19, minute=0)
        night_end = datetime.time(hour=7, minute=45)
        day_start = datetime.time(hour=8, minute=30)
        day_end = datetime.time(hour=13, minute=20)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_local = now_utc.astimezone(market_tz)
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
