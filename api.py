import os
import requests
import json

DATA_URL = "wss://tasty-openapi-ws.dxfeed.com/realtime"
TASTY_API = "https://api.tastyworks.com"
ACCOUNT_NUMBER = os.getenv("ACCOUNT_NUMBER")


def login():
    headers = {"Authorization": os.getenv("SESSION_TOKEN")}
    try:
        res = requests.get(f"{TASTY_API}/customers/me", headers=headers)
        res.raise_for_status()
        print("Successfully Logged In")
        
    except requests.HTTPError as e:
        login_params = {
            "login": os.getenv("USERNAME"),
            "password": os.getenv("PASSWORD"),
            "remember-me": True
        }
        res = requests.post(f"{TASTY_API}/sessions", json=login_params)
        os.environ["SESSION_TOKEN"] = res.json()["data"]["session-token"]
        print("Successfully Logged In")


def get_quote_token(attempts=0):
    headers = {"Authorization": os.getenv("SESSION_TOKEN")}
    try:
        res = requests.get(f"{TASTY_API}/api-quote-tokens", headers=headers)
        res.raise_for_status()
        os.environ["QUOTE_TOKEN"] = res.json()["data"]["token"]

    except requests.HTTPError as e:
        print(e)
        if attempts < 3:
            attempts += 1
            login()
            get_quote_token(attempts=attempts)


def get_buying_power(attempts=0):
    headers = {"Authorization": os.getenv("SESSION_TOKEN")}
    try:
        res = requests.get(f"{TASTY_API}/accounts/{ACCOUNT_NUMBER}/balances", headers=headers)
        res.raise_for_status()
        return float(res.json()["data"]["derivative-buying-power"])
    
    except requests.HTTPError as e:
        if attempts < 3:
            attempts += 1
            login()
            get_buying_power(attempts=attempts)


def get_positions(attempts=0):
    headers = {"Authorization": os.getenv("SESSION_TOKEN")}
    try:
        res = requests.get(TASTY_API + f"/accounts/{ACCOUNT_NUMBER}/positions", headers=headers)
        res.raise_for_status()
        items = res.json()["data"]["items"]
        positions = {}
        for item in items:
            symbol = item["symbol"]
            quantity = item["quantity"]
            direction = item["quantity-direction"]
            if direction == "Long":
                positions[symbol] = quantity
            else:
                positions[symbol] = -quantity

        return positions
    
    except requests.HTTPError as e:
        login()
        get_positions(attempts=attempts)

