import requests
import json

# Upbit API

server_url = "https://api.upbit.com"

params = {"markets": "KRW-BTC"}

res = requests.get(server_url + "/v1/ticker", params=params)
print(res.json())
