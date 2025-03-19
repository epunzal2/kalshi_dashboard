import requests
import base64
import time
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum
import json
import os

from requests.exceptions import HTTPError

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature

import websockets

class Environment(Enum):
    DEMO = "demo"
    PROD = "prod"

class KalshiBaseClient:
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        self.key_id = key_id
        self.private_key = private_key
        self.environment = environment
        self.last_api_call = datetime.now()

        if self.environment == Environment.DEMO:
            self.HTTP_BASE_URL = os.environ.get("DEMO_HTTP_BASE_URL", "https://demo-api.kalshi.co")
            self.WS_BASE_URL = os.environ.get("DEMO_WS_BASE_URL", "wss://demo-api.kalshi.co")
        elif self.environment == Environment.PROD:
            self.HTTP_BASE_URL = os.environ.get("PROD_HTTP_BASE_URL", "https://api.elections.kalshi.com")
            self.WS_BASE_URL = os.environ.get("PROD_WS_BASE_URL", "wss://api.elections.kalshi.com")
        else:
            raise ValueError("Invalid environment")

    def request_headers(self, method: str, path: str) -> Dict[str, Any]:
        current_time_milliseconds = int(time.time() * 1000)
        timestamp_str = str(current_time_milliseconds)

        path_parts = path.split('?')
        msg_string = timestamp_str + method + path_parts[0]
        signature = self.sign_pss_text(msg_string)

        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        }

    def sign_pss_text(self, text: str) -> str:
        message = text.encode('utf-8')
        signature = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

class KalshiHttpClient(KalshiBaseClient):
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        super().__init__(key_id, private_key, environment)
        self.host = self.HTTP_BASE_URL
        self.markets_url = "/trade-api/v2/markets"
        self.portfolio_url = "/trade-api/v2/portfolio"

    def rate_limit(self) -> None:
        THRESHOLD_IN_MILLISECONDS = 100
        now = datetime.now()
        threshold_in_seconds = THRESHOLD_IN_MILLISECONDS / 1000
        if (now - self.last_api_call).total_seconds() * 1000 < THRESHOLD_IN_MILLISECONDS:
            time.sleep(threshold_in_seconds)
        self.last_api_call = datetime.now()

    def raise_if_bad_response(self, response: requests.Response) -> None:
        if not 200 <= response.status_code < 300:
            response.raise_for_status()

    def get(self, path: str, params: Dict[str, Any] = {}) -> Any:
        self.rate_limit()
        response = requests.get(
            self.host + path,
            headers=self.request_headers("GET", path),
            params=params
        )
        self.raise_if_bad_response(response)
        return response.json()

    def get_balance(self) -> Dict[str, Any]:
        balance = self.get(f"{self.portfolio_url}/balance")
        print(f"Raw balance response: {balance}")  # Add logging
        return balance

    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        params = {k: v for k, v in {"ticker": ticker, "limit": limit}.items() if v is not None}
        return self.get(f"{self.markets_url}/trades", params=params)

    def get_market(self, ticker: str) -> Dict[str, Any]:
        params = {'tickers': ticker}
        markets = self.get(self.markets_url, params=params).get('markets', [])
        return markets[0] if markets else None

    def get_market_history(self, ticker: str, limit: int = 100) -> Dict[str, Any]:
        return self.get_trades(ticker=ticker, limit=limit)

class KalshiWebSocketClient(KalshiBaseClient):
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        super().__init__(key_id, private_key, environment)
        self.url_suffix = "/trade-api/ws/v2"
        self.message_id = 1

    async def connect(self):
        host = f"{self.WS_BASE_URL}{self.url_suffix}"
        auth_headers = self.request_headers("GET", self.url_suffix)
        async with websockets.connect(host, extra_headers=auth_headers) as websocket:
            await self.on_open(websocket)
            await self.handler(websocket)

    async def on_open(self, ws):
        self.ws = ws
        await self.subscribe_to_tickers()

    async def subscribe_to_tickers(self):
        message = {
            "id": self.message_id,
            "cmd": "subscribe",
            "params": {"channels": ["ticker"]}
        }
        await self.ws.send(json.dumps(message))
        self.message_id += 1

    async def handler(self, ws):
        try:
            async for message in ws:
                await self.on_message(message)
        except websockets.ConnectionClosed as e:
            await self.on_close(e.code, e.reason)
        except Exception as e:
            await self.on_error(e)

    async def on_message(self, message):
        print("Received real-time data:", message)

    async def on_error(self, error):
        print("WebSocket error:", error)

    async def on_close(self, code, reason):
        print("WebSocket closed", code, reason)
