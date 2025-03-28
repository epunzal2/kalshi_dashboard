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
        
        import logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("=== Debug: Signature Components ===")
        self.logger.debug(f"Timestamp: {timestamp_str}")
        self.logger.debug(f"Method: {method}")
        self.logger.debug(f"Path: {path}")

        path_parts = path.split('?')
        msg_string = timestamp_str + method + path_parts[0]
        print(f"Message String: {msg_string}")
        
        signature = self.sign_pss_text(msg_string)
        print(f"Signature: {signature[:50]}...")  # Print first 50 chars of signature

        headers = {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        }
        
        print("\n=== Request Headers ===")
        for k, v in headers.items():
            print(f"{k}: {v[:100]}{'...' if len(v) > 100 else ''}")  # Truncate long values
        
        return headers

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
        self.base_url = self.HTTP_BASE_URL
        self.markets_url = "/trade-api/v2/markets"
        self.portfolio_url = "/trade-api/v2/portfolio"
        self.series_url = "/trade-api/v2/series"
        self.events_url = "/trade-api/v2/events"

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

    def get(self, path: str, params: Dict[str, Any] = {}, verbose=False) -> Any:
        self.rate_limit()
        print(f"\n=== Sending GET Request ===")
        print(f"Full URL: {self.host}{path}")
        response = requests.get(
            self.host + path,
            headers=self.request_headers("GET", path),
            params=params
        )
        self.raise_if_bad_response(response)
        if verbose:
            print(f"\n=== Raw Response ===")
            print(response.text)  # Add raw response logging
        try:
            return response.json()
        except json.JSONDecodeError as e:
            print(f"JSON Decode Failed. Status: {response.status_code}")
            raise ValueError(f"Invalid JSON response: {response.text[:200]}") from e

    def _get(self, path: str) -> dict:
        """Unified GET request handler with error handling"""
        try:
            response = self.get(path)
            return response
        except HTTPError as e:
            print(f"API error: {e}")
            return None
    # public methods
    def get_api_version(self) -> str:
        """
        Fetches the API version from the Kalshi API.

        Returns:
            str: The API version as a string.
        """
        return self._get(f"{self.host}/trade-api/v2/api_version")
    # portfolio methods
    def get_balance(self) -> Dict[str, Any]:
        balance = self.get(f"{self.portfolio_url}/balance")
        print(f"Raw balance response: {balance}")  # Add logging
        return balance
    # market methods
    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        params = {k: v for k, v in {"ticker": ticker, "limit": limit}.items() if v is not None}
        return self.get(f"{self.markets_url}/trades", params=params)

    # market methods
    def get_market(self, ticker: str) -> Dict[str, Any]:
        params = {'tickers': ticker}
        try:
            markets = self.get(self.markets_url, params=params).get('markets', [])
            return markets[0] if markets else None
        except HTTPError as e:
            raise ValueError(f"API error: {e}") from e

    def get_markets(
        self,
        event_ticker: Optional[str] = None,
        series_ticker: Optional[str] = None,
        max_close_ts: Optional[int] = None,
        min_close_ts: Optional[int] = None,
        status: Optional[str] = None,
        tickers: Optional[str] = None,
        limit: int = 100
    ) -> list[dict]:
        """Fetches markets with pagination and filtering.
        
        Args:
            event_ticker: Filter by event ticker
            series_ticker: Filter by series ticker
            max_close_ts: Maximum close timestamp (inclusive)
            min_close_ts: Minimum close timestamp (inclusive)
            status: Comma-separated statuses (unopened, open, closed, settled)
            tickers: Comma-separated market tickers
            limit: Number of results per page (1-1000, default self.rate_limit())
            
        Returns:
            List of market dictionaries
        """
        params = {
            'event_ticker': event_ticker,
            'series_ticker': series_ticker,
            'max_close_ts': max_close_ts,
            'min_close_ts': min_close_ts,
            'status': status,
            'tickers': tickers,
            'limit': min(max(limit, 1), 1000)  # Enforce API limits
        }
        params = {k: v for k, v in params.items() if v is not None}
        
        all_markets = []
        cursor = ''
        
        while True:
            if cursor:
                params['cursor'] = cursor
                
            response = self.get(self.markets_url, params=params)
            all_markets.extend(response.get('markets', []))
            cursor = response.get('cursor', '')
            
            if not cursor:
                break

        return all_markets

    def get_market_history(self, ticker: str, limit: int = 100) -> Dict[str, Any]:
        return self.get_trades(ticker=ticker, limit=limit)

    def get_series(self, series_ticker: str) -> dict:
        """Get series details matching starter's ExchangeClient.get_series()"""
        series_data = self._get(f"{self.series_url}/{series_ticker}")
        if series_data:
            return series_data
        else:
            return {'series': None}

    def get_event(self, event_ticker: str, with_nested_markets: bool = False) -> dict:
        """Get event details with market list like starter's get_event()"""
        params = {'event_ticker': event_ticker,
                  'with_nested_markets': with_nested_markets
        }
        # Call self.get directly as it handles params
        event_data = self.get(f"{self.events_url}/{event_ticker}", params=params)
        return event_data

    def get_series_markets(self, series_ticker: str) -> dict:
        """Get all markets associated with a series"""
        return self._get(f"{self.series_url}/{series_ticker}/markets")

    def get_market_orderbook(self, ticker: str, depth: int) -> dict:
        params = {
            'ticker': ticker,
            'depth': depth
        }
        return self._get(f"{self.markets_url}/orderbook", params=params)

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
        try:
            data = json.loads(message)
            if "ticker" in data:
                ticker = data["ticker"]
                yes_price = data["yes_price"]
                no_price = data["no_price"]
                
                # Update session state
                st.session_state.market_data[ticker] = {
                    "yes_price": yes_price,
                    "no_price": no_price
                }
                print(f"Updated market data for {ticker}: {yes_price}, {no_price}")
        except Exception as e:
            print(f"Error processing message: {e}")

    async def on_error(self, error):
        print("WebSocket error:", error)

    async def on_close(self, code, reason):
        print("WebSocket closed", code, reason)

def detect_ticker_type(ticker: str) -> str:
    if '-' not in ticker:
        if ticker.startswith('KX'):
            return 'series'
        return 'market'
    
    parts = ticker.split('-')
    
    # Handle event tickers (one hyphen)
    if len(parts) == 2 and ticker.startswith('KX'):
        return 'event'
    
    # Handle market tickers (two hyphens)
    if len(parts) == 3 and ticker.startswith('KX'):
        return 'market'
    
    return 'market'  # Default case
