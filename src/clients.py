import requests
import base64
import time
from typing import Any, Dict, Optional, List, Union, Callable, Coroutine
from datetime import datetime, timedelta
from enum import Enum
import json
import os
import asyncio
import logging
from collections import defaultdict

from requests.exceptions import HTTPError

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK

# Configure logging according to project standards
logger = logging.getLogger(__name__)
# Basic configuration example, adjust as needed based on project setup
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Environment(Enum):
    DEMO = "demo"
    PROD = "prod"

class KalshiBaseClient:
    """
    Base client for Kalshi API interactions, handling authentication and environment setup.

    Attributes:
        key_id (str): Kalshi API Key ID.
        private_key (rsa.RSAPrivateKey): RSA private key for signing requests.
        environment (Environment): The Kalshi environment (DEMO or PROD).
        last_api_call (datetime): Timestamp of the last HTTP API call for rate limiting.
        HTTP_BASE_URL (str): Base URL for HTTP API requests.
        WS_BASE_URL (str): Base URL for WebSocket connections.
        logger (logging.Logger): Logger instance for the client.
    """
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        """
        Initializes the KalshiBaseClient.

        Args:
            key_id: Kalshi API Key ID.
            private_key: RSA private key object.
            environment: Target environment (DEMO or PROD). Defaults to DEMO.

        Raises:
            ValueError: If an invalid environment is provided.
        """
        self.key_id = key_id
        self.private_key = private_key
        self.environment = environment
        self.last_api_call = datetime.now()
        self.logger = logging.getLogger(__name__) # Initialize logger here

        if self.environment == Environment.DEMO:
            self.HTTP_BASE_URL = os.environ.get("DEMO_HTTP_BASE_URL", "https://demo-api.kalshi.co")
            self.WS_BASE_URL = os.environ.get("DEMO_WS_BASE_URL", "wss://demo-api.kalshi.co")
        elif self.environment == Environment.PROD:
            # Updated PROD URL based on user input
            self.HTTP_BASE_URL = os.environ.get("PROD_HTTP_BASE_URL", "https://api.elections.kalshi.com")
            self.WS_BASE_URL = os.environ.get("PROD_WS_BASE_URL", "wss://api.elections.kalshi.com")
        else:
            raise ValueError("Invalid environment")

    def request_headers(self, method: str, path: str, body: Optional[str] = None) -> Dict[str, Any]:
        """
        Generates the required headers for authenticating Kalshi API requests.

        Args:
            method (str): The HTTP method (e.g., "GET", "POST", "DELETE").
            path (str): The request path (including query parameters if any).
            body (Optional[str]): The request body as a JSON string, required for POST/DELETE requests with body.

        Returns:
            Dict[str, Any]: A dictionary containing the necessary request headers.
        """
        current_time_milliseconds = int(time.time() * 1000)
        timestamp_str = str(current_time_milliseconds)

        self.logger.debug("=== Debug: Signature Components ===")
        self.logger.debug(f"Timestamp: {timestamp_str}")
        self.logger.debug(f"Method: {method}")
        self.logger.debug(f"Path: {path}")
        if body:
             self.logger.debug(f"Body (first 100 chars): {body[:100]}...")

        path_parts = path.split('?')
        msg_string = timestamp_str + method + path_parts[0]
        if body:
            msg_string += body # Append body for POST/DELETE with body

        self.logger.debug(f"Message String for Signing: {msg_string}")

        signature = self.sign_pss_text(msg_string)
        self.logger.debug(f"Signature (first 50 chars): {signature[:50]}...")

        headers = {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        }

        self.logger.debug("\n=== Request Headers ===")
        for k, v in headers.items():
            self.logger.debug(f"{k}: {v[:100]}{'...' if len(v) > 100 else ''}")

        return headers

    def sign_pss_text(self, text: str) -> str:
        """
        Signs the provided text using the RSA private key with PSS padding.

        Args:
            text (str): The string to sign.

        Returns:
            str: The base64 encoded signature.
        """
        message = text.encode('utf-8')
        signature = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

class KalshiHttpClient(KalshiBaseClient):
    """
    Client for interacting with the Kalshi HTTP REST API (v2).

    Provides methods for fetching market data, managing portfolio (orders, positions, balance),
    and accessing exchange information.

    Attributes:
        host (str): The base host URL for API requests.
        base_url (str): Alias for host.
        markets_url (str): Base path for market-related endpoints.
        portfolio_url (str): Base path for portfolio-related endpoints.
        series_url (str): Base path for series-related endpoints.
        events_url (str): Base path for event-related endpoints.
        exchange_url (str): Base path for exchange-related endpoints.
        milestones_url (str): Base path for milestone-related endpoints.
        structured_targets_url (str): Base path for structured target endpoints.
    """
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
    ):
        """
        Initializes the KalshiHttpClient.

        Args:
            key_id: Kalshi API Key ID.
            private_key: RSA private key object.
            environment: Target environment (DEMO or PROD). Defaults to DEMO.
        """
        super().__init__(key_id, private_key, environment)
        self.host = self.HTTP_BASE_URL
        self.base_url = self.HTTP_BASE_URL
        # Define base paths for different API sections
        self.markets_url = "/trade-api/v2/markets"
        self.portfolio_url = "/trade-api/v2/portfolio"
        self.series_url = "/trade-api/v2/series"
        self.events_url = "/trade-api/v2/events"
        self.exchange_url = "/trade-api/v2/exchange"
        self.milestones_url = "/trade-api/v2/milestones"
        self.structured_targets_url = "/trade-api/v2/structured_targets"

        # Note: Communications and Collection methods are not implemented yet.

        # Fetch initial exchange status on initialization
        self.logger.info("Fetching initial exchange status...")
        try:
            # Use the instance's own get method
            status_response = self.get_exchange_status()
            # Check for the actual keys in the response based on logs {'exchange_active': True, 'trading_active': True}
            if status_response and 'exchange_active' in status_response and 'trading_active' in status_response:
                # Log the actual status details
                self.logger.info(f"Initial Exchange Status: Active={status_response['exchange_active']}, Trading={status_response['trading_active']}")
                # Optionally store it if needed later: self.initial_exchange_status = status_response
            else:
                self.logger.warning(f"Could not retrieve valid initial exchange status structure. Response: {status_response}")
        except Exception as init_status_err:
            # Log error but allow client initialization to continue
            self.logger.error(f"Failed to fetch initial exchange status during client init: {init_status_err}", exc_info=False)


    def rate_limit(self) -> None:
        """
        Ensures requests do not exceed the API rate limit by introducing a small delay if necessary.
        """
        THRESHOLD_IN_MILLISECONDS = 100 # Adjust as needed based on Kalshi limits
        now = datetime.now()
        threshold_in_seconds = THRESHOLD_IN_MILLISECONDS / 1000.0
        time_since_last_call = (now - self.last_api_call).total_seconds()

        if time_since_last_call < threshold_in_seconds:
            sleep_duration = threshold_in_seconds - time_since_last_call
            self.logger.debug(f"Rate limiting: sleeping for {sleep_duration:.3f} seconds.")
            time.sleep(sleep_duration)
        self.last_api_call = datetime.now()

    def raise_if_bad_response(self, response: requests.Response) -> None:
        """
        Checks the HTTP response status code and raises an HTTPError for non-2xx responses.

        Args:
            response (requests.Response): The response object from the requests library.

        Raises:
            HTTPError: If the response status code is not in the 200-299 range.
        """
        if not 200 <= response.status_code < 300:
            self.logger.error(f"API Error: Status={response.status_code}, Response={response.text[:500]}")
            response.raise_for_status() # Raises HTTPError with details

    def _make_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None, verbose: bool = False) -> Any:
        """
        Internal helper method to make HTTP requests (GET, POST, DELETE).

        Handles rate limiting, header generation, request execution, error checking, and JSON parsing.

        Args:
            method (str): HTTP method ("GET", "POST", "DELETE").
            path (str): API endpoint path (e.g., "/trade-api/v2/markets").
            params (Optional[Dict[str, Any]]): URL query parameters. Defaults to None.
            data (Optional[Dict[str, Any]]): Request body data (for POST/DELETE). Defaults to None.
            verbose (bool): If True, logs the raw response text. Defaults to False.

        Returns:
            Any: The parsed JSON response.

        Raises:
            HTTPError: If the API returns a non-2xx status code.
            ValueError: If the response body is not valid JSON.
            requests.exceptions.RequestException: For network or request-related errors.
        """
        self.rate_limit()

        full_url = self.host + path
        body_json = None
        if data is not None:
            # Ensure data is dict before dumping, handle potential non-dict data if necessary
            if isinstance(data, dict):
                body_json = json.dumps(data) # Serialize body for signing and sending
            else:
                # Log warning or raise error if data is not a dict for POST/DELETE
                self.logger.warning(f"Request data for {method} {path} is not a dictionary: {type(data)}. Sending as is.")
                body_json = str(data) # Fallback? Or raise error? Let's try sending as string.

        # Construct the path string including query params for header generation
        query_string = ""
        if params:
            # Ensure params is a dictionary before iterating
            if isinstance(params, dict):
                 query_string = "?" + "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
            else:
                 self.logger.warning(f"Query parameters for {method} {path} is not a dictionary: {type(params)}. Ignoring params.")
                 params = None # Clear params if not a dict

        path_for_headers = path + query_string

        headers = self.request_headers(method, path_for_headers, body=body_json)

        self.logger.info(f"Sending {method} Request to {full_url}")
        if params: self.logger.debug(f"Query Params: {params}")
        if body_json: self.logger.debug(f"Request Body: {body_json}")

        try:
            response = requests.request(
                method=method,
                url=full_url,
                headers=headers,
                params=params, # requests library handles URL encoding for params
                data=body_json # Send serialized JSON string as data
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network or request error during {method} to {full_url}: {e}", exc_info=True)
            raise # Re-raise the original exception

        self.raise_if_bad_response(response) # Check for 4xx/5xx errors

        if verbose:
            self.logger.debug(f"Raw Response ({response.status_code}): {response.text}")

        # Handle potential empty responses (e.g., 204 No Content for DELETE)
        if response.status_code == 204 or not response.content:
             self.logger.debug(f"Received empty response body (Status: {response.status_code}).")
             # Return None for empty body cases.
             return None

        try:
            return response.json()
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON response. Status: {response.status_code}, Response: {response.text[:200]}...")
            raise ValueError(f"Invalid JSON response received from API: {response.text[:200]}") from e

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, verbose: bool = False) -> Any:
        """
        Performs a GET request to the specified API path.

        Args:
            path (str): API endpoint path.
            params (Optional[Dict[str, Any]]): URL query parameters. Defaults to None.
            verbose (bool): If True, logs the raw response text. Defaults to False.

        Returns:
            Any: The parsed JSON response.
        """
        # Filter out None values from params before passing
        filtered_params = {k: v for k, v in params.items() if v is not None} if params else None
        return self._make_request("GET", path, params=filtered_params, verbose=verbose)

    def post(self, path: str, data: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None, verbose: bool = False) -> Any:
        """
        Performs a POST request to the specified API path.

        Args:
            path (str): API endpoint path.
            data (Optional[Dict[str, Any]]): Request body data. Defaults to None.
            params (Optional[Dict[str, Any]]): URL query parameters. Defaults to None.
            verbose (bool): If True, logs the raw response text. Defaults to False.

        Returns:
            Any: The parsed JSON response.
        """
        filtered_params = {k: v for k, v in params.items() if v is not None} if params else None
        return self._make_request("POST", path, params=filtered_params, data=data, verbose=verbose)

    def delete(self, path: str, data: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None, verbose: bool = False) -> Any:
        """
        Performs a DELETE request to the specified API path.

        Args:
            path (str): API endpoint path.
            data (Optional[Dict[str, Any]]): Request body data (used by BatchCancelOrders). Defaults to None.
            params (Optional[Dict[str, Any]]): URL query parameters. Defaults to None.
            verbose (bool): If True, logs the raw response text. Defaults to False.

        Returns:
            Any: The parsed JSON response (often None or the cancelled object for Kalshi DELETE).
        """
        filtered_params = {k: v for k, v in params.items() if v is not None} if params else None
        return self._make_request("DELETE", path, params=filtered_params, data=data, verbose=verbose)

    # --- Public API Methods ---

    def get_api_version(self) -> Dict[str, Any]:
        """
        Fetches the API version from the Kalshi API.

        Returns:
            Dict[str, Any]: A dictionary containing the API version information.
                            Example: {'version': 'v2.X.Y'}
        """
        # Assuming the endpoint is /trade-api/v2/api_version based on original code
        # Adjust path if needed.
        path = "/trade-api/v2/api_version"
        return self.get(path)

    # --- Event Methods ---
    def get_events(
        self,
        limit: Optional[int] = 100,
        cursor: Optional[str] = None,
        status: Optional[str] = None,
        series_ticker: Optional[str] = None,
        with_nested_markets: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Fetches data about events, with optional filtering and pagination.

        Args:
            limit (Optional[int]): Number of results per page (1-200). Defaults to 100.
            cursor (Optional[str]): Pagination cursor from a previous request.
            status (Optional[str]): Comma-separated list of statuses (unopened, open, closed, settled).
            series_ticker (Optional[str]): Filter events by series ticker.
            with_nested_markets (Optional[bool]): Include market data nested within each event.

        Returns:
            Dict[str, Any]: The API response containing the list of events and pagination cursor.
                            Example: {'events': [...], 'cursor': '...'}
        """
        params = {
            "limit": limit,
            "cursor": cursor,
            "status": status,
            "series_ticker": series_ticker,
            "with_nested_markets": with_nested_markets
        }
        return self.get(self.events_url, params=params)

    def get_event(self, event_ticker: str, with_nested_markets: bool = False) -> Dict[str, Any]:
        """
        Fetches details for a specific event, optionally including its markets.

        Args:
            event_ticker (str): The unique ticker for the event (e.g., 'KXCPIYOY-25MAR').
            with_nested_markets (bool): If True, includes market data nested within the event. Defaults to False.

        Returns:
            Dict[str, Any]: The API response containing the event details.
                            Example: {'event': {...}}
        """
        # Path parameter based on typical REST patterns for fetching a single resource.
        path = f"{self.events_url}/{event_ticker}"
        params = {'with_nested_markets': with_nested_markets}
        return self.get(path, params=params)

    # --- Market Methods ---
    def get_market(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetches details for a specific market by its ticker.

        Args:
            ticker (str): The unique ticker for the market (e.g., 'KXCPIYOY-25MAR-T2.5').

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing market details, or None if not found.
                                      Example: {'market': {...}} or None

        Raises:
            HTTPError: If the API returns an error status code (other than 404).
            ValueError: If the response is not valid JSON or the structure is unexpected.
        """
        path = f"{self.markets_url}/{ticker}"
        try:
            response = self.get(path)
            # Assuming response structure is {'market': {...}}
            return response.get('market') if isinstance(response, dict) else None
        except HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Market with ticker '{ticker}' not found (404).")
                return None
            self.logger.error(f"API error fetching market '{ticker}': {e}", exc_info=True)
            raise

    def get_markets(
        self,
        event_ticker: Optional[str] = None,
        series_ticker: Optional[str] = None,
        max_close_ts: Optional[int] = None,
        min_close_ts: Optional[int] = None,
        status: Optional[str] = None,
        tickers: Optional[List[str]] = None,
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetches a list of markets with optional filtering and pagination.

        Args:
            event_ticker (Optional[str]): Filter by event ticker.
            series_ticker (Optional[str]): Filter by series ticker.
            max_close_ts (Optional[int]): Maximum close timestamp (Unix timestamp, inclusive).
            min_close_ts (Optional[int]): Minimum close timestamp (Unix timestamp, inclusive).
            status (Optional[str]): Comma-separated statuses (unopened, open, closed, settled).
            tickers (Optional[List[str]]): Filter by a list of specific market tickers.
            limit (int): Number of results per page (1-1000). Defaults to 100.
            cursor (Optional[str]): Pagination cursor from a previous request.

        Returns:
            Dict[str, Any]: The API response containing the list of markets and pagination cursor.
                            Example: {'markets': [...], 'cursor': '...'}
        """
        params = {
            'event_ticker': event_ticker,
            'series_ticker': series_ticker,
            'max_close_ts': max_close_ts,
            'min_close_ts': min_close_ts,
            'status': status,
            'tickers': ",".join(tickers) if tickers else None,
            'limit': min(max(limit, 1), 1000),
            'cursor': cursor
        }
        return self.get(self.markets_url, params=params)

    def get_market_orderbook(self, ticker: str, depth: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetches the order book for a specific market.

        Args:
            ticker (str): The unique ticker for the market.
            depth (Optional[int]): Maximum number of price levels per side. Defaults to full depth.

        Returns:
            Dict[str, Any]: The API response containing the order book data.
                            Example: {'orderbook': {'yes': [[price, size], ...], 'no': [[price, size], ...]}}
        """
        path = f"{self.markets_url}/{ticker}/orderbook"
        params = {'depth': depth}
        # Assuming response structure is {'orderbook': {...}}
        return self.get(path, params=params)

    def get_market_candlesticks(
        self,
        series_ticker: str,
        ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int
    ) -> Dict[str, Any]:
        """
        Fetches historical candlestick data for a market.

        Args:
            series_ticker (str): The series ticker the market belongs to.
            ticker (str): The market ticker.
            start_ts (int): Start timestamp (Unix timestamp, inclusive). Candlesticks ending on or after this time.
            end_ts (int): End timestamp (Unix timestamp, inclusive). Candlesticks ending on or before this time.
                          Must be within 5000 * period_interval minutes after start_ts.
            period_interval (int): Candlestick period in minutes (e.g., 1, 60, 1440).

        Returns:
            Dict[str, Any]: The API response containing the list of candlesticks.
                            Example: {'candlesticks': [{'ts': ..., 'open': ..., 'high': ..., 'low': ..., 'close': ..., 'volume': ...}, ...]}
        """
        path = f"{self.series_url}/{series_ticker}/markets/{ticker}/candlesticks"
        params = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval
        }
        return self.get(path, params=params)

    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: Optional[int] = 100, # Default based on original code
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetches recent trades, optionally filtered by market.

        Args:
            ticker (Optional[str]): Filter trades by market ticker.
            limit (Optional[int]): Maximum number of trades to return. Defaults to 100.
            cursor (Optional[str]): Pagination cursor for fetching older trades.

        Returns:
            Dict[str, Any]: The API response containing the list of trades and pagination cursor.
                            Example: {'trades': [...], 'cursor': '...'}
        """
        path = f"{self.markets_url}/trades"
        params = {
            "ticker": ticker,
            "limit": limit,
            "cursor": cursor
        }
        return self.get(path, params=params)

    def get_market_history(self, ticker: str, limit: int = 100) -> Dict[str, Any]:
        """
        Alias for get_trades, fetching recent trade history for a market.

        Args:
            ticker (str): The market ticker.
            limit (int): Maximum number of trades to return. Defaults to 100.

        Returns:
            Dict[str, Any]: The API response containing the list of trades.
        """
        return self.get_trades(ticker=ticker, limit=limit)

    # --- Series Methods ---
    def get_series(self, series_ticker: str) -> Dict[str, Any]:
        """
        Fetches details for a specific series by its ticker.

        Args:
            series_ticker (str): The unique ticker for the series (e.g., 'KXCPIYOY').

        Returns:
            Dict[str, Any]: The API response containing the series details.
                            Example: {'series': {...}}
        """
        path = f"{self.series_url}/{series_ticker}"
        # Assuming response structure is {'series': {...}}
        return self.get(path)

    def get_series_markets(self, series_ticker: str) -> Dict[str, Any]:
        """
        Fetches all markets associated with a specific series.

        Args:
            series_ticker (str): The unique ticker for the series.

        Returns:
            Dict[str, Any]: The API response containing the list of markets for the series.
                            Example: {'markets': [...]}
        """
        path = f"{self.series_url}/{series_ticker}/markets"
        return self.get(path)

    # --- Exchange Methods ---
    def get_exchange_status(self) -> Dict[str, Any]:
        """
        Fetches the current status of the Kalshi exchange.

        Returns:
            Dict[str, Any]: The API response containing exchange status details.
                            Example: {'exchange_status': {'trading_active': bool, ...}}
        """
        path = f"{self.exchange_url}/status"
        return self.get(path)

    def get_exchange_schedule(self) -> Dict[str, Any]:
        """
        Fetches the trading schedule for the Kalshi exchange.

        Returns:
            Dict[str, Any]: The API response containing the exchange schedule.
                            Example: {'schedule': {'open_time': ..., 'close_time': ..., 'holidays': [...]}}
        """
        path = f"{self.exchange_url}/schedule"
        return self.get(path)

    def get_exchange_announcements(self, limit: Optional[int] = None, cursor: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches exchange-wide announcements, with optional pagination.

        Args:
            limit (Optional[int]): Number of announcements per page.
            cursor (Optional[str]): Pagination cursor.

        Returns:
            Dict[str, Any]: The API response containing a list of announcements and cursor.
                            Example: {'announcements': [...], 'cursor': '...'}
        """
        path = f"{self.exchange_url}/announcements"
        params = {"limit": limit, "cursor": cursor}
        return self.get(path, params=params)

    # --- Milestone Methods ---
    def get_milestones(
        self,
        minimum_start_date: Optional[str] = None,
        category: Optional[str] = None,
        type: Optional[str] = None,
        related_event_ticker: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetches data about milestones with optional filtering and pagination.

        Args:
            minimum_start_date (Optional[str]): Filter milestones starting on or after this date (ISO 8601 format, e.g., "YYYY-MM-DDTHH:MM:SSZ").
            category (Optional[str]): Filter by category.
            type (Optional[str]): Filter by type.
            related_event_ticker (Optional[str]): Filter by related event ticker.
            limit (int): Number of items per page (1-500). Defaults to 100.
            cursor (Optional[str]): Pagination cursor from a previous request.

        Returns:
            Dict[str, Any]: The API response containing the list of milestones and pagination cursor.
                            Example: {'milestones': [...], 'cursor': '...'}
        """
        params = {
            "minimum_start_date": minimum_start_date,
            "category": category,
            "type": type,
            "related_event_ticker": related_event_ticker,
            "limit": min(max(limit, 1), 500),
            "cursor": cursor
        }
        return self.get(self.milestones_url, params=params)

    def get_milestone(self, milestone_id: str) -> Dict[str, Any]:
        """
        Fetches data about a specific milestone by its ID.

        Args:
            milestone_id (str): The unique ID of the milestone.

        Returns:
            Dict[str, Any]: The API response containing the milestone details.
                            Example: {'milestone': {...}}
        """
        path = f"{self.milestones_url}/{milestone_id}"
        # Assuming response structure is {'milestone': {...}}
        return self.get(path)

    # --- Portfolio Methods ---
    def get_balance(self) -> Dict[str, Any]:
        """
        Fetches the current account balance.

        Returns:
            Dict[str, Any]: A dictionary containing balance details.
                            Example: {'balance': {'user_id': ..., 'available_balance': ..., 'total_balance': ...}}
        """
        path = f"{self.portfolio_url}/balance"
        balance = self.get(path)
        self.logger.debug(f"Raw balance response: {balance}")
        # Assuming response structure is {'balance': {...}}
        return balance

    def get_fills(
        self,
        ticker: Optional[str] = None,
        order_id: Optional[str] = None,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetches the user's trade fills with optional filtering and pagination.

        Args:
            ticker (Optional[str]): Filter fills by market ticker.
            order_id (Optional[str]): Filter fills related to a specific order ID.
            min_ts (Optional[int]): Filter fills executed at or after this Unix timestamp.
            max_ts (Optional[int]): Filter fills executed at or before this Unix timestamp.
            limit (int): Number of results per page (1-1000). Defaults to 100.
            cursor (Optional[str]): Pagination cursor from a previous request.

        Returns:
            Dict[str, Any]: The API response containing the list of fills and pagination cursor.
                            Example: {'fills': [...], 'cursor': '...'}
        """
        path = f"{self.portfolio_url}/fills"
        params = {
            "ticker": ticker,
            "order_id": order_id,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "limit": min(max(limit, 1), 1000),
            "cursor": cursor
        }
        return self.get(path, params=params)

    def get_orders(
        self,
        ticker: Optional[str] = None,
        event_ticker: Optional[str] = None,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        status: Optional[str] = None, # resting, canceled, executed
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetches the user's orders with optional filtering and pagination.

        Args:
            ticker (Optional[str]): Filter orders by market ticker.
            event_ticker (Optional[str]): Filter orders by event ticker.
            min_ts (Optional[int]): Filter orders created at or after this Unix timestamp.
            max_ts (Optional[int]): Filter orders created at or before this Unix timestamp.
            status (Optional[str]): Filter orders by status (resting, canceled, executed).
            limit (int): Number of results per page (1-1000). Defaults to 100.
            cursor (Optional[str]): Pagination cursor from a previous request.

        Returns:
            Dict[str, Any]: The API response containing the list of orders and pagination cursor.
                            Example: {'orders': [...], 'cursor': '...'}
        """
        path = f"{self.portfolio_url}/orders"
        params = {
            "ticker": ticker,
            "event_ticker": event_ticker,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "status": status,
            "limit": min(max(limit, 1), 1000),
            "cursor": cursor
        }
        return self.get(path, params=params)

    def get_order(self, order_id: str) -> Dict[str, Any]:
        """
        Fetches details for a specific order by its ID.

        Args:
            order_id (str): The unique ID of the order.

        Returns:
            Dict[str, Any]: The API response containing the order details.
                            Example: {'order': {...}}
        """
        path = f"{self.portfolio_url}/orders/{order_id}"
        # Assuming response structure is {'order': {...}}
        return self.get(path)

    def create_order(
        self,
        ticker: str,
        action: str, # buy, sell
        side: str,   # yes, no
        count: int,
        type: str,   # limit, market
        client_order_id: str,
        yes_price: Optional[int] = None,
        no_price: Optional[int] = None,
        buy_max_cost: Optional[int] = None,
        sell_position_floor: Optional[int] = None,
        expiration_ts: Optional[int] = None,
        post_only: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Submits a new order to the exchange.

        Args:
            ticker (str): Market ticker.
            action (str): 'buy' or 'sell'.
            side (str): 'yes' or 'no'.
            count (int): Number of contracts.
            type (str): 'limit' or 'market'.
            client_order_id (str): A unique client-generated ID for the order.
            yes_price (Optional[int]): Limit price in cents for the 'yes' side. Required for limit orders.
            no_price (Optional[int]): Limit price in cents for the 'no' side. Required for limit orders.
                                      Exactly one of yes_price or no_price must be provided for limit orders.
            buy_max_cost (Optional[int]): Max cost in cents for market buy orders.
            sell_position_floor (Optional[int]): Floor for market sell orders (e.g., 0 to prevent flipping position).
            expiration_ts (Optional[int]): Order expiration time (Unix timestamp). None for GTC, past for IOC.
            post_only (Optional[bool]): If True, reject order if it would execute immediately.

        Returns:
            Dict[str, Any]: The API response containing the newly created order details.
                            Example: {'order': {...}}

        Raises:
            ValueError: If price conditions for limit orders are not met.
        """
        path = f"{self.portfolio_url}/orders"
        if type == 'limit' and not (yes_price is not None) ^ (no_price is not None):
             raise ValueError("For limit orders, exactly one of 'yes_price' or 'no_price' must be provided.")

        data = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": count,
            "type": type,
            "client_order_id": client_order_id,
            "yes_price": yes_price,
            "no_price": no_price,
            "buy_max_cost": buy_max_cost,
            "sell_position_floor": sell_position_floor,
            "expiration_ts": expiration_ts,
            "post_only": post_only
        }
        # Filter out None values from the data payload
        payload = {k: v for k, v in data.items() if v is not None}
        return self.post(path, data=payload)

    def batch_create_orders(self, orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Submits a batch of orders (up to 20). Advanced access required.

        Each order dictionary in the list should conform to the parameters of `create_order`.

        Args:
            orders (List[Dict[str, Any]]): A list of order creation dictionaries.

        Returns:
            Dict[str, Any]: The API response, likely containing results for each order in the batch.
                            Example: {'results': [{'order': {...}, 'status': 'accepted/rejected', 'reason': '...'}]}
        """
        path = f"{self.portfolio_url}/orders/batched"
        if not orders:
             raise ValueError("Orders list cannot be empty for batch creation.")
        if len(orders) > 20:
             self.logger.warning(f"Batch size ({len(orders)}) exceeds the typical limit of 20.")

        data = {"orders": orders}
        return self.post(path, data=data)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancels (reduces to zero) a specific resting order.

        Args:
            order_id (str): The unique ID of the order to cancel.

        Returns:
            Dict[str, Any]: The API response containing the state of the order after cancellation.
                            Example: {'order': {...}} (with remaining count likely zeroed)
        """
        path = f"{self.portfolio_url}/orders/{order_id}"
        return self.delete(path)

    def batch_cancel_orders(self, order_ids: List[str]) -> Dict[str, Any]:
        """
        Cancels a batch of orders (up to 20) by their IDs. Advanced access required.

        Args:
            order_ids (List[str]): A list of order IDs to cancel.

        Returns:
            Dict[str, Any]: The API response, likely containing results for each cancellation attempt.
                            Example: {'results': [{'order_id': ..., 'status': 'cancelled/not_found/error', 'reason': '...'}]}
        """
        path = f"{self.portfolio_url}/orders/batched"
        if not order_ids:
             raise ValueError("order_ids list cannot be empty for batch cancellation.")
        if len(order_ids) > 20:
             self.logger.warning(f"Batch cancel size ({len(order_ids)}) exceeds the typical limit of 20.")

        data = {"ids": order_ids}
        return self.delete(path, data=data)

    def amend_order(
        self,
        order_id: str,
        count: int,
        client_order_id: str,
        updated_client_order_id: str,
        # Required original fields for validation by API:
        action: str,
        side: str,
        ticker: str,
        # Optional price amendment fields:
        yes_price: Optional[int] = None,
        no_price: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Amends the price and/or maximum fillable count of an existing order.

        NOTE: Requires providing original action, side, ticker, and client_order_id
              along with a new updated_client_order_id for validation.

        Args:
            order_id (str): The ID of the order to amend.
            count (int): The new maximum number of contracts to be filled.
            client_order_id (str): The *original* client_order_id of the order being amended.
            updated_client_order_id (str): A *new*, unique client ID for this amended state.
            action (str): Original order action ('buy'/'sell') for validation.
            side (str): Original order side ('yes'/'no') for validation.
            ticker (str): Original order ticker for validation.
            yes_price (Optional[int]): The new limit price in cents for the 'yes' side.
            no_price (Optional[int]): The new limit price in cents for the 'no' side.
                                      Exactly one of yes_price or no_price must be provided.

        Returns:
            Dict[str, Any]: The API response containing the amended order details.
                            Example: {'order': {...}}

        Raises:
            ValueError: If price conditions are not met.
        """
        path = f"{self.portfolio_url}/orders/{order_id}/amend"
        if not (yes_price is not None) ^ (no_price is not None):
             raise ValueError("Exactly one of 'yes_price' or 'no_price' must be provided for amending.")

        data = {
            "count": count,
            "client_order_id": client_order_id, # Original ID
            "updated_client_order_id": updated_client_order_id, # New ID
            "yes_price": yes_price,
            "no_price": no_price,
            # Include original fields for validation as required by API docs
            "action": action,
            "side": side,
            "ticker": ticker,
        }
        payload = {k: v for k, v in data.items() if v is not None}
        return self.post(path, data=payload)

    def decrease_order(
        self,
        order_id: str,
        reduce_by: Optional[int] = None,
        reduce_to: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Decreases the number of remaining contracts in an existing order.

        Args:
            order_id (str): The ID of the order to decrease.
            reduce_by (Optional[int]): Decrease the remaining count by this amount.
            reduce_to (Optional[int]): Decrease the remaining count to this amount.
                                      Exactly one of reduce_by or reduce_to must be provided.

        Returns:
            Dict[str, Any]: The API response containing the modified order details.
                            Example: {'order': {...}} (with updated count)

        Raises:
            ValueError: If neither or both of reduce_by/reduce_to are provided.
        """
        path = f"{self.portfolio_url}/orders/{order_id}/decrease"
        if not (reduce_by is not None) ^ (reduce_to is not None):
             raise ValueError("Exactly one of 'reduce_by' or 'reduce_to' must be provided.")

        data = {
            "reduce_by": reduce_by,
            "reduce_to": reduce_to
        }
        payload = {k: v for k, v in data.items() if v is not None}
        return self.post(path, data=payload)

    def get_positions(
        self,
        cursor: Optional[str] = None,
        limit: int = 100,
        count_filter: Optional[str] = None,
        settlement_status: Optional[str] = None,
        ticker: Optional[str] = None,
        event_ticker: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetches the user's market positions with optional filtering and pagination.

        Args:
            cursor (Optional[str]): Pagination cursor from a previous request.
            limit (int): Number of results per page (1-1000). Defaults to 100.
            count_filter (Optional[str]): Comma-separated list to filter positions with non-zero
                                          values in 'position', 'total_traded', or 'resting_order_count'.
            settlement_status (Optional[str]): Filter by settlement status ('all', 'settled', 'unsettled'). Defaults to 'unsettled'.
            ticker (Optional[str]): Filter positions by market ticker.
            event_ticker (Optional[str]): Filter positions by event ticker.

        Returns:
            Dict[str, Any]: The API response containing the list of positions and pagination cursor.
                            Example: {'positions': [...], 'cursor': '...'}
        """
        path = f"{self.portfolio_url}/positions"
        params = {
            "cursor": cursor,
            "limit": min(max(limit, 1), 1000),
            "count_filter": count_filter,
            "settlement_status": settlement_status,
            "ticker": ticker,
            "event_ticker": event_ticker
        }
        return self.get(path, params=params)

    def get_portfolio_settlements(
        self,
        limit: int = 100,
        min_ts: Optional[int] = None,
        max_ts: Optional[int] = None,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetches the user's settlement history with optional filtering and pagination.

        Args:
            limit (int): Number of results per page (1-1000). Defaults to 100.
            min_ts (Optional[int]): Filter settlements at or after this Unix timestamp.
            max_ts (Optional[int]): Filter settlements at or before this Unix timestamp.
            cursor (Optional[str]): Pagination cursor from a previous request.

        Returns:
            Dict[str, Any]: The API response containing the list of settlements and pagination cursor.
                            Example: {'settlements': [...], 'cursor': '...'}
        """
        path = f"{self.portfolio_url}/settlements"
        params = {
            "limit": min(max(limit, 1), 1000),
            "min_ts": min_ts,
            "max_ts": max_ts,
            "cursor": cursor
        }
        return self.get(path, params=params)

    # --- Structured Target Methods ---
    def get_structured_target(self, structured_target_id: str) -> Dict[str, Any]:
        """
        Fetches data about a specific structured target by its ID.

        Args:
            structured_target_id (str): The unique ID of the structured target.

        Returns:
            Dict[str, Any]: The API response containing the structured target details.
                            Example: {'structured_target': {...}}
        """
        path = f"{self.structured_targets_url}/{structured_target_id}"
        # Assuming response structure is {'structured_target': {...}}
        return self.get(path)

    def get_bid_ask_spread(self, ticker: str) -> Optional[int]:
        """
        Calculates the current bid-ask spread for the 'yes' side of a market.

        The spread is the difference between the lowest ask price and the highest bid price.
        Note: In the Kalshi API response, 'yes' bids are under 'yes', but 'yes' asks are under 'no'.

        Args:
            ticker (str): The unique ticker for the market.

        Returns:
            Optional[int]: The bid-ask spread in cents, or None if the spread cannot be calculated
                           (e.g., empty order book, missing bids or asks).

        Raises:
            HTTPError: If the API call to get the order book fails (other than 404).
            ValueError: If the order book response is invalid.
        """
        self.logger.info(f"Fetching order book to calculate spread for ticker: {ticker}")
        try:
            # Fetch the full order book first
            orderbook_response = self.get_market_orderbook(ticker=ticker)
            # Example structure: {'orderbook': {'yes': [[90, 10], [88, 5]], 'no': [[92, 8], [95, 12]]}}
            # 'yes' list contains bids for 'yes' contracts (sorted high to low)
            # 'no' list contains asks for 'yes' contracts (sorted low to high)

        except HTTPError as e:
            # Log specific error for spread calculation context
            self.logger.error(f"Failed to get order book for spread calculation (ticker: {ticker}): {e}", exc_info=True)
            # Re-raise the original error to signal the API failure
            raise
        except Exception as e:
             self.logger.error(f"Unexpected error getting order book for spread (ticker: {ticker}): {e}", exc_info=True)
             raise # Re-raise unexpected errors

        if not orderbook_response or 'orderbook' not in orderbook_response:
            self.logger.warning(f"Invalid or empty order book data received for ticker: {ticker}")
            return None

        orderbook = orderbook_response['orderbook']
        yes_bids = orderbook.get('yes', [])
        yes_asks = orderbook.get('no', []) # Asks for 'yes' contracts are on the 'no' side

        # Ensure lists are not empty and contain valid price/size pairs
        if not yes_bids or not isinstance(yes_bids[0], list) or len(yes_bids[0]) < 1:
            highest_bid = None
            self.logger.debug(f"No valid bids found for 'yes' side of {ticker}.")
        else:
            # Assuming bids are sorted [[price, size], ...] descending by price
            highest_bid = yes_bids[0][0] # Price is the first element

        if not yes_asks or not isinstance(yes_asks[0], list) or len(yes_asks[0]) < 1:
            lowest_ask = None
            self.logger.debug(f"No valid asks found for 'yes' side of {ticker} (checked 'no' side of orderbook).")
        else:
            # Assuming asks are sorted [[price, size], ...] ascending by price
            lowest_ask = yes_asks[0][0] # Price is the first element

        if highest_bid is not None and lowest_ask is not None:
            # Ensure prices are integers before subtraction
            try:
                highest_bid_int = int(highest_bid)
                lowest_ask_int = int(lowest_ask)
                spread = lowest_ask_int - highest_bid_int
                self.logger.info(f"Calculated spread for {ticker}: Ask={lowest_ask_int}, Bid={highest_bid_int}, Spread={spread}")
                # Ensure spread is non-negative, although theoretically lowest ask >= highest bid
                return max(0, spread)
            except (ValueError, TypeError) as e:
                 self.logger.error(f"Error converting bid/ask to int for spread calculation (ticker: {ticker}): Bid={highest_bid}, Ask={lowest_ask}. Error: {e}")
                 return None
        else:
            self.logger.warning(f"Could not calculate spread for {ticker}: Missing bids ({highest_bid is None}) or asks ({lowest_ask is None}). Bids: {yes_bids}, Asks: {yes_asks}")
            return None


# --- WebSocket Client (Keep existing code below) ---
class KalshiWebSocketClient(KalshiBaseClient):
    """
    Client for interacting with the Kalshi WebSocket API (v2).

    Handles connection, authentication, subscriptions, and message processing
    for real-time market data feeds.

    Attributes:
        key_id (str): Kalshi API Key ID.
        private_key (rsa.RSAPrivateKey): RSA private key for signing requests.
        environment (Environment): The Kalshi environment (DEMO or PROD).
        on_message_callback (Callable[[Dict[str, Any]], Coroutine]): Async callback for all messages.
        on_error_callback (Callable[[Exception], Coroutine]): Async callback for errors.
        on_close_callback (Callable[[int, str], Coroutine]): Async callback for connection close.
        on_open_callback (Callable[[], Coroutine]): Async callback for successful connection.
    """
    def __init__(
        self,
        key_id: str,
        private_key: rsa.RSAPrivateKey,
        environment: Environment = Environment.DEMO,
        on_message_callback: Optional[Callable[[Dict[str, Any]], Coroutine]] = None,
        on_error_callback: Optional[Callable[[Exception], Coroutine]] = None,
        on_close_callback: Optional[Callable[[int, str], Coroutine]] = None,
        on_open_callback: Optional[Callable[[], Coroutine]] = None,
        auto_reconnect: bool = True,
        reconnect_delay: int = 5, # seconds
        ping_interval: Optional[int] = 10, # seconds, None to disable client pings
        ping_timeout: Optional[int] = 10, # seconds, None to disable server pong timeout
    ):
        """
        Initializes the KalshiWebSocketClient.

        Args:
            key_id: Kalshi API Key ID.
            private_key: RSA private key object.
            environment: Target environment (DEMO or PROD).
            on_message_callback: Async function called with each received message data.
            on_error_callback: Async function called on WebSocket errors.
            on_close_callback: Async function called when the connection closes.
            on_open_callback: Async function called when the connection opens successfully.
            auto_reconnect: Whether to automatically attempt reconnection on closure.
            reconnect_delay: Delay in seconds before attempting reconnection.
            ping_interval: Interval in seconds to send PING frames. Kalshi requires ~10s.
            ping_timeout: Timeout in seconds waiting for PONG response. Kalshi requires ~10s.
        """
        super().__init__(key_id, private_key, environment)
        self.url_suffix = "/trade-api/ws/v2"
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._message_id_counter = 1
        self._subscriptions: Dict[int, Dict[str, Any]] = {} # {sid: {'channels': [], 'markets': [], 'cmd_id': int}}
        self._pending_commands: Dict[int, Dict[str, Any]] = {} # {cmd_id: command_details}
        self._orderbooks: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'yes': [], 'no': [], 'last_seq': 0}) # {market_ticker: {'yes': [[price, size]], 'no': [...], 'last_seq': int}}
        self._message_handlers: Dict[str, Callable[[Dict[str, Any]], Coroutine]] = self._register_handlers()
        self._callback_registry: Dict[str, List[Callable[[Dict[str, Any]], Coroutine]]] = defaultdict(list)

        # User-defined callbacks
        self.on_message_callback = on_message_callback
        self.on_error_callback = on_error_callback
        self.on_close_callback = on_close_callback
        self.on_open_callback = on_open_callback

        # Connection management
        self._is_connected = False
        self._connect_lock = asyncio.Lock()
        self._disconnect_requested = False
        self._auto_reconnect = auto_reconnect
        self._reconnect_delay = reconnect_delay
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._connection_task: Optional[asyncio.Task] = None

        logger.info(f"KalshiWebSocketClient initialized for {environment.name} environment.")

    def _register_handlers(self) -> Dict[str, Callable[[Dict[str, Any]], Coroutine]]:
        """Registers internal handlers for different message types."""
        return {
            "subscribed": self._handle_subscribed,
            "unsubscribed": self._handle_unsubscribed,
            "ok": self._handle_ok,
            "error": self._handle_error,
            "ticker": self._handle_ticker,
            "trade": self._handle_trade,
            "fill": self._handle_fill,
            "orderbook_snapshot": self._handle_orderbook_snapshot,
            "orderbook_delta": self._handle_orderbook_delta,
            "market_lifecycle": self._handle_market_lifecycle,
            "event_lifecycle": self._handle_event_lifecycle,
            "multivariate_lookup": self._handle_multivariate_lookup,
            # Add other message types as needed
        }

    async def connect(self):
        """Establishes and maintains the WebSocket connection."""
        if self._connection_task and not self._connection_task.done():
             logger.warning("Connection attempt already in progress.")
             return

        self._disconnect_requested = False
        # Ensure auto_reconnect is enabled if it was previously disabled by disconnect()
        # This assumes connect() implies a desire for the connection to persist.
        self._auto_reconnect = True
        self._connection_task = asyncio.create_task(self._connection_loop())
        logger.info("Connection task created.")
        # Optionally wait for the initial connection attempt here if needed by caller
        # await asyncio.wait_for(self._connection_task, timeout=10) # Example wait

    async def _connection_loop(self):
        """The main loop managing connection, reconnection, and message handling."""
        while not self._disconnect_requested:
            connection_attempt_successful = False
            async with self._connect_lock:
                if self._is_connected: # Should not happen if lock works, but safety first
                    logger.warning("Already connected, skipping connection attempt in loop.")
                    await asyncio.sleep(1) # Prevent tight loop if state is inconsistent
                    continue

                host = f"{self.WS_BASE_URL}{self.url_suffix}"
                # Use the base class method for headers, ensuring body is None for WS GET
                auth_headers = self.request_headers("GET", self.url_suffix, body=None)
                logger.info(f"Attempting to connect to WebSocket: {host}")
                websocket = None # Define websocket in the outer scope for finally block
                try:
                    # Connect with specified ping interval and timeout
                    websocket = await websockets.connect(
                        host,
                        extra_headers=auth_headers,
                        ping_interval=self._ping_interval,
                        ping_timeout=self._ping_timeout
                    )
                    self._websocket = websocket
                    self._is_connected = True
                    connection_attempt_successful = True # Mark success for finally block
                    logger.info("WebSocket connection established successfully.")

                    # Trigger on_open callback
                    if self.on_open_callback:
                        try:
                            await self.on_open_callback()
                        except Exception as cb_err:
                             logger.error(f"Error in on_open_callback: {cb_err}", exc_info=True)


                    # Resubscribe if needed (implement _resubscribe logic)
                    await self._resubscribe()

                    # Start handling messages
                    await self._handler(websocket)

                except (websockets.exceptions.InvalidStatusCode, websockets.exceptions.WebSocketException) as e:
                    logger.error(f"WebSocket connection failed: {e}")
                    if self.on_error_callback:
                         try:
                              await self.on_error_callback(e)
                         except Exception as cb_err:
                              logger.error(f"Error in on_error_callback for connection failure: {cb_err}", exc_info=True)
                except asyncio.TimeoutError:
                     logger.error("WebSocket connection attempt timed out.")
                     if self.on_error_callback:
                          try:
                               await self.on_error_callback(TimeoutError("Connection timed out"))
                          except Exception as cb_err:
                               logger.error(f"Error in on_error_callback for timeout: {cb_err}", exc_info=True)
                except Exception as e:
                    logger.error(f"An unexpected error occurred during connection or handling: {e}", exc_info=True)
                    if self.on_error_callback:
                         try:
                              await self.on_error_callback(e)
                         except Exception as cb_err:
                              logger.error(f"Error in on_error_callback for unexpected error: {cb_err}", exc_info=True)
                finally:
                    # Cleanup connection state regardless of outcome
                    was_connected = self._is_connected
                    self._is_connected = False
                    self._websocket = None # Clear websocket reference

                    # Only log closure/failure if it wasn't explicitly requested
                    if not self._disconnect_requested:
                         if connection_attempt_successful and was_connected:
                              # This means _handler exited, likely due to ConnectionClosed
                              logger.info("WebSocket connection closed.")
                         elif not connection_attempt_successful:
                              logger.info("WebSocket connection attempt failed.")
                         # Trigger on_close only if it was previously connected and not explicitly disconnected
                         if was_connected and self.on_close_callback:
                              # Provide default close code/reason if not available from exception
                              # Exception 'e' might not be defined if _handler exited cleanly but unexpectedly
                              close_code = 1006 # Default to Abnormal Closure
                              close_reason = "Connection closed unexpectedly"
                              if 'e' in locals() and isinstance(e, ConnectionClosed):
                                   close_code = e.code
                                   close_reason = e.reason
                              elif 'e' in locals():
                                   close_reason = str(e)

                              try:
                                   await self.on_close_callback(close_code, close_reason)
                              except Exception as cb_err:
                                   logger.error(f"Error in on_close_callback: {cb_err}", exc_info=True)


            # Reconnection logic
            if self._auto_reconnect and not self._disconnect_requested:
                logger.info(f"Attempting to reconnect in {self._reconnect_delay} seconds...")
                await asyncio.sleep(self._reconnect_delay)
            elif self._disconnect_requested:
                logger.info("Disconnect requested, stopping connection loop.")
                break
            else:
                 logger.info("Auto-reconnect disabled, stopping connection loop.")
                 break
        logger.info("Connection loop finished.")


    async def disconnect(self):
        """Requests disconnection and closes the WebSocket connection."""
        logger.info("Disconnect requested.")
        self._disconnect_requested = True
        self._auto_reconnect = False # Prevent reconnection attempts after explicit disconnect

        async with self._connect_lock:
            if self._websocket and self._is_connected:
                logger.info("Closing WebSocket connection.")
                try:
                    await self._websocket.close(code=1000, reason="Client requested disconnect")
                except ConnectionClosed:
                     logger.info("Connection already closed during disconnect request.")
                except Exception as e:
                     logger.error(f"Error closing websocket during disconnect: {e}")
            else:
                logger.info("WebSocket already closed or not connected during disconnect request.")
            self._is_connected = False # Ensure state is updated
            self._websocket = None

        if self._connection_task and not self._connection_task.done():
             logger.info("Waiting for connection loop task to finish...")
             # Give the loop some time to exit based on _disconnect_requested flag
             try:
                 await asyncio.wait_for(self._connection_task, timeout=max(1.0, self._reconnect_delay / 2))
             except asyncio.TimeoutError:
                 logger.warning("Connection loop did not finish within timeout during disconnect. Cancelling task.")
                 self._connection_task.cancel()
                 try:
                      await self._connection_task # Allow cancellation to propagate
                 except asyncio.CancelledError:
                      logger.info("Connection loop task cancelled.")
                 except Exception as e:
                      logger.error(f"Error awaiting cancelled connection task: {e}")
             except Exception as e:
                 logger.error(f"Error waiting for connection task during disconnect: {e}")
        self._connection_task = None

        logger.info("Disconnection process complete.")


    async def _handler(self, ws: websockets.WebSocketClientProtocol):
        """Continuously listens for and processes messages from the WebSocket."""
        try:
            async for message in ws:
                await self._on_message(message)
        except ConnectionClosedOK:
            logger.info(f"WebSocket connection closed normally (OK). Code=1000")
            # Normal closure, let _connection_loop handle callbacks/state
        except ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed unexpectedly: Code={e.code}, Reason='{e.reason}'")
            # Let _connection_loop handle callbacks/state
            raise # Re-raise to be caught by _connection_loop
        except asyncio.CancelledError:
             logger.info("Message handler task cancelled.")
             raise # Propagate cancellation
        except Exception as e:
            logger.error(f"Error during message handling: {e}", exc_info=True)
            if self.on_error_callback:
                 try:
                      await self.on_error_callback(e)
                 except Exception as cb_err:
                      logger.error(f"Error in on_error_callback during message handling: {cb_err}", exc_info=True)
            raise # Re-raise to be caught by _connection_loop


    async def _send_command(self, command: Dict[str, Any]) -> int:
        """
        Sends a command to the WebSocket server and tracks it.

        Args:
            command: The command dictionary (including 'cmd' and 'params').

        Returns:
            The unique command ID assigned to this command.

        Raises:
            ConnectionError: If the WebSocket is not connected.
            Exception: If sending fails.
        """
        if not self._is_connected or not self._websocket:
            raise ConnectionError("WebSocket is not connected.")

        cmd_id = self._message_id_counter
        self._message_id_counter += 1
        command['id'] = cmd_id

        self._pending_commands[cmd_id] = command.copy()

        try:
            cmd_json = json.dumps(command)
            await self._websocket.send(cmd_json)
            logger.debug(f"Sent command (ID: {cmd_id}): {cmd_json}")
            return cmd_id
        except ConnectionClosed as e:
             logger.error(f"Failed to send command (ID: {cmd_id}) due to connection closed: {e}")
             self._pending_commands.pop(cmd_id, None) # Clean up pending command
             raise ConnectionError(f"WebSocket closed while trying to send command: {e}") from e
        except Exception as e:
            self._pending_commands.pop(cmd_id, None)
            logger.error(f"Failed to send command (ID: {cmd_id}): {e}")
            raise

    # --- Public Command Methods ---

    async def subscribe(
        self,
        channels: List[str],
        market_ticker: Optional[str] = None,
        market_tickers: Optional[List[str]] = None
    ) -> int:
        """
        Subscribes to specified channels for given markets.

        Args:
            channels: A list of channel names (e.g., ["ticker", "orderbook_delta"]).
            market_ticker: A single market ticker for single-market subscriptions.
            market_tickers: A list of market tickers for multi-market subscriptions.
                           If neither market_ticker nor market_tickers is provided,
                           subscribes to 'all markets' mode if supported by the channel.

        Returns:
            The command ID for this subscription request.

        Raises:
            ValueError: If both market_ticker and market_tickers are provided.
            ConnectionError: If not connected.
            Exception: If sending fails.
        """
        if market_ticker and market_tickers:
            raise ValueError("Provide either market_ticker or market_tickers, not both.")

        params = {"channels": channels}
        if market_ticker:
            params["market_ticker"] = market_ticker
        elif market_tickers:
            params["market_tickers"] = market_tickers

        command = {"cmd": "subscribe", "params": params}
        return await self._send_command(command)

    async def unsubscribe(self, sids: List[int]) -> int:
        """
        Unsubscribes from one or more active subscriptions.

        Args:
            sids: A list of subscription IDs (sid) to cancel.

        Returns:
            The command ID for this unsubscription request.

        Raises:
            ValueError: If sids list is empty.
            ConnectionError: If not connected.
            Exception: If sending fails.
        """
        if not sids:
             raise ValueError("sids list cannot be empty for unsubscribe.")
        command = {"cmd": "unsubscribe", "params": {"sids": sids}}
        return await self._send_command(command)

    async def update_subscription(
        self,
        sid: int,
        action: str,
        market_ticker: Optional[str] = None,
        market_tickers: Optional[List[str]] = None
    ) -> int:
        """
        Updates an existing subscription by adding or removing markets.

        Args:
            sid: The subscription ID (sid) to update.
            action: The action to perform ("add_markets" or "delete_markets").
            market_ticker: A single market ticker to add/remove.
            market_tickers: A list of market tickers to add/remove.

        Returns:
            The command ID for this update request.

        Raises:
            ValueError: If the action is invalid or market specification is wrong.
            ConnectionError: If not connected.
            Exception: If sending fails.
        """
        if action not in ["add_markets", "delete_markets"]:
            raise ValueError("Invalid action. Must be 'add_markets' or 'delete_markets'.")
        if market_ticker and market_tickers:
            raise ValueError("Provide either market_ticker or market_tickers, not both.")
        if not market_ticker and not market_tickers:
             raise ValueError("Must provide market_ticker or market_tickers for update.")

        params = {"sids": [sid], "action": action}
        if market_ticker:
            params["market_ticker"] = market_ticker
        elif market_tickers:
            params["market_tickers"] = market_tickers

        command = {"cmd": "update_subscription", "params": params}
        return await self._send_command(command)

    # --- Message Processing ---

    async def _on_message(self, message: Union[str, bytes]):
        """Handles incoming WebSocket messages."""
        try:
            # Ensure message is string for JSON decoding
            if isinstance(message, bytes):
                message_str = message.decode('utf-8')
            else:
                message_str = message

            data = json.loads(message_str)
            logger.debug(f"Received message: {data}")

            # General message callback first
            if self.on_message_callback:
                 try:
                      await self.on_message_callback(data)
                 except Exception as cb_err:
                      logger.error(f"Error in on_message_callback: {cb_err}", exc_info=True)


            # Type-specific internal handler
            msg_type = data.get("type")
            handler = self._message_handlers.get(msg_type)

            if handler:
                 try:
                      await handler(data)
                 except Exception as handler_err:
                      logger.error(f"Error in internal handler for {msg_type}: {handler_err}", exc_info=True)
            elif msg_type: # Only warn if type exists but no handler
                logger.warning(f"No internal handler registered for message type: {msg_type}")

            # Type-specific external callbacks (using the inner 'msg' part)
            if msg_type:
                callbacks = self._callback_registry.get(msg_type, [])
                msg_content = data.get("msg", {})
                for callback in callbacks:
                    try:
                        # Pass the inner 'msg' content, or the full data if 'msg' doesn't exist
                        await callback(msg_content if msg_content else data)
                    except Exception as cb_err:
                        logger.error(f"Error executing registered callback for {msg_type}: {cb_err}", exc_info=True)

        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON message: {message_str[:200] if isinstance(message_str, str) else message[:200]}...")
        except UnicodeDecodeError:
             logger.error(f"Failed to decode message bytes: {message[:200]}...")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    # --- Internal Message Handlers ---

    async def _handle_subscribed(self, data: Dict[str, Any]):
        """Handles 'subscribed' confirmation messages."""
        cmd_id = data.get("id")
        msg_data = data.get("msg", {})
        sid = msg_data.get("sid")
        channel = msg_data.get("channel") # Note: API v2 might return 'channels' list here

        if cmd_id is None or sid is None or channel is None: # Adjust check if 'channels' is returned
             logger.warning(f"Received incomplete 'subscribed' message: {data}")
             return

        pending_cmd = self._pending_commands.get(cmd_id)
        if pending_cmd:
            original_params = pending_cmd.get('params', {})
            # Determine markets from original request
            markets = original_params.get('market_tickers') or \
                      ([original_params.get('market_ticker')] if original_params.get('market_ticker') else ['ALL'])

            # Store subscription details
            self._subscriptions[sid] = {
                'channel': channel, # Store the specific channel confirmed by this message
                'markets': markets, # Store the markets intended for the original command
                'cmd_id': cmd_id,
                'original_params': original_params # Store for potential resubscribe
            }
            logger.info(f"Subscription confirmed: sid={sid}, channel={channel}, markets={markets}, cmd_id={cmd_id}")

            # TODO: Refine logic for removing pending command if multiple channels were requested.
            # If original command requested multiple channels, wait for all confirmations?
            # For now, remove it assuming simple cases or that 'ok' handles multi-channel updates.
            self._pending_commands.pop(cmd_id, None)

        else:
            logger.warning(f"Received 'subscribed' confirmation for unknown/completed command ID: {cmd_id}, sid: {sid}")

    async def _handle_unsubscribed(self, data: Dict[str, Any]):
        """Handles 'unsubscribed' confirmation messages."""
        msg_data = data.get("msg", {})
        sid = msg_data.get("sid") # API v2 seems to put sid inside 'msg'
        cmd_id = data.get("id")

        if sid is None:
             logger.warning(f"Received incomplete 'unsubscribed' message: {data}")
             return

        if sid in self._subscriptions:
            removed_sub = self._subscriptions.pop(sid)
            logger.info(f"Unsubscribed successfully: sid={sid}, channel={removed_sub.get('channel')}")
            # Clean up associated orderbook data if necessary
            # This needs a robust mapping from sid to market(s)
        else:
            logger.warning(f"Received 'unsubscribed' confirmation for unknown sid: {sid}")

        if cmd_id and cmd_id in self._pending_commands:
            self._pending_commands.pop(cmd_id, None)
            logger.debug(f"Removed pending unsubscribe command ID: {cmd_id}")


    async def _handle_ok(self, data: Dict[str, Any]):
        """Handles 'ok' confirmation messages (e.g., for update_subscription)."""
        cmd_id = data.get("id")
        msg_data = data.get("msg", {})
        sid = msg_data.get("sid")
        updated_markets = msg_data.get("market_tickers") # API v2 puts details in 'msg'

        if cmd_id is None or sid is None:
             logger.warning(f"Received incomplete 'ok' message: {data}")
             return

        pending_cmd = self._pending_commands.get(cmd_id)
        if pending_cmd:
            logger.info(f"Command confirmed (OK): cmd_id={cmd_id}, sid={sid}")
            if sid in self._subscriptions:
                 if updated_markets is not None:
                      # Update the markets list associated with this subscription
                      self._subscriptions[sid]['markets'] = updated_markets
                      # Also update the stored original params for accurate resubscription
                      self._subscriptions[sid]['original_params']['market_tickers'] = updated_markets
                      self._subscriptions[sid]['original_params'].pop('market_ticker', None) # Remove single ticker if list is now present
                      logger.info(f"Subscription updated: sid={sid}, new markets list={updated_markets}")
                 else:
                      # OK might confirm actions other than market updates
                      logger.info(f"Received 'ok' for sid {sid} without market updates (e.g., unsubscribe confirmation).")
            else:
                 logger.warning(f"Received 'ok' for unknown sid: {sid}")
            self._pending_commands.pop(cmd_id, None)
        else:
            logger.warning(f"Received 'ok' confirmation for unknown/completed command ID: {cmd_id}")

    async def _handle_error(self, data: Dict[str, Any]):
        """Handles 'error' messages."""
        cmd_id = data.get("id")
        error_msg_data = data.get("msg", {})
        error_msg = error_msg_data.get("msg", "Unknown error")
        error_code = error_msg_data.get("code", -1)

        pending_cmd_details = ""
        if cmd_id and cmd_id in self._pending_commands:
            pending_cmd_details = f" (Command: {self._pending_commands[cmd_id].get('cmd', 'N/A')}, Params: {self._pending_commands[cmd_id].get('params', {})})"
            self._pending_commands.pop(cmd_id, None)
        elif cmd_id:
             pending_cmd_details = f" (Command ID was {cmd_id}, but not found in pending list)"

        logger.error(f"Command failed: code={error_code}, message='{error_msg}'{pending_cmd_details}")

    async def _handle_ticker(self, data: Dict[str, Any]):
        """Handles 'ticker' data messages."""
        logger.debug(f"Processed ticker message for sid {data.get('sid')}")
        pass # Callbacks handled by _on_message

    async def _handle_trade(self, data: Dict[str, Any]):
        """Handles 'trade' data messages."""
        logger.debug(f"Processed trade message for sid {data.get('sid')}")
        pass # Callbacks handled by _on_message

    async def _handle_fill(self, data: Dict[str, Any]):
        """Handles 'fill' data messages."""
        logger.debug(f"Processed fill message for sid {data.get('sid')}")
        pass # Callbacks handled by _on_message

    async def _handle_orderbook_snapshot(self, data: Dict[str, Any]):
        """Handles 'orderbook_snapshot' messages."""
        sid = data.get("sid")
        msg = data.get("msg", {})
        seq = msg.get("seq") # Seq number inside msg
        market_ticker = msg.get("market_ticker")

        if not market_ticker or sid is None or seq is None:
            logger.warning(f"Received incomplete orderbook snapshot: {data}")
            return

        if sid not in self._subscriptions or self._subscriptions[sid]['channel'] not in ['orderbook_delta', 'orderbook_snapshot']: # Allow snapshot channel too
             logger.warning(f"Received orderbook snapshot for non-orderbook or unknown sid: {sid}")
             return

        logger.info(f"Received orderbook snapshot for {market_ticker} (sid: {sid}, seq: {seq})")
        self._orderbooks[market_ticker] = {
            "yes": msg.get("yes", []),
            "no": msg.get("no", []),
            "last_seq": seq
        }

    async def _handle_orderbook_delta(self, data: Dict[str, Any]):
        """Handles 'orderbook_delta' messages."""
        sid = data.get("sid")
        msg = data.get("msg", {})
        seq = msg.get("seq") # Seq number inside msg
        market_ticker = msg.get("market_ticker")
        price = msg.get("price")
        delta = msg.get("delta")
        side = msg.get("side")

        if not market_ticker or sid is None or seq is None or price is None or delta is None or side not in ["yes", "no"]:
            logger.warning(f"Received incomplete orderbook delta: {data}")
            return

        if sid not in self._subscriptions or self._subscriptions[sid]['channel'] != 'orderbook_delta':
             logger.warning(f"Received orderbook delta for non-orderbook or unknown sid: {sid}")
             return

        if market_ticker not in self._orderbooks:
             logger.warning(f"Received delta for {market_ticker} (sid: {sid}) before snapshot. State inconsistent.")
             # TODO: Optionally trigger resubscribe or request snapshot
             return

        last_seq = self._orderbooks[market_ticker].get("last_seq", 0)
        if seq <= last_seq:
             logger.debug(f"Received old or duplicate delta for {market_ticker} (sid: {sid}). Expected > {last_seq}, got {seq}. Ignoring.")
             return
        if seq != last_seq + 1:
             logger.warning(f"Sequence gap detected for {market_ticker} (sid: {sid})! Expected {last_seq + 1}, got {seq}. Orderbook state might be inconsistent.")
             # TODO: Trigger resubscribe or request snapshot
             return # Don't apply delta if sequence is wrong

        logger.debug(f"Applying orderbook delta for {market_ticker} (sid: {sid}, seq: {seq}): price={price}, delta={delta}, side={side}")

        book_side = self._orderbooks[market_ticker].get(side, [])
        new_book_side = []
        updated = False

        for level_price, level_contracts in book_side:
            if level_price == price:
                new_contracts = level_contracts + delta
                if new_contracts > 0:
                    new_book_side.append([level_price, new_contracts])
                updated = True
            else:
                new_book_side.append([level_price, level_contracts])

        if not updated and delta > 0:
            new_book_side.append([price, delta])
            # Sort based on price - assuming ascending for both sides
            new_book_side.sort(key=lambda x: x[0])

        self._orderbooks[market_ticker][side] = new_book_side
        self._orderbooks[market_ticker]["last_seq"] = seq

    async def _handle_market_lifecycle(self, data: Dict[str, Any]):
        """Handles 'market_lifecycle' messages."""
        logger.debug(f"Processed market_lifecycle message for sid {data.get('sid')}")
        pass

    async def _handle_event_lifecycle(self, data: Dict[str, Any]):
        """Handles 'event_lifecycle' messages."""
        logger.debug(f"Processed event_lifecycle message for sid {data.get('sid')}")
        pass

    async def _handle_multivariate_lookup(self, data: Dict[str, Any]):
        """Handles 'multivariate_lookup' messages."""
        logger.debug(f"Processed multivariate_lookup message for sid {data.get('sid')}")
        pass

    # --- Callback Registration ---

    def register_callback(self, event_type: str, callback: Callable[[Dict[str, Any]], Coroutine]):
        """
        Registers an async callback function for a specific message type.

        The callback will receive the inner 'msg' dictionary from the WebSocket message.

        Args:
            event_type: The message type string (e.g., "ticker", "fill", "orderbook_delta").
            callback: An async function that accepts the message's 'msg' dictionary as an argument.
        """
        if not asyncio.iscoroutinefunction(callback):
             raise TypeError(f"Callback for {event_type} must be an async function (coroutine).")
        self._callback_registry[event_type].append(callback)
        logger.info(f"Registered callback for event type: {event_type}")

    def unregister_callback(self, event_type: str, callback: Callable[[Dict[str, Any]], Coroutine]):
         """Unregisters a specific callback function for a message type."""
         if event_type in self._callback_registry:
              try:
                   self._callback_registry[event_type].remove(callback)
                   logger.info(f"Unregistered callback for event type: {event_type}")
              except ValueError:
                   logger.warning(f"Callback not found for event type {event_type} during unregistration.")
         else:
              logger.warning(f"No callbacks registered for event type {event_type} to unregister.")


    # --- Recovery and Resubscription ---
    async def _resubscribe(self):
        """Resubscribes using the original parameters of active subscriptions upon reconnection."""
        if not self._subscriptions:
             logger.info("No active subscriptions to resubscribe.")
             return

        logger.info(f"Attempting to resubscribe to {len(self._subscriptions)} previous subscriptions...")
        # Group subscriptions by original command ID to potentially batch resubscriptions
        commands_to_resend = defaultdict(lambda: {'channels': set(), 'market_ticker': None, 'market_tickers': set()})
        sids_to_clear = list(self._subscriptions.keys()) # Get sids before modifying dict

        for sid, sub_details in self._subscriptions.items():
             original_params = sub_details.get('original_params')
             cmd_id = sub_details.get('cmd_id') # Use original cmd_id if available for grouping
             if not original_params or not cmd_id:
                  logger.warning(f"Cannot resubscribe for sid {sid}: Original parameters or cmd_id not found.")
                  continue

             channels = original_params.get('channels')
             if not channels:
                  logger.warning(f"Cannot resubscribe for sid {sid}: No channels found in original parameters.")
                  continue

             # Add channels to the set for this command
             commands_to_resend[cmd_id]['channels'].update(channels)

             # Consolidate market info - prioritize market_tickers if present
             if 'market_tickers' in original_params:
                  commands_to_resend[cmd_id]['market_tickers'].update(original_params['market_tickers'])
             elif 'market_ticker' in original_params:
                  # If only single tickers were used, collect them. If 'ALL' was used, keep it separate.
                  ticker = original_params['market_ticker']
                  if ticker == 'ALL':
                       commands_to_resend[cmd_id]['market_ticker'] = 'ALL' # Mark as all markets
                  elif commands_to_resend[cmd_id]['market_ticker'] != 'ALL': # Don't add if already marked as ALL
                       commands_to_resend[cmd_id]['market_tickers'].add(ticker)


        # Clear current state before resubscribing
        self._subscriptions.clear()
        self._pending_commands.clear()
        self._orderbooks.clear()

        # Send the reconstructed subscribe commands
        for cmd_id, params_to_send in commands_to_resend.items():
             channels_list = list(params_to_send['channels'])
             market_ticker_final = None
             market_tickers_final = None

             if params_to_send['market_ticker'] == 'ALL':
                  market_ticker_final = None # Use 'all markets' mode
                  market_tickers_final = None
             elif params_to_send['market_tickers']:
                  market_tickers_final = list(params_to_send['market_tickers'])
                  # Decide if single ticker optimization is useful (API might prefer list)
                  # if len(market_tickers_final) == 1:
                  #      market_ticker_final = market_tickers_final[0]
                  #      market_tickers_final = None
             # Else: No specific markets specified (should imply 'ALL' for supported channels)

             try:
                  log_markets = market_ticker_final or market_tickers_final or "ALL"
                  logger.info(f"Resubscribing to channels {channels_list} for markets {log_markets}")
                  await self.subscribe(
                       channels=channels_list,
                       market_ticker=market_ticker_final,
                       market_tickers=market_tickers_final
                  )
             except ConnectionError:
                  logger.error(f"Connection lost during resubscribe attempt for {channels_list}. Will retry on next connection.")
                  break # Stop trying to resubscribe on this attempt
             except Exception as e:
                  logger.error(f"Failed to resubscribe to {channels_list} for {log_markets}: {e}")


    # --- Helper Methods ---
    def get_orderbook(self, market_ticker: str) -> Optional[Dict[str, Any]]:
         """
         Retrieves the current known order book state for a market, including last sequence number.

         Args:
             market_ticker: The market ticker.

         Returns:
             A dictionary with 'yes' and 'no' lists of [price, size] and 'last_seq',
             or None if not available. Returns a deep copy.
         """
         if market_ticker in self._orderbooks:
              # Return a deep copy to prevent modification of internal state
              return json.loads(json.dumps(self._orderbooks[market_ticker]))
         return None

# --- Utility Functions (Keep existing code below) ---
def detect_ticker_type(ticker: str) -> str:
    """
    Detects whether a Kalshi ticker represents a series, event, or market.

    Args:
        ticker (str): The Kalshi ticker string.

    Returns:
        str: 'series', 'event', or 'market'.
    """
    if '-' not in ticker:
        # Series tickers typically don't have hyphens (e.g., KXCPIYOY)
        # This is a heuristic and might need refinement based on all possible ticker formats.
        # Assuming tickers starting with KX and no hyphens are series.
        if ticker.startswith('KX'):
            return 'series'
        # Fallback assumption for non-hyphenated, non-KX tickers (if any exist)
        return 'market' # Or potentially 'unknown'

    parts = ticker.split('-')

    # Event tickers often have one hyphen (e.g., KXCPIYOY-25MAR)
    if len(parts) == 2 and ticker.startswith('KX'):
        # Further checks could involve validating the date part if needed
        return 'event'

    # Market tickers often have two hyphens (e.g., KXCPIYOY-25MAR-T2.5)
    if len(parts) >= 3 and ticker.startswith('KX'): # Use >= 3 for flexibility
        # Further checks could involve validating the structure (e.g., '-T' part)
        return 'market'

    # Default or fallback if pattern doesn't match known types
    # Could be a less common format or an error. Defaulting to market might be risky.
    # Consider returning 'unknown' or raising an error for unrecognized formats.
    logger.warning(f"Could not definitively determine ticker type for '{ticker}'. Defaulting to 'market'.")
    return 'market'

def calculate_bid_ask_spread(orderbook):
    """
    Calculate the bid-ask spread and other metrics from a Kalshi orderbook
    
    Args:
        orderbook (dict): The Kalshi orderbook object with structure {orderbook: {yes: [...], no: [...]}}
    
    Returns:
        dict: Object containing bid-ask spread metrics and market analysis
    
    Raises:
        ValueError: If the orderbook format is invalid
    """
    # Input validation
    if not isinstance(orderbook, dict) or 'orderbook' not in orderbook:
        raise ValueError('Invalid orderbook format. Expected {orderbook: {yes: [...], no: [...]}}')
    
    # Safely get 'yes' and 'no' lists, defaulting to empty list if key is missing or value is None
    yes_bids_raw = orderbook['orderbook'].get('yes', [])
    no_asks_raw = orderbook['orderbook'].get('no', [])

    # Ensure we have lists before sorting
    yes_bids_list = yes_bids_raw if isinstance(yes_bids_raw, list) else []
    no_asks_list = no_asks_raw if isinstance(no_asks_raw, list) else []

    # Sort arrays by price if they are not empty
    yes_bids = sorted(yes_bids_list, key=lambda x: x[0], reverse=True) if yes_bids_list else []
    no_asks = sorted(no_asks_list, key=lambda x: x[0]) if no_asks_list else []

    # Get best bid (highest price someone is willing to buy at)
    best_bid = yes_bids[0][0] if yes_bids else 0
    best_bid_volume = yes_bids[0][1] if yes_bids else 0
    
    # Get best ask (lowest price someone is willing to sell at)
    best_ask = no_asks[0][0] if no_asks else 100
    best_ask_volume = no_asks[0][1] if no_asks else 0
    
    # Calculate spread
    spread = best_ask - best_bid
    spread_percentage = (spread / best_ask) * 100 if best_ask != 0 else 0
    
    # Calculate mid price
    mid_price = (best_bid + best_ask) / 2

    # Ensure lists are valid before summing or calculating liquidity
    yes_bids_list_for_calc = yes_bids_list if isinstance(yes_bids_list, list) else []
    no_asks_list_for_calc = no_asks_list if isinstance(no_asks_list, list) else []

    # Calculate total volumes using the validated lists
    total_yes_volume = sum(order[1] for order in yes_bids_list_for_calc)
    total_no_volume = sum(order[1] for order in no_asks_list_for_calc)

    # Calculate liquidity within 5¢ of best prices using validated lists
    liquidity_near_bid = sum(
        order[1] for order in yes_bids_list_for_calc
        if 0 <= best_bid - order[0] <= 5
    )

    liquidity_near_ask = sum(
        order[1] for order in no_asks_list_for_calc
        if 0 <= order[0] - best_ask <= 5
    )

    # Check for crossed market (negative spread)
    is_crossed_market = spread < 0
    
    # Determine implied probability based on mid price
    implied_probability = mid_price
    
    # Calculate theoretical fair value (in a prediction market, YES + NO should = 100)
    fair_value_gap = 100 - (best_bid + (100 - best_ask))
    
    # Create results dictionary
    results = {
        # Basic spread data
        'best_bid': best_bid,
        'best_bid_volume': best_bid_volume,
        'best_ask': best_ask,
        'best_ask_volume': best_ask_volume,
        'spread': spread,
        'spread_percentage': spread_percentage,
        'mid_price': mid_price,
        
        # Market analysis
        'implied_probability': implied_probability,
        'is_crossed_market': is_crossed_market,
        'fair_value_gap': fair_value_gap,
        
        # Liquidity metrics
        'total_yes_volume': total_yes_volume,
        'total_no_volume': total_no_volume,
        'liquidity_near_bid': liquidity_near_bid,
        'liquidity_near_ask': liquidity_near_ask,
    }
    
    return results
