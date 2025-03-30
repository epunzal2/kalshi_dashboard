import requests
import base64
import time
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
from enum import Enum
import requests
import base64
import time
from typing import Any, Dict, Optional, List, Callable, Coroutine
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
    
    # market methods
    def get_trades(
        self,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        params = {k: v for k, v in {"ticker": ticker, "limit": limit}.items() if v is not None}
        return self.get(f"{self.markets_url}/trades", params=params)

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

    # portfolio methods
    def get_balance(self) -> Dict[str, Any]:
        balance = self.get(f"{self.portfolio_url}/balance")
        logger.debug(f"Raw balance response: {balance}") # Use logger
        return balance

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
                auth_headers = self.request_headers("GET", self.url_suffix)
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
        # Do not acquire lock here, assume caller (public methods) handles it
        # or that connect() ensures _websocket is valid before _handler runs.
        # If called outside the connection loop context, it needs protection.
        # Let's assume it's called by methods that ensure connection or handle errors.
        if not self._is_connected or not self._websocket:
            raise ConnectionError("WebSocket is not connected.")

        cmd_id = self._message_id_counter
        self._message_id_counter += 1
        command['id'] = cmd_id

        # Store command details before sending
        # Make a copy to avoid modification issues if command dict is reused
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
            # Clean up pending command if send fails
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
        # Else: All markets mode (no market param)

        command = {"cmd": "subscribe", "params": params}
        # Acquire lock here if sending can happen outside the main connection loop context
        # async with self._connect_lock: # Consider if needed
        return await self._send_command(command)

    async def unsubscribe(self, sids: List[int]) -> int:
        """
        Unsubscribes from one or more active subscriptions.

        Args:
            sids: A list of subscription IDs (sid) to cancel.

        Returns:
            The command ID for this unsubscription request.

        Raises:
            ConnectionError: If not connected.
            Exception: If sending fails.
        """
        if not sids:
             raise ValueError("sids list cannot be empty for unsubscribe.")
        command = {"cmd": "unsubscribe", "params": {"sids": sids}}
        # async with self._connect_lock: # Consider if needed
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

        params = {"sids": [sid], "action": action} # API expects sids as a list, even for one
        if market_ticker:
            params["market_ticker"] = market_ticker
        elif market_tickers:
            params["market_tickers"] = market_tickers

        command = {"cmd": "update_subscription", "params": params}
        # async with self._connect_lock: # Consider if needed
        return await self._send_command(command)

    # --- Message Processing ---

    async def _on_message(self, message: str):
        """Handles incoming WebSocket messages."""
        try:
            data = json.loads(message)
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
            logger.error(f"Failed to decode JSON message: {message[:200]}...")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            # Don't trigger on_error_callback here, as it might be a processing issue,
            # not a connection issue. Let higher levels handle processing errors if needed.

    # --- Internal Message Handlers ---

    async def _handle_subscribed(self, data: Dict[str, Any]):
        """Handles 'subscribed' confirmation messages."""
        cmd_id = data.get("id")
        msg_data = data.get("msg", {})
        sid = msg_data.get("sid")
        channel = msg_data.get("channel")

        if cmd_id is None or sid is None or channel is None:
             logger.warning(f"Received incomplete 'subscribed' message: {data}")
             return

        pending_cmd = self._pending_commands.get(cmd_id)
        if pending_cmd:
            # Associate sid with the original command details
            original_params = pending_cmd.get('params', {})
            markets = original_params.get('market_tickers') or \
                      ([original_params.get('market_ticker')] if original_params.get('market_ticker') else ['ALL'])

            self._subscriptions[sid] = {
                'channel': channel,
                'markets': markets,
                'cmd_id': cmd_id,
                'original_params': original_params # Store original params for potential resubscribe
            }
            logger.info(f"Subscription confirmed: sid={sid}, channel={channel}, markets={markets}, cmd_id={cmd_id}")

            # TODO: Need logic to determine when a command ID is fully resolved
            # if multiple channels were in the original request.
            # For now, we don't remove the pending command here, assume one channel per subscribe for simplicity,
            # or handle removal when all expected sids for a cmd_id arrive.
            # Let's tentatively remove it, assuming simple cases work.
            # self._pending_commands.pop(cmd_id, None) # Revisit this logic

        else:
            logger.warning(f"Received 'subscribed' confirmation for unknown/completed command ID: {cmd_id}, sid: {sid}")

    async def _handle_unsubscribed(self, data: Dict[str, Any]):
        """Handles 'unsubscribed' confirmation messages."""
        sid = data.get("sid")
        cmd_id = data.get("id") # Unsubscribe command *might* have an ID in response? Docs unclear. Assume not usually.

        if sid is None:
             logger.warning(f"Received incomplete 'unsubscribed' message: {data}")
             return

        if sid in self._subscriptions:
            removed_sub = self._subscriptions.pop(sid)
            logger.info(f"Unsubscribed successfully: sid={sid}, channel={removed_sub.get('channel')}")
            # Also remove associated orderbook data if it exists
            # This assumes one market per orderbook subscription, which might be wrong.
            # Need better mapping if multiple markets share an orderbook sid.
            # market_to_clear = removed_sub.get('markets', [None])[0] # Simplistic guess
            # if market_to_clear and market_to_clear != 'ALL':
            #      self._orderbooks.pop(market_to_clear, None)

        else:
            logger.warning(f"Received 'unsubscribed' confirmation for unknown sid: {sid}")

        # If the unsubscribe command itself had an ID we were tracking
        if cmd_id and cmd_id in self._pending_commands:
            # TODO: Similar to subscribe, need logic if one command unsubscribed multiple sids.
            # Tentatively remove.
            self._pending_commands.pop(cmd_id, None)
            logger.debug(f"Removed pending unsubscribe command ID: {cmd_id}")


    async def _handle_ok(self, data: Dict[str, Any]):
        """Handles 'ok' confirmation messages (e.g., for update_subscription)."""
        cmd_id = data.get("id")
        sid = data.get("sid")
        updated_markets = data.get("market_tickers") # API returns full list

        if cmd_id is None or sid is None:
             logger.warning(f"Received incomplete 'ok' message: {data}")
             return

        pending_cmd = self._pending_commands.get(cmd_id)
        if pending_cmd:
            logger.info(f"Command confirmed (OK): cmd_id={cmd_id}, sid={sid}")
            if sid in self._subscriptions:
                 if updated_markets is not None:
                      self._subscriptions[sid]['markets'] = updated_markets
                      # Update original params as well?
                      self._subscriptions[sid]['original_params']['market_tickers'] = updated_markets
                      self._subscriptions[sid]['original_params'].pop('market_ticker', None)
                      logger.info(f"Subscription updated: sid={sid}, new markets list={updated_markets}")
                 else:
                      logger.warning(f"Received 'ok' for sid {sid} but no 'market_tickers' field in response.")
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

        # Log with more context if the command is known
        pending_cmd_details = ""
        if cmd_id and cmd_id in self._pending_commands:
            pending_cmd_details = f" (Command: {self._pending_commands[cmd_id].get('cmd', 'N/A')}, Params: {self._pending_commands[cmd_id].get('params', {})})"
            # Remove the failed command
            self._pending_commands.pop(cmd_id, None)
        elif cmd_id:
             pending_cmd_details = f" (Command ID was {cmd_id}, but not found in pending list)"


        logger.error(f"Command failed: code={error_code}, message='{error_msg}'{pending_cmd_details}")

        # Handle specific errors, e.g., resubscribe on sequence gap errors if applicable
        # if error_code == SOME_SEQUENCE_ERROR_CODE:
        #     await self._trigger_resubscribe_for_sid(...)


    async def _handle_ticker(self, data: Dict[str, Any]):
        """Handles 'ticker' data messages."""
        # No internal state update needed, callbacks handled by _on_message
        logger.debug(f"Processed ticker message for sid {data.get('sid')}")
        pass

    async def _handle_trade(self, data: Dict[str, Any]):
        """Handles 'trade' data messages."""
        # No internal state update needed, callbacks handled by _on_message
        logger.debug(f"Processed trade message for sid {data.get('sid')}")
        pass

    async def _handle_fill(self, data: Dict[str, Any]):
        """Handles 'fill' data messages."""
        # No internal state update needed, callbacks handled by _on_message
        logger.debug(f"Processed fill message for sid {data.get('sid')}")
        pass

    async def _handle_orderbook_snapshot(self, data: Dict[str, Any]):
        """Handles 'orderbook_snapshot' messages."""
        sid = data.get("sid")
        seq = data.get("seq")
        msg = data.get("msg", {})
        market_ticker = msg.get("market_ticker")

        if not market_ticker or sid is None or seq is None:
            logger.warning(f"Received incomplete orderbook snapshot: {data}")
            return

        # TODO: Check if sid corresponds to an active orderbook subscription
        if sid not in self._subscriptions or self._subscriptions[sid]['channel'] != 'orderbook_delta':
             logger.warning(f"Received orderbook snapshot for non-orderbook or unknown sid: {sid}")
             return

        logger.info(f"Received orderbook snapshot for {market_ticker} (sid: {sid}, seq: {seq})")
        # Replace the entire orderbook state for this market
        # This assumes one market per orderbook subscription SID. If multiple markets can share
        # an SID (e.g. via update_subscription), this logic needs adjustment.
        self._orderbooks[market_ticker] = {
            "yes": msg.get("yes", []),
            "no": msg.get("no", []),
            "last_seq": seq # Store sequence number
        }
        # Callbacks handled by _on_message

    async def _handle_orderbook_delta(self, data: Dict[str, Any]):
        """Handles 'orderbook_delta' messages."""
        sid = data.get("sid")
        seq = data.get("seq")
        msg = data.get("msg", {})
        market_ticker = msg.get("market_ticker")
        price = msg.get("price")
        delta = msg.get("delta")
        side = msg.get("side") # "yes" or "no"

        if not market_ticker or sid is None or seq is None or price is None or delta is None or side not in ["yes", "no"]:
            logger.warning(f"Received incomplete orderbook delta: {data}")
            return

        # Check if sid corresponds to an active orderbook subscription
        if sid not in self._subscriptions or self._subscriptions[sid]['channel'] != 'orderbook_delta':
             logger.warning(f"Received orderbook delta for non-orderbook or unknown sid: {sid}")
             return

        # Check if we have a snapshot for this market
        if market_ticker not in self._orderbooks:
             logger.warning(f"Received delta for {market_ticker} (sid: {sid}) before snapshot. State inconsistent. Requesting resubscribe might be needed.")
             # TODO: Trigger resubscribe logic for this sid
             return

        # Check sequence number
        last_seq = self._orderbooks[market_ticker].get("last_seq", 0)
        if seq <= last_seq:
             logger.debug(f"Received old or duplicate delta for {market_ticker} (sid: {sid}). Expected > {last_seq}, got {seq}. Ignoring.")
             return
        if seq != last_seq + 1:
             logger.warning(f"Sequence gap detected for {market_ticker} (sid: {sid})! Expected {last_seq + 1}, got {seq}. Orderbook state might be inconsistent. Requesting resubscribe.")
             # TODO: Trigger resubscribe logic for this sid
             # Don't apply the delta if sequence is wrong, wait for snapshot/resubscribe
             return

        logger.debug(f"Applying orderbook delta for {market_ticker} (sid: {sid}, seq: {seq}): price={price}, delta={delta}, side={side}")

        # Apply the delta
        book_side = self._orderbooks[market_ticker].get(side, [])
        new_book_side = []
        updated = False

        for level_price, level_contracts in book_side:
            if level_price == price:
                new_contracts = level_contracts + delta
                if new_contracts > 0: # Keep level if contracts remain
                    new_book_side.append([level_price, new_contracts])
                # Else: Level removed if contracts <= 0
                updated = True
            else:
                new_book_side.append([level_price, level_contracts])

        if not updated and delta > 0: # Add new price level if it didn't exist and delta is positive
            new_book_side.append([price, delta])
            # Keep the book sorted by price
            # Yes side: Lower prices (higher bids) first? -> Ascending price
            # No side: Higher prices (lower asks) first? -> Ascending price
            # Let's assume ascending price sort for both sides for internal representation
            new_book_side.sort(key=lambda x: x[0])

        self._orderbooks[market_ticker][side] = new_book_side # Update the stored side
        self._orderbooks[market_ticker]["last_seq"] = seq # Update sequence number

        # Callbacks handled by _on_message

    async def _handle_market_lifecycle(self, data: Dict[str, Any]):
        """Handles 'market_lifecycle' messages."""
        logger.debug(f"Processed market_lifecycle message for sid {data.get('sid')}")
        pass # Callbacks handled by _on_message

    async def _handle_event_lifecycle(self, data: Dict[str, Any]):
        """Handles 'event_lifecycle' messages."""
        logger.debug(f"Processed event_lifecycle message for sid {data.get('sid')}")
        pass # Callbacks handled by _on_message

    async def _handle_multivariate_lookup(self, data: Dict[str, Any]):
        """Handles 'multivariate_lookup' messages."""
        logger.debug(f"Processed multivariate_lookup message for sid {data.get('sid')}")
        pass # Callbacks handled by _on_message

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
        # This attempts to reconstruct subscribe commands based on stored original params.
        if not self._subscriptions:
             logger.info("No active subscriptions to resubscribe.")
             return

        logger.info(f"Attempting to resubscribe to {len(self._subscriptions)} previous subscriptions...")
        subscriptions_to_resend = list(self._subscriptions.values()) # Get details before clearing

        # Clear current state before resubscribing
        self._subscriptions.clear()
        self._pending_commands.clear() # Clear pending from previous connection
        self._orderbooks.clear() # Orderbooks need fresh snapshots

        for sub_details in subscriptions_to_resend:
             original_params = sub_details.get('original_params')
             if not original_params:
                  logger.warning(f"Cannot resubscribe for sid {sub_details.get('sid', 'N/A')}: Original parameters not found.")
                  continue

             channels = original_params.get('channels')
             market_ticker = original_params.get('market_ticker')
             market_tickers = original_params.get('market_tickers')

             if not channels:
                  logger.warning(f"Cannot resubscribe for sid {sub_details.get('sid', 'N/A')}: No channels found in original parameters.")
                  continue

             try:
                  log_markets = market_ticker or market_tickers or "ALL"
                  logger.info(f"Resubscribing to channels {channels} for markets {log_markets}")
                  # Use the public subscribe method which handles command sending and tracking
                  await self.subscribe(
                       channels=channels,
                       market_ticker=market_ticker,
                       market_tickers=market_tickers
                  )
             except ConnectionError:
                  logger.error(f"Connection lost during resubscribe attempt for {channels}. Will retry on next connection.")
                  # If connection drops during resubscribe, the loop will handle reconnecting again.
                  break # Stop trying to resubscribe on this attempt
             except Exception as e:
                  logger.error(f"Failed to resubscribe to {channels} for {log_markets}: {e}")

        # TODO: Resend any commands that were genuinely pending (not subscription confirmations)
        # This requires better tracking of command types in _pending_commands.


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

# Example Usage (requires async context)
# async def main():
#     # Load key_id and private_key securely (e.g., from env vars or config)
#     # IMPORTANT: Replace with your actual key loading mechanism
#     try:
#         key_id = os.environ["KALSHI_API_KEY_ID"]
#         private_key_pem = os.environ["KALSHI_PRIVATE_KEY"] # Expects PEM format in env var
#         private_key_password = os.environ.get("KALSHI_PRIVATE_KEY_PASSWORD") # Optional password
#     except KeyError:
#         print("Error: Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY environment variables.")
#         return

#     try:
#         private_key = serialization.load_pem_private_key(
#             private_key_pem.encode(),
#             password=private_key_password.encode() if private_key_password else None
#         )
#     except Exception as e:
#         print(f"Error loading private key: {e}")
#         return

#     async def handle_ticker(ticker_data):
#         print(f"Callback - Ticker: {ticker_data.get('market_ticker')} Price: {ticker_data.get('price')}")

#     async def handle_orderbook_update(orderbook_data):
#         # This callback gets the raw snapshot/delta message 'msg' part
#         print(f"Callback - Orderbook Update ({orderbook_data.get('market_ticker')}): {orderbook_data}")

#     async def handle_fill(fill_data):
#         print(f"Callback - Fill: {fill_data}")

#     async def handle_all_messages(msg):
#          # Example: Print only non-data messages for debugging
#          if msg.get("type") not in ["ticker", "trade", "fill", "orderbook_snapshot", "orderbook_delta"]:
#               print(f"Raw Message: {msg}")
#          pass

#     async def handle_error(error):
#          print(f"WebSocket Error Callback: {error}")

#     async def handle_close(code, reason):
#          print(f"WebSocket Close Callback: Code={code}, Reason='{reason}'")

#     async def handle_open():
#          print("WebSocket Open Callback: Connection established!")


#     # --- Client Initialization ---
#     client = KalshiWebSocketClient(
#          key_id,
#          private_key,
#          environment=Environment.DEMO, # Use Environment.PROD for production
#          on_message_callback=handle_all_messages,
#          on_error_callback=handle_error,
#          on_close_callback=handle_close,
#          on_open_callback=handle_open
#     )

#     # --- Register Callbacks for Specific Types ---
#     client.register_callback("ticker", handle_ticker)
#     client.register_callback("orderbook_snapshot", handle_orderbook_update)
#     client.register_callback("orderbook_delta", handle_orderbook_update)
#     client.register_callback("fill", handle_fill)

#     # --- Connect and Subscribe ---
#     await client.connect() # Start connection loop (runs in background)

#     # Wait briefly for connection before subscribing
#     # A more robust approach uses an event set by on_open_callback
#     await asyncio.sleep(3)

#     if not client._is_connected:
#          print("Failed to connect after initial wait.")
#          return

#     try:
#         # Subscribe to ticker for specific markets
#         cmd_id_ticker = await client.subscribe(channels=["ticker"], market_tickers=["INX-DEMO", "FEDFUND-DEMO"])
#         print(f"Sent ticker subscription request (ID: {cmd_id_ticker})")

#         # Subscribe to orderbook for specific markets
#         cmd_id_ob = await client.subscribe(channels=["orderbook_delta"], market_tickers=["INX-DEMO"])
#         print(f"Sent orderbook subscription request (ID: {cmd_id_ob})")

#         # Subscribe to fills for all markets
#         cmd_id_fill = await client.subscribe(channels=["fill"])
#         print(f"Sent fill subscription request (ID: {cmd_id_fill})")


#         # --- Keep the client running ---
#         print("Client running. Listening for messages for 60 seconds...")
#         await asyncio.sleep(60) # Keep running

#         # --- Example: Get current orderbook state ---
#         inx_ob = client.get_orderbook("INX-DEMO")
#         if inx_ob:
#              print("\n--- Current INX-DEMO Orderbook ---")
#              print(f"Last Seq: {inx_ob.get('last_seq')}")
#              print(f"Yes Bids: {inx_ob.get('yes')}") # Assuming yes = bids
#              print(f"No Asks: {inx_ob.get('no')}")   # Assuming no = asks
#              print("---------------------------------")


#         # --- Example: Unsubscribe ---
#         # Need to get the SID from the 'subscribed' message or track it.
#         # This requires more robust state management than shown in this basic example.
#         # sid_to_unsubscribe = ... # Get SID from self._subscriptions
#         # if sid_to_unsubscribe:
#         #     cmd_id_unsub = await client.unsubscribe(sids=[sid_to_unsubscribe])
#         #     print(f"Sent unsubscribe request (ID: {cmd_id_unsub})")
#         #     await asyncio.sleep(5)


#     except ConnectionError as e:
#          print(f"Operation failed due to connection error: {e}")
#     except ValueError as e:
#          print(f"Operation failed due to invalid value: {e}")
#     except Exception as e:
#          print(f"An unexpected error occurred: {e}", exc_info=True)
#     finally:
#         print("Disconnecting client...")
#         await client.disconnect()
#         print("Client disconnected.")

# if __name__ == "__main__":
#      # Setup logging properly here
#      log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#      logging.basicConfig(level=logging.INFO, format=log_format)
#      # Set websockets logger level higher to reduce noise if needed
#      logging.getLogger('websockets').setLevel(logging.WARNING)

#      try:
#           asyncio.run(main())
#      except KeyboardInterrupt:
#           print("Interrupted by user.")

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
