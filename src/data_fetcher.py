import os
import logging
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv # Keep for local mode
from cryptography.hazmat.primitives import serialization
import google.cloud.secretmanager as secretmanager
from google.cloud import storage # Added for GCS
from flask import Flask, request, jsonify # Added for Flask
import traceback # Import traceback

print("--- data_fetcher.py START ---", flush=True) # <<< ADDED

from src.clients import KalshiHttpClient, Environment, detect_ticker_type, calculate_bid_ask_spread # Added calculate_bid_ask_spread
from requests.exceptions import HTTPError # Added for specific error handling

# ---- Configuration ----
load_dotenv() # Load .env file for local development

# Configure root logger to stdout
logging.basicConfig(
    stream=sys.stdout, # Cloud Run logs to stdout by default
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---- Constants ----
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
MARKET_DATA_DIR = "market_data" # Used only in local mode
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "kalshi-market-data-storage") # Get from env var
GCS_BASE_PATH = "market_data" # Base 'folder' in GCS
TICKER_FILE = "tickers.txt" # Assumed to be in the container's working directory

# ---- Flask App Setup ----
# Moved initialization after client setup to ensure clients are attempted first

# ---- Google Cloud Clients ----
# Initialize as None at the top level. Actual initialization will be deferred.
secret_manager_client = None
storage_client = None
print(f"--- Top-level script execution (LOCAL_MODE={LOCAL_MODE}) ---", flush=True) # <<< MODIFIED

# Removed top-level client initialization block

print("--- Flask App Initialization START ---", flush=True) # <<< ADDED
try:
    app = Flask(__name__)
    print("--- Flask App Initialization SUCCESS ---", flush=True) # <<< ADDED
except Exception as e:
    print(f"--- CRITICAL ERROR initializing Flask app: {e} ---", flush=True) # <<< ADDED
    print(traceback.format_exc(), flush=True) # <<< ADDED
    raise SystemExit("Failed to initialize Flask app") # Force exit if Flask fails

# ---- Secret Manager Functions ----
def access_secret_version(secret_id, version_id="latest"):
    """Access a secret stored in Google Cloud Secret Manager."""
    if LOCAL_MODE:
        logger.error("Secret Manager access is not supported in LOCAL_MODE.")
        raise NotImplementedError("Secret Manager access is disabled in LOCAL_MODE.")
    if not secret_manager_client:
        raise RuntimeError("Secret Manager client not initialized.")

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") # Use standard Cloud Run env var
    if not project_id:
        # Fallback for local testing if GOOGLE_CLOUD_PROJECT isn't set but gcloud is configured
        project_id = os.getenv("GCP_PROJECT_ID", "kalshi-dashboard-gcp")
        logger.warning(f"GOOGLE_CLOUD_PROJECT not set, falling back to GCP_PROJECT_ID: {project_id}")
        if not project_id:
             raise ValueError("Could not determine GCP Project ID.")

    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = secret_manager_client.access_secret_version(name=name)
        logger.info(f"Successfully accessed secret: {secret_id}")
        # Decode and strip leading/trailing whitespace (like newlines)
        return response.payload.data.decode("UTF-8").strip()
    except Exception as e:
        logger.error(f"Failed to access secret {secret_id} in project {project_id}: {e}")
        raise  # Re-raise the exception

# ---- Kalshi Client Functions ----
def load_client(env=Environment.PROD):
    """Initialize the Kalshi HTTP client with appropriate credentials."""
    # Use the global LOCAL_MODE constant
    logger.info(f"Initializing Kalshi client in {'local' if LOCAL_MODE else 'gcloud'} mode for environment: {env.value}")

    key_id = None
    private_key = None

    try:
        if LOCAL_MODE:
            # Local development mode - load keys from environment variables and files
            logger.info("Using local mode credentials")
            if env == Environment.DEMO:
                key_id = os.getenv('DEMO_KEYID')
                keyfile_path = os.getenv('DEMO_KEYFILE')
                if not key_id or not keyfile_path:
                    raise ValueError("DEMO_KEYID and DEMO_KEYFILE env vars must be set in local mode for DEMO env")
                keyfile_path = os.path.expanduser(keyfile_path)
                logger.info(f"Loading DEMO key ID from env, key file from: {keyfile_path}")
            elif env == Environment.PROD:
                key_id = os.getenv('PROD_KEYID')
                keyfile_path = os.getenv('PROD_KEYFILE')
                if not key_id or not keyfile_path:
                    raise ValueError("PROD_KEYID and PROD_KEYFILE env vars must be set in local mode for PROD env")
                keyfile_path = os.path.expanduser(keyfile_path)
                logger.info(f"Loading PROD key ID from env, key file from: {keyfile_path}")
            else:
                 raise ValueError(f"Unsupported environment for local mode: {env}")

            with open(keyfile_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None  # Assuming key is not password protected
                )

        else:
            # Cloud mode - load keys from Secret Manager
            logger.info("Using gcloud mode credentials (Secret Manager)")
            if env == Environment.DEMO:
                # If Demo secrets are needed, they should be named appropriately in Secret Manager
                # key_id = access_secret_version("demo-keyid")
                # private_key_pem = access_secret_version("demo-keyfile")
                logger.error("Demo environment not currently configured for GCP Secret Manager")
                raise NotImplementedError("Demo environment secrets not configured in GCP")
            elif env == Environment.PROD:
                key_id = access_secret_version("prod-keyid")
                private_key_pem = access_secret_version("prod-keyfile")
                logger.info("Loading PROD credentials from Secret Manager")
                private_key = serialization.load_pem_private_key(
                    private_key_pem.encode('utf-8'),
                    password=None
                )
            else:
                 raise ValueError(f"Unsupported environment for gcloud mode: {env}")

        if not key_id or not private_key:
             raise ValueError("Failed to load key_id or private_key")

        # Create the Kalshi client
        client = KalshiHttpClient(
            key_id=key_id,
            private_key=private_key,
            environment=env
        )

        logger.info("Kalshi client initialized | Environment: %s | API endpoint: %s",
                  env.value, client.base_url)
        return client

    except FileNotFoundError as e:
        logger.error(f"Key file not found: {e.filename}")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize Kalshi client: {type(e).__name__} - {str(e)}")
        raise # Re-raise the exception

# ---- Data Loading/Saving Functions ----
def load_tickers(file=TICKER_FILE):
    """Load ticker symbols from file."""
    try:
        with open(file) as f:
            tickers = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(tickers)} tickers from {file}")
            return tickers
    except FileNotFoundError:
        logger.error(f"Ticker file not found: {file}")
        raise # Stop execution if ticker file is missing

def fetch_and_save_markets(client, tickers):
    """Fetch market data for tickers and save to structured JSON files."""
    fetch_time = datetime.now(timezone.utc)
    fetch_timestamp_str = fetch_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info(f"Starting market data fetch at {fetch_timestamp_str}")

    saved_count = 0
    error_count = 0

    for ticker_input in tickers:
        ticker_type = detect_ticker_type(ticker_input)
        all_markets_for_ticker = [] # Initialize list to accumulate markets
        cursor = None # Initialize cursor

        try:
            logger.info(f"Fetching markets for {ticker_type}: {ticker_input}")

            # Handle single market case separately (no pagination needed)
            if ticker_type == 'market':
                market_data = client.get_market(ticker=ticker_input)
                if market_data:
                    all_markets_for_ticker.append(market_data)
                logger.info(f"Fetched {len(all_markets_for_ticker)} market for single ticker {ticker_input}")

            # Handle series and event cases with pagination
            elif ticker_type in ['series', 'event']:
                while True: # Loop until no more cursors
                    params = {'cursor': cursor}
                    if ticker_type == 'series':
                        params['series_ticker'] = ticker_input
                    else: # ticker_type == 'event'
                        params['event_ticker'] = ticker_input

                    logger.debug(f"Calling get_markets with params: {params}")
                    markets_response = client.get_markets(**params)

                    current_page_markets = markets_response.get('markets', []) if isinstance(markets_response, dict) else []
                    if current_page_markets:
                         all_markets_for_ticker.extend(current_page_markets)
                         logger.debug(f"Accumulated {len(all_markets_for_ticker)} markets so far for {ticker_input}")

                    # Check for next cursor
                    cursor = markets_response.get('cursor') if isinstance(markets_response, dict) else None
                    if not cursor:
                        logger.debug(f"No more cursors found for {ticker_input}.")
                        break # Exit pagination loop
                    else:
                         logger.debug(f"Found next cursor for {ticker_input}: {cursor[:10]}...")

                logger.info(f"Finished fetching all pages. Total markets for {ticker_input}: {len(all_markets_for_ticker)}")

            else: # Unknown type
                logger.warning(f"Unknown ticker type for {ticker_input}, skipping fetch.")
                continue # Skip to next ticker_input

            # Now iterate over the accumulated list of all markets for this ticker_input
            for market in all_markets_for_ticker:
                 # Ensure market is a dictionary before proceeding (should be, but safety check)
                 if not isinstance(market, dict):
                      logger.warning(f"Skipping non-dictionary item found in markets list for {ticker_input}: {market}")
                      continue
                 try:
                    # Determine the correct series/event based on the INPUT ticker type
                    series_ticker_from_input = ticker_input if ticker_type == 'series' else None
                    event_ticker_from_input = ticker_input if ticker_type == 'event' else None

                    # Determine the correct series/event based on the INPUT ticker type
                    series_ticker_from_input = ticker_input if ticker_type == 'series' else None
                    event_ticker_from_input = ticker_input if ticker_type == 'event' else None

                    # Get initial values from market data or input
                    series_ticker = series_ticker_from_input or market.get('series_ticker')
                    event_ticker = event_ticker_from_input or market.get('event_ticker')
                    market_ticker = market.get('ticker')

                    # --- Fallback logic for missing series_ticker ---
                    if not series_ticker and event_ticker:
                        logger.debug(f"Market {market_ticker} missing series_ticker, attempting fallback via get_event({event_ticker})")
                        try:
                            event_details = client.get_event(event_ticker=event_ticker)
                            if event_details and 'event' in event_details and 'series_ticker' in event_details['event']:
                                series_ticker = event_details['event']['series_ticker']
                                logger.debug(f"Found series_ticker '{series_ticker}' for event {event_ticker}")
                                # Optionally add the found series_ticker back to the market data dict
                                market['series_ticker'] = series_ticker
                            else:
                                logger.warning(f"get_event response for {event_ticker} missing expected structure or series_ticker. Response: {event_details}")
                        except Exception as event_err:
                            logger.error(f"Failed to get event details for {event_ticker} to find series_ticker: {event_err}")
                    # --- End Fallback logic ---

                    # Use defaults if still missing after fallback
                    series_ticker = series_ticker or 'unknown_series'
                    event_ticker = event_ticker or 'unknown_event'
                    market_ticker = market_ticker or 'unknown_market'


                    if market_ticker == 'unknown_market':
                        logger.warning(f"Market data missing 'ticker' field for item under {ticker_input}. Data: {market}")
                        continue

                    # --- Add Order Book and Spread Metrics ---
                    orderbook_data = None
                    spread_metrics = None
                    if market_ticker != 'unknown_market': # Only fetch if we have a valid ticker
                        try:
                            logger.info(f"Fetching order book for {market_ticker} (depth=20)...")
                            # Use the existing client instance passed to the function
                            orderbook_response = client.get_market_orderbook(ticker=market_ticker, depth=20)

                            if orderbook_response and 'orderbook' in orderbook_response:
                                # Store the raw orderbook structure {yes: [...], no: [...]}
                                orderbook_data = orderbook_response['orderbook']
                                logger.debug(f"Successfully fetched order book for {market_ticker}.")

                                # Calculate spread metrics using the raw response dict
                                logger.info(f"Calculating spread metrics for {market_ticker}...")
                                spread_metrics = calculate_bid_ask_spread(orderbook_response) # Pass the full response dict
                                logger.debug(f"Calculated spread metrics for {market_ticker}: {spread_metrics}")
                            else:
                                logger.warning(f"Order book response for {market_ticker} was empty or invalid: {orderbook_response}")

                        except HTTPError as http_err:
                            # Log specific HTTP errors
                            if http_err.response.status_code == 404:
                                logger.warning(f"Order book not found (404) for market {market_ticker}. Skipping spread calculation.")
                            else:
                                logger.error(f"HTTP error fetching order book for {market_ticker}: {http_err}", exc_info=False)
                        except ValueError as val_err:
                             # Catch potential errors from calculate_bid_ask_spread if format is bad
                             logger.error(f"Error calculating spread for {market_ticker} (likely invalid orderbook format): {val_err}", exc_info=False)
                        except Exception as e:
                            logger.error(f"Unexpected error fetching/calculating spread for {market_ticker}: {e}", exc_info=True)

                    # Add the fetched/calculated data (or None if errors occurred)
                    market['orderbook_depth_20'] = orderbook_data
                    market['spread_metrics'] = spread_metrics
                    # --- End Order Book / Spread ---


                    # Add fetch timestamp (already exists)
                    market['fetch_timestamp'] = fetch_timestamp_str

                    # --- Saving Logic (Append Mode) ---
                    market_data_list = []
                    if LOCAL_MODE:
                        # Append locally
                        market_dir_local = os.path.join(MARKET_DATA_DIR, series_ticker, event_ticker)
                        market_file_local = os.path.join(market_dir_local, f"{market_ticker}.json")
                        os.makedirs(market_dir_local, exist_ok=True)
                        try:
                            if os.path.exists(market_file_local):
                                with open(market_file_local, 'r') as f:
                                    content = f.read()
                                    if content: # Avoid error on empty file
                                        market_data_list = json.loads(content)
                                        if not isinstance(market_data_list, list):
                                            logger.warning(f"Existing local file {market_file_local} is not a JSON list. Overwriting.")
                                            market_data_list = []
                        except json.JSONDecodeError:
                            logger.warning(f"Could not decode JSON from existing local file {market_file_local}. Overwriting.")
                            market_data_list = []
                        except Exception as read_err:
                             logger.error(f"Error reading local file {market_file_local}: {read_err}. Overwriting.")
                             market_data_list = []

                        market_data_list.append(market) # Append the new market data

                        try:
                            with open(market_file_local, 'w') as f: # Overwrite with updated list
                                json.dump(market_data_list, f, indent=2)
                            logger.debug(f"Appended market data locally to {market_file_local}")
                        except Exception as write_err:
                             logger.error(f"Error writing local file {market_file_local}: {write_err}")
                             error_count += 1
                             continue # Skip saved_count increment

                    else:
                        # Append to GCS
                        if not storage_client:
                             logger.error("Storage client not initialized. Cannot save to GCS.")
                             raise RuntimeError("Storage client not initialized.")
                        try:
                            bucket = storage_client.bucket(GCS_BUCKET_NAME)
                            blob_name = f"{GCS_BASE_PATH}/{series_ticker}/{event_ticker}/{market_ticker}.json"
                            blob = bucket.blob(blob_name)

                            # Download existing data if blob exists
                            if blob.exists():
                                try:
                                    existing_data_str = blob.download_as_string()
                                    if existing_data_str:
                                        market_data_list = json.loads(existing_data_str)
                                        if not isinstance(market_data_list, list):
                                            logger.warning(f"Existing GCS blob gs://{GCS_BUCKET_NAME}/{blob_name} is not a JSON list. Overwriting.")
                                            market_data_list = []
                                except json.JSONDecodeError:
                                     logger.warning(f"Could not decode JSON from existing GCS blob gs://{GCS_BUCKET_NAME}/{blob_name}. Overwriting.")
                                     market_data_list = []
                                except Exception as download_err:
                                     logger.error(f"Error downloading GCS blob gs://{GCS_BUCKET_NAME}/{blob_name}: {download_err}. Overwriting.")
                                     market_data_list = []

                            market_data_list.append(market) # Append the new market data

                            # Upload the updated list, overwriting the blob
                            blob.upload_from_string(
                                data=json.dumps(market_data_list, indent=2),
                                content_type='application/json'
                            )
                            logger.debug(f"Appended market data to GCS: gs://{GCS_BUCKET_NAME}/{blob_name}")

                        except Exception as gcs_e:
                            logger.error(f"Failed to append market {market_ticker} to GCS: {gcs_e}")
                            error_count += 1 # Increment error count for GCS save failure
                            continue # Skip saved_count increment

                    saved_count += 1
                 # This except block catches errors during the processing of a single market
                 # (e.g., fetching orderbook, calculating spread, saving) - CORRECTED INDENTATION
                 except Exception as market_proc_err:
                    logger.error(f"Failed to process/save market {market.get('ticker', 'N/A')}: {market_proc_err}")
                    error_count += 1
        # This except block catches errors during the initial fetch for the ticker_input
        # (e.g., client.get_markets fails)
        except Exception as fetch_err:
            logger.error(f"Failed to fetch markets for {ticker_input}: {str(fetch_err)}")
            error_count += 1

    logger.info(f"Market data fetch completed. Saved: {saved_count}, Errors: {error_count}")
    return saved_count, error_count

# ---- Flask HTTP Endpoints ----
@app.route('/', methods=['GET'])
def hello_world():
    """Simple endpoint for testing."""
    print("--- / endpoint START (GET) ---", flush=True) # <<< ADDED (for testing)
    logger.info(f"Received request at root endpoint. Request path: {request.path}")
    return "Kalshi Data Fetcher is running. Trigger /run endpoint via POST.", 200

@app.route('/run', methods=['POST'])
def run_fetcher():
    """Flask endpoint triggered by Cloud Scheduler."""
    global secret_manager_client, storage_client # Declare intent to modify globals
    print("--- /run endpoint START (POST) ---", flush=True) # <<< ADDED
    port = os.environ.get("PORT")
    logger.info(f"Received request to run data fetcher. Request path: {request.path}, PORT: {port}")
    start_time = datetime.now()

    try:
        # Log environment variables for debugging - REMOVED SENSITIVE LOGGING
        # print(f"--- Environment variables: {os.environ} ---", flush=True) # <<< REMOVED
        # logger.info(f"Environment variables: {os.environ}") # <<< REMOVED
        logger.info("Starting /run endpoint processing.") # Added generic start log

        # --- Deferred GCP Client Initialization ---
        if not LOCAL_MODE:
            print("--- Initializing GCP clients inside /run ---", flush=True)
            try:
                # Initialize only if not already done (though per-request is ok for debug)
                if secret_manager_client is None:
                    secret_manager_client = secretmanager.SecretManagerServiceClient()
                    print("--- Initialized Secret Manager client ---", flush=True)
                if storage_client is None:
                    storage_client = storage.Client()
                    print("--- Initialized Storage client ---", flush=True)
                logger.info("Initialized Google Cloud clients inside /run.")
            except Exception as client_init_err:
                print(f"--- FAILED to initialize GCP clients inside /run: {client_init_err} ---", flush=True)
                print(traceback.format_exc(), flush=True) # Print full traceback
                logger.error("Failed to initialize Google Cloud clients inside /run.", exc_info=True)
                return jsonify({"status": "error", "message": "GCP client initialization failed"}), 500
        else:
            print("--- Skipping GCP client init inside /run (LOCAL_MODE=True) ---", flush=True)
        # --- End Deferred Initialization ---


        # --- Test Secret Access (Only if NOT in Local Mode) ---
        if not LOCAL_MODE:
            print("--- Attempting test secret access ---", flush=True)
            try:
                if secret_manager_client is None:
                     raise RuntimeError("Secret Manager client should have been initialized but is None.")
                test_secret = access_secret_version("prod-keyid")
                print(f"--- Successfully accessed test secret: prod-keyid ---", flush=True)
                logger.info(f"Successfully accessed test secret: prod-keyid")
            except Exception as secret_err:
                print(f"--- Failed to access test secret: {type(secret_err).__name__} ---", flush=True)
                logger.error(f"Failed to access test secret: {type(secret_err).__name__}")
                # Decide if this is fatal
                # return jsonify({"status": "error", "message": f"Failed test secret access: {secret_err}"}), 500
        else:
            print("--- Skipping test secret access (LOCAL_MODE=True) ---", flush=True)
        # --- End Test Secret Access ---


        # Determine environment (e.g., based on an env var, default to PROD if not set)
        # Note: KALSHI_ENV should be set as an environment variable in Cloud Run
        env_str = os.getenv("KALSHI_ENV", "PROD").upper()
        kalshi_env = Environment.PROD if env_str == "PROD" else Environment.DEMO
        logger.info(f"Using Kalshi environment: {kalshi_env.value}")

        # Client initialization is now handled above within the try block

        client = load_client(env=kalshi_env) # load_client needs access_secret_version, which needs the client
        tickers_to_fetch = load_tickers() # Reads from TICKER_FILE in cwd

        if not tickers_to_fetch:
            logger.warning("No tickers loaded.")
            message = "No tickers loaded"
            saved_count = 0
            error_count = 0
        else:
            saved_count, error_count = fetch_and_save_markets(client, tickers_to_fetch)
            message = f"Fetch completed. Saved: {saved_count}, Errors: {error_count}"

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Data fetcher run finished in {duration}. {message}")

        # Return success response
        return jsonify({
            "status": "success",
            "message": message,
            "saved_count": saved_count,
            "error_count": error_count,
            "duration_seconds": duration.total_seconds()
        }), 200

    except FileNotFoundError as e:
        logger.error(f"Critical error: Ticker file '{TICKER_FILE}' not found.", exc_info=True)
        return jsonify({"status": "error", "message": f"Ticker file not found: {e}"}), 500
    except NotImplementedError as e:
        logger.error(f"Configuration error: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    except Exception as e:
        print(f"--- Unhandled exception in /run: {type(e).__name__} - {str(e)} ---", flush=True) # <<< ADDED
        print(traceback.format_exc(), flush=True) # <<< ADDED
        logger.exception(f"Fetcher run failed with unhandled exception: {type(e).__name__} - {str(e)}", exc_info=True)
        # Return error response
        return jsonify({"status": "error", "message": f"Unhandled exception: {str(e)}"}), 500

# Note: The 'if __name__ == "__main__":' block is removed.
# Gunicorn will be used to run the Flask app in the Docker container.
# For local testing (python src/data_fetcher.py), you might add:
# if __name__ == "__main__":
#     # For local debugging only - Cloud Run uses Gunicorn
#     app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
