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
from src.clients import KalshiHttpClient, Environment, detect_ticker_type

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
app = Flask(__name__)

# ---- Google Cloud Clients ----
# Initialize clients only if not in local mode to avoid unnecessary auth attempts locally
secret_manager_client = None
storage_client = None
if not LOCAL_MODE:
    try:
        secret_manager_client = secretmanager.SecretManagerServiceClient()
        storage_client = storage.Client() # Assumes ADC or service account credentials
        logger.info("Initialized Google Cloud clients.")
    except Exception as e:
        logger.error(f"Failed to initialize Google Cloud clients: {e}. Ensure credentials are set up correctly for non-local mode.")
        # Depending on requirements, might want to raise here or handle later

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
        return response.payload.data.decode("UTF-8")
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
        markets = []
        try:
            logger.debug(f"Fetching markets for {ticker_type}: {ticker_input}")
            if ticker_type == 'series':
                markets = client.get_markets(series_ticker=ticker_input)
            elif ticker_type == 'event':
                 markets = client.get_markets(event_ticker=ticker_input)
            elif ticker_type == 'market':
                 # Assuming get_market method exists or get_markets can handle single market tickers
                 # Adjust if API requires a different method for single market tickers
                 market_data = client.get_market(ticker=ticker_input) # Hypothetical method
                 markets = [market_data] if market_data else []
            else:
                logger.warning(f"Unknown ticker type for {ticker_input}, skipping.")
                continue

            logger.info(f"Fetched {len(markets)} markets for ticker {ticker_input}")

            for market in markets:
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

                    # Add fetch timestamp
                    market['fetch_timestamp'] = fetch_timestamp_str

                    # --- Saving Logic ---
                    if LOCAL_MODE:
                        # Save locally
                        market_dir_local = os.path.join(MARKET_DATA_DIR, series_ticker, event_ticker)
                        market_file_local = os.path.join(market_dir_local, f"{market_ticker}.json")
                        os.makedirs(market_dir_local, exist_ok=True)
                        with open(market_file_local, 'w') as f:
                            json.dump(market, f, indent=2)
                        logger.debug(f"Saved market data locally to {market_file_local}")
                    else:
                        # Save to GCS
                        if not storage_client:
                             logger.error("Storage client not initialized. Cannot save to GCS.")
                             raise RuntimeError("Storage client not initialized.")
                        try:
                            bucket = storage_client.bucket(GCS_BUCKET_NAME)
                            # Construct GCS path: market_data/series_ticker/event_ticker/market_ticker.json
                            blob_name = f"{GCS_BASE_PATH}/{series_ticker}/{event_ticker}/{market_ticker}.json"
                            blob = bucket.blob(blob_name)
                            # Upload data as JSON string
                            blob.upload_from_string(
                                data=json.dumps(market, indent=2),
                                content_type='application/json'
                            )
                            logger.debug(f"Saved market data to GCS: gs://{GCS_BUCKET_NAME}/{blob_name}")
                        except Exception as gcs_e:
                            logger.error(f"Failed to save market {market_ticker} to GCS: {gcs_e}")
                            error_count += 1 # Increment error count for GCS save failure
                            continue # Skip saved_count increment for this market

                    saved_count += 1

                except Exception as e:
                    logger.error(f"Failed to process/save market {market.get('ticker', 'N/A')}: {e}")
                    error_count += 1

        except Exception as e:
            logger.error(f"Failed to fetch markets for {ticker_input}: {str(e)}")
            error_count += 1

    logger.info(f"Market data fetch completed. Saved: {saved_count}, Errors: {error_count}")
    return saved_count, error_count

# ---- Flask HTTP Endpoint ----
@app.route('/run', methods=['POST'])
def run_fetcher():
    """Flask endpoint triggered by Cloud Scheduler."""
    logger.info("Received request to run data fetcher.")
    start_time = datetime.now()

    try:
        # Determine environment (e.g., based on an env var, default to PROD if not set)
        # Note: KALSHI_ENV should be set as an environment variable in Cloud Run
        env_str = os.getenv("KALSHI_ENV", "PROD").upper()
        kalshi_env = Environment.PROD if env_str == "PROD" else Environment.DEMO
        logger.info(f"Using Kalshi environment: {kalshi_env.value}")

        # Ensure clients are initialized if not in local mode
        if not LOCAL_MODE and (not secret_manager_client or not storage_client):
             logger.error("Cloud clients were not initialized properly.")
             return jsonify({"status": "error", "message": "Cloud client initialization failed"}), 500

        client = load_client(env=kalshi_env)
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
        logger.critical(f"Fetcher run failed with unhandled exception: {type(e).__name__} - {str(e)}", exc_info=True)
        # Return error response
        return jsonify({"status": "error", "message": f"Unhandled exception: {str(e)}"}), 500

# Note: The 'if __name__ == "__main__":' block is removed.
# Gunicorn will be used to run the Flask app in the Docker container.
# For local testing (python src/data_fetcher.py), you might add:
# if __name__ == "__main__":
#     # For local debugging only - Cloud Run uses Gunicorn
#     app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
