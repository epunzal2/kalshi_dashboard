import os
import logging
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
import google.cloud.secretmanager as secretmanager
from src.clients import KalshiHttpClient, Environment, detect_ticker_type

# ---- Configuration ----
load_dotenv()

# Configure root logger to stdout
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---- Constants ----
MARKET_DATA_DIR = "market_data"
TICKER_FILE = "tickers.txt"

# ---- Secret Manager Functions ----
def access_secret_version(secret_id, version_id="latest"):
    """Access a secret stored in Google Cloud Secret Manager."""
    # TODO: Replace 'kalshi-dashboard-gcp' with your actual GCP project ID if different
    project_id = os.getenv("GCP_PROJECT_ID", "kalshi-dashboard-gcp")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(name=name)
        logger.info(f"Successfully accessed secret: {secret_id}")
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to access secret {secret_id} in project {project_id}: {e}")
        # In a script, we might want to raise the exception or exit
        raise  # Re-raise the exception to halt execution if secrets are critical

# ---- Kalshi Client Functions ----
def load_client(env=Environment.PROD):
    """Initialize the Kalshi HTTP client with appropriate credentials."""
    local_mode = os.getenv("LOCAL_MODE", "false").lower() == "true"
    logger.info(f"Initializing Kalshi client in {'local' if local_mode else 'gcloud'} mode for environment: {env.value}")

    key_id = None
    private_key = None

    try:
        if local_mode:
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
        logger.info(f"Processing ticker from file: {ticker_input}") # Confirm processing start
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

                    # Define directory and file path using potentially updated series_ticker
                    market_dir = os.path.join(MARKET_DATA_DIR, series_ticker, event_ticker)
                    market_file = os.path.join(market_dir, f"{market_ticker}.json")

                    # Create directories if they don't exist
                    os.makedirs(market_dir, exist_ok=True)

                    # Save data to JSON file
                    with open(market_file, 'w') as f:
                        json.dump(market, f, indent=2)
                    logger.debug(f"Saved market data to {market_file}")
                    saved_count += 1

                except Exception as e:
                    logger.error(f"Failed to process/save market {market.get('ticker', 'N/A')}: {e}")
                    error_count += 1

        except Exception as e:
            logger.error(f"Failed to fetch markets for {ticker_input}: {str(e)}")
            error_count += 1

    logger.info(f"Market data fetch completed. Saved: {saved_count}, Errors: {error_count}")
    return saved_count, error_count

# ---- Main Execution ----
if __name__ == "__main__":
    logger.info("Starting data fetcher script")
    start_time = datetime.now()

    try:
        # Determine environment (e.g., based on an env var, default to PROD if not set)
        env_str = os.getenv("KALSHI_ENV", "PROD").upper()
        kalshi_env = Environment.PROD if env_str == "PROD" else Environment.DEMO

        client = load_client(env=kalshi_env)
        tickers_to_fetch = load_tickers()

        if not tickers_to_fetch:
            logger.warning("No tickers loaded, exiting.")
        else:
            fetch_and_save_markets(client, tickers_to_fetch)

    except Exception as e:
        logger.critical(f"Script failed with unhandled exception: {type(e).__name__} - {str(e)}", exc_info=True)
        sys.exit(1) # Exit with error code

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Data fetcher script finished in {duration}")
    sys.exit(0) # Exit successfully
