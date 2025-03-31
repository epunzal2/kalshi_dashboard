LOG_FILE = None
import os
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import time
from dotenv import load_dotenv
import traceback # Added for detailed error logging

# Assuming src is importable (e.g., running with python -m tests.test_kalshi_client_final)
# Adjust import path if necessary based on execution context
try:
    from src.clients import KalshiHttpClient, Environment, detect_ticker_type, calculate_bid_ask_spread
    from cryptography.hazmat.primitives import serialization
except ImportError as e:
    print(f"Error importing src.clients: {e}. Ensure PYTHONPATH is set or run with -m.")
    # Attempt relative import as fallback if running directly from project root
    try:
        # Assuming tests/ is one level down from project root where src/ is
        import sys
        # Add project root to path if not already there
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from src.clients import KalshiHttpClient, Environment, detect_ticker_type, calculate_bid_ask_spread
        from cryptography.hazmat.primitives import serialization
    except ImportError:
        raise ImportError(f"Could not import src.clients. Original error: {e}")


# --- Configuration ---
# Load .env.local specifically if it exists, otherwise fall back to .env
dotenv_path = Path('.env.local')
if not dotenv_path.is_file():
    dotenv_path = Path('.env') # Fallback to .env

if dotenv_path.is_file():
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(f"Loaded environment variables from: {dotenv_path}")
else:
    print("Warning: No .env or .env.local file found. Relying on system environment variables.")


TICKER_FILE = Path("tickers.txt") # Assuming relative to project root

# --- Logging Setup ---
# Basic console logger
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# File logger (will be added in main test function after directory creation)
file_handler = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Capture all levels
# Prevent adding handlers multiple times if script is re-run in same process
if not logger.handlers:
    logger.addHandler(console_handler)
# Remove basicConfig if it was set elsewhere to avoid conflicts
# logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')


# --- Helper Functions ---

def load_test_credentials(env: Environment):
    """Loads API credentials for the specified test environment."""
    logger.info(f"Loading credentials for {env.value} environment...")
    key_id = None
    private_key = None
    keyfile_path_str = None

    if env == Environment.DEMO:
        key_id = os.getenv('DEMO_KEYID')
        keyfile_path_str = os.getenv('DEMO_KEYFILE')
        if not key_id:
            raise ValueError("DEMO_KEYID environment variable not set.")
        if not keyfile_path_str:
            raise ValueError("DEMO_KEYFILE environment variable not set.")
        logger.info(f"Found DEMO_KEYID: {key_id[:4]}... , DEMO_KEYFILE: {keyfile_path_str}")
    elif env == Environment.PROD:
        key_id = os.getenv('PROD_KEYID')
        keyfile_path_str = os.getenv('PROD_KEYFILE')
        if not key_id:
            raise ValueError("PROD_KEYID environment variable not set.")
        if not keyfile_path_str:
            raise ValueError("PROD_KEYFILE environment variable not set.")
        logger.info(f"Found PROD_KEYID: {key_id[:4]}... , PROD_KEYFILE: {keyfile_path_str}")
    else:
        raise ValueError(f"Testing environment {env.value} not supported/configured")

    keyfile_path = Path(os.path.expanduser(keyfile_path_str))
    logger.info(f"Attempting to load key file from resolved path: {keyfile_path}")

    if not keyfile_path.is_file():
        raise FileNotFoundError(f"Key file not found at {keyfile_path}")

    try:
        with open(keyfile_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )
        logger.info(f"Successfully loaded private key for Key ID: {key_id}")
        return key_id, private_key
    except Exception as e:
        logger.error(f"Failed to load private key from {keyfile_path}: {e}", exc_info=True)
        raise

def load_test_tickers(file_path: Path):
    """Loads tickers from the specified file."""
    if not file_path.is_file():
        logger.error(f"Ticker file not found: {file_path}")
        return None, None, None # Return None for all three

    tickers = []
    try:
        with open(file_path) as f:
            tickers = [line.strip() for line in f if line.strip() and not line.startswith('#')] # Ignore comments
    except Exception as e:
        logger.error(f"Failed to read ticker file {file_path}: {e}")
        return None, None, None

    if not tickers:
        logger.error(f"No valid tickers found in {file_path}")
        return None, None, None

    # Find the first event, market, and series ticker for testing specific endpoints
    event_ticker = next((t for t in tickers if detect_ticker_type(t) == 'event'), None)
    market_ticker = next((t for t in tickers if detect_ticker_type(t) == 'market'), None)
    series_ticker = next((t for t in tickers if detect_ticker_type(t) == 'series'), None)


    if not event_ticker: logger.warning("Could not find an event ticker in tickers.txt for testing.")
    if not market_ticker: logger.warning("Could not find a market ticker in tickers.txt for testing.")
    if not series_ticker: logger.warning("Could not find a series ticker in tickers.txt for testing.")


    logger.info(f"Loaded {len(tickers)} tickers. Using event='{event_ticker}', market='{market_ticker}', series='{series_ticker}' for tests.")
    return event_ticker, market_ticker, series_ticker


def run_api_test(client: KalshiHttpClient, endpoint_name: str, method_to_call, *args, **kwargs):
    """Runs a single API endpoint test and logs results."""
    global file_handler # Allow modification of the file handler
    start_time = datetime.now(timezone.utc)
    timestamp_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info(f"[{timestamp_str}] Testing endpoint: {endpoint_name}...")

    result_data = None
    error_data = None
    success = False

    try:
        result_data = method_to_call(*args, **kwargs)
        success = True
        logger.info(f"[{timestamp_str}] SUCCESS: {endpoint_name}")
    except Exception as e:
        error_data = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc() # Use traceback module
        }
        logger.error(f"[{timestamp_str}] FAILED: {endpoint_name} - {type(e).__name__}: {str(e)}")

    # Log detailed results to file
    log_entry = {
        "timestamp": timestamp_str,
        "endpoint": endpoint_name,
        "status": "success" if success else "failure",
        "args": [str(a) for a in args], # Basic string representation
        "kwargs": {k: str(v) for k, v in kwargs.items()}, # Basic string representation
        "response": result_data,
        "error": error_data,
    }

    # Ensure file handler is configured before writing
    if file_handler:
        try:
            # Use a dedicated file logger or write directly
            with open(LOG_FILE, 'a') as f:
                 # Use default=str for non-serializable objects like datetime
                 json.dump(log_entry, f, indent=2, default=str)
                 f.write("\n---\n") # Separator
        except Exception as log_e:
             print(f"CRITICAL: Failed to write to log file {LOG_FILE}: {log_e}") # Use print as logger might fail
    else:
         print(f"CRITICAL: File logger not initialized. Cannot write log entry for {endpoint_name}.")


    return success

# --- Main Test Execution ---

def load_orderbook_from_json(file_path: Path) -> dict:
    """Loads orderbook data from a JSON file."""
    if not file_path.is_file():
        raise FileNotFoundError(f"Orderbook file not found: {file_path}")

    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise ValueError(f"Failed to load orderbook from {file_path}: {e}")


def test_calculate_bid_ask_spread():
    """Tests the calculate_bid_ask_spread function with a sample orderbook."""
    try:
        orderbook_data = load_orderbook_from_json(Path("tests/sample_orderbook.json"))
        results = calculate_bid_ask_spread(orderbook_data)

        # Assertions based on the data in tests/sample_orderbook.json
        assert results['best_bid'] == 41, "Best bid should be 41"
        assert results['best_ask'] == 11, "Best ask should be 11"
        assert results['spread'] == -30, "Spread should be -30"
        # Add more assertions to validate other metrics

    except Exception as e:
        logger.error(f"Test test_calculate_bid_ask_spread failed: {e}", exc_info=True)
        raise # Re-raise to mark the test as failed


def main():
    """Main function to run the Kalshi client tests."""
    global file_handler  # Allow modification

    for env in [Environment.DEMO, Environment.PROD]:
        print(f"--- Starting Kalshi Client Test Run for {env.value} ---")
        overall_start_time = datetime.now(timezone.utc)

        # 1. Create output directory
        timestamp_str = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M')
        test_output_dir_name = f"{timestamp_str}_{env.value}"
        TEST_OUTPUT_DIR = Path("tests") / test_output_dir_name
        global LOG_FILE
        LOG_FILE = TEST_OUTPUT_DIR / "test_run.log"

        try:
            TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            print(f"Ensured test output directory exists: {TEST_OUTPUT_DIR}")
        except Exception as e:
            print(f"CRITICAL ERROR: Failed to ensure output directory {TEST_OUTPUT_DIR}: {e}")
            continue  # Move to the next environment

        # 2. Configure file logging
        try:
            # Clear existing log file if it exists
            if LOG_FILE.exists():
                LOG_FILE.unlink()
                print(f"Cleared existing log file: {LOG_FILE}")

            file_handler = logging.FileHandler(LOG_FILE)
            file_handler.setLevel(logging.DEBUG)  # Log everything to file
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logger.addHandler(file_handler)
            logger.info(f"File logging configured to: {LOG_FILE}")
        except Exception as e:
            print(f"CRITICAL ERROR: Failed to configure file logging to {LOG_FILE}: {e}")
            # Continue without file logging? Or exit? Let's try to continue.
            file_handler = None  # Ensure it's None if setup failed

        # 3. Load Credentials & Tickers
        try:
            key_id, private_key = load_test_credentials(env)
            event_ticker, market_ticker, series_ticker = load_test_tickers(TICKER_FILE)
        except (ValueError, FileNotFoundError) as e:
            logger.critical(f"Setup failed: {e}. Aborting tests for {env.value}.")
            continue  # Move to the next environment
        except Exception as e:
            logger.critical(f"Unexpected error during setup: {e}", exc_info=True)
            continue

        # Check if essential tickers were found
        if not market_ticker:
            logger.critical("Market ticker not found in tickers file. Cannot run market-specific tests. Aborting.")
            continue
        # Add similar checks for event_ticker/series_ticker if endpoints absolutely require them

        # 4. Instantiate Client
        try:
            client = KalshiHttpClient(key_id=key_id, private_key=private_key, environment=env)
            logger.info(f"KalshiHttpClient instantiated for {env.value}")
        except Exception as e:
            logger.critical(f"Failed to instantiate KalshiHttpClient: {e}", exc_info=True)
            continue

        # 5. Define and Run Tests
        tests_passed = 0
        tests_failed = 0

        # Example tests - select a representative subset
        test_cases = [
            ("get_exchange_status", client.get_exchange_status),
            # Add more tests, ensuring required tickers are available
            ("calculate_bid_ask_spread", test_calculate_bid_ask_spread) # Moved here
        ]
        if event_ticker:
            # Pass kwargs as a dictionary for clarity when using _make_request indirectly
            test_cases.append(("get_event", client.get_event, event_ticker))
            # Removed get_events_by_event test as client method doesn't support event_ticker filter
        if series_ticker:
            test_cases.append(("get_events_by_series", client.get_events, (), {"series_ticker": series_ticker, "limit": 5}))
        if market_ticker:
            test_cases.append(("get_market", client.get_market, market_ticker))
            test_cases.append(("get_market_orderbook", client.get_market_orderbook, market_ticker))
            # Candlesticks require timestamps - calculate recent range
            now_ts = int(time.time())
            start_ts = now_ts - (60 * 60 * 24)  # 1 day ago
            # Ensure series_ticker is available for candlesticks
            if series_ticker:
                # Pass positional args correctly for get_market_candlesticks
                test_cases.append(("get_market_candlesticks", client.get_market_candlesticks, (series_ticker, market_ticker, start_ts, now_ts, 60)))  # 60 min interval
            else:
                logger.warning("Skipping get_market_candlesticks test: series_ticker not found.")
            test_cases.append(("get_trades", client.get_trades, (), {"ticker": market_ticker, "limit": 10}))
        # Portfolio tests (require account activity/setup in DEMO) - uncomment cautiously
        # test_cases.append(("get_balance", client.get_balance))
        # test_cases.append(("get_positions", client.get_positions, (), {"limit": 10}))
        # test_cases.append(("get_orders", client.get_orders, (), {"limit": 10}))
        # test_cases.append(("get_fills", client.get_fills, (), {"limit": 10}))


        for test_info in test_cases:
            name = test_info[0]
            method = test_info[1]
            # Handle args/kwargs based on test_info structure
            args = []
            kwargs = {}
            if len(test_info) > 2:
                if isinstance(test_info[2], dict):
                    kwargs = test_info[2]
                elif isinstance(test_info[2], (list, tuple)):
                    args = test_info[2]
                else:  # Assume single positional argument
                    args = [test_info[2]]
            if len(test_info) > 3 and isinstance(test_info[3], dict):
                kwargs = test_info[3]  # Allow specifying kwargs separately

            if run_api_test(client, name, method, *args, **kwargs):
                tests_passed += 1
            else:
                tests_failed += 1
            time.sleep(0.2)  # Small delay between API calls

        # 6. Log Summary
        overall_end_time = datetime.now(timezone.utc)
        duration = overall_end_time - overall_start_time
        summary = f"--- Test Run Summary for {env.value} ---\n" \
                  f"Completed At: {overall_end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n" \
                  f"Duration: {duration}\n" \
                  f"Tests Passed: {tests_passed}\n" \
                  f"Tests Failed: {tests_failed}\n" \
                  f"Detailed logs: {LOG_FILE.resolve()}"
        logger.info(summary)
        print(summary)  # Also print summary to console

if __name__ == "__main__":
    main()
