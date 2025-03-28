import streamlit as st
import time
import json
import os
import logging
import sys
import subprocess
from datetime import datetime, timezone
from pydantic import BaseModel
from src.clients import detect_ticker_type
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import google.cloud.secretmanager as secretmanager
from dotenv import load_dotenv

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

# ---- Container Startup Logs ----
logger.info("Starting Kalshi Dashboard")
logger.info("Environment: %s", "PROD" if os.getenv('PROD_KEYID') else "DEMO")
logger.info("Python version: %s", sys.version.split()[0])
logger.info("Working directory: %s", os.getcwd())
logger.info("Key file location: %s", os.path.basename(os.getenv('PROD_KEYFILE', 'demo.key')))  # Show filename only

# ---- Secret Manager Functions ----
def access_secret_version(secret_id, version_id="latest"):
    """Access a secret stored in Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/kalshi-dashboard-gcp/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to access secret {secret_id}: {e}")
        st.error(f"Failed to access secret {secret_id}: {e}")
        st.stop()

# ---- Data Loading Functions ----
@st.cache_data
def load_markets_from_disk(data_dir="/app/market_data"):
    """Load market data from structured JSON files in the specified directory."""
    all_markets = []
    logger.info(f"Loading market data from disk: {data_dir}")
    start_time = time.time()

    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        market_data = json.load(f)
                        all_markets.append(market_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to load JSON from {file_path}: {e}")
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {e}")

    load_time = time.time() - start_time
    logger.info(f"Loaded {len(all_markets)} markets from disk in {load_time:.2f} seconds")
    return all_markets


@st.cache_data
def check_data_freshness(data_dir="/app/market_data", freshness_threshold_hours=1):
    """Check if market data in data_dir is fresh based on fetch_timestamp."""
    logger.info(f"Checking data freshness in: {data_dir}")
    if not os.path.exists(data_dir) or not os.listdir(data_dir):
        logger.info(f"Data directory '{data_dir}' is empty or does not exist, data is not fresh.")
        return False

    latest_fetch_timestamp = None
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        market_data = json.load(f)
                        timestamp_str = market_data.get('fetch_timestamp')
                        if timestamp_str:
                            # Parse the string and make it timezone-aware (UTC)
                            naive_dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
                            file_fetch_time = naive_dt.replace(tzinfo=timezone.utc)
                            if latest_fetch_timestamp is None or file_fetch_time > latest_fetch_timestamp:
                                latest_fetch_timestamp = file_fetch_time
                except Exception as e:
                    logger.error(f"Error reading timestamp from {file_path}: {e}")

    if latest_fetch_timestamp:
        hours_diff = (datetime.now(timezone.utc) - latest_fetch_timestamp).total_seconds() / 3600
        is_fresh = hours_diff <= freshness_threshold_hours
        logger.info(f"Latest data fetch timestamp: {latest_fetch_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}, Data is {'fresh' if is_fresh else 'stale'} ({hours_diff:.2f} hours old, threshold={freshness_threshold_hours} hours)")
        return is_fresh
    else:
        logger.info("No fetch timestamps found in data directory, data is not fresh.")
        return False


def load_tickers(file="tickers.txt"): # Keep load_tickers for now, might be used later
    """Load ticker symbols from file."""
    try:
        with open(file) as f:
            tickers = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(tickers)} tickers from {file}")
            return tickers
    except FileNotFoundError:
        logger.error(f"Ticker file not found: {file}")
        st.error(f"Ticker file not found: {file}")
        st.stop()


# ---- UI Rendering Functions ----
def display_market_card(market):
    """Display market information in a streamlit card."""
    st.divider()
    bet_markdown = f"##### {market['title']}\n"
    bet_markdown += f"###### {market['yes_sub_title']} - {market['ticker']}\n"
    
    # Add event and series ticker information if available
    if 'event_ticker' in market:
        bet_markdown += f"Event: {market['event_ticker']}\n"
    if 'series_ticker' in market:
        bet_markdown += f"Series: {market['series_ticker']}\n\n"
    
    bet_markdown += f"Yes Bid: {market['yes_bid']}, Yes Ask {market['yes_ask']}\n\n"
    bet_markdown += f"No bid: {market['no_bid']}, No Ask {market['no_ask']}\n\n"
    bet_markdown += f"Volume (24 h): {market['volume_24h']}\n\n"
    st.markdown(bet_markdown)

def filter_markets_by_search(markets, search_term):
    """Filter markets based on search term."""
    if not search_term:
        return markets
    
    filtered_markets = []
    for market in markets:
        if search_term.lower() in market['title'].lower():
            filtered_markets.append(market)
    
    logger.info(f"Found {len(filtered_markets)} markets matching search term: {search_term}")
    return filtered_markets

def categorize_markets(markets):
    """Group markets by category."""
    categories = {}
    for market in markets:
        category = market.get('category', 'Uncategorized')
        if category not in categories:
            categories[category] = []
        categories[category].append(market)
    
    return categories

def display_analysis(analysis):
    """Display analysis in a modal dialog."""
    with st.modal("Analysis"):
        st.write(analysis)

def main():
    st.header("Prediction Market Assistant")
    
    st.write("Data is loaded from disk, updated periodically by `src/data_fetcher.py`.")
    st.write("Refresh this page to reload market data, possibly triggering data fetch if needed.")
    st.divider()

    # Check data freshness and load/fetch accordingly
    data_dir = "/app/market_data"
    all_markets = [] # Initialize empty list

    if check_data_freshness(data_dir):
        st.write("Market data is fresh.")
        # Load fresh data from disk
        start_time = time.time()
        st.write("Loading Market Data from Disk...")
        all_markets = load_markets_from_disk(data_dir)
        load_time = time.time() - start_time
        st.write(f"Loaded {len(all_markets)} markets from disk in {load_time:.2f} seconds")
    else:
        st.write("Market data is not fresh, fetching new data...")
        fetcher_start_time = time.time()
        
        command = f"{sys.executable} src/data_fetcher.py" # Use sys.executable
        logger.info(f"Executing data fetcher: {command}")
        
        try:
            # Execute data_fetcher.py
            subprocess.run([sys.executable, 'src/data_fetcher.py'], check=True, capture_output=True, text=True) # Capture output
            
            fetcher_duration = time.time() - fetcher_start_time
            st.write(f"Data fetcher script executed in {fetcher_duration:.2f} seconds.")
            st.success("Data fetch successful.")

            # Now load the newly fetched data
            st.write("Loading newly fetched Market Data from Disk...")
            start_time = time.time()
            all_markets = load_markets_from_disk(data_dir) # Load data after fetch
            load_time = time.time() - start_time
            st.write(f"Loaded {len(all_markets)} markets from disk in {load_time:.2f} seconds")

        except subprocess.CalledProcessError as e:
            st.error(f"Error running data fetcher: {e}")
            logger.error(f"Data fetcher failed. Return code: {e.returncode}")
            logger.error(f"Stdout: {e.stdout}")
            logger.error(f"Stderr: {e.stderr}")
            st.text_area("Fetcher Error Output", e.stderr, height=200)
            st.warning("Could not fetch fresh data. Dashboard may be empty or display stale information if available.")
            # Attempt to load potentially stale data as a fallback if fetch fails
            if os.path.exists(data_dir) and os.listdir(data_dir):
                 st.write("Attempting to load existing (stale) data...")
                 all_markets = load_markets_from_disk(data_dir)
            else:
                 st.error("No market data available.")
                 st.stop() # Stop if fetch fails and no prior data exists

        except Exception as e:
            st.error(f"An unexpected error occurred during data fetch: {e}")
            logger.error(f"Unexpected error during data fetch: {e}", exc_info=True)
            st.warning("Could not fetch fresh data due to an unexpected error.")
            # Attempt to load potentially stale data as a fallback
            if os.path.exists(data_dir) and os.listdir(data_dir):
                 st.write("Attempting to load existing (stale) data...")
                 all_markets = load_markets_from_disk(data_dir)
            else:
                 st.error("No market data available.")
                 st.stop() # Stop if fetch fails unexpectedly and no prior data exists

    # Display sample structure only if data was successfully loaded
    if all_markets: # Check if list is not empty
        st.write("Sample market structure (loaded):", all_markets[-1])
    elif not trigger_data_fetch: # Only show if not attempting fetch
         st.warning("No market data loaded.")
    
    # Search functionality
    search = st.text_input("Search Markets")
    
    # Display filter options - 3 columns layout
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Category filter
        categories = categorize_markets(all_markets)
        category_options = ["All Categories"] + sorted(categories.keys())
        selected_category = st.selectbox("Categories (Optional)", category_options)
    
    with col2:
        # Series/Event filter
        display_by_options = ["All", "Series", "Event"]
        display_by = st.selectbox("Display by", display_by_options)
        
        if display_by != "All":
            # Get unique series or event tickers based on selection
            if display_by == "Series":
                unique_identifiers = sorted(list(set([m.get('series_ticker', '') for m in all_markets if 'series_ticker' in m])))
            else:  # display_by == "Event"
                unique_identifiers = sorted(list(set([m.get('event_ticker', '') for m in all_markets if 'event_ticker' in m])))
            
            # Add "All" option at the beginning
            unique_identifiers = ["All"] + unique_identifiers
            selected_identifier = st.selectbox(f"Select {display_by}", unique_identifiers)
    
    with col3:
        # Status filter
        statuses = sorted(list(set([m.get('status', 'unknown') for m in all_markets if 'status' in m])))
        status_options = ["All Statuses"] + statuses
        selected_status = st.selectbox("Status", status_options)
    
    # Sorting options
    sort_options = ["Default", "Volume (High to Low)", "Volume 24h (High to Low)", 
                    "Liquidity (High to Low)", "Open Interest (High to Low)"]
    sort_by = st.selectbox("Sort by", sort_options)
    
    # Apply filters
    filtered_markets = filter_markets_by_search(all_markets, search)
    
    if selected_category != "All Categories":
        filtered_markets = [m for m in filtered_markets if m.get('category') == selected_category]
    
    # Apply Series/Event filter if selected
    if display_by != "All" and selected_identifier != "All":
        if display_by == "Series":
            filtered_markets = [m for m in filtered_markets if m.get('series_ticker') == selected_identifier]
        else:  # display_by == "Event"
            filtered_markets = [m for m in filtered_markets if m.get('event_ticker') == selected_identifier]
    
    # Apply status filter
    if selected_status != "All Statuses":
        filtered_markets = [m for m in filtered_markets if m.get('status') == selected_status]
    
    # Apply sorting
    if sort_by != "Default":
        if sort_by == "Volume (High to Low)":
            filtered_markets = sorted(filtered_markets, key=lambda x: x.get('volume', 0), reverse=True)
        elif sort_by == "Volume 24h (High to Low)":
            filtered_markets = sorted(filtered_markets, key=lambda x: x.get('volume_24h', 0), reverse=True)
        elif sort_by == "Liquidity (High to Low)":
            filtered_markets = sorted(filtered_markets, key=lambda x: x.get('liquidity', 0), reverse=True)
        elif sort_by == "Open Interest (High to Low)":
            filtered_markets = sorted(filtered_markets, key=lambda x: x.get('open_interest', 0), reverse=True)
    
    # Display markets
    st.subheader(f"Displaying {len(filtered_markets)} markets")
    
    # Group markets by series or event if appropriate
    if display_by != "All" and selected_identifier == "All":
        # Group by selected filter type
        if display_by == "Series":
            grouped_markets = {}
            for market in filtered_markets:
                group_key = market.get('series_ticker', 'Uncategorized')
                if group_key not in grouped_markets:
                    grouped_markets[group_key] = []
                grouped_markets[group_key].append(market)
            
            # Display grouped markets
            for group_key, markets in sorted(grouped_markets.items()):
                st.markdown(f"### Series: {group_key}")
                st.write(f"{len(markets)} markets")
                for market in markets:
                    display_market_card(market)
        else:  # display_by == "Event"
            grouped_markets = {}
            for market in filtered_markets:
                group_key = market.get('event_ticker', 'Uncategorized')
                if group_key not in grouped_markets:
                    grouped_markets[group_key] = []
                grouped_markets[group_key].append(market)
            
            # Display grouped markets
            for group_key, markets in sorted(grouped_markets.items()):
                st.markdown(f"### Event: {group_key}")
                st.write(f"{len(markets)} markets")
                for market in markets:
                    display_market_card(market)
    else:
        # Display markets without grouping
        for market in filtered_markets:
            display_market_card(market)

if __name__ == "__main__":
    main()
