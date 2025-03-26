import streamlit as st
import requests
import time
import json
import os
import logging
import sys
from datetime import datetime
from pydantic import BaseModel
from src.clients import KalshiHttpClient, Environment, detect_ticker_type
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

# ---- Kalshi Client Functions ----
def load_client(env=Environment.PROD):
    """Initialize the Kalshi HTTP client with appropriate credentials."""
    local_mode = os.getenv("LOCAL_MODE", "false").lower() == "true"
    logger.info(f"Initializing Kalshi client in {'local' if local_mode else 'gcloud'} mode")
    
    try:
        if local_mode:
            # Local development mode - load keys from environment variables and files
            if env == Environment.DEMO:
                key_id = os.getenv('DEMO_KEYID')
                keyfile_path = os.path.expanduser(os.getenv('DEMO_KEYFILE'))
                
                with open(keyfile_path, "rb") as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None
                    )
            elif env == Environment.PROD:
                key_id = os.getenv('PROD_KEYID')
                keyfile_path = os.path.expanduser(os.getenv('PROD_KEYFILE'))
                
                with open(keyfile_path, "rb") as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None
                    )
        else:
            # Cloud mode - load keys from Secret Manager
            if env == Environment.DEMO:
                logger.error("Demo mode not supported with GCP secrets")
                st.error("Demo mode not supported with GCP secrets")
                st.stop()
            elif env == Environment.PROD:
                key_id = access_secret_version("prod-keyid")
                private_key_pem = access_secret_version("prod-keyfile")
                
                private_key = serialization.load_pem_private_key(
                    private_key_pem.encode('utf-8'),
                    password=None
                )
        
        # Create the Kalshi client with the loaded credentials
        client = KalshiHttpClient(
            key_id=key_id,
            private_key=private_key,
            environment=env
        )
        
        logger.info("Kalshi client initialized | Environment: %s | API endpoint: %s",
                  env.value, client.base_url)
        return client
        
    except Exception as e:
        logger.error(f"Failed to initialize Kalshi client: {type(e).__name__} - {str(e)}")
        st.error(f"Failed to initialize Kalshi client: {type(e).__name__}")
        st.stop()

# ---- Data Loading Functions ----
@st.cache_data
def load_tickers(file="tickers.txt"):
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

@st.cache_data
def fetch_markets_data(_client, tickers):
    """Fetch market data for all tickers."""
    all_markets = []
    errors = []
    
    for ticker in tickers:
        ticker_type = detect_ticker_type(ticker)
        try:
            if ticker_type == 'series':
                markets = _client.get_markets(series_ticker=ticker)
                # Add series_ticker to each market if not already present
                for market in markets:
                    if 'series_ticker' not in market:
                        market['series_ticker'] = ticker
            else:  # Handle event tickers
                markets = _client.get_markets(event_ticker=ticker)
                # Ensure event_ticker is explicitly set in each market
                for market in markets:
                    if 'event_ticker' not in market:
                        market['event_ticker'] = ticker

            all_markets.extend(markets)
            logger.info(f"Fetched {len(markets)} markets for ticker {ticker}")
        except Exception as e:
            error_msg = f"Failed to fetch markets for {ticker}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
    
    if errors:
        st.warning(f"Encountered {len(errors)} errors while fetching market data")
    
    return all_markets

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

# ---- Main Application ----
def main():
    st.header("Kalshi Prediction Market Dashboard")
    
@st.cache_data(ttl=86400, persist="disk")  # Cache data for 1 day (24 hours * 60 minutes * 60 seconds)
def fetch_markets_data(_client, tickers):
    """Fetch market data for all tickers."""
    all_markets = []
    errors = []
    
    for ticker in tickers:
        ticker_type = detect_ticker_type(ticker)
        try:
            if ticker_type == 'series':
                markets = _client.get_markets(series_ticker=ticker)
                # Add series_ticker to each market if not already present
                for market in markets:
                    if 'series_ticker' not in market:
                        market['series_ticker'] = ticker
            else:  # Handle event tickers
                markets = _client.get_markets(event_ticker=ticker)
                # Ensure event_ticker is explicitly set in each market
                for market in markets:
                    if 'event_ticker' not in market:
                        market['event_ticker'] = ticker

            all_markets.extend(markets)
            logger.info(f"Fetched {len(markets)} markets for ticker {ticker}")
        except Exception as e:
            error_msg = f"Failed to fetch markets for {ticker}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
    
    if errors:
        logger.warning(f"Encountered {len(errors)} errors while fetching market data")
    
    return all_markets

def main():
    st.header("Prediction Market Assistant")
    
    # Add refresh button at the top
    if st.button("🔄 Refresh Market Data"):
        st.write("Clearing cache and refreshing data...")
        # Clear the cached data
        fetch_markets_data.clear()
        st.experimental_rerun()
    st.divider()

    # Initialize client
    start_time = time.time()
    st.write("Loading Data...")
    client = load_client()
    
    # Load tickers and market data
    tickers = load_tickers()
    st.write(f"Will load data for {len(tickers)} tickers.")
    st.write(f"The tickers are: {tickers}.")
    
    all_markets = fetch_markets_data(client, tickers)
    
    load_time = time.time() - start_time
    st.write(f"Loaded {len(all_markets)} markets in {load_time:.2f} seconds")
    
    if len(all_markets) > 0:
        st.write("Sample market structure:", all_markets[-1])
    
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