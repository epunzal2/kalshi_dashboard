import streamlit as st
import requests, time, json, os
import logging
import sys
from datetime import datetime
from pydantic import BaseModel
from src.clients import KalshiHttpClient, Environment, detect_ticker_type
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dotenv import load_dotenv
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

st.header("Prediction Market Assistant")

@st.cache_data 
def load_data(env=Environment.DEMO):
    if env == Environment.DEMO:
        key_id = os.getenv('DEMO_KEYID')
        keyfile_path = os.getenv('DEMO_KEYFILE')
        keyfile_path = os.path.expanduser(keyfile_path)
    elif env == Environment.PROD:
        key_id = os.getenv('PROD_KEYID')
        keyfile_path = os.getenv('PROD_KEYFILE')
        keyfile_path = os.path.expanduser(keyfile_path)

    if not os.path.exists(keyfile_path):
        st.error(f"Key file not found at {keyfile_path}")
        st.stop()

    if not key_id or not keyfile_path:
        st.error("Missing KEYID or KEYFILE environment variables")
        st.stop()

    private_key = None    
    try:
        with open(keyfile_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )
    except Exception as e:
        st.error(f"Failed to load private key: {e}")
        st.stop()

    client = KalshiHttpClient(
        key_id=key_id,
        private_key=private_key,
        environment=env
    )
    logger.info("Kalshi client initialized | Environment: %s | API endpoint: %s", 
           env.value, client.base_url)

    try:
        with open("tickers.txt") as f:
            tickers = [line.strip() for line in f if line.strip()]
            st.write(f"Will load data for {len(tickers)} tickers.")
            st.write(f"The tickers are: {tickers}.")
    except FileNotFoundError:
        st.error("tickers.txt file not found")
        st.stop()

    all_markets = []
    for ticker in tickers:
        ticker_type = detect_ticker_type(ticker)
        try:
            if ticker_type == 'series':
                markets = client.get_markets(series_ticker=ticker)
            else:  # Handle event tickers
                markets = client.get_markets(event_ticker=ticker)
            
            all_markets.extend(markets)
        except Exception as e:
            st.error(f"Failed to fetch markets for {ticker}: {str(e)}")
    
    return all_markets

start_time = time.time()
st.write("Loading Data...")
all_markets = load_data(env=Environment.PROD)
st.write(f"Loaded {len(all_markets)} markets in {time.time()-start_time} seconds")
st.write("Sample event structure:", all_markets[0])

search = st.text_input("Search Markets")

# Option to display markets by category
categories = {}
for market in all_markets:
    category = market['category']
    if category not in categories:
        categories[category] = []
    categories[category].append(market)

category_selectbox = st.selectbox("Categories (Optional)", sorted(categories.keys()))

def display_analysis(analysis):
    with st.modal("Analysis"):
        st.write(analysis)

if search:
    logger.info(f"Search term entered: {search}")
    for market in all_markets:
        if search.lower() in market['title'].lower():
            st.divider()
            bet_markdown = f"##### {market['title']}\n"
            # for market in event['markets']:
            bet_markdown += f"###### {market['yes_sub_title']} - {market['ticker']}\n"
            bet_markdown += f"Yes Bid: {market['yes_bid']}, Yes Ask {market['yes_ask']}\n\n"
            bet_markdown += f"No bid: {market['no_bid']}, No Ask {market['no_ask']}\n\n"
            bet_markdown += f"Volume (24 h): {market['volume_24h']}\n\n"
            st.markdown(bet_markdown)
