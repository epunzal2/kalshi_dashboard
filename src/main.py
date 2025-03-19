import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import asyncio
from cryptography.hazmat.primitives import serialization
from clients import KalshiHttpClient, KalshiWebSocketClient, Environment

load_dotenv()

# Configuration
env = Environment.DEMO
key_id = os.getenv('DEMO_KEYID')
keyfile_path = os.getenv('DEMO_KEYFILE')

# Load private key
with open(keyfile_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None
    )


# Initialize clients
http_client = KalshiHttpClient(key_id, private_key, env)
ws_client = KalshiWebSocketClient(key_id, private_key, env)

# Streamlit app
st.title("Kalshi Markets Dashboard")

# Account Balance
balance = http_client.get_balance()
balance_value = balance.get('balance', 0) / 100.0  # Divide by 100
st.write(f"**Account Balance**: {balance_value:.2f}")

# Historical Trades
st.subheader("Recent Trades")
trades = http_client.get_trades(limit=10)
df = pd.DataFrame(trades.get("trades", []))
st.dataframe(df)

# Real-time Data (WebSocket)
st.subheader("Real-time Market Updates")
if st.button("Start Real-time Feed"):
    st.write("Connecting...")
    import threading
    def run_websocket():
        asyncio.run(ws_client.connect())
    threading.Thread(target=run_websocket).start()

st.subheader("Market Metrics")
trades_data = http_client.get_trades(limit=100).get("trades", [])
if trades_data:
    def calculate_profit(trade):
        if trade['taker_side'] == 'yes':
            return 100 - trade['yes_price']
        else:
            return 100 - trade['no_price']

    total_profit = sum([calculate_profit(trade) for trade in trades_data])
    avg_return = total_profit / len(trades_data) if trades_data else 0
    win_trades = sum(1 for trade in trades_data if calculate_profit(trade) > 0)
    win_rate = (win_trades / len(trades_data)) * 100 if trades_data else 0

    st.write(f"Win Rate: {win_rate:.1f}%")
    st.write(f"Average Return: ${avg_return:.2f}")
    st.write(f"Total Profit: ${total_profit:.2f}")
else:
    st.write("No trade data available for metrics")

# Instructions
st.info("Ensure your .env file has valid credentials and the private key file exists at the specified path.")

# Market Data
st.subheader("Market Data")
ticker = st.text_input("Enter Ticker:")
if ticker:
    market = http_client.get_market(ticker)
    if market:
        st.write(f"**Ticker**: {market.get('ticker', 'N/A')}")
        st.write(f"**Yes Price**: {market.get('last_yes_price', 'N/A')}")
        st.write(f"**No Price**: {market.get('last_no_price', 'N/A')}")
    else:
        st.write("Market not found.")
