import os
from dotenv import load_dotenv
import json
import csv
from cryptography.hazmat.primitives import serialization
from src.clients import KalshiHttpClient, Environment

load_dotenv()

# Configuration
env = Environment.DEMO
key_id = os.environ.get('DEMO_KEYID')
keyfile_path = os.environ.get('DEMO_KEYFILE')

if not key_id or not keyfile_path:
    raise ValueError("DEMO_KEYID and DEMO_KEYFILE environment variables must be set")

# Load private key
with open(keyfile_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None
    )


# Initialize client
http_client = KalshiHttpClient(key_id, private_key, env)

# Test ticker
test_ticker = "NGDP-22-C7.5"  # Replace with a valid ticker

# Get market data
market_data = http_client.get_market(test_ticker)
print(f"Market data for {test_ticker}:")
print(market_data)

# Get market history
market_history = http_client.get_market_history(test_ticker)
print(f"\nMarket history for {test_ticker}:")
print(market_history)

# Find specific markets
market_params = {'limit': 10, 'tickers': 'NGDP-22-C7.5,NGDP-22-C8.0'}
markets_response = http_client.get(http_client.markets_url, params=market_params)
print(f"\nSpecific markets:")
print(markets_response)

# Save to JSON
def save_to_json(data, filename="market_history.json"):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

# Save to CSV
def save_to_csv(data, filename="market_history.csv"):
    trades = data.get('trades', [])
    if trades:
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=trades[0].keys())
            writer.writeheader()
            writer.writerows(trades)

save_to_json(market_history)
save_to_csv(market_history)
