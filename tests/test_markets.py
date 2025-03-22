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
keyfile_path = os.path.expanduser(keyfile_path)

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

output_dir = "./test_outputs/demo"
os.makedirs(output_dir, exist_ok=True)
# Save to JSON
def save_to_json(data, filename=f"{output_dir}/market_history.json"):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

# Save to CSV
def save_to_csv(data, filename=f"{output_dir}/market_history.csv"):
    trades = data.get('trades', [])
    if trades:
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=trades[0].keys())
            writer.writeheader()
            writer.writerows(trades)

save_to_json(market_history)
save_to_csv(market_history)

# Test Series
def test_series_data():
    series_data = http_client.get_series("KXNETFLIXRANKSHOW")
    assert series_data is not None
    assert 'series' in series_data
    assert series_data['series']['ticker'] == "KXNETFLIXRANKSHOW"

# Test Event
def test_event_data():
    event_data = http_client.get_event("KXNETFLIXRANKSHOW-25MAR17")
    assert event_data is not None
    assert 'event' in event_data
    assert event_data['event']['event_ticker'] == "KXNETFLIXRANKSHOW-25MAR17"

# Test Series Markets
def test_series_markets():
    series_data = http_client.get_series("KXNETFLIXRANKSHOW")
    assert series_data is not None
    assert 'series' in series_data
    if 'markets' in series_data['series']:
        series_markets = series_data['series']['markets']
    else:
        series_markets = None
    assert series_markets is None or isinstance(series_markets, list)

test_series_data()
test_event_data()
test_series_markets()
