import os
from dotenv import load_dotenv
import asyncio
import time
from threading import Thread
from cryptography.hazmat.primitives import serialization
from src.clients import KalshiHttpClient, KalshiWebSocketClient, Environment

load_dotenv()

# Configuration
env = Environment.DEMO
key_id = os.getenv('DEMO_KEYID')
keyfile_path = os.getenv('DEMO_KEYFILE')
keyfile_path = os.path.expanduser(keyfile_path)

# Load private key
private_key = None  # Initialize private_key
try:
    with open(keyfile_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
except Exception as e:
    print(f"Error loading private key: {e}")
    private_key = None


# Initialize clients
http_client = KalshiHttpClient(key_id, private_key, env)
ws_client = KalshiWebSocketClient(key_id, private_key, env)

# WebSocket thread starter
def start_ws():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(ws_client.connect())

# Thread(target=start_ws, daemon=True).start()
