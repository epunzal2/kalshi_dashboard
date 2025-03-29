# src/minimal_app.py
import logging
import os
from flask import Flask

# Basic logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/', methods=['GET'])
def minimal_hello():
    """Minimal root endpoint for testing Cloud Run routing."""
    logger.info("--- Minimal app / endpoint reached! ---")
    # You can add more logs here if needed
    return "Minimal App OK", 200

# No need for if __name__ == '__main__' block; Gunicorn handles execution.
# If you wanted to run this locally directly (python src/minimal_app.py), you'd add:
# if __name__ == "__main__":
#     port = int(os.environ.get('PORT', 8080))
#     app.run(host='0.0.0.0', port=port, debug=True)