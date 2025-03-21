# Kalshi Markets Dashboard

## Directory Structure

```
.
├── .env                    # Environment variables (API keys, base URLs, etc.)
├── .gitignore              # Specifies intentionally untracked files that Git should ignore
├── README.md               # Project documentation
├── requirements.txt        # List of Python dependencies
├── src/                    # Source code directory
│   ├── app.py              # Streamlit application entry point
│   ├── clients.py          # Kalshi API client
│   ├── core.py             # Core functions and classes
├── starter/                # Jupyter notebooks for exploration
├── test_outputs/           # Output data from tests
│   ├── market_history.csv
│   ├── market_history.json
│   └── prod/               # Production data

```

## Installation

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    ```
2.  Navigate to the project directory:

    ```bash
    cd kalshi-dashboard
    ```
3.  Install the dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  Set the environment variables in the `.env` file.
2.  Run the Streamlit application:

    ```bash
    streamlit run src/app.py
    ```

## Context Priming

Read README.md, DESIGN.md, src/ to understand this codebase
