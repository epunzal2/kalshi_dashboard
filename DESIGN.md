graph TD
    A[Kalshi API] -->|JSON data| B(Trading Engine)
    B --> C{Market Analysis}
    C -->|Signals| D[Dashboard UI]
    C -->|History| E[Market Database]
```

# Architecture Overview

This diagram illustrates the high-level architecture of the Kalshi Dashboard.

- **Kalshi API**: The external API providing market data.
- **Trading Engine**: Core component for executing trades and managing positions.
- **Market Analysis**: Analyzes market data to generate trading signals.
- **Dashboard UI**: User interface for visualizing market data and managing trades.
- **Market Database**: Stores historical market data for analysis.
