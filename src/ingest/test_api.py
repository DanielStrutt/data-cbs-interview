"""
Quick test: fetches the last 5 days of daily price data for a few symbols
using yfinance (no API key required) and prints as JSON.

Usage:
    uv run src/ingest/test_api.py
"""

import json
import pandas as pd
import yfinance as yf

SYMBOLS = ["IBM", "AAPL", "MSFT"]

# Download each symbol individually to avoid SQLite cache locking
frames = []
for symbol in SYMBOLS:
    df = yf.download(symbol, period="5d", interval="1d", auto_adjust=True, progress=False)
    df = df.reset_index()
    # Flatten MultiIndex columns (newer yfinance returns them even for single tickers)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.insert(0, "ticker", symbol)
    frames.append(df)

df = pd.concat(frames, ignore_index=True)
df.columns = [c.lower().replace(" ", "_") for c in df.columns]
df["date"] = df["date"].astype(str)

# Drop any all-NaN rows (e.g. failed downloads)
df = df.dropna(subset=["close"])

records = df.to_dict(orient="records")
print(json.dumps(records, indent=2, default=str))
