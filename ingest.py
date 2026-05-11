import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "META", "NVDA",
    "JPM", "GS", "BAC", "MS", "V",
    "JNJ", "PFE", "UNH", "ABBV", "MRK",
    "XOM", "CVX", "COP", "SLB", "EOG",
    "AMZN", "TSLA", "WMT", "HD", "MCD",
    "CAT", "BA", "GE", "MMM", "HON"
]

def fetch_stock_data():
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365)

    all_data = []

    print(f"Fetching data for {len(TICKERS)} equities...")

    for ticker in TICKERS:
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            df = df.rename(columns={"Date": "date", "Open": "open", "High": "high",
                                     "Low": "low", "Close": "close", "Volume": "volume"})
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            df["ticker"] = ticker
            df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]
            all_data.append(df)
            print(f"  ✓ {ticker} — {len(df)} rows")
        except Exception as e:
            print(f"  ✗ {ticker} failed: {e}")

    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.dropna()

    os.makedirs("data/raw", exist_ok=True)
    output_path = f"data/raw/stock_data_{datetime.today().strftime('%Y%m%d')}.csv"
    combined.to_csv(output_path, index=False)

    print(f"\nDone! {len(combined)} records saved to {output_path}")
    print(combined.head())
    return combined

if __name__ == "__main__":
    fetch_stock_data()