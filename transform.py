import pandas as pd
import numpy as np
from datetime import datetime
import os

def transform_stock_data():
    # Load latest raw file
    raw_files = sorted(os.listdir("data/raw"))
    latest = f"data/raw/{raw_files[-1]}"
    print(f"Loading {latest}...")
    
    df = pd.read_csv(latest)
    
    # Fix column names from yfinance multi-ticker format
    df.columns = [col[0].lower() if isinstance(col, tuple) else col.lower() 
                  for col in df.columns]
    
    # Rename date column
    df = df.rename(columns={"date, ": "date", "date,": "date"})
    df.columns = df.columns.str.strip().str.replace(",", "").str.strip()
    
    # Keep only needed columns
    df = df[["date", "ticker", "open", "high", "low", "close", "volume"]].copy()
    
    # Drop nulls
    df = df.dropna()
    df = df.drop_duplicates()
    
    # Ensure correct types
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    
    # Sort
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    
    # Compute derived metrics per ticker
    df["rolling_7d_avg"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    ).round(4)
    
    df["rolling_30d_avg"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(30, min_periods=1).mean()
    ).round(4)
    
    df["volatility_score"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(30, min_periods=1).std()
    ).round(4)
    
    df["daily_return_pct"] = df.groupby("ticker")["close"].transform(
        lambda x: x.pct_change() * 100
    ).round(4)
    
    df["price_range"] = (df["high"] - df["low"]).round(4)
    
    # Save
    os.makedirs("data/processed", exist_ok=True)
    output_path = f"data/processed/stock_data_transformed_{datetime.today().strftime('%Y%m%d')}.csv"
    df.to_csv(output_path, index=False)
    
    print(f"Done! {len(df)} clean records saved to {output_path}")
    print(f"Tickers: {df['ticker'].nunique()}, Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(df[["date", "ticker", "close", "rolling_7d_avg", "rolling_30d_avg", "volatility_score"]].head(10))
    
    return df

if __name__ == "__main__":
    df = transform_stock_data()