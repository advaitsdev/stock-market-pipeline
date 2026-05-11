import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import yfinance as yf

BUCKET_NAME = "stock-pipeline-raw-data"
PROJECT_ID = "stock-market-pipeline-496007"
DATASET_ID = "stock_data"
TABLE_ID = "equity_metrics"

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "META", "NVDA",
    "JPM", "GS", "BAC", "MS", "V",
    "JNJ", "PFE", "UNH", "ABBV", "MRK",
    "XOM", "CVX", "COP", "SLB", "EOG",
    "AMZN", "TSLA", "WMT", "HD", "MCD",
    "CAT", "BA", "GE", "MMM", "HON"
]


def run_pipeline(request=None):
    print("Starting pipeline...")

    # INGEST
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365)
    all_data = []

    for ticker in TICKERS:
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            df = df.rename(columns={
                "Date": "date", "Open": "open", "High": "high",
                "Low": "low", "Close": "close", "Volume": "volume"
            })
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            df["ticker"] = ticker
            df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]
            all_data.append(df)
        except Exception as e:
            print(f"Failed {ticker}: {e}")

    raw_df = pd.concat(all_data, ignore_index=True).dropna()

    # TRANSFORM
    raw_df["date"] = pd.to_datetime(raw_df["date"])
    raw_df = raw_df.sort_values(["ticker", "date"]).reset_index(drop=True)

    raw_df["rolling_7d_avg"] = raw_df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    ).round(4)

    raw_df["rolling_30d_avg"] = raw_df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(30, min_periods=1).mean()
    ).round(4)

    raw_df["volatility_score"] = raw_df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(30, min_periods=1).std()
    ).round(4)

    raw_df["daily_return_pct"] = raw_df.groupby("ticker")["close"].transform(
        lambda x: x.pct_change() * 100
    ).round(4)

    raw_df["price_range"] = (raw_df["high"] - raw_df["low"]).round(4)

    # UPLOAD TO GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    date_str = datetime.today().strftime("%Y%m%d")

    raw_csv = raw_df.to_csv(index=False)
    bucket.blob(f"raw/stock_data_{date_str}.csv").upload_from_string(raw_csv)

    processed_csv = raw_df.to_csv(index=False)
    bucket.blob(f"processed/stock_data_transformed_{date_str}.csv").upload_from_string(processed_csv)

    print("Uploaded to GCS")

    # LOAD TO BIGQUERY
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True)

    job = bq_client.load_table_from_dataframe(raw_df, table_ref, job_config=job_config)
    job.result()

    print(f"Pipeline complete. {len(raw_df)} rows loaded into BigQuery.")
    return "Pipeline complete", 200