# Stock Market Data Pipeline

An automated, end-to-end batch data pipeline built on GCP that ingests daily OHLCV data for 30 equities, computes derived metrics, and loads everything into BigQuery for analysis — running daily at 9am IST with zero manual intervention.

## Architecture
yfinance API → Cloud Function (ingest + transform) → GCS (raw + processed) → BigQuery → Looker Studio
GitHub Actions triggers the Cloud Function daily via Cloud Scheduler.

## What it does

- **Ingests** daily OHLCV data for 30 equities (~7,500 records per run) using yfinance
- **Transforms** raw data by computing 5 derived metrics per ticker:
  - 7-day and 30-day rolling averages
  - 30-day volatility score (rolling std dev of close price)
  - Daily return percentage
  - Price range (high - low)
- **Validates** data with deduplication and null handling before loading
- **Loads** clean data into BigQuery using `WRITE_TRUNCATE` to avoid duplicates across daily runs
- **Visualises** price trends and volatility across all 30 equities in a Looker Studio dashboard

## Tech stack

- **Cloud**: GCP (Cloud Functions, Cloud Storage, BigQuery, Cloud Scheduler, Looker Studio)
- **Orchestration**: GitHub Actions CI/CD
- **Language**: Python 3
- **Libraries**: pandas, numpy, yfinance, google-cloud-storage, google-cloud-bigquery

## File structure

├── main.py              # Cloud Function entrypoint — runs full pipeline (ingest → transform → load)
├── ingest.py            # Fetches raw OHLCV data from yfinance
├── transform.py         # Cleans and computes derived metrics locally
├── upload_to_gcs.py     # Uploads raw/processed CSVs to GCS
├── load_bq.py           # Loads processed data into BigQuery
├── .github/workflows/   # GitHub Actions workflow for daily scheduling
└── requirements.txt     # Python dependencies
## Design decisions

**Why `WRITE_TRUNCATE` instead of `WRITE_APPEND` in BigQuery?**  
The pipeline re-ingests the last 365 days of data on every run. Appending would create duplicate rows for historical dates. Truncating and reloading keeps the table clean without needing a separate deduplication step at query time.

**Why batch over streaming?**  
The use case is end-of-day analysis — intraday latency has no value here. Batch keeps the architecture simple and the GCP costs low.

**Why Cloud Functions over Dataflow?**  
The data volume (~7,500 records/run) doesn't justify the overhead of a Dataflow pipeline. Cloud Functions are cheaper, simpler to deploy, and sufficient for this scale.

## A real bug I hit

yfinance returns oddly formatted column names (with trailing commas and whitespace) when downloading multiple tickers in one call. Had to add defensive column cleaning in `transform.py` to handle this before any downstream processing would work.
