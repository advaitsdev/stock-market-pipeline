import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Advait S\Downloads\stock market pipeline\gcp-key.json"

PROJECT_ID = "stock-market-pipeline-496007"
DATASET_ID = "stock_data"
TABLE_ID = "equity_metrics"

def load_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    # Load processed CSV
    processed_files = sorted(os.listdir("data/processed"))
    latest = f"data/processed/{processed_files[-1]}"
    print(f"Loading {latest}...")

    df = pd.read_csv(latest)
    df["date"] = pd.to_datetime(df["date"])

    # Define table reference
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Upload to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"Done! {table.num_rows} rows loaded into {table_ref}")

if __name__ == "__main__":
    load_to_bigquery()