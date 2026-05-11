import os
from google.cloud import storage
from datetime import datetime

# Point to your key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Advait S\Downloads\stock market pipeline\gcp-key.json"

BUCKET_NAME = "stock-pipeline-raw-data"

def upload_to_gcs(local_folder, gcs_folder):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    files = os.listdir(local_folder)
    for filename in files:
        local_path = os.path.join(local_folder, filename)
        gcs_path = f"{gcs_folder}/{filename}"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"  ✓ Uploaded {filename} to gs://{BUCKET_NAME}/{gcs_path}")

if __name__ == "__main__":
    print("Uploading raw data...")
    upload_to_gcs("data/raw", "raw")
    
    print("Uploading processed data...")
    upload_to_gcs("data/processed", "processed")
    
    print("\nAll files uploaded successfully!")