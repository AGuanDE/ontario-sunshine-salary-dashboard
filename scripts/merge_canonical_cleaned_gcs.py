# merge_canonical_cleaned_gcs.py

import tempfile
import re
from pathlib import Path
import pandas as pd
from google.cloud import storage

 
def parse_gcs_path(gcs_path:str) -> tuple[str, str]:
    """separates gcs path into bucket_name and blob_path"""
    match = re.match(r"gs://([^/]+)/(.+)", gcs_path)
    if not match:
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    return match.group(1), match.group(2)

def list_cleaned_files(bucket_name, prefix="cleaned/"):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".csv") and "sunshine_cleaned" in blob.name]

def download_gcs_file(gcs_uri: str) -> Path:
    """Download a file from Google Cloud Storage to a local temporary directory and returns local path"""
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    temp_dir = tempfile.mkdtemp()
    local_path = Path(temp_dir) / Path(blob_path).name
    blob.download_to_filename(str(local_path))
    print(f"✅ Downloaded {gcs_uri} to {local_path}")
    return local_path

def upload_to_gcs(local_path, gcs_uri):
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    print(f"📤 Uploaded {local_path} to {gcs_uri}")

def merge_cleaned_files(bucket_name, prefix="cleaned/", output_path="canonical/sunshine_canonical_cleaned.csv"):
    cleaned_blobs = list_cleaned_files(bucket_name, prefix=prefix)
    print(f"🔍 Found {len(cleaned_blobs)} cleaned files")

    dfs = []
    for blob_path in cleaned_blobs:
        gcs_uri = f"gs://{bucket_name}/{blob_path}"
        local_path = download_gcs_file(gcs_uri)
        try:
            df = pd.read_csv(local_path)
            dfs.append(df)
        except Exception as e:
            print(f"❌ Failed to load {blob_path}: {e}")

    if not dfs:
        print("⚠️ No valid files to merge. Exiting.")
        return

    canonical_df = pd.concat(dfs, ignore_index=True)
    print(f"🔧 Combined rows: {len(canonical_df)}")

    temp_dir = Path(tempfile.mkdtemp())
    output_local = temp_dir / "sunshine_canonical_cleaned.csv"
    canonical_df.to_csv(output_local, index=False)

    upload_to_gcs(output_local, f"gs://{bucket_name}/{output_path}")

if __name__ == "__main__":
    merge_cleaned_files("sunshine-list-bucket")
