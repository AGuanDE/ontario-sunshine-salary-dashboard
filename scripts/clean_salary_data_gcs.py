import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import argparse
import re
import tempfile
from google.cloud import storage
from clean_salary_data import clean_sunshine_data

def parse_gcs_path(gcs_path):
    match = re.match(r"gs://([^/]+)/(.+)", gcs_path)
    if not match:
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    return match.group(1), match.group(2)

def download_gcs_file(gcs_uri):
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

def list_merged_files(bucket_name, prefix="merged/"):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".csv") and "merged_salary" in blob.name]

def clean_all_merged_files(bucket_name, merged_prefix="merged/", cleaned_prefix="cleaned/"):
    merged_files = list_merged_files(bucket_name, prefix=merged_prefix)
    print(f"🔍 Found {len(merged_files)} merged files to clean")

    for blob_path in merged_files:
        print(f"\n✨ Cleaning: {blob_path}")
        gcs_uri = f"gs://{bucket_name}/{blob_path}"
        local_path = download_gcs_file(gcs_uri)

        output_tempdir = Path(tempfile.mkdtemp())
        try:
            clean_sunshine_data(local_path, output_tempdir)
            year = re.search(r"(19\d{2}|20\d{2})", blob_path).group(1)
            cleaned_file = output_tempdir / f"sunshine_cleaned_{year}.csv"
            output_gcs_uri = f"gs://{bucket_name}/{cleaned_prefix}sunshine_cleaned_{year}.csv"
            upload_to_gcs(cleaned_file, output_gcs_uri)
        except Exception as e:
            print(f"❌ Failed to clean {blob_path}: {e}")

if __name__ == "__main__":
    clean_all_merged_files("sunshine-list-bucket")
