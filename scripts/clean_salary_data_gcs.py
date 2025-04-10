# clean_salary_data_gcs.py

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import argparse
import re
import tempfile
from google.cloud import storage

# Our modules
from clean_salary_data import clean_sunshine_data
from gcs_modules import parse_gcs_path, download_gcs_file, upload_to_gcs, gcs_blob_exists

def list_merged_files(bucket_name, prefix="merged/"):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".csv") and "merged_salary" in blob.name]

def clean_all_merged_files(bucket_name, merged_prefix="merged/", cleaned_prefix="cleaned/"):
    merged_files = list_merged_files(bucket_name, prefix=merged_prefix)
    print("=" * 100)
    print(f"Found {len(merged_files)} merged files to clean")

    for blob_path in merged_files:
        print("\n")
        print("=" * 30)
        print(f"Cleaning: {blob_path}")
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