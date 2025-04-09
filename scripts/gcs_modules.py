# gcs_modules.py

import re
import tempfile
from pathlib import Path
import argparse
from google.cloud import storage

# 1
def parse_gcs_path(gcs_path):
    """Split gs://bucket/path/to/blob.csv into (bucket, path/to/blob.csv)"""
    match = re.match(r"gs://([^/]+)/(.+)", gcs_path)
    if not match:
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    return match.group(1), match.group(2)

# 2
def download_gcs_file(gcs_uri):
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    temp_dir = tempfile.mkdtemp()
    local_path = Path(temp_dir) / Path(blob_path).name
    blob.download_to_filename(str(local_path))
    print(f"> Downloaded {blob_path}")
    return local_path

# 3
def upload_to_gcs(local_path: Path, gcs_uri: str):
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))

# 4
def gcs_blob_exists(gcs_uri: str) -> bool:
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_path).exists()