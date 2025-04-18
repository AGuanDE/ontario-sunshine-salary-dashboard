# gcs_modules.py

import re
import tempfile
import time
from pathlib import Path
from google.cloud import storage
from google.api_core.exceptions import TooManyRequests

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
# def upload_to_gcs(local_path: Path, gcs_uri: str):
#     bucket_name, blob_path = parse_gcs_path(gcs_uri)
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(blob_path)
#     blob.upload_from_filename(str(local_path))

def upload_to_gcs(local_path: Path, gcs_uri: str):
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    try:
        print(f"📤 Uploading {local_path} to gs://{bucket_name}/{blob_path}")
        blob.upload_from_filename(str(local_path))
        print(f"✅ Upload successful: gs://{bucket_name}/{blob_path}")

    except TooManyRequests as e:
        print(f"⚠️ GCS rate limit hit: {e}")
        print("🔍 Checking if file was uploaded despite error...")
        
        # Short wait to allow consistency (avoid rate limit)
        time.sleep(2)

        if blob.exists():
            print(f"✅ File already exists in GCS: gs://{bucket_name}/{blob_path}")
            print("🟢 Proceeding without raising error.")
        else:
            print(f"❌ File not found in GCS after rate limit error.")
            raise

    except Exception as e:
        print(f"❌ Unexpected error during upload: {e}")
        raise

# 4
def gcs_blob_exists(gcs_uri: str) -> bool:
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_path).exists()