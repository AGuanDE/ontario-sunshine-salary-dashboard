# merge_addendum_gcs.py

import re
import tempfile
from pathlib import Path
import argparse
from google.cloud import storage

# Import the merge logic from your local merge_addendum.py script
from merge_addendum import merge_addendum

def parse_gcs_path(gcs_path):
    """Split gs://bucket/path/to/blob.csv into (bucket, path/to/blob.csv)"""
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

def upload_to_gcs(local_path: Path, gcs_uri: str):
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))

def gcs_blob_exists(gcs_uri: str) -> bool:
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_path).exists()

def list_years(bucket_name, prefix, pattern):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    years = set()
    for blob in blobs:
        match = re.search(pattern, blob.name)
        if match:
            years.add(match.group(1))
    return sorted(years)

def main(bucket_name):
    # Extract available years from salary and addendum files
    salary_years = list_years(bucket_name, "raw/salaries/", r"sunshine_salaries_(\d{4})\.csv")

    print(f"Salary files found: {salary_years}")

    for year in salary_years:
        print(f"\nProcessing year {year}...")
        salary_uri = f"gs://{bucket_name}/raw/salaries/sunshine_salaries_{year}.csv"
        addendum_uri = f"gs://{bucket_name}/raw/addendums/sunshine_addendums_{year}.csv"
        output_uri = f"gs://{bucket_name}/merged/merged_salary_{year}_uncleaned.csv"

        if gcs_blob_exists(output_uri):
            print(f"⏭️ Skipping year {year} — merged file already exists at {output_uri}")
            continue

        # check if addendum exists
        addendum_exists = gcs_blob_exists(addendum_uri)

        salary_path = download_gcs_file(salary_uri)

        # download addendum (if it exists), else set to dummy path
        addendum_path = Path("non_existent_addendum.csv")
        if addendum_exists:
            addendum_path = download_gcs_file(addendum_uri)
        else:
            print(f"No addendum found for {year}, passing salary file through unchanged")
        
        output_dir = Path(tempfile.mkdtemp())

        # Call the local merge logic from merge_addendum.py
        merge_addendum(salary_path, addendum_path, output_dir)

        output_file = output_dir / f"merged_salary_{year}_uncleaned.csv"
        upload_to_gcs(output_file, output_uri)
        print(f"📤 Uploaded merged file to GCS: {output_uri}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge salary and addendum CSVs from GCS using local merge logic."
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="sunshine-list-bucket",
        help="GCS bucket name (default: sunshine-list-bucket)"
    )
    args = parser.parse_args()
    main(args.bucket)
