# scripts/upload_raw_to_gcs.py

from google.cloud import storage
from pathlib import Path
import re

def extract_year(filename:str) -> str:

    match = re.search(r"(20\d{2}|19\d{2})", filename)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"COULD NOT EXTRACT YEAR FROM FILENAME: {filename}")

def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    """
    Uploads a file to Google Cloud Storage.

    Parameters:
        bucket_name (str): Name of your GCS bucket.
        source_file_path (str): Local path to the file.
        destination_blob_name (str): Desired GCS path (e.g. 'raw/2023/sunshine.csv').
    """
    # Create GCS client (uses GOOGLE_APPLICATION_CREDENTIALS env variable)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)
    print(f"✅ Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}")

def standardize_and_upload(folder: Path, category: str, bucket_name: str):
    for file in folder.glob("*.csv"):
        try:
            year = extract_year(file.name.strip())
            standardized_name = f"sunshine_{category}_{year}.csv"
            destination_blob = f"raw/{category}/{standardized_name}"
            upload_to_gcs(bucket_name, str(file), destination_blob)
        except ValueError as e:
            print(e)

if __name__ == "__main__":
    # GCS bucket name
    bucket_name = "sunshine-list-bucket"
    
    # Base paths for data
    salary_path = Path("/home/aguan/ontario-sunshine-salary-dashboard/data/raw/salaries")
    addendum_path = Path("/home/aguan/ontario-sunshine-salary-dashboard/data/raw/addendums")
    
    # Upload files
    standardize_and_upload(salary_path, category="salaries", bucket_name=bucket_name)
    standardize_and_upload(addendum_path, category="addendums", bucket_name=bucket_name)
