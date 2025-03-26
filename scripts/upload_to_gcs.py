# scripts/upload_to_gcs.py

from google.cloud import storage
from pathlib import Path

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


if __name__ == "__main__":
    # GCS bucket name
    bucket_name = "sunshine-list-bucket"
    
    # Base paths for data
    salary_path = Path("/home/aguan/ontario-sunshine-salary-dashboard/data/raw/salaries")
    addendum_path = Path("/home/aguan/ontario-sunshine-salary-dashboard/data/raw/addendums")
    
    # Upload salary data files
    for salary_file in salary_path.glob("*.csv"):
        destination_blob = f"raw/salaries/{salary_file.name}"
        upload_to_gcs(bucket_name, str(salary_file), destination_blob)
    
    # Upload addendum data files
    for addendum_file in addendum_path.glob("*.csv"):
        destination_blob = f"raw/addendums/{addendum_file.name}"
        upload_to_gcs(bucket_name, str(addendum_file), destination_blob)
