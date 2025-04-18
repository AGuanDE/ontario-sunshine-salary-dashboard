# scripts/upload_raw_to_gcs.py

from google.cloud import storage
from google.api_core.exceptions import TooManyRequests
from pathlib import Path
import re
import argparse

def extract_year(filename:str) -> str:
    match = re.search(r"(20\d{2}|19\d{2})", filename)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"COULD NOT EXTRACT YEAR FROM FILENAME: {filename}")

def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    # Create GCS client (uses GOOGLE_APPLICATION_CREDENTIALS env variable)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    # sometimes GCS will raise a ratelimit issue, this checks if the file was actually uploaded and marks 
    # the airflow tasks as successful so the next task can run
    try:
        print(f"⬆️ Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}")
        blob.upload_from_filename(source_file_path)
        print(f"✅ Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}")

    except TooManyRequests as e:
        print(f"⚠️ Rate limit error during upload: {e}")
        print("🔎 Verifying if the object was uploaded anyway...")

        # Check if object exists despite the 429 error
        if blob.exists():
            print("✅ Upload actually succeeded. Continuing task...")
        else:
            print("❌ Upload failed and object does not exist in GCS.")
            raise  # re-raise the exception to mark the task as failed

    except Exception as e:
        print(f"❌ Unexpected error during upload: {e}")
        raise

def standardize_and_upload(folder: Path, category: str, bucket_name: str, target_year: str = None):
    print(f"\nScanning {category} folder: {folder.resolve()}")
    files = list(folder.glob("*.csv"))
    
    if not files:
        print(f"No CSV files found in {category} folder")
        return

    processed = 0
    for file in files:
        try:
            file_year = extract_year(file.name.strip())
            
            # Skip if target_year is specified and doesn't match
            if target_year and file_year != target_year:
                print(f"⏭️ Skipping {file.name} (year {file_year} doesn't match target {target_year})")
                continue
                
            standardized_name = f"sunshine_{category}_{file_year}.csv"
            destination_blob = f"raw/{category}/{standardized_name}"
            upload_to_gcs(bucket_name, str(file), destination_blob)
            processed += 1
            
        except ValueError as e:
            print(f"Skipping {file.name}: {e}")

    print(f"Processed {processed}/{len(files)} files in {category} folder")

# def standardize_and_upload(folder: Path, category: str, bucket_name: str):
#     print(f"Scanning folder: {folder.resolve()}")
#     files = list(folder.glob("*.csv"))
#     print(f"Found {len(files)} .csv files: {[f.name for f in files]}")
#     for file in folder.glob("*.csv"):
#         try:
#             year = extract_year(file.name.strip())
#             standardized_name = f"sunshine_{category}_{year}.csv"
#             destination_blob = f"raw/{category}/{standardized_name}"
#             upload_to_gcs(bucket_name, str(file), destination_blob)
#         except ValueError as e:
#             print(f"Skipping {file.name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw salary/addendum files to GCS")
    parser.add_argument(
        "--bucket",
        type=str,
        default="sunshine-list-bucket",
        help="GCS bucket name"
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Specific year to upload (YYYY format). If not provided, upload all years."
    )
    args = parser.parse_args()

    # Validate year format if provided
    if args.year and not re.match(r"^(19|20)\d{2}$", args.year):
        raise ValueError("Year must be a 4-digit year between 1900-2099")

    project_root = Path(__file__).parents[1]
    
    # Base paths for data
    salary_path = project_root / "data" / "raw" / "salary"
    addendum_path = project_root / "data" / "raw" / "addendum"

    # Check folder existence
    for p in (salary_path, addendum_path):
        if not p.exists():
            print(f"⚠️ WARNING: Folder does not exist: {p.resolve()}")

    # Upload files with year filtering
    standardize_and_upload(salary_path, "salary", args.bucket, args.year)
    standardize_and_upload(addendum_path, "addendum", args.bucket, args.year)

###
# Upload all files (original behavior)
    ## python upload_raw_to_gcs.py --bucket sunshine-list-bucket

# Upload specific year only
    ## python upload_raw_to_gcs.py --bucket sunshine-list-bucket --year 2020


## previous version

# if __name__ == "__main__":
#     bucket_name = "sunshine-list-bucket" # replace with your bucket name
#     project_root = Path(__file__).parents[1]
    
#     # Base paths for data
#     # replace with your actual file path
#     salary_path   = project_root / "data" / "raw" / "salary"
#     addendum_path = project_root / "data" / "raw" / "addendum"

#     # Ensure directories exist
#     for p in (salary_path, addendum_path):
#         if not p.exists():
#             print(f"WARNING: folder does not exist: {p.resolve()}")
    
#     # Upload files
#     standardize_and_upload(salary_path, category="salary", bucket_name=bucket_name)
#     standardize_and_upload(addendum_path, category="addendum", bucket_name=bucket_name)
