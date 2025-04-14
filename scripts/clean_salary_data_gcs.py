# clean_salary_data_gcs.py

# clean_salary_data_gcs.py

from pathlib import Path
import re
import tempfile
import argparse
from google.cloud import storage
from typing import Optional

# Our modules
from clean_salary_data import clean_sunshine_data
from gcs_modules import parse_gcs_path, download_gcs_file, upload_to_gcs, gcs_blob_exists

def list_merged_files(bucket_name: str, prefix: str = "merged/") -> list:
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".csv") and "merged_salary" in blob.name]

def clean_single_year(bucket_name: str, year: str, cleaned_prefix: str = "cleaned/") -> None:
    """Clean data for a single year"""
    merged_blob = f"merged/merged_salary_{year}_uncleaned.csv"
    merged_uri = f"gs://{bucket_name}/{merged_blob}"
    
    if not gcs_blob_exists(merged_uri):
        print(f"⚠️ No merged file found for year {year}")
        return

    print("\n" + "=" * 30)
    print(f"Cleaning year {year}")
    
    try:
        # Download and process
        local_path = download_gcs_file(merged_uri)
        output_tempdir = Path(tempfile.mkdtemp())
        
        # Clean data using existing logic
        clean_sunshine_data(local_path, output_tempdir)
        
        # Upload cleaned file
        cleaned_file = output_tempdir / f"sunshine_cleaned_{year}.csv"
        output_gcs_uri = f"gs://{bucket_name}/{cleaned_prefix}sunshine_cleaned_{year}.csv"
        upload_to_gcs(cleaned_file, output_gcs_uri)
        print(f"✅ Successfully cleaned and uploaded {year}")
        
    except Exception as e:
        print(f"❌ Failed to clean {year}: {e}")

def clean_all_merged_files(bucket_name: str, target_year: Optional[str] = None) -> None:
    """Clean either specific year or all years"""
    if target_year:
        clean_single_year(bucket_name, target_year)
    else:
        # Original all-years behavior
        merged_files = list_merged_files(bucket_name)
        print("=" * 100)
        print(f"Found {len(merged_files)} merged files to clean")
        for blob_path in merged_files:
            year_match = re.search(r"(19\d{2}|20\d{2})", blob_path)
            if year_match:
                clean_single_year(bucket_name, year_match.group(1))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean merged salary data from GCS using local cleaning logic."
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="sunshine-list-bucket",
        help="GCS bucket name (default: sunshine-list-bucket)"
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Specific year to clean (YYYY format). If not provided, clean all years."
    )
    args = parser.parse_args()

    # Validate year format if provided
    if args.year and not re.match(r"^(19|20)\d{2}$", args.year):
        raise ValueError("Year must be a 4-digit year between 1900-2099")

    clean_all_merged_files(args.bucket, args.year)

###
# Clean all years (original behavior)
    ## python clean_salary_data_gcs.py --bucket sunshine-list-bucket

# Clean specific year
    ## python clean_salary_data_gcs.py --bucket sunshine-list-bucket --year 2020

### previous version

# from pathlib import Path
# import re
# import tempfile
# from google.cloud import storage

# # Our modules
# from clean_salary_data import clean_sunshine_data
# from gcs_modules import parse_gcs_path, download_gcs_file, upload_to_gcs, gcs_blob_exists

# def list_merged_files(bucket_name, prefix="merged/"):
#     client = storage.Client()
#     blobs = client.list_blobs(bucket_name, prefix=prefix)
#     return [blob.name for blob in blobs if blob.name.endswith(".csv") and "merged_salary" in blob.name]

# def clean_all_merged_files(bucket_name, merged_prefix="merged/", cleaned_prefix="cleaned/"):
#     merged_files = list_merged_files(bucket_name, prefix=merged_prefix)
#     print("=" * 100)
#     print(f"Found {len(merged_files)} merged files to clean")

#     for blob_path in merged_files:
#         print("\n")
#         print("=" * 30)
#         print(f"Cleaning: {blob_path}")
#         gcs_uri = f"gs://{bucket_name}/{blob_path}"
#         local_path = download_gcs_file(gcs_uri)

#         output_tempdir = Path(tempfile.mkdtemp())
        
#         try:
#             clean_sunshine_data(local_path, output_tempdir)
#             year = re.search(r"(19\d{2}|20\d{2})", blob_path).group(1)
#             cleaned_file = output_tempdir / f"sunshine_cleaned_{year}.csv"
#             output_gcs_uri = f"gs://{bucket_name}/{cleaned_prefix}sunshine_cleaned_{year}.csv"
#             upload_to_gcs(cleaned_file, output_gcs_uri)
#         except Exception as e:
#             print(f"❌ Failed to clean {blob_path}: {e}")

# if __name__ == "__main__":
#     clean_all_merged_files("sunshine-list-bucket")