# validate_cleaning_gcs.py

import os
import re
import json
import tempfile
import argparse
from pathlib import Path
from google.cloud import storage
import pandas as pd

REQUIRED_COLUMNS = [
    "sector", "first_name", "last_name", "employer", "job_title",
    "calendar_year", "salary_paid", "taxable_benefits"
]

EXPECTED_DTYPES = {
    "sector": "object",
    "first_name": "object",
    "last_name": "object",
    "employer": "object",
    "job_title": "object",
    "calendar_year": "int64",
    "salary_paid": "float64",
    "taxable_benefits": "float64"
}

NON_NULL_COLS = {
    "sector", "first_name", "last_name", "employer", "job_title", "calendar_year"
}

def list_gcs_files(bucket_name: str, prefix: str) -> list:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith(".csv")]

def download_blob_to_tempfile(bucket_name: str, blob_path: str) -> Path:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_path)
    temp_dir = tempfile.mkdtemp()
    local_path = Path(temp_dir) / Path(blob_path).name
    blob.download_to_filename(str(local_path))
    return local_path

def validate_file(cleaned_path: Path, merged_path: Path) -> list:
    cleaned_df = pd.read_csv(cleaned_path)
    merged_df = pd.read_csv(merged_path)

    errors = []
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in cleaned_df.columns]
    if missing_cols:
        errors.append(f"❌ Missing columns: {missing_cols}")

    for col in NON_NULL_COLS:
        if col in cleaned_df.columns:
            null_indices = cleaned_df[cleaned_df[col].isnull()].index.tolist()
            if null_indices:
                errors.append(f"❌ Null values in column: {col} at rows: {null_indices}")

    for col, expected_dtype in EXPECTED_DTYPES.items():
        if col in cleaned_df.columns:
            actual_dtype = str(cleaned_df[col].dtype)
            if actual_dtype != expected_dtype:
                errors.append(f"❌ {col} dtype mismatch: expected={expected_dtype}, actual={actual_dtype}")

    return errors

def validate_single_year(bucket_name: str, year: str) -> bool:
    """Validate cleaning for a specific year"""
    print(f"\n🔍 Validating year {year}...")
    
    # Build GCS paths
    cleaned_blob = f"cleaned/sunshine_cleaned_{year}.csv"
    merged_blob = f"merged/merged_salary_{year}_uncleaned.csv"
    
    # Check if both files exist
    client = storage.Client()
    cleaned_exists = client.bucket(bucket_name).blob(cleaned_blob).exists()
    merged_exists = client.bucket(bucket_name).blob(merged_blob).exists()
    
    if not cleaned_exists:
        print(f"⚠️ Cleaned file not found for year {year}")
        return False
    if not merged_exists:
        print(f"⚠️ Merged file not found for year {year}")
        return False

    try:
        # Download files
        cleaned_local = download_blob_to_tempfile(bucket_name, cleaned_blob)
        merged_local = download_blob_to_tempfile(bucket_name, merged_blob)
        
        # Run validation
        errors = validate_file(cleaned_local, merged_local)
        
        if errors:
            print("\n".join(errors))
            return False
        print("✅ File passed validation")
        return True
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
        return False

def main(bucket_name: str, target_year: str = None) -> None:
    """Main validation function"""
    failed_files = []

    if target_year:
        # Validate single year
        if not validate_single_year(bucket_name, target_year):
            failed_files.append(target_year)
    else:
        # Original all-years behavior
        cleaned_files = list_gcs_files(bucket_name, "cleaned/")
        merged_files = list_gcs_files(bucket_name, "merged/")

        # Match files by year
        cleaned_years = {
            re.search(r"(\d{4})", Path(f).stem).group(1): f
            for f in cleaned_files if re.search(r"(\d{4})", Path(f).stem)
        }
        merged_years = {
            re.search(r"(\d{4})", Path(f).stem).group(1): f
            for f in merged_files if re.search(r"(\d{4})", Path(f).stem)
        }

        matched_years = sorted(set(cleaned_years.keys()) & set(merged_years.keys()))
        print(f"📅 Found {len(matched_years)} years to validate")

        for year in matched_years:
            if not validate_single_year(bucket_name, year):
                failed_files.append(year)

    # Print summary
    print("\n📋 Validation Summary:")
    if failed_files:
        print("❌ Failed validations for:")
        for item in failed_files:
            print(f"- {item}")
        exit(1)
    else:
        print("✅ All validations passed!")
        exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate cleaned salary data in GCS"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="sunshine-list-bucket",
        help="GCS bucket name"
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Specific year to validate (YYYY format)"
    )
    args = parser.parse_args()

    # Validate year format if provided
    if args.year and not re.match(r"^(19|20)\d{2}$", args.year):
        raise ValueError("Invalid year format. Must be YYYY between 1900-2099")

    main(args.bucket, args.year)

###
# Validate all years
     ## python validate_cleaning_gcs.py --bucket sunshine-list-bucket

# Validate specific year
    ## python validate_cleaning_gcs.py --bucket sunshine-list-bucket --year 2020
###

## previous version
# import os
# import re
# import json
# import tempfile
# from pathlib import Path
# from google.cloud import storage
# import pandas as pd

# REQUIRED_COLUMNS = [
#     "sector", "first_name", "last_name", "employer", "job_title",
#     "calendar_year", "salary_paid", "taxable_benefits"
# ]

# EXPECTED_DTYPES = {
#     "sector": "object",
#     "first_name": "object",
#     "last_name": "object",
#     "employer": "object",
#     "job_title": "object",
#     "calendar_year": "int64",
#     "salary_paid": "float64",
#     "taxable_benefits": "float64"
# }

# NON_NULL_COLS = {
#     "sector", "first_name", "last_name", "employer", "job_title", "calendar_year"
# }

# BUCKET_NAME = "sunshine-list-bucket"
# MERGED_PREFIX = "merged/"
# CLEANED_PREFIX = "cleaned/"

# client = storage.Client()
# bucket = client.bucket(BUCKET_NAME)

# def list_gcs_files(prefix):
#     return [blob.name for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith(".csv")]

# def download_blob_to_tempfile(blob_path):
#     blob = bucket.blob(blob_path)
#     temp_dir = tempfile.mkdtemp()
#     local_path = Path(temp_dir) / Path(blob_path).name
#     blob.download_to_filename(str(local_path))
#     return local_path

# def validate_file(cleaned_path: Path, merged_path: Path):
#     cleaned_df = pd.read_csv(cleaned_path)
#     merged_df = pd.read_csv(merged_path)

#     errors = []

#     # find Missing columns
#     missing_cols = [col for col in REQUIRED_COLUMNS if col not in cleaned_df.columns]
#     if missing_cols:
#         errors.append(f"❌ Missing columns: {missing_cols}")

#     # Null checks + return row number
#     for col in NON_NULL_COLS:
#         if col in cleaned_df.columns:
#             null_indices = cleaned_df[cleaned_df[col].isnull()].index.tolist()
#             if null_indices:
#                 errors.append(f"❌ Null values in column: {col} at rows: {null_indices}")

#     # Dtype check
#     for col, expected_dtype in EXPECTED_DTYPES.items():
#         if col in cleaned_df.columns:
#             actual_dtype = str(cleaned_df[col].dtype)
#             if actual_dtype != expected_dtype:
#                 errors.append(f"❌ {col} dtype mismatch: expected={expected_dtype}, actual={actual_dtype}")

#     return errors

# if __name__ == "__main__":
#     cleaned_files = list_gcs_files(CLEANED_PREFIX)
#     merged_files = list_gcs_files(MERGED_PREFIX)

#     # Match files by year
#     cleaned_years = {
#         re.search(r"(\d{4})", Path(f).stem).group(1): f
#         for f in cleaned_files if re.search(r"(\d{4})", Path(f).stem)
#     }
#     merged_years = {
#         re.search(r"(\d{4})", Path(f).stem).group(1): f
#         for f in merged_files if re.search(r"(\d{4})", Path(f).stem)
#     }

#     matched_years = sorted(set(cleaned_years.keys()) & set(merged_years.keys()))
#     print(f"📅 Found {len(matched_years)} matched years to validate: {matched_years}")

#     failed_files = []
#     for year in matched_years:
#         print(f"\n🔍 Validating year {year}...")
#         cleaned_local = download_blob_to_tempfile(cleaned_years[year])
#         merged_local = download_blob_to_tempfile(merged_years[year])
#         errors = validate_file(cleaned_local, merged_local)

#         if errors:
#             print("\n".join(errors))
#             failed_files.append(cleaned_years[year])
#         else:
#             print("✅ File passed validation")

#     print("\n📋 Summary:")
#     if failed_files:
#         print("❌ Files that failed validation:")
#         for f in failed_files:
#             print(f"- {f}")
#     else:
#         print("✅ All files passed validation")