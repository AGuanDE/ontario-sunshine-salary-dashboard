# validate_cleaning_gcs.py

import os
import re
import json
import tempfile
from pathlib import Path
from google.cloud import storage
import pandas as pd

REQUIRED_COLUMNS = [
    "first_name", "last_name", "employer", "job_title",
    "calendar_year", "salary_paid", "taxable_benefits"
]

EXPECTED_DTYPES = {
    "first_name": "object",
    "last_name": "object",
    "employer": "object",
    "job_title": "object",
    "calendar_year": "int64",
    "salary_paid": "float64",
    "taxable_benefits": "float64"
}

NON_NULL_COLS = {
    "first_name", "last_name", "employer", "job_title", "calendar_year"
}

SALARY_RANGE = (0, 5_000_000)
BENEFITS_RANGE = (0, 1_000_000)

BUCKET_NAME = "sunshine-list-bucket"
MERGED_PREFIX = "merged/"
CLEANED_PREFIX = "cleaned/"

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

def list_gcs_files(prefix):
    return [blob.name for blob in bucket.list_blobs(prefix=prefix) if blob.name.endswith(".csv")]

def download_blob_to_tempfile(blob_path):
    blob = bucket.blob(blob_path)
    temp_dir = tempfile.mkdtemp()
    local_path = Path(temp_dir) / Path(blob_path).name
    blob.download_to_filename(str(local_path))
    return local_path

def validate_file(cleaned_path: Path, merged_path: Path):
    cleaned_df = pd.read_csv(cleaned_path)
    merged_df = pd.read_csv(merged_path)

    errors = []

    # Row count comparison (clean script does remove rows if numeric expecations broken)
    if abs(len(cleaned_df) - len(merged_df)) > 10:
        errors.append(f"⚠️ Row count mismatch: merged={len(merged_df)}, cleaned={len(cleaned_df)} /n note: clean_salary_data script removes rows if salary/benefits expecations broken")

    # Required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in cleaned_df.columns]
    if missing_cols:
        errors.append(f"❌ Missing columns: {missing_cols}")

    # Null checks + return row number
    for col in NON_NULL_COLS:
        if col in cleaned_df.columns:
            null_indices = cleaned_df[cleaned_df[col].isnull()].index.tolist()
            if null_indices:
                errors.append(f"❌ Null values in column: {col} at rows: {null_indices}")
    
    # # Null checks
    # for col in NON_NULL_COLS:
    #     if col in cleaned_df.columns and cleaned_df[col].isnull().any():
    #         errors.append(f"❌ Null values in column: {col}")

    # Range checks
    error_cols = ["first_name", "last_name", "employer", "calendar_year", "salary_paid", "taxable_benefits"]

    if "salary_paid" in cleaned_df.columns:
        salary_mask = ~cleaned_df["salary_paid"].between(SALARY_RANGE[0], SALARY_RANGE[1], inclusive="left")
        if salary_mask.any():
            errors.append("❌ salary_paid out of expected range")
            print("Rows with invalid salary_paid values:")
            print(cleaned_df.loc[salary_mask, error_cols])

    if "taxable_benefits" in cleaned_df.columns:
        benefits_mask = ~cleaned_df["taxable_benefits"].between(BENEFITS_RANGE[0], BENEFITS_RANGE[1], inclusive="left")
        if benefits_mask.any():
            errors.append("❌ taxable_benefits out of expected range")
            print("Rows with invalid taxable_benefits values:")
            print(cleaned_df.loc[benefits_mask, error_cols])

    # if "salary_paid" in cleaned_df.columns:
    #     if not cleaned_df["salary_paid"].between(SALARY_RANGE[0], SALARY_RANGE[1], inclusive="left").all():
    #         errors.append("❌ salary_paid out of expected range")
    # if "taxable_benefits" in cleaned_df.columns:
    #     if not cleaned_df["taxable_benefits"].between(BENEFITS_RANGE[0], BENEFITS_RANGE[1], inclusive="left").all():
    #         errors.append("❌ taxable_benefits out of expected range")

    # Dtype check
    for col, expected_dtype in EXPECTED_DTYPES.items():
        if col in cleaned_df.columns:
            actual_dtype = str(cleaned_df[col].dtype)
            if actual_dtype != expected_dtype:
                errors.append(f"❌ {col} dtype mismatch: expected={expected_dtype}, actual={actual_dtype}")

    return errors

if __name__ == "__main__":
    cleaned_files = list_gcs_files(CLEANED_PREFIX)
    merged_files = list_gcs_files(MERGED_PREFIX)

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
    print(f"📅 Found {len(matched_years)} matched years to validate: {matched_years}")

    failed_files = []
    for year in matched_years:
        print(f"\n🔍 Validating year {year}...")
        cleaned_local = download_blob_to_tempfile(cleaned_years[year])
        merged_local = download_blob_to_tempfile(merged_years[year])
        errors = validate_file(cleaned_local, merged_local)

        if errors:
            print("\n".join(errors))
            failed_files.append(cleaned_years[year])
        else:
            print("✅ File passed validation")

    print("\n📋 Summary:")
    if failed_files:
        print("❌ Files that failed validation:")
        for f in failed_files:
            print(f"- {f}")
    else:
        print("✅ All files passed validation")






# def parse_gcs_path(gcs_path):
#     match = re.match(r"gs://([^/]+)/(.+)", gcs_path)
#     if not match:
#         raise ValueError(f"Invalid GCS path: {gcs_path}")
#     return match.group(1), match.group(2)

# def list_cleaned_files(bucket_name, prefix="cleaned/"):
#     client = storage.Client()
#     blobs = client.list_blobs(bucket_name, prefix=prefix)
#     return [blob.name for blob in blobs if blob.name.endswith(".csv") and "sunshine_cleaned" in blob.name]

# def download_gcs_file(gcs_uri):
#     bucket_name, blob_path = parse_gcs_path(gcs_uri)
#     client = storage.Client()
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(blob_path)
#     temp_dir = tempfile.mkdtemp()
#     local_path = Path(temp_dir) / Path(blob_path).name
#     blob.download_to_filename(str(local_path))
#     print(f"✅ Downloaded {gcs_uri} to {local_path}")
#     return local_path

# def validate_cleaned_file(local_path):
#     try:
#         df = pd.read_csv(local_path)
#     except Exception as e:
#         return False, f"Failed to read CSV: {e}"

#     missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
#     if missing_cols:
#         return False, f"Missing required columns: {missing_cols}"

#     if df["calendar_year"].isnull().any():
#         return False, "Null values in calendar_year"

#     if (df["salary_paid"] < 0).any():
#         return False, "Negative salary values found"

#     if (df["taxable_benefits"] < 0).any():
#         return False, "Negative taxable benefits found"

#     return True, "Valid"

# def validate_all_cleaned(bucket_name):
#     cleaned_files = list_cleaned_files(bucket_name)
#     print(f"🔍 Found {len(cleaned_files)} cleaned files to validate")

#     failed = []
#     for blob_path in cleaned_files:
#         gcs_uri = f"gs://{bucket_name}/{blob_path}"
#         local_path = download_gcs_file(gcs_uri)
#         is_valid, reason = validate_cleaned_file(local_path)
#         if not is_valid:
#             failed.append((blob_path, reason))

#     if failed:
#         print("\n❌ Failed Validations:")
#         for path, reason in failed:
#             print(f"- {path}: {reason}")
#     else:
#         print("\n✅ All cleaned files passed validation!")

# if __name__ == "__main__":
#     validate_all_cleaned("sunshine-list-bucket")