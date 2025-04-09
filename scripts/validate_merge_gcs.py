# validate_merge_gcs.py

import re
import tempfile
from pathlib import Path
import argparse
from google.cloud import storage

# our modules
from gcs_modules import download_gcs_file, gcs_blob_exists
from validate_merge import validate_merge

def list_merged_files(bucket, prefix="merged/"):
    client = storage.Client()
    blobs = client.list_blobs(bucket, prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith(".csv") and "merged_salary" in blob.name]

def validate_all_merges(bucket_name):
    merged_files = list_merged_files(bucket_name)
    failed_validations = []

    for merged_blob_path in merged_files:
        print("\n")
        print("=" * 100)
        print(f"🔍 Validating {merged_blob_path}...")
        year_match = re.search(r"(19\d{2}|20\d{2})", merged_blob_path)
        if not year_match:
            print("Year not found in blob name; skipping.")
            continue
        year = year_match.group(1)

        salary_uri = f"gs://{bucket_name}/raw/salaries/sunshine_salaries_{year}.csv"
        addendum_uri = f"gs://{bucket_name}/raw/addendums/sunshine_addendums_{year}.csv"
        merged_uri = f"gs://{bucket_name}/{merged_blob_path}"

        try:
            # check addendum exists
            addendum_exists = gcs_blob_exists(addendum_uri)

            merged_local = download_gcs_file(merged_uri)
            salary_local = download_gcs_file(salary_uri)

            addendum_local = None
            if addendum_exists:
                addendum_local = download_gcs_file(addendum_uri)
            else:
                print(f"No addendum found for {year}, no validation necessary")
                
            success = validate_merge(
                salary_path=salary_local,
                merged_path=merged_local,
                addendum_path=addendum_local
            )
            
            if not success:
                failed_validations.append(merged_blob_path)

        except Exception as e:
            print("\n")
            print(f"❌ Error processing {merged_blob_path}: {e}")
            failed_validations.append(merged_blob_path)

    print("\n====== SUMMARY ======")
    if failed_validations:
        print("❌ The following files failed validation:")
        for path in failed_validations:
            print(f"- {path}")
    else:
        print("✅ All merged files passed validation!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate merged salary data from GCS using local validation logic."
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="sunshine-list-bucket",
        help="GCS bucket name (default: sunshine-list-bucket)"
    )
    args = parser.parse_args()
    validate_all_merges(args.bucket)
