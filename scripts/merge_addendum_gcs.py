# merge_addendum_gcs.py

import re
import tempfile
from pathlib import Path
import argparse
from google.cloud import storage
from gcs_modules import download_gcs_file, upload_to_gcs, gcs_blob_exists
from merge_addendum import merge_addendum

def list_years(bucket_name, prefix, pattern):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    years = set()
    for blob in blobs:
        match = re.search(pattern, blob.name)
        if match:
            years.add(match.group(1))
    return sorted(years)

def process_year(bucket_name: str, year: str) -> None:
    """Process a single year's data"""
    print(f"\nProcessing year {year}...")
    salary_uri = f"gs://{bucket_name}/raw/salary/sunshine_salary_{year}.csv"
    addendum_uri = f"gs://{bucket_name}/raw/addendum/sunshine_addendum_{year}.csv"
    output_uri = f"gs://{bucket_name}/merged/merged_salary_{year}_uncleaned.csv"

    # Download files
    salary_path = download_gcs_file(salary_uri)
    addendum_path = Path("non_existent_addendum.csv")
    
    if gcs_blob_exists(addendum_uri):
        addendum_path = download_gcs_file(addendum_uri)
    else:
        print(f"No addendum found for {year}, passing salary file through unchanged")

    # Process and upload
    output_dir = Path(tempfile.mkdtemp())
    merge_addendum(salary_path, addendum_path, output_dir)
    output_file = output_dir / f"merged_salary_{year}_uncleaned.csv"
    upload_to_gcs(output_file, output_uri)
    print(f"📤 Uploaded merged file to GCS: {output_uri}")

def main(bucket_name: str, target_year: str = None) -> None:
    """Main processing function, handles both single year and all years"""
    if target_year:
        process_year(bucket_name, target_year)
    else:
        # Process all years if no single year provided as arg
        salary_years = list_years(bucket_name, "raw/salary/", r"sunshine_salary_(\d{4})\.csv")
        print(f"Salary files found: {salary_years}")
        for year in salary_years:
            process_year(bucket_name, year)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge salary and addendum CSVs from GCS using logic from merge_addendum.py."
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
        help="Specific year to process (YYYY format). If not provided, process all years."
    )
    args = parser.parse_args()
    
    # Validate year format if provided
    if args.year and not re.match(r"^\d{4}$", args.year):
        raise ValueError("Year must be in YYYY format")
    
    main(args.bucket, args.year)


# Process all years CLI command
# python merge_addendum_gcs.py --bucket sunshine-list-bucket

# Process specific year CLI command
# python merge_addendum_gcs.py --bucket sunshine-list-bucket --year 2020