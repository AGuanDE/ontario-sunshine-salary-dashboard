import pandas as pd
from pathlib import Path
import json
import argparse
import tempfile
import re
from clean_salary_data import standardize_column_names, normalize_text
from google.cloud import storage

def is_gcs_path(path):
    return str(path).startswith("gs://")

def parse_gcs_path(gcs_path):
    """Split gs://bucket/path/to/blob.csv → (bucket, path/to/blob.csv)"""
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
    local_path  = Path(temp_dir) / Path(blob_path).name
    blob.download_to_filename(str(local_path))

    print(f"✅ downloaded {gcs_uri} to {local_path}")
    return local_path

def upload_to_gcs(local_path: Path, gcs_uri: str):
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))

    print(f"✅ Uploaded {local_path} to {gcs_uri}")

def gcs_blob_exists(gcs_uri):
    bucket_name, blob_path = parse_gcs_path(gcs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_path).exists()

def standardize_columns_only(df):
    df_standardized, _ = standardize_column_names(df)
    return df_standardized

def deduplicate_with_salary_resolution(df):
    """
    Deduplicate rows that have same person/job but different salaries.
    Returns both deduplicated dataframe and a report of what was changed.
    """
    # First remove exact duplicates
    original_len = len(df)
    df = df.drop_duplicates()
    exact_dupes = original_len - len(df)
    
    # Group by everything except salary and benefits to find non-exact dupes
    group_cols = ['first_name', 'last_name', 'employer', 'job_title', 'calendar_year']
    
    # Find groups with multiple entries
    duplicated_groups = df.groupby(group_cols).size().reset_index(name='count')
    duplicated_groups = duplicated_groups[duplicated_groups['count'] > 1]
    
    if len(duplicated_groups) > 0:
        print("\n🔍 Found non-exact duplicates:")
        print(f"Number of duplicate groups: {len(duplicated_groups)}")
        
        # Show a few examples
        print("\nExample duplicates (keeping highest total compensation):")
        for _, group in duplicated_groups.head(3).iterrows():
            dupes = df[
                (df['first_name'] == group['first_name']) &
                (df['last_name'] == group['last_name']) &
                (df['employer'] == group['employer']) &
                (df['job_title'] == group['job_title']) &
                (df['calendar_year'] == group['calendar_year'])
            ]
            print(f"\n{group['first_name']} {group['last_name']} at {group['employer']}")
            print(dupes[['salary_paid', 'taxable_benefits']].to_string())
        
        # Create a clean copy of the dataframe to avoid the SettingWithCopyWarning
        df = df.copy()
        
        # Keep row with highest total compensation for each group
        df['total_comp'] = (
            pd.to_numeric(df['salary_paid'], errors='coerce').fillna(0) +
            pd.to_numeric(df['taxable_benefits'], errors='coerce').fillna(0)
        )
        df = (df.sort_values('total_comp', ascending=False)
                .drop_duplicates(subset=group_cols)
                .drop(columns=['total_comp']))
        
        non_exact_dupes = len(duplicated_groups)
    else:
        non_exact_dupes = 0
    
    return df, {
        'exact_duplicates_removed': exact_dupes,
        'non_exact_duplicates_resolved': non_exact_dupes
    }

def match_key(row):
    """Create a unique identifier for each row."""
    return (
        normalize_text(str(row.get("first_name", ""))),
        normalize_text(str(row.get("last_name", ""))),
        normalize_text(str(row.get("employer", ""))),
        normalize_text(str(row.get("job_title", ""))),
        row.get("calendar_year")
    )

def compare_rows(row1, row2):
    """Compare two rows to see if they are effectively the same."""
    cols_to_compare = ['first_name', 'last_name', 'employer', 'job_title', 
                      'calendar_year', 'salary_paid', 'taxable_benefits']
    return all(row1.get(col) == row2.get(col) for col in cols_to_compare)

def load_csv_with_encoding(file_path):
    """Load CSV file with appropriate encoding."""
    encodings = ['utf-8', 'utf-8-sig', 'iso-8859-1', 'cp1252']
    
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding, keep_default_na=False, na_values=[''])
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Could not read file {file_path} with any of the attempted encodings: {encodings}")

def merge_addendum(salary_path: Path, addendum_path: Path, output_path: Path):
    """Merge addendum changes into the salary data, then lightly clean the result."""
    # Load and standardize column names for salary
    print("Loading salary file...")
    salary_df = load_csv_with_encoding(salary_path)
    salary_df = standardize_columns_only(salary_df.copy())
    
    # Deduplicate base file
    print("\nDeduplicating base file...")
    salary_df, salary_dedup_stats = deduplicate_with_salary_resolution(salary_df)
    if salary_dedup_stats['exact_duplicates_removed'] > 0:
        print(f"ℹ️ Removed {salary_dedup_stats['exact_duplicates_removed']} exact duplicate rows")
    if salary_dedup_stats['non_exact_duplicates_resolved'] > 0:
        print(f"ℹ️ Resolved {salary_dedup_stats['non_exact_duplicates_resolved']} duplicate entries by keeping highest compensation")

    if not addendum_path.exists():
        print("ℹ️ Addendum file not found. Passing base file through unchanged.")
        output_path.mkdir(parents=True, exist_ok=True)
        year = int(salary_df["calendar_year"].mode()[0])
        salary_df.to_csv(output_path / f"merged_salary_{year}_uncleaned.csv", index=False)
        return

    # Load and standardize column names for addendum
    print("\nLoading addendum file...")
    addendum_df = load_csv_with_encoding(addendum_path)
    addendum_df = standardize_columns_only(addendum_df.copy())

    # Deduplicate addendum file
    print("Deduplicating addendum file...")
    addendum_df, addendum_dedup_stats = deduplicate_with_salary_resolution(addendum_df)
    if addendum_dedup_stats['exact_duplicates_removed'] > 0:
        print(f"ℹ️ Removed {addendum_dedup_stats['exact_duplicates_removed']} exact duplicate rows")
    if addendum_dedup_stats['non_exact_duplicates_resolved'] > 0:
        print(f"ℹ️ Resolved {addendum_dedup_stats['non_exact_duplicates_resolved']} duplicate entries by keeping highest compensation")

    # Look for status column (case-insensitive)
    status_col = next((col for col in addendum_df.columns if col.lower().strip() == "status"), None)
    if not status_col:
        print("❌ No status column found in addendum. Passing base file through unchanged")
        output_path.mkdir(parents=True, exist_ok=True)
        year = int(salary_df["calendar_year"].mode()[0])
        salary_df.to_csv(output_path / f"merged_salary_{year}_uncleaned.csv", index=False)
        return

    # Split addendum by operation type
    to_add = addendum_df[addendum_df[status_col].str.lower() == 'addition'].copy()
    to_delete = addendum_df[addendum_df[status_col].str.lower() == 'deletion'].copy()
    to_change = addendum_df[addendum_df[status_col].str.lower() == 'changed'].copy()

    print(f"\nProcessing addendum operations:")
    print(f"Base file rows: {len(salary_df)}")
    print(f"➕ Adding {len(to_add)} rows")
    print(f"❌ Deleting {len(to_delete)} rows")
    print(f"🔄 Changing {len(to_change)} rows")

    # Initialize changes metadata
    changes_metadata = {
        "total_changes_requested": len(to_change),
        "changes_skipped": 0,
        "changes_applied": 0
    }

    # Add match key to salary dataframe
    salary_df["_match_key"] = salary_df.apply(match_key, axis=1)
    
    # Process deletions
    if not to_delete.empty:
        to_delete["_match_key"] = to_delete.apply(match_key, axis=1)
        delete_keys = set(to_delete["_match_key"])
        salary_df = salary_df[~salary_df["_match_key"].isin(delete_keys)]

    # Process changes
    if not to_change.empty:
        to_change["_match_key"] = to_change.apply(match_key, axis=1)
        
        # Track which changes were actually needed
        skipped_changes = []
        needed_changes = []
        
        for _, change_row in to_change.iterrows():
            matching_base_rows = salary_df[salary_df["_match_key"] == change_row["_match_key"]]
            if len(matching_base_rows) > 0:
                # Check if any of the matching rows are identical
                if any(compare_rows(change_row, base_row) for _, base_row in matching_base_rows.iterrows()):
                    skipped_changes.append(change_row)
                else:
                    needed_changes.append(change_row)
                    # Remove ALL matching rows from salary_df
                    salary_df = salary_df[salary_df["_match_key"] != change_row["_match_key"]]
            else:
                # No matching row found, so this is a new addition
                needed_changes.append(change_row)
        
        if skipped_changes:
            print(f"\nℹ️ Skipping {len(skipped_changes)} changes that are identical to base data")
            skipped_df = pd.DataFrame(skipped_changes)
            output_path.mkdir(parents=True, exist_ok=True)
            skipped_df.to_csv(output_path / "skipped_changes.csv", index=False)
        
        if needed_changes:
            print(f"ℹ️ Applying {len(needed_changes)} necessary changes")
            needed_changes_df = pd.DataFrame(needed_changes)
            # Save applied changes for review
            needed_changes_df.to_csv(output_path / "applied_changes.csv", index=False)
            # Apply the changes - old rows should already be removed above
            salary_df = pd.concat([salary_df, needed_changes_df], ignore_index=True)
        
        # Update metadata with final counts
        changes_metadata.update({
            "changes_skipped": len(skipped_changes),
            "changes_applied": len(needed_changes)
        })

    # Process additions
    if not to_add.empty:
        salary_df = pd.concat([salary_df, to_add], ignore_index=True)

    # Drop helper columns
    if "_match_key" in salary_df.columns:
        salary_df = salary_df.drop(columns="_match_key")
    if status_col in salary_df.columns:
        salary_df = salary_df.drop(columns=status_col)

    # Final deduplication after all operations
    print("\nPerforming final deduplication...")
    final_len = len(salary_df)
    salary_df, final_dedup_stats = deduplicate_with_salary_resolution(salary_df)
    if final_dedup_stats['exact_duplicates_removed'] > 0 or final_dedup_stats['non_exact_duplicates_resolved'] > 0:
        print(f"ℹ️ Final deduplication removed {final_len - len(salary_df)} total duplicate entries")
        if final_dedup_stats['exact_duplicates_removed'] > 0:
            print(f"  - {final_dedup_stats['exact_duplicates_removed']} exact duplicates")
        if final_dedup_stats['non_exact_duplicates_resolved'] > 0:
            print(f"  - {final_dedup_stats['non_exact_duplicates_resolved']} non-exact duplicates (kept highest compensation)")

    # Save changes metadata
    output_path.mkdir(parents=True, exist_ok=True)
    with open(output_path / "changes_metadata.json", "w") as f:
        json.dump(changes_metadata, f, indent=2)

    # Save merged result (uncleaned)
    year = int(salary_df["calendar_year"].mode()[0])
    output_file = output_path / f"merged_salary_{year}_uncleaned.csv"
    salary_df.to_csv(output_file, index=False)
    print(f"\n✅ Merged file saved: {output_file}")



if __name__ == "__main__":
    BUCKET = "sunshine-list-bucket"

    def list_years(bucket_name, prefix, pattern):
        client = storage.Client()
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        years = set()
        for blob in blobs:
            match = re.search(pattern, blob.name)
            if match:
                years.add(match.group(1))
        return sorted(years)

    # Extract available years
    salary_years = list_years(BUCKET, "raw/salaries/", r"sunshine_salaries_(\d{4})\.csv")
    addendum_years = list_years(BUCKET, "raw/addendums/", r"sunshine_addendums_(\d{4})\.csv")
    matched_years = sorted(set(salary_years) & set(addendum_years))

    print(f"📅 Matched years with both salary and addendum files: {matched_years}")

    for year in matched_years:
        print(f"\n Processing year {year}...")

        salary_uri = f"gs://{BUCKET}/raw/salaries/sunshine_salaries_{year}.csv"
        addendum_uri = f"gs://{BUCKET}/raw/addendums/sunshine_addendums_{year}.csv"
        output_uri = f"gs://{BUCKET}/merged/merged_salary_{year}_uncleaned.csv"

        if gcs_blob_exists(output_uri):
            print(f"⏭️ Skipping year {year} — merged file already exists at {output_uri}")
            continue

        salary_path = download_gcs_file(salary_uri)
        addendum_path = download_gcs_file(addendum_uri)
        output_dir = Path(tempfile.mkdtemp())

        merge_addendum(salary_path, addendum_path, output_dir)

        output_file = output_dir / f"merged_salary_{year}_uncleaned.csv"
        upload_to_gcs(output_file, output_uri)
        print(f"📤 Uploaded merged file to GCS: {output_uri}")

    # CLI commands
    # parser = argparse.ArgumentParser(description="Merge salary + addendum into merged_salary_yyyy.csv")
    # parser.add_argument("--salary", required=True, help="Path to the salary CSV")
    # parser.add_argument("--addendum", required=True, help="Path to the addendum CSV")
    # parser.add_argument("--output", default=Path("data/merged"), help="Output directory")
    # args = parser.parse_args()

    # salary_path = download_gcs_file(args.salary) if is_gcs_path(args.salary) else Path(args.salary)
    # addendum_path = download_gcs_file(args.addendum) if is_gcs_path(args.addendum) else Path(args.addendum)

    # output_dir = Path(tempfile.mkdtemp()) if is_gcs_path(args.output) else Path(args.output)
    # merge_addendum(salary_path, addendum_path, output_dir)

    # if is_gcs_path(args.output):
    #     year = re.search(r"(19\d{2}|20\d{2})", args.salary).group(1)
    #     output_file = output_dir / f"merged_salary_{year}_uncleaned.csv"
    #     upload_to_gcs(output_file, args.output)
    #     print(f"📤 Uploaded merged file to GCS: {args.output}")
