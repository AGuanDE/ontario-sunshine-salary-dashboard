# validate_merge.py

import pandas as pd
from pathlib import Path
import argparse
from clean_salary_data import standardize_column_names, normalize_text

def load_csv_with_encoding(file_path):
    """Load CSV file with appropriate encoding."""
    encodings = ['utf-8', 'utf-8-sig', 'iso-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not read file {file_path} with any of the attempted encodings: {encodings}")

def validate_merge(salary_path: Path, merged_path: Path, addendum_path: Path = None):
    """Validate that the merge was performed correctly"""
    print("\n")
    print("Loading files...")
    salary_df = load_csv_with_encoding(salary_path)
    salary_df, _ = standardize_column_names(salary_df)
    merged_df = load_csv_with_encoding(merged_path)
    merged_df, _ = standardize_column_names(merged_df)

    # Early exit if no addendum exists
    if addendum_path is None or not addendum_path.exists():
        print("\nNo addendum found - no validation required (make sure raw files were properly DLed)")
        return True

    # continue if addendum exists
    addendum_df = load_csv_with_encoding(addendum_path)
    addendum_df, _ = standardize_column_names(addendum_df)
    print(f" Salary rows: {len(salary_df)}")
    print(f" Addendum rows: {len(addendum_df)}")
    print(f" Merged rows: {len(merged_df)}")

    def match_key(row):
        return (
            normalize_text(str(row.get("first_name", ""))),
            normalize_text(str(row.get("last_name", ""))),
            normalize_text(str(row.get("employer", ""))),
            normalize_text(str(row.get("job_title", ""))),
            row.get("calendar_year")
        )

    # Add match keys to all dataframes
    addendum_df["_match_key"] = addendum_df.apply(match_key, axis=1)
    salary_df["_match_key"] = salary_df.apply(match_key, axis=1)
    merged_df["_match_key"] = merged_df.apply(match_key, axis=1)

    # Validate status column exists
    status_col = next((col for col in addendum_df.columns if col.lower().strip() == "status"), None)
    if not status_col:
        raise ValueError("No status column found in addendum file - no changes will be made to salary file")

    # drop rows with any nulls in addendum
    pre_drop_len_add = len(addendum_df)
    drop_cols_add = ['sector', 'first_name', 'last_name', 'employer', 'job_title', 'salary_paid', 'taxable_benefits']
    addendum_df = addendum_df.dropna(subset=drop_cols_add)
    post_drop = pre_drop_len_add - len(addendum_df)
    print(f"\nDropped {post_drop} row(s) in addendum with null values")

    # Count rows in addendum by status
    status_counts = addendum_df[status_col].str.lower().value_counts()

    print("\n🧾 Addendum Summary")
    print("=" * 20)
    print(f" Additions: {status_counts.get('addition', 0)}")
    print(f" Deletions: {status_counts.get('deletion', 0)}")
    print(f" Changes:   {status_counts.get('changed', 0)}")
    
    # Check each operation type
    to_delete = addendum_df[addendum_df[status_col].str.lower() == 'deletion'].copy()
    delete_keys = set(to_delete["_match_key"])
    deletions_still_exist = merged_df[merged_df["_match_key"].isin(delete_keys)]

    to_change = addendum_df[addendum_df[status_col].str.lower() == 'changed'].copy()
    change_keys = set(to_change["_match_key"])

    # find rows with same match key in both datasets
    skipped_changes = to_change.merge(
        salary_df,
        on="_match_key",
        suffixes=("_addendum", "_salary"),
        how="inner"
    )
    
    cols_to_compare = ["first_name", "last_name", "employer", "job_title", "calendar_year", "salary_paid", "taxable_benefits"]

    # identical_rows_mask = pd series of boolean values where:
    # True = the intended change (from to_change) is already reflected in the current salary data (salary_df)
    # False = at least one of the compared columns has diff values between the "_addendum" and "_salary" versions for that row.
    identical_rows_mask = skipped_changes.apply(
        lambda row: all(
            row[f"{col}_addendum"] == row[f"{col}_salary"] for col in cols_to_compare),
        axis=1
    )

    # Get the keys of the identical/skipped rows
    skipped_keys = set(skipped_changes[identical_rows_mask]["_match_key"])
    change_keys = change_keys - skipped_keys

    # Changes actually found in the merged data
    changes_in_merged = merged_df[merged_df["_match_key"].isin(change_keys)]

    # additions
    to_add = addendum_df[addendum_df[status_col].str.lower() == 'addition'].copy()
    add_keys = set(to_add["_match_key"])
    additions_missing = to_add[~to_add["_match_key"].isin(merged_df["_match_key"])]

    print("\n📊 Validation Report")
    print("=" * 20)
    print("\n")
    
    if len(deletions_still_exist) > 0:
        print(f"Deletion rows that still exist: {len(deletions_still_exist)}")
        print(deletions_still_exist[["first_name", "last_name", "employer", "salary_paid"]].head(10))

    if len(changes_in_merged) > 0:
        print(f"Confirmed {len(changes_in_merged)} valid 'changed' rows found in merged file.")
        print(f"Skipped {len(skipped_keys)} 'changed' rows that are already in salary file")

    if len(additions_missing) > 0:
        print(f"'Addition' rows in addendum: {len(to_add)}")
        print(f"'Addition' rows missing in merge: {len(additions_missing)}")
        print("First few missing additions:")
        print(additions_missing[["first_name", "last_name", "employer", "salary_paid"]].head(10))

    # Final validation
    # change operation is not checked here because duplicates in salary/addendum are frequent causing false flags
    has_errors = (
        len(deletions_still_exist) > 0 or 
        len(additions_missing) > 0
    )

    if has_errors:
        print("\n❌ Validation has issues - Please review above")
        return False
    else:
        print("\n✅ All validation checks passed!")
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate merged salary data")
    parser.add_argument("--salary", type=Path, required=True, help="Path to original salary file")
    parser.add_argument("--merged", type=Path, required=True, help="Path to merged output file")
    parser.add_argument("--addendum", type=Path, required=False, help="Path to addendum file")
    args = parser.parse_args()

    success = validate_merge(args.salary, args.merged, args.addendum)
    if not success:
        exit(1)
