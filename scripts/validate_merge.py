import pandas as pd
from pathlib import Path
import argparse
import json
from clean_salary_data import standardize_column_names

def load_csv_with_encoding(file_path):
    """Load CSV file with appropriate encoding."""
    encodings = ['utf-8', 'utf-8-sig', 'iso-8859-1', 'cp1252']
    
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Could not read file {file_path} with any of the attempted encodings: {encodings}")

def validate_merge(base_path: Path, addendum_path: Path, merged_path: Path):
    """Validate that the merge was performed correctly."""
    print("Loading files...")
    base_df = load_csv_with_encoding(base_path)
    addendum_df = load_csv_with_encoding(addendum_path)
    merged_df = load_csv_with_encoding(merged_path)

    # Validate all required files loaded
    print(f"Base rows: {len(base_df)}")
    print(f"Addendum rows: {len(addendum_df)}")
    print(f"Merged rows: {len(merged_df)}")

    # Standardize columns before matching
    print("Standardizing column names...")
    base_df, _ = standardize_column_names(base_df)
    addendum_df, _ = standardize_column_names(addendum_df)
    merged_df, _ = standardize_column_names(merged_df)

    def match_key(row):
        """Create a unique identifier for each row."""
        return (
            str(row.get("first_name", "")),
            str(row.get("last_name", "")),
            str(row.get("employer", "")),
            str(row.get("job_title", "")),
            row.get("calendar_year")
        )

    # Add match keys to all dataframes
    addendum_df["_match_key"] = addendum_df.apply(match_key, axis=1)
    base_df["_match_key"] = base_df.apply(match_key, axis=1)
    merged_df["_match_key"] = merged_df.apply(match_key, axis=1)

    # Load changes metadata if it exists
    changes_metadata = None
    metadata_path = merged_path.parent / "changes_metadata.json"
    if metadata_path.exists():
        with open(metadata_path) as f:
            changes_metadata = json.load(f)

    # Validate status column exists
    status_col = next((col for col in addendum_df.columns if col.lower().strip() == "status"), None)
    if not status_col:
        raise ValueError("No status column found in addendum file")

    # Check each operation type
    to_delete = addendum_df[addendum_df[status_col].str.lower() == 'deletion']
    delete_keys = set(to_delete["_match_key"])
    deletions_still_exist = merged_df[merged_df["_match_key"].isin(delete_keys)]

    to_change = addendum_df[addendum_df[status_col].str.lower() == 'changed']
    change_keys = set(to_change["_match_key"])
    
    # Load skipped changes if available
    skipped_changes_path = merged_path.parent / "skipped_changes.csv"
    if skipped_changes_path.exists():
        skipped_changes = pd.read_csv(skipped_changes_path)
        skipped_keys = set(skipped_changes.apply(match_key, axis=1))
        # Remove skipped changes from the change keys
        change_keys = change_keys - skipped_keys
    
    # Check if actual changes exist in merged file (excluding skipped ones)
    changes_in_merged = merged_df[merged_df["_match_key"].isin(change_keys)]

    to_add = addendum_df[addendum_df[status_col].str.lower() == 'addition']
    add_keys = set(to_add["_match_key"])
    additions_missing = to_add[~to_add["_match_key"].isin(merged_df["_match_key"])]

    # Additional checks
    unexpected_status = addendum_df[~addendum_df[status_col].str.lower().isin(['addition', 'deletion', 'changed'])]
    duplicate_keys = merged_df[merged_df.duplicated("_match_key", keep=False)]

    print("\n📊 Validation Report")
    print("=" * 50)
    
    if len(deletions_still_exist) > 0:
        print(f"❌ Deletion rows that still exist: {len(deletions_still_exist)}")
        print("First few problematic deletions:")
        print(deletions_still_exist[["first_name", "last_name", "employer"]].head())

    if len(changes_in_merged) > 0:
        if changes_metadata:
            print(f"\n🔄 Changed rows status:")
            print(f"  - Total changes requested: {changes_metadata['total_changes_requested']}")
            print(f"  - Changes skipped (identical): {changes_metadata['changes_skipped']}")
            print(f"  - Changes applied: {changes_metadata['changes_applied']}")
            
            # Show the actual rows that were changed
            print("\nRows with applied changes:")
            applied_changes_path = merged_path.parent / "applied_changes.csv"
            if applied_changes_path.exists():
                applied_changes = pd.read_csv(applied_changes_path)
                print(applied_changes[["first_name", "last_name", "employer", "salary_paid", "taxable_benefits"]].head())
                
                # Verify these changes are actually in the merged file
                problematic_changes = []
                for _, change_row in applied_changes.iterrows():
                    matching_merged = merged_df[
                        (merged_df["first_name"] == change_row["first_name"]) &
                        (merged_df["last_name"] == change_row["last_name"]) &
                        (merged_df["employer"] == change_row["employer"]) &
                        (merged_df["salary_paid"] == change_row["salary_paid"]) &
                        (merged_df["taxable_benefits"] == change_row["taxable_benefits"])
                    ]
                    if len(matching_merged) == 0:
                        problematic_changes.append(change_row)
                
                if problematic_changes:
                    print("\n❌ Some applied changes are missing from merged file!")
                    print("Missing changes:")
                    problematic_df = pd.DataFrame(problematic_changes)
                    print(problematic_df[["first_name", "last_name", "employer"]].head())
            else:
                print("No applied changes file found")

    if len(additions_missing) > 0:
        print(f"\n➕ Addition rows missing in merge: {len(additions_missing)}")
        print("First few missing additions:")
        print(additions_missing[["first_name", "last_name", "employer"]].head())

    if len(unexpected_status) > 0:
        print(f"\n⚠️ Rows with unexpected status values: {len(unexpected_status)}")
        print("Unique unexpected status values:", unexpected_status[status_col].unique())

    if len(duplicate_keys) > 0:
        print(f"\n👥 Duplicate entries in merged file: {len(duplicate_keys)}")
        print("First few duplicates:")
        print(duplicate_keys[["first_name", "last_name", "employer"]].head())

    # Final validation
    has_errors = (len(deletions_still_exist) > 0 or 
                 len(additions_missing) > 0 or 
                 len(unexpected_status) > 0 or 
                 len(duplicate_keys) > 0)

    # Only consider changes an error if the numbers don't match the metadata
    if changes_metadata:
        expected_changes = changes_metadata['changes_applied']
        if len(changes_in_merged) != expected_changes:
            has_errors = True
            print(f"\nDebug info:")
            print(f"  Expected changed rows in merged file: {expected_changes}")
            print(f"  Actually found changed rows: {len(changes_in_merged)}")

    if has_errors:
        print("\n❌ Validation FAILED - Please review issues above")
        return False
    else:
        print("\n✅ All validation checks passed!")
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate merged salary data")
    parser.add_argument("--base", type=Path, required=True, help="Path to original base file")
    parser.add_argument("--addendum", type=Path, required=True, help="Path to addendum file")
    parser.add_argument("--merged", type=Path, required=True, help="Path to merged output file")
    args = parser.parse_args()

    success = validate_merge(args.base, args.addendum, args.merged)
    if not success:
        exit(1)

