# merge_addendum.py

import pandas as pd
import numpy as np
from pathlib import Path
import json
import re
import argparse
from clean_salary_data import standardize_column_names, normalize_text, normalize_data

def get_status_mapping():
    """Define standard status values in addendum
    """
    return {
        "addition": [
            "added", "add", "added ", "add ", "addition", "addition "
        ],
        "changed": [
            "changed", "change", "changed ", "change ", "promotion", "position change", "salary change"
        ],
        "deletion": [
            "deletion", "delete", "deleted", "delete ", "deleted "
        ]
    }

def normalize_status_col(status: str) -> str:
    """Normalize statuses to standard format per status_mapping."""
    if pd.isna(status):
        return status
    
    normalized_status = normalize_text(status).strip().lower()

    status_mapping = get_status_mapping()
    
    for standard, synonyms in status_mapping.items():
        normalized_synonyms = [syn.strip().lower() for syn in synonyms]
        if normalized_status in normalized_synonyms:
            return standard

def load_csv_with_encoding(file_path: Path):
    encodings = ['utf-8', 'utf-8-sig', 'iso-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding, keep_default_na=False, na_values=[''])
        except UnicodeDecodeError:
            pass
    raise ValueError(f"Could not read file {file_path} with any of the attempted encodings: {encodings}")

def standardize_columns_only(df):
    df_standardized, _ = standardize_column_names(df)
    return df_standardized

def deduplicate_with_salary_resolution(df):
    """
    Deduplicate rows that have same person/job but different salaries.
    keeps row with higher salary if all else the same.
    """
    df = df.copy()

    # First remove exact duplicates
    original_len = len(df)
    df = df.drop_duplicates()
    exact_dupes = original_len - len(df)

    # find groups with non-exact duplicates (differences in salary or benefits)
    group_cols = ['sector', 'first_name', 'last_name', 'employer', 'job_title', 'calendar_year']
    duplicated_groups = df.groupby(group_cols).size().reset_index(name='count')
    duplicated_groups = duplicated_groups[duplicated_groups['count'] > 1].copy()

    if len(duplicated_groups) > 0:
        # keep row with highest total comp
        df.loc[:, 'total_comp'] = (
            pd.to_numeric(df['salary_paid'], errors='coerce').fillna(0).infer_objects(copy=False) +
            pd.to_numeric(df['taxable_benefits'], errors='coerce').fillna(0).infer_objects(copy=False)
        )
        df.sort_values('total_comp', ascending=False, inplace=True)
        df.drop_duplicates(subset=group_cols, inplace=True)
        df.drop(columns=['total_comp'], inplace=True)

        non_exact_dupes = len(duplicated_groups)
    else:
        non_exact_dupes = 0

    return df, {
        'exact_duplicates_removed': exact_dupes,
        'non_exact_duplicates_resolved': non_exact_dupes
    }

def match_key(row):
    return (
        normalize_text(str(row.get("first_name", ""))),
        normalize_text(str(row.get("last_name", ""))),
        normalize_text(str(row.get("employer", ""))),
        normalize_text(str(row.get("job_title", ""))),
        row.get("calendar_year")
    )

def compare_rows(row1, row2):
    cols_to_compare = [
        'first_name', 'last_name', 'employer', 'job_title',
        'calendar_year', 'salary_paid', 'taxable_benefits'
    ]
    return all(row1.get(col) == row2.get(col) for col in cols_to_compare)


def merge_addendum(salary_path: Path, addendum_path: Path, output_path: Path):
    """
    Merge the addendum CSV changes into the base salary CSV
    """
    # Load the base salary
    print(f"\nLoading salary file: {salary_path}")
    salary_df = load_csv_with_encoding(salary_path)
    salary_df = standardize_columns_only(salary_df.copy())

    # Deduplicate salary file
    print("Deduplicating salary file...")
    salary_df, salary_dedup_stats = deduplicate_with_salary_resolution(salary_df)
    print(f"- Removed {salary_dedup_stats['exact_duplicates_removed']} exact duplicates")
    print(f"- Resolved {salary_dedup_stats['non_exact_duplicates_resolved']} non-exact duplicates")

    # If addendum doesn’t exist, just pass the base file through
    if not addendum_path.exists():
        print("Addendum file not found; outputting the base file unchanged.")
        output_path.mkdir(parents=True, exist_ok=True)
        year = int(salary_df["calendar_year"].mode()[0])
        final_file = output_path / f"merged_salary_{year}_uncleaned.csv"
        salary_df.to_csv(final_file, index=False)
        print(f"Saved merged file: {final_file}")
        return

    # Load & standardize addendum
    print(f"\nLoading addendum file: {addendum_path}")
    addendum_df = load_csv_with_encoding(addendum_path)
    addendum_df = standardize_columns_only(addendum_df.copy())

    # Deduplicate addendum
    print("Deduplicating addendum file...")
    addendum_df, addendum_dedup_stats = deduplicate_with_salary_resolution(addendum_df)
    print(f"- Removed {addendum_dedup_stats['exact_duplicates_removed']} exact duplicates")
    print(f"- Resolved {addendum_dedup_stats['non_exact_duplicates_resolved']} non-exact duplicates")

    # drop rows with any nulls in addendum
    pre_drop_len_add = len(addendum_df)
    drop_cols_add = ['sector', 'first_name', 'last_name', 'employer', 'job_title', 'salary_paid', 'taxable_benefits']
    addendum_df = addendum_df.dropna(subset=drop_cols_add)
    post_drop = pre_drop_len_add - len(addendum_df)
    print(f"\nDropped {post_drop} row(s) in addendum with null values")

    # Split addendum by status
    status_col = next((col for col in addendum_df.columns if col.lower().strip() == "status"), None)
    if not status_col:
        print("❌ No status column found in addendum; outputting base file unchanged.")
        output_path.mkdir(parents=True, exist_ok=True)
        year = int(salary_df["calendar_year"].mode()[0])
        final_file = output_path / f"merged_salary_{year}_uncleaned.csv"
        salary_df.to_csv(final_file, index=False)
        print(f"Saved merged file: {final_file}")
        return

    # standardize status col in addendum
    addendum_df['status'] = addendum_df['status'].apply(normalize_status_col)

    # Split addendum into 3 dfs by operation type
    to_add = addendum_df[addendum_df[status_col].str.lower() == 'addition'].copy()
    to_delete = addendum_df[addendum_df[status_col].str.lower() == 'deletion'].copy()
    to_change = addendum_df[addendum_df[status_col].str.lower() == 'changed'].copy()

    print("\nApplying addendum operations:")
    print(f"Salary file rows: {len(salary_df)}")
    print(f"Addendum file rows: {len(addendum_df)}")
    print(f"-> Additions: {len(to_add)}   Deletions: {len(to_delete)}   Changes: {len(to_change)}")

    changes_metadata = {
        "total_changes_requested": len(to_change),
        "changes_skipped": 0,
        "changes_applied": 0
    }
    
    salary_df["_match_key"] = salary_df.apply(match_key, axis=1)

    # Deletions
    if not to_delete.empty:
        to_delete["_match_key"] = to_delete.apply(match_key, axis=1)
        delete_keys = set(to_delete["_match_key"]) # set of keys to delete
        salary_df = salary_df[~salary_df["_match_key"].isin(delete_keys)]

    # Changes
    # If it finds an exact match (using compare_rows func), it skips the change. 
    # If it finds a match based on _match_key but the rows are different, it removes the old and adds the new. 
    # If no match is found, it treats it as an addition.
    if not to_change.empty:
        to_change["_match_key"] = to_change.apply(match_key, axis=1)
        skipped_changes = []
        needed_changes = []

        for _, change_row in to_change.iterrows():
            matching_base_rows = salary_df[salary_df["_match_key"] == change_row["_match_key"]]
            if len(matching_base_rows) > 0:
                # If row is identical, skip. Else remove old row + add new row
                if any(compare_rows(change_row, row) for _, row in matching_base_rows.iterrows()):
                    skipped_changes.append(change_row)
                else:
                    needed_changes.append(change_row)
                    salary_df = salary_df[salary_df["_match_key"] != change_row["_match_key"]]
            else:
                # No match => treat as new addition
                needed_changes.append(change_row)

        if skipped_changes:
            print(f"Skipping {len(skipped_changes)} change(s) that match salary data exactly.")

        if needed_changes:
            print(f"Applying {len(needed_changes)} valid change(s).")
            needed_changes_df = pd.DataFrame(needed_changes)
            salary_df = pd.concat([salary_df, needed_changes_df], ignore_index=True)

        changes_metadata["changes_skipped"] = len(skipped_changes)
        changes_metadata["changes_applied"] = len(needed_changes)

    # Additions
    if not to_add.empty:
        salary_df = pd.concat([salary_df, to_add], ignore_index=True)

    # Remove helper columns 
    if "_match_key" in salary_df.columns:
        salary_df.drop(columns="_match_key", inplace=True)
    if status_col in salary_df.columns:
        salary_df.drop(columns=status_col, inplace=True)

    # Final dedupe
    pre_final_len = len(salary_df)
    salary_df, final_dedup_stats = deduplicate_with_salary_resolution(salary_df)
    final_removed = pre_final_len - len(salary_df)
    print(f"\nFinal deduplication removed {final_removed} rows total")

    # Save merged CSV
    year = int(salary_df["calendar_year"].mode()[0])
    output_file = output_path / f"merged_salary_{year}_uncleaned.csv"
    salary_df.to_csv(output_file, index=False)
    print(f"\n✅ Merged file saved: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge local salary + addendum CSVs (no GCS references).")
    parser.add_argument("--salary", required=True, type=Path, help="Path to local salary CSV.")
    parser.add_argument("--addendum", required=True, type=Path, help="Path to local addendum CSV.")
    parser.add_argument("--output", default="merged_output", type=Path, help="Output directory for merged file.")
    args = parser.parse_args()

    merge_addendum(args.salary, args.addendum, args.output)
