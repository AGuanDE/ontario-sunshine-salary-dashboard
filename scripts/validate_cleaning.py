import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import argparse
from clean_salary_data import clean_sunshine_data, normalize_job_title, normalize_employer, normalize_name
import re

def load_schema(schema_path: Path) -> dict:
    """Load schema information from a JSON file."""
    with open(schema_path, 'r') as f:
        return json.load(f)

def analyze_column_changes(original_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> dict:
    """Analyze changes in column names and data types."""
    changes = {
        "column_renames": {},
        "dtype_changes": {},
        "missing_columns": [],
        "new_columns": []
    }
    
    # Check column renames
    for col in original_df.columns:
        if col not in cleaned_df.columns:
            changes["missing_columns"].append(col)
    
    for col in cleaned_df.columns:
        if col not in original_df.columns:
            changes["new_columns"].append(col)
    
    # Check data type changes
    for col in cleaned_df.columns:
        if col in original_df.columns:
            if str(original_df[col].dtype) != str(cleaned_df[col].dtype):
                changes["dtype_changes"][col] = {
                    "original": str(original_df[col].dtype),
                    "cleaned": str(cleaned_df[col].dtype)
                }
    
    return changes

def analyze_job_titles(df: pd.DataFrame) -> dict:
    """Analyze job title standardization."""
    try:
        analysis = {
            "unique_titles": len(df["job_title"].unique()),
            "most_common": df["job_title"].value_counts().head(10).to_dict(),
            "bilingual_titles": len(df[df["job_title"].str.contains("/", na=False)]),
            "title_lengths": {
                "min": df["job_title"].str.len().min(),
                "max": df["job_title"].str.len().max(),
                "avg": df["job_title"].str.len().mean()
            }
        }
    except KeyError as e:
        print(f"Warning: Could not analyze job titles - {e}")
        analysis = {"error": f"Column not found: {e}"}
    return analysis

def analyze_employers(df: pd.DataFrame) -> dict:
    """Analyze employer name standardization."""
    analysis = {
        "unique_employers": len(df["employer"].unique()),
        "most_common": df["employer"].value_counts().head(10).to_dict(),
        "bilingual_employers": len(df[df["employer"].str.contains("/", na=False)]),
        "employer_lengths": {
            "min": df["employer"].str.len().min(),
            "max": df["employer"].str.len().max(),
            "avg": df["employer"].str.len().mean()
        }
    }
    return analysis

def analyze_salaries(df: pd.DataFrame) -> dict:
    """Analyze salary data quality."""
    analysis = {
        "salary_stats": {
            "min": df["salary_paid"].min(),
            "max": df["salary_paid"].max(),
            "mean": df["salary_paid"].mean(),
            "median": df["salary_paid"].median(),
            "std": df["salary_paid"].std()
        },
        "benefits_stats": {
            "min": df["taxable_benefits"].min(),
            "max": df["taxable_benefits"].max(),
            "mean": df["taxable_benefits"].mean(),
            "median": df["taxable_benefits"].median(),
            "std": df["taxable_benefits"].std()
        },
        "missing_values": {
            "salary_paid": df["salary_paid"].isna().sum(),
            "taxable_benefits": df["taxable_benefits"].isna().sum()
        }
    }
    return analysis

def validate_cleaning(input_path: Path, output_dir: Path):
    """Validate the data cleaning process."""
    print(f"🔍 Validating cleaning process for: {input_path}")
    
    # Create temporary output directory for validation
    temp_output = output_dir / "validation"
    temp_output.mkdir(parents=True, exist_ok=True)
    
    # Run the cleaning process
    clean_sunshine_data(input_path, temp_output)
    
    # Load the original and cleaned data
    original_df = pd.read_csv(input_path, encoding="utf-8-sig")
    
    # Try different possible column names for year
    year_column = None
    for col in ["calendar_year", "Calendar Year", "CALENDAR_YEAR"]:
        if col in original_df.columns:
            year_column = col
            break
    
    if year_column is None:
        raise ValueError("Could not find calendar year column")
        
    year = original_df[year_column].mode()[0]
    cleaned_csv = temp_output / f"sunshine_cleaned_{year}.csv"
    schema_file = temp_output / "schema" / f"schema_{year}.json"
    
    cleaned_df = pd.read_csv(cleaned_csv)
    schema = load_schema(schema_file)
    
    # Perform validation analyses
    validation_results = {
        "timestamp": datetime.now().isoformat(),
        "input_file": str(input_path),
        "output_file": str(cleaned_csv),
        "row_counts": {
            "original": len(original_df),
            "cleaned": len(cleaned_df)
        },
        "column_changes": analyze_column_changes(original_df, cleaned_df),
        "job_title_analysis": analyze_job_titles(cleaned_df),
        "employer_analysis": analyze_employers(cleaned_df),
        "salary_analysis": analyze_salaries(cleaned_df)
    }
    
    # Save validation results
    validation_file = temp_output / f"validation_results_{year}.json"
    with open(validation_file, 'w') as f:
        def convert_numpy(obj):
            if isinstance(obj, (np.integer, np.floating)):
                return obj.item()
            elif isinstance(obj, (np.ndarray,)):
                return obj.tolist()
            return obj

        json.dump(validation_results, f, indent=2, default=convert_numpy)
    
    # Print summary
    print("\n📊 Validation Summary:")
    print(f"Original rows: {validation_results['row_counts']['original']}")
    print(f"Cleaned rows: {validation_results['row_counts']['cleaned']}")
    print(f"Unique job titles: {validation_results['job_title_analysis']['unique_titles']}")
    print(f"Unique employers: {validation_results['employer_analysis']['unique_employers']}")
    print(f"Bilingual titles: {validation_results['job_title_analysis']['bilingual_titles']}")
    print(f"Missing salary values: {validation_results['salary_analysis']['missing_values']['salary_paid']}")
    print(f"\n✅ Validation results saved to: {validation_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate the data cleaning process for Ontario Sunshine List salary data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        required=True,
        help="Path to the input CSV file"
    )
    
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=Path("data/processed"),
        help="Directory to save validation results"
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not args.input.exists():
        print(f"❌ Error: Input file not found: {args.input}")
        exit(1)
    
    # Validate input is a CSV file
    if args.input.suffix.lower() != '.csv':
        print(f"❌ Error: Input file must be a CSV file: {args.input}")
        exit(1)
    
    print(f"📂 Input file: {args.input}")
    print(f"📂 Output directory: {args.output_dir}")
    
    try:
        validate_cleaning(args.input, args.output_dir)
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        exit(1) 