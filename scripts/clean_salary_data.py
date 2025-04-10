# clean_salary_data.py

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import argparse
import re
import csv

def get_column_mapping():
    """Define standard column name mappings for common variations. *Consider changing to fuzzy matching for sustainability later"""
    return {
        "job_title": ["jobtitle", "job title", "jobtitle", "position", "position title", "Job Title", "job_title", "Job Title "],
        "salary_paid": ["salary", "salary paid", "salarypaid", "Salary Paid", " Salary Paid ", "Salary Paid ", "salary_paid"],
        "taxable_benefits": ["benefits", "taxable benefits", "taxablebenefits", "Taxable Benefits", " Taxable Benefits ", "Taxable Benefits ", "taxable_benefits"],
        "first_name": ["firstname", "first name", "given name", "First Name", "first_name"],
        "last_name": ["lastname", "last name", "surname", "Last Name", "last_name"],
        "calendar_year": ["year", "fiscal year", "fiscal_year", "Calendar Year", "calendar_year"],
        "employer": ["organization", "organization name", "org", "Employer", "employer", "employer_name"],
        "sector": ["sector name", "sectorname", "Sector", "ï»¿Sector", "sector"],
        "status": ["status", "status ", "Status", "Status "]
    }

def get_job_title_mapping():
    """Define standard job title mappings for common variations."""
    return {
        # Education - Teachers
        "TEACHER": "Teacher",
        "TEACHER, ELEMENTARY": "Elementary Teacher",
        "TEACHER - ELEMENTARY": "Elementary Teacher",
        "TEACHER/ELEMENTARY": "Elementary Teacher",
        "ELEMENTARY CONTRACT TEACHER": "Elementary Teacher",
        "ELEMENTARY SCHOOL TEACHER": "Elementary Teacher",
        "TEACHER, SECONDARY": "Secondary Teacher",
        "TEACHER - SECONDARY": "Secondary Teacher",
        "SECONDARY CONTRACT TEACHER": "Secondary Teacher",
        "ENSEIGNANT": "Teacher",
        "ENSEIGNANT(E)": "Teacher",

        # Education - Leadership
        "PRINCIPAL": "Principal",
        "ELEMENTARY PRINCIPAL": "Elementary Principal",
        "CHAIR, ELEMENTARY": "Elementary Chair",
        "VICE PRINCIPAL": "Vice Principal",
        "VP": "Vice Principal",
        "ASSISTANT CURRICULUM LEADER, SECONDARY": "Assistant Curriculum Leader",
        "ASSOCIATE DEAN": "Associate Dean",
        "DEAN": "Dean",
        "ACTING DEAN": "Dean",
        "ASSISTANT DEAN": "Assistant Dean",
        "ACADEMIC DEAN": "Dean",

        # Education - Faculty
        "PROFESSOR": "Professor",
        "ASSOCIATE PROFESSOR": "Associate Professor",
        "ASSISTANT PROFESSOR": "Assistant Professor",
        "FACULTY MEMBER": "Faculty Member",
        "FULL PROFESSOR": "Professor",
        "LECTURER": "Lecturer",
        "RESEARCH PROFESSOR": "Research Professor",
        "UNIVERSITY PROFESSOR": "Professor",

        # Healthcare - Nurses
        "REGISTERED NURSE": "Registered Nurse",
        "REGISTERED NURSE / INFIRMIÈRE AUTORISÉE": "Registered Nurse",
        "REGISTERED NURSE/INFIRMIÈRE AUTORISÉE": "Registered Nurse",
        "NURSE, REGISTERED": "Registered Nurse",
        "NURSE PRACTITIONER": "Nurse Practitioner",
        "REGISTERED PRACTICAL NURSE": "Registered Practical Nurse",
        "RPN": "Registered Practical Nurse",

        # Healthcare - Physicians & Pathology
        "PHYSICIAN": "Physician",
        "PHARMACIST": "Pharmacist",
        "PATHOLOGIST": "Pathologist",
        "FORENSIC PATHOLOGIST": "Pathologist",
        "LAB PHYSICIAN": "Pathologist",
        "ANATOMIC PATHOLOGIST": "Pathologist",
        "RADIOLOGIST": "Radiologist",
        "ONCOLOGIST MEDICAL": "Medical Oncologist",
        "ONCOLOGIST RADIATION": "Radiation Oncologist",
        "DENTIST": "Dentist",
        "DENTIST-IN-CHIEF": "Dentist",

        # Healthcare - Other
        "PHYSIOTHERAPIST": "Physiotherapist",
        "OCCUPATIONAL THERAPIST": "Occupational Therapist",
        "SOCIAL WORKER": "Social Worker",
        "PSYCHIATRIST": "Psychiatrist",
        "MEDICAL OFFICER OF HEALTH": "Medical Officer of Health",
        "MEDICAL DIRECTOR": "Medical Director",

        # Emergency Services - Police
        "CONSTABLE": "Constable",
        "POLICE CONSTABLE": "Police Constable",
        "PLAINCLOTHES POLICE CONSTABLE": "Plainclothes Police Constable",
        "LAW ENFORCEMENT OFFICER / AGENT D'EXÉCUTION DE LA LOI": "Law Enforcement Officer",
        "GENERAL DUTY OFFICER / AGENT DES SERVICES GÉNÉRAUX": "General Duty Officer",
        "SERGEANT": "Sergeant",
        "DETECTIVE": "Detective",
        "INVESTIGATOR / ENQUÊTEUR": "Investigator",
        "POLICE CHIEF": "Police Chief",
        "POLICE SUPERINTENDENT": "Police Superintendent",
        "POLICE INSPECTOR": "Police Inspector",

        # Emergency Services - Fire
        "FIREFIGHTER": "Firefighter",
        "FIREFIGHTER OPERATION": "Firefighter",
        "CAPTAIN": "Captain",
        "FIRE CHIEF": "Fire Chief",
        "FIRE MARSHAL": "Fire Marshal",

        # Emergency Services - Paramedics
        "PRIMARY CARE PARAMEDIC": "Primary Care Paramedic",
        "ADVANCED CARE PARAMEDIC": "Advanced Care Paramedic",

        # Operations
        "OPERATOR": "Operator",
        "NUCLEAR OPERATOR": "Nuclear Operator",
        "MANAGER": "Manager",
        "MANAGER, FINANCE": "Finance Manager",
        "FINANCE MANAGER": "Finance Manager",
        "HUMAN RESOURCES MANAGER": "HR Manager",
        "EXECUTIVE DIRECTOR": "Executive Director",
        "TEAM LEADER / CHEF D'ÉQUIPE": "Team Leader",
        "SUPERVISOR": "Supervisor",

        # Executive
        "PRESIDENT": "President",
        "CEO": "Chief Executive Officer",
        "CHIEF EXECUTIVE OFFICER": "Chief Executive Officer",
        "CHIEF FINANCIAL OFFICER": "Chief Financial Officer",
        "CFO": "Chief Financial Officer",
        "CHIEF OPERATING OFFICER": "Chief Operating Officer",
        "COO": "Chief Operating Officer",
        "VICE PRESIDENT": "Vice President",
        "VP": "Vice President",
        "EXECUTIVE VICE PRESIDENT": "Executive Vice President",
        "SENIOR VICE PRESIDENT": "Senior Vice President",

        # Common suffixes to remove
        "THE": "",
        "I": "",
        "II": "",
        "III": "",
        "IV": "",
        "V": "",
        "JR": "",
        "SR": "",
        "INC": "",
        "LLC": "",
        "LTD": "",
        "LIMITED": "",
        "CORP": "",
        "CORPORATION": "",
        "INCORPORATED": ""
    }


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names using predefined mappings."""
    # Store original column names as a copy for schema tracking
    original_columns = df.columns.tolist()
    
    # Create reverse mapping for easier lookup
    reverse_mapping = {}
    for standard, variations in get_column_mapping().items():
        for variation in variations:
            reverse_mapping[variation.lower()] = standard
    
    # Rename columns based on mapping
    rename_dict = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in reverse_mapping:
            rename_dict[col] = reverse_mapping[col_lower]
    
    df = df.rename(columns=rename_dict)
    
    # Log any unmapped columns for review
    unmapped = [col for col in df.columns if col.lower() not in reverse_mapping]
    if unmapped:
        print(f"Note: Found unmapped columns: {unmapped}")
    
    return df, original_columns

def normalize_text(text: str) -> str:
    """Normalize text by removing extra spaces, standardizing case, and handling special characters."""
    if pd.isna(text):
        return text
    
    text = str(text).strip()
    
    # Collapse multiple spaces to a single space
    text = re.sub(r'\s+', ' ', text)
    
    # Handle common special characters
    text = text.replace("‘", "'").replace("’", "'")   # Curly single quotes → straight
    text = text.replace("–", "-").replace("—", "-")   # En-dash/em-dash → hyphen

    # remove non printable char
    text = ''.join(char for char in text if char.isprintable())
    
    text = text.replace('"', '""')

    return text

def normalize_job_title(title: str) -> str:
    """Normalize job titles to standard format."""
    if pd.isna(title):
        return title
    
    # Get the mapping dictionary
    title_mapping = get_job_title_mapping()
    
    title = normalize_text(title)
    
    # Split into words and normalize each word
    words = title.split()
    normalized_words = []
    
    # Try to match the entire title first
    full_title = ' '.join(words).upper()
    if full_title in title_mapping:
        return title_mapping[full_title]
    
    # If no full match, try word by word
    for word in words:
        # Check if the word is in our mapping
        if word.upper() in title_mapping:
            normalized_words.append(title_mapping[word.upper()])
        else:
            normalized_words.append(word)
    
    # Join words back together
    title = ' '.join(normalized_words)
    
    # Remove any double spaces
    title = re.sub(r'\s+', ' ', title)
    
    # Remove any trailing/leading spaces
    title = title.strip()
    
    return title

def normalize_employer(employer: str) -> str:
    """Normalize employer names to standard format."""
    if pd.isna(employer):
        return employer
    
    employer = normalize_text(employer)

    # Common organization standardizations
    employer = employer.replace('Univ.', 'University').replace('Univ ', 'University ')
    employer = employer.replace('Hosp.', 'Hospital').replace('Hosp ', 'Hospital ')
    employer = employer.replace('Corp.', 'Corporation').replace('Corp ', 'Corporation ')
    employer = employer.replace('Inc.', 'Incorporated').replace('Inc ', 'Incorporated ')
    
    # Remove common unnecessary words
    employer = re.sub(r'\b(?:the|a|an)\b', '', employer, flags=re.IGNORECASE)
    
    return employer.strip()

def normalize_name(name: str) -> str:
    """Normalize person names to standard format."""
    if pd.isna(name):
        return name
    
    name = normalize_text(name)
    
    # Capitalize first letter of each word
    name = ' '.join(word.capitalize() for word in name.split())
    
    return name.strip()

def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Normalizing data...")
    
    if df is None:
        print("Error: normalize_data received a None DataFrame!")
        return None
    if not isinstance(df, pd.DataFrame):
        print(f"Error: df is not a dataframe, type: {type(df)}")
        return None
    
    # Normalize text columns
    text_columns = ["first_name", "last_name", "employer", "job_title", "sector"]
    for col in text_columns:
        if col in df.columns:
            if col == "job_title":
                df.loc[:, col] = df.loc[:, col].apply(normalize_job_title)
            elif col == "employer":
                df.loc[:, col] = df.loc[:, col].apply(normalize_employer)
            elif col in ["first_name", "last_name"]:
                df.loc[:, col] = df.loc[:, col].apply(normalize_name)
            else:
                df.loc[:, col] = df.loc[:, col].apply(normalize_text)

    def clean_numeric_column(series: pd.Series) -> pd.Series:
        """Clean numeric columns without dtype warnings"""
        return (
            pd.to_numeric(
                series.astype(str).str.replace(r"[^0-9\.]+", "", regex=True),
                errors="coerce"
            )
            .round(2)
            .fillna(0)
        )

    if "salary_paid" in df.columns:
        df.loc[:, "salary_paid"] = clean_numeric_column(df["salary_paid"])

    if "taxable_benefits" in df.columns:
        df.loc[:, "taxable_benefits"] = clean_numeric_column(df["taxable_benefits"])

    return df

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

def clean_sunshine_data(input_path: Path, output_dir: Path):
    encoding_list = ['utf-8', 'utf-8-sig', 'iso-8859-1', 'cp1252']
    
    df = None

    for encoding in encoding_list:
        try:
            df = pd.read_csv(
                input_path,
                encoding=encoding,
                engine="python",
                on_bad_lines="warn",
                keep_default_na=False,
                na_values=['']
            )
            break
        except UnicodeDecodeError:
            print(f"Failed to read with encoding: {encoding}")
            continue
    else:
        # This block runs if the loop completes without breaking (i.e., all encodings failed)
        raise ValueError(f"Could not read file {input_path} with any of the attempted encodings: {encodings}")
        
    if df is None or not isinstance(df, pd.DataFrame):
        print(f"ERROR: CSV read failed or returned invalid type: {type(df)}")
        return
    
    print("\nCleaning and standardizing data...")

    # Standardize column names using mapping
    df, original_columns = standardize_column_names(df)
    
    if df is None:
        print("Error: df became None before normalize_data")
        return
    if not isinstance(df, pd.DataFrame):
        print(f"Error: df is not a DataFrame before normalize_data, type: {type(df)}")
        return
    
    df = normalize_data(df).copy()

    # Define target schema (with forced types)
    target_columns = {
        "salary_paid": "float64",
        "taxable_benefits": "float64",
        "calendar_year": "Int64",  # supports NaN
        "first_name": "string",
        "last_name": "string",
        "employer": "string",
        "job_title": "string",
        "sector": "string"
    }

    # Create full name
    if "first_name" in df.columns and "last_name" in df.columns:
        df["full_name"] = df["first_name"] + " " + df["last_name"]

    # Ensure calendar year is numeric
    if "calendar_year" in df.columns:
        df["calendar_year"] = pd.to_numeric(df["calendar_year"], errors="coerce")
        df["calendar_year"] = df["calendar_year"].ffill().infer_objects(copy=False)

    # Create total_compensation column
    if "salary_paid" in df.columns and "taxable_benefits" in df.columns:
        df["total_compensation"] = (df["salary_paid"] + df["taxable_benefits"]).round(2)


    # Enforce target schema
    for col, dtype in target_columns.items():
        if col in df.columns:
            try: df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"Error converting {col} to {dtype}: {e}")
        else:
            print(f"Warning: Column: {col} - not found in DataFrame")

    # Create output paths
    year = df["calendar_year"].dropna().astype(int).mode()[0] if "calendar_year" in df.columns else "unknown"
    output_csv = output_dir / f"sunshine_cleaned_{year}.csv"

    # Final dedupe
    pre_final_len = len(df)
    df, final_dedup_stats = deduplicate_with_salary_resolution(df)
    final_removed = pre_final_len - len(df)
    print(f"\nFinal deduplication removed {final_removed} rows total")

    # drop rows where any nulls or empty strings are found in the non-numeric columns
    pre_drop_len = len(df)
    drop_cols = ['sector', 'first_name', 'last_name', 'employer', 'job_title']
    df[drop_cols] = df[drop_cols].replace('', np.nan)
    df = df.dropna(subset=drop_cols)
    post_drop = pre_drop_len - len(df)
    print(f"\nDropped {post_drop} rows with nulls in final cleaned file")

    # Save cleaned data
    # encapsulate all nonnumeric columns with double quotes to solve parsing issue of commas being in job_titles
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        output_csv,
        index=False,
        quotechar='"',
        quoting=csv.QUOTE_ALL
    )

    # After writing the CSV, validate column counts
    expected_columns = len(df.columns)
    with open(output_csv, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for i, row in enumerate(reader):
            if len(row) != expected_columns:
                print(f"Row {i+2} has {len(row)} columns (expected {expected_columns})")

    print(f"✅ Saved cleaned CSV: {output_csv}")
    
    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean and standardize Ontario Sunshine List salary data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        required=True,
        help="Path to the input merged CSV file"
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=Path("data/cleaned"),
        help="Directory to save cleaned files"
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
        clean_sunshine_data(args.input, args.output_dir)
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        exit(1) 