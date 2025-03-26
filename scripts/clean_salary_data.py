import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import argparse
import re

def get_column_mapping():
    """Define standard column name mappings for common variations."""
    return {
        "job_title": ["jobtitle", "job title", "jobtitle", "position", "position title", "Job Title"],
        "salary_paid": ["salary", "salary paid", "salarypaid", "Salary Paid", "Salary Paid "],
        "taxable_benefits": ["benefits", "taxable benefits", "taxablebenefits", "Taxable Benefits"],
        "first_name": ["firstname", "first name", "given name", "First Name"],
        "last_name": ["lastname", "last name", "surname", "Last Name"],
        "calendar_year": ["year", "fiscal year", "fiscal_year", "Calendar Year"],
        "employer": ["organization", "organization name", "org", "Employer"],
        "sector": ["sector name", "sectorname", "Sector", "ï»¿Sector"]  # Handle BOM character
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
        "CHAIR, ELEMENTARY": "Elementary Principal",
        "VICE PRINCIPAL": "Vice Principal",
        "VP": "Vice Principal",
        "ASSISTANT CURRICULUM LEADER, SECONDARY": "Assistant Curriculum Leader",
        
        # Education - Faculty
        "PROFESSOR": "Professor",
        "ASSOCIATE PROFESSOR": "Associate Professor",
        "ASSISTANT PROFESSOR": "Assistant Professor",
        "FACULTY MEMBER": "Faculty Member",
        
        # Healthcare - Nurses
        "REGISTERED NURSE": "Registered Nurse",
        "REGISTERED NURSE / INFIRMIÈRE AUTORISÉE": "Registered Nurse",
        "REGISTERED NURSE/INFIRMIÈRE AUTORISÉE": "Registered Nurse",
        "NURSE, REGISTERED": "Registered Nurse",
        "NURSE PRACTITIONER": "Nurse Practitioner",
        "REGISTERED PRACTICAL NURSE": "Registered Practical Nurse",
        "RPN": "Registered Practical Nurse",
        
        # Healthcare - Other
        "PHARMACIST": "Pharmacist",
        "PHYSIOTHERAPIST": "Physiotherapist",
        "OCCUPATIONAL THERAPIST": "Occupational Therapist",
        "SOCIAL WORKER": "Social Worker",
        
        # Emergency Services - Police
        "CONSTABLE": "Constable",
        "POLICE CONSTABLE": "Police Constable",
        "PLAINCLOTHES POLICE CONSTABLE": "Plainclothes Police Constable",
        "LAW ENFORCEMENT OFFICER / AGENT D'EXÉCUTION DE LA LOI": "Law Enforcement Officer",
        "GENERAL DUTY OFFICER / AGENT DES SERVICES GÉNÉRAUX": "General Duty Officer",
        "SERGEANT": "Sergeant",
        "DETECTIVE": "Detective",
        "INVESTIGATOR / ENQUÊTEUR": "Investigator",
        
        # Emergency Services - Fire
        "FIREFIGHTER": "Firefighter",
        "FIREFIGHTER OPERATION": "Firefighter",
        "CAPTAIN": "Captain",
        
        # Emergency Services - Paramedics
        "PRIMARY CARE PARAMEDIC": "Primary Care Paramedic",
        "ADVANCED CARE PARAMEDIC": "Advanced Care Paramedic",
        
        # Operations
        "OPERATOR": "Operator",
        "NUCLEAR OPERATOR": "Nuclear Operator",
        "MANAGER": "Manager",
        "EXECUTIVE DIRECTOR": "Executive Director",
        "TEAM LEADER / CHEF D'ÉQUIPE": "Team Leader",
        
        # Common suffixes to remove
        "THE": "",
        "A": "",
        "AN": "",
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
    # Store original column names for schema tracking
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
    
    # Convert to string and strip whitespace
    text = str(text).strip()
    
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    
    # Handle common special characters
    text = text.replace("‘", "'").replace("’", "'")   # Curly single quotes → straight
    text = text.replace("–", "-").replace("—", "-")   # En-dash/em-dash → hyphen

    
    # Remove any non-printable characters
    text = ''.join(char for char in text if char.isprintable())
    
    return text

def normalize_job_title(title: str) -> str:
    """Normalize job titles to standard format."""
    if pd.isna(title):
        return title
    
    # Get the mapping dictionary
    title_mapping = get_job_title_mapping()
    
    title = normalize_text(title)
    
    # Handle bilingual titles with forward slash
    if '/' in title:
        # Take the English part (before the slash)
        title = title.split('/')[0].strip()
    
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
    
    # Handle bilingual employer names with forward slash
    if '/' in employer:
        employer = employer.split('/')[0].strip()

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
    
    # Handle common name variations
    name = name.replace('Mc ', 'Mc').replace('Mac ', 'Mac')
    
    # Capitalize first letter of each word
    name = ' '.join(word.capitalize() for word in name.split())
    
    return name.strip()

def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply normalization rules to the DataFrame"""
    print("Normalizing data formatting...")
    
    # Normalize text columns
    text_columns = ["first_name", "last_name", "employer", "job_title", "sector"]
    for col in text_columns:
        if col in df.columns:
            if col == "job_title":
                df[col] = df[col].apply(normalize_job_title)
            elif col == "employer":
                df[col] = df[col].apply(normalize_employer)
            elif col in ["first_name", "last_name"]:
                df[col] = df[col].apply(normalize_name)
            else:
                df[col] = df[col].apply(normalize_text)
    
    # Normalize numeric columns
    if "salary_paid" in df.columns:
        df["salary_paid"] = pd.to_numeric(df["salary_paid"], errors="coerce")
        # Round to 2 decimal places
        df["salary_paid"] = df["salary_paid"].round(2)
    
    if "taxable_benefits" in df.columns:
        df["taxable_benefits"] = pd.to_numeric(df["taxable_benefits"], errors="coerce")
        # Round to 2 decimal places
        df["taxable_benefits"] = df["taxable_benefits"].round(2)
    
    if "total_compensation" in df.columns:
        df["total_compensation"] = df["total_compensation"].round(2)
    
    return df

def clean_sunshine_data(input_path: Path, output_dir: Path):
    print(f"🔍 Reading: {input_path}")

    # Read the CSV with UTF-8-SIG encoding to handle BOM
    try:
        df = pd.read_csv(
            input_path,
            encoding="utf-8-sig",  # Try UTF-8 with BOM first
            engine="python",
            on_bad_lines="warn"
        )
    except UnicodeDecodeError:
        # Fallback to ISO-8859-1 if UTF-8 fails
        df = pd.read_csv(
            input_path,
            encoding="ISO-8859-1",
            engine="python",
            on_bad_lines="warn"
        )

    print("Cleaning and standardizing data...")

    # Store original schema before cleaning
    original_schema = {
        "columns": list(df.columns),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "row_count": len(df),
        "timestamp": datetime.now().isoformat()
    }

    # Standardize column names using mapping
    df, original_columns = standardize_column_names(df)

    # Normalize remaining column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # Apply data normalization
    df = normalize_data(df)

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

    # Create total_compensation column
    if "salary_paid" in df.columns and "taxable_benefits" in df.columns:
        df["total_compensation"] = df["salary_paid"] + df["taxable_benefits"]


    # Enforce target schema
    for col, dtype in target_columns.items():
        if col in df.columns:
            try: df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"Error converting {col} to {dtype}: {e}")
        else:
            print(f"Warning: Column {col} not found in DataFrame")

    # Create output paths
    year = df["calendar_year"].dropna().astype(int).mode()[0] if "calendar_year" in df.columns else "unknown"
    output_csv = output_dir / f"sunshine_cleaned_{year}.csv"
    output_parquet = output_dir / f"sunshine_cleaned_{year}.parquet"
    schema_dir = output_dir / "schema"
    schema_file = schema_dir / f"schema_{year}.json"

    # Save cleaned data
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    df.to_parquet(output_parquet, index=False)

    # Save schema information
    schema_dir.mkdir(parents=True, exist_ok=True)
    cleaned_schema = {
        "original_schema": original_schema,
        "column_mapping": {
            "original": original_columns,
            "mapped": list(df.columns)
        },
        "cleaned_schema": {
            "columns": list(df.columns),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "row_count": len(df),
            "timestamp": datetime.now().isoformat()
        }
    }
    with open(schema_file, 'w') as f:
        json.dump(cleaned_schema, f, indent=2)

    print(f"✅ Saved cleaned CSV:     {output_csv}")
    print(f"✅ Saved cleaned Parquet: {output_parquet}")
    print(f"✅ Saved schema info:     {schema_file}")

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
        help="Path to the input CSV file"
    )
    
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        default=Path("data/processed"),
        help="Directory to save processed files"
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