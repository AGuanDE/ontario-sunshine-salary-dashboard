# scripts/ingest_data.py

import os
import requests
import pandas as pd
from pathlib import Path

# Set up paths
RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Sunshine List CSV URL (you can update this)
CSV_URL = "https://www.ontario.ca/en/sunshine-list.csv"  # Replace with actual URL
LOCAL_CSV_PATH = RAW_DATA_DIR / "sunshine_2023_raw.csv"

def download_csv():
    print(f"Downloading data from {CSV_URL}...")
    response = requests.get(CSV_URL)
    if response.status_code == 200:
        with open(LOCAL_CSV_PATH, "wb") as f:
            f.write(response.content)
        print(f"Saved raw CSV to {LOCAL_CSV_PATH}")
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

def clean_and_save():
    print("Loading raw data...")
    df = pd.read_csv(LOCAL_CSV_PATH)

    print("Cleaning data...")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Optional: convert salary columns to numeric
    if 'salary_paid' in df.columns:
        df['salary_paid'] = pd.to_numeric(df['salary_paid'], errors='coerce')

    processed_path = PROCESSED_DATA_DIR / "sunshine_2023_processed.csv"
    df.to_csv(processed_path, index=False)
    print(f"Saved cleaned data to {processed_path}")

if __name__ == "__main__":
    download_csv()
    clean_and_save()
