import requests
import yaml
import argparse
from pathlib import Path

def load_url_config(config_path: Path) -> dict:
    """
    Load the url_config.yaml containing both salary_urls and addendum_urls.
    Validate that required keys exist.
    """
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        # Validate required keys
        if "salary_urls" not in config or not isinstance(config["salary_urls"], dict):
            raise KeyError("The 'salary_urls' key is missing or invalid in the config file.")
        if "addendum_urls" not in config or not isinstance(config["addendum_urls"], dict):
            raise KeyError("The 'addendum_urls' key is missing or invalid in the config file.")
        
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at path: {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")

def get_url(urls: dict, year: str) -> str:
    """
    Retrieve the URL for the given year from the mapping.
    Raise an error if no URL is found for the given year.
    """
    url = urls.get(year)
    if not url:
        raise ValueError(f"No URL found in config for year {year}")
    return url

def download_file(url: str, local_path: Path):
    """
    Download the file from the URL and save it to local path.
    """
    print(f"Downloading file from {url}")
    response = requests.get(url, timeout=15)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"Saved file to {local_path}")
    else:
        raise Exception(f"Failed to download file from {url}. Status code: {response.status_code}")

def main():
    parser = argparse.ArgumentParser(
        description="Download salary and addendum files for a given year"
    )
    parser.add_argument(
        "--year",
        type=str,
        required=True,
        help="Year of data to ingest (YYYY)"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("scripts/url_config.yaml"),
        help="Path to the url_config.yaml config file (I saved it in the scripts directory)"
    )
    args = parser.parse_args()
    year = args.year

    try:
        # Load URL config with error handling
        config = load_url_config(args.config)
        salary_urls = config.get("salary_urls", {})
        addendum_urls = config.get("addendum_urls", {})

        # Define directories
        salary_raw_dir = Path("data/raw/salary")
        addendum_raw_dir = Path("data/raw/addendum")
        salary_raw_dir.mkdir(parents=True, exist_ok=True)
        addendum_raw_dir.mkdir(parents=True, exist_ok=True)

        # Get URL for year with error handling
        try:
            salary_url = get_url(salary_urls, year)
        except ValueError as e:
            print(f"Error retrieving salary URL: {e}")
            return
        
        try:
            addendum_url = get_url(addendum_urls, year)
        except ValueError as e:
            print(f"Error retrieving addendum URL: {e}")
            addendum_url = None

        # Define local file paths
        salary_file_path = salary_raw_dir / f"salary_{year}_raw.csv"
        addendum_file_path = addendum_raw_dir / f"addendum_{year}_raw.csv"

        # Download files
        download_file(salary_url, salary_file_path)
        
        if addendum_url and addendum_url.strip():
            download_file(addendum_url, addendum_file_path)
        else:
            print(f"No addendum URL found for year {year}.")

    except (FileNotFoundError, KeyError, ValueError) as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()