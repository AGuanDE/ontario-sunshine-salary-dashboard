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
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-CA,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://data.ontario.ca/en/dataset/public-sector-salary-disclosure-1996/resource/ed43dcd3-6c33-47b6-bc71-e94be3ce6dc0",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "sec-ch-ua": "\"Google Chrome\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
        "sec-ch-ua-platform": "\"Windows\"",
    }
    print(f"Downloading file from {url}")
    response = requests.get(url, timeout=15, headers=headers)
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
        default="1996",
        help="Year of data to ingest (YYYY)"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "url_config.yaml", # url_config.yaml must be in the same directory as ingest_data.py.
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
        salary_raw_dir = Path("/home/aguan/ontario-sunshine-salary-dashboard/data/raw/salary")
        addendum_raw_dir = Path("/home/aguan/ontario-sunshine-salary-dashboard/data/raw/addendum")
        salary_raw_dir.mkdir(parents=True, exist_ok=True)
        addendum_raw_dir.mkdir(parents=True, exist_ok=True)

        print("Starting ingest_data.py with year:", year)
        print("Current working directory:", Path.cwd())
        print("Salary file will be saved to:", (salary_raw_dir / f"salary_{year}_raw.csv").resolve())
        print("Addendum file will be saved to:", (addendum_raw_dir / f"addendum_{year}_raw.csv").resolve())

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