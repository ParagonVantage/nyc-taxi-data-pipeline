from pathlib import Path
import urllib.request

from config import DATA_RAW, RAW_PARQUET_FILE, ZONE_LOOKUP_FILE, TAXI_TYPE, YEAR, MONTH

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"Already exists: {dest.name}")
        return
    print(f"Downloading {url} -> {dest}")
    urllib.request.urlretrieve(url, dest)
    print("Done.")

def main():
    parquet_name = f"{TAXI_TYPE}_tripdata_{YEAR}-{MONTH:02d}.parquet"
    parquet_url = f"{BASE}/{parquet_name}"

    download(parquet_url, RAW_PARQUET_FILE)
    download(ZONE_URL, ZONE_LOOKUP_FILE)

if __name__ == "__main__":
    main()
