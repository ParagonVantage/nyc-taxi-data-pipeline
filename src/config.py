from pathlib import Path

# Project root = folder containing this file's parent (src/)
ROOT = Path(__file__).resolve().parents[1]

DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"

# Choose ONE month to start (keep it small)
# We'll start with a yellow taxi month because it's common for tutorials.
TAXI_TYPE = "yellow"
YEAR = 2024
MONTH = 1

RAW_PARQUET_FILE = DATA_RAW / f"{TAXI_TYPE}_tripdata_{YEAR}-{MONTH:02d}.parquet"
ZONE_LOOKUP_FILE = DATA_RAW / "taxi_zone_lookup.csv"
