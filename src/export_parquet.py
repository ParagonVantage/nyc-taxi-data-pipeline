import pandas as pd
import numpy as np
from pathlib import Path

def main():
    ROOT = Path(__file__).resolve().parents[1]
    raw_parquet = ROOT / "data" / "raw" / "yellow_tripdata_2024-01.parquet"
    out_dir = ROOT / "data" / "processed" / "yellow_cleaned_parquet"

    df = pd.read_parquet(raw_parquet)

    # Normalize passenger_count: 0 means unknown
    df.loc[df["passenger_count"] == 0, "passenger_count"] = np.nan

    # Duration
    duration_min = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60

    # Hard rules
    hard_fail = (
        (duration_min <= 0) |
        (df["trip_distance"].isna()) | (df["trip_distance"] <= 0) |
        (df["fare_amount"] < 0) |
        (df["total_amount"] < 0) |
        (df["tip_amount"] < 0)
    )
    pickup_year = df["tpep_pickup_datetime"].dt.year
    bad_year = (pickup_year < 2017) | (pickup_year > 2025)

    hard_fail = hard_fail | bad_year

    df_clean = df.loc[~hard_fail].copy()

    # Soft flags
    duration_min_clean = (df_clean["tpep_dropoff_datetime"] - df_clean["tpep_pickup_datetime"]).dt.total_seconds() / 60
    df_clean["flag_unknown_payment_type"] = (df_clean["payment_type"] == 0)
    df_clean["flag_long_duration"] = (duration_min_clean > 360)
    df_clean["flag_extreme_distance"] = (df_clean["trip_distance"] > 200)
    df_clean["flag_high_passenger_count"] = df_clean["passenger_count"].notna() & (df_clean["passenger_count"] > 8)

    # Partition columns
    df_clean["pickup_year"] = df_clean["tpep_pickup_datetime"].dt.year
    df_clean["pickup_month"] = df_clean["tpep_pickup_datetime"].dt.month

    out_dir.mkdir(parents=True, exist_ok=True)

    df_clean.to_parquet(
        out_dir,
        engine="pyarrow",
        index=False,
        partition_cols=["pickup_year", "pickup_month"]
    )

    print("Saved:", out_dir)
    print("Rows:", len(df_clean), "Cols:", df_clean.shape[1])

if __name__ == "__main__":
    main()
