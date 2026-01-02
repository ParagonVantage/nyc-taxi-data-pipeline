from pathlib import Path
import pandas as pd

def main():
    ROOT = Path(__file__).resolve().parents[1]

    marts_spark = ROOT / "data" / "processed" / "marts_spark"
    marts_pandas = ROOT / "data" / "processed" / "marts"
    publish_dir = ROOT / "data" / "published"

    publish_dir.mkdir(parents=True, exist_ok=True)

    def load(name: str) -> pd.DataFrame:
        # Prefer Spark marts if they exist (proves Spark pipeline output is production-usable)
        spark_path = marts_spark / name
        pandas_path = marts_pandas / f"{name}.parquet"
        if spark_path.exists():
            return pd.read_parquet(spark_path)
        return pd.read_parquet(pandas_path)

    # Load marts
    agg_hour = load("agg_hour")
    agg_dow = load("agg_dow")
    agg_zone = load("agg_zone")
    pay_unk = load("pay_unknown_by_hour")

    # Load zones dim
    zones = pd.read_csv(ROOT / "data" / "raw" / "taxi_zone_lookup.csv")

    # Save Parquet + CSV (stable filenames)
    tables = {
        "mart_hour": agg_hour,
        "mart_dow": agg_dow,
        "mart_zone": agg_zone,
        "mart_quality_hour": pay_unk,
        "dim_zones": zones,
    }

    for name, df in tables.items():
        df.to_parquet(publish_dir / f"{name}.parquet", index=False)
        df.to_csv(publish_dir / f"{name}.csv", index=False)

    print("Published tables to:", publish_dir)
    for name, df in tables.items():
        print(name, df.shape)

if __name__ == "__main__":
    main()
