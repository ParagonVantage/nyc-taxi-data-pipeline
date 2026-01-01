from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # --- 1) Create Spark session (the entry point to Spark) ---
    # local[*] = use all CPU cores available on your machine
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Marts")
        .master("local[*]")
        .getOrCreate()
    )

    ROOT = Path(__file__).resolve().parents[1]
    clean_dir = str(ROOT / "data" / "processed" / "yellow_cleaned_parquet")
    zone_csv = str(ROOT / "data" / "raw" / "taxi_zone_lookup.csv")
    marts_out = str(ROOT / "data" / "processed" / "marts_spark")

    # --- 2) Read cleaned Parquet (Spark reads folders/partitions naturally) ---
    # Spark will automatically read all partitions under the folder.
    df = spark.read.parquet(clean_dir)

    # --- 3) Read zones CSV and join for human-readable zone names ---
    zones = (
        spark.read
        .option("header", True)
        .csv(zone_csv)
        .select(
            F.col("LocationID").cast("int").alias("LocationID"),
            F.col("Borough").alias("PU_Borough"),
            F.col("Zone").alias("PU_Zone"),
            F.col("service_zone").alias("PU_service_zone")
        )
    )

    df = (
        df.join(zones, df["PULocationID"] == zones["LocationID"], how="left")
          .drop("LocationID")
    )

    # --- 4) Create time features in Spark (same idea as Pandas) ---
    # hour = 0..23
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))

    # day name is trickier in Spark; we can use date_format('EEEE') for day name
    df = df.withColumn("pickup_dow", F.date_format(F.col("tpep_pickup_datetime"), "EEEE"))

    # pickup_date = date portion only (useful for daily trends)
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))

    # --- 5) Build marts (aggregations) ---

    # A) agg_hour
    agg_hour = (
        df.groupBy("pickup_hour")
          .agg(
              F.count("*").alias("trips"),
              F.avg("fare_amount").alias("avg_fare"),
              F.avg("trip_distance").alias("avg_distance")
          )
          .orderBy("pickup_hour")
    )

    # B) agg_dow (we will order later when reading into Pandas/dashboard)
    agg_dow = (
        df.groupBy("pickup_dow")
          .agg(
              F.count("*").alias("trips"),
              F.avg("fare_amount").alias("avg_fare"),
              F.avg("trip_distance").alias("avg_distance")
          )
    )

    # C) agg_zone
    agg_zone = (
        df.groupBy("PU_Borough", "PU_Zone")
          .agg(
              F.count("*").alias("trips"),
              F.avg("total_amount").alias("avg_total"),
              F.avg("tip_amount").alias("avg_tip"),
              F.avg("trip_distance").alias("avg_distance")
          )
          .orderBy(F.desc("trips"))
    )

    # D) payment unknown rate by hour (quality transparency)
    pay_unknown_by_hour = (
        df.groupBy("pickup_hour")
          .agg(
              F.count("*").alias("trips"),
              F.sum(F.col("flag_unknown_payment_type").cast("int")).alias("unknown_payments")
          )
          .withColumn("unknown_payment_rate", F.col("unknown_payments") / F.col("trips"))
          .orderBy("pickup_hour")
    )

    # --- 6) Write marts as Parquet ---
    # overwrite lets you re-run without manually deleting files
    agg_hour.write.mode("overwrite").parquet(f"{marts_out}/agg_hour")
    agg_dow.write.mode("overwrite").parquet(f"{marts_out}/agg_dow")
    agg_zone.write.mode("overwrite").parquet(f"{marts_out}/agg_zone")
    pay_unknown_by_hour.write.mode("overwrite").parquet(f"{marts_out}/pay_unknown_by_hour")

    print("Saved marts to:", marts_out)

    spark.stop()

if __name__ == "__main__":
    main()
