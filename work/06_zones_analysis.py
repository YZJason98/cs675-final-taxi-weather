"""Broadcast-join the taxi zone lookup CSV with the yellow-taxi Parquet:
top 10 pickup zones and average fare per pickup borough.

The lookup is ~265 rows; the fact table is ~3M. F.broadcast() ships the small
dimension to every executor so the big fact table never shuffles. Spark would
auto-broadcast a table this small anyway — the explicit hint documents intent.
"""
import time

from pyspark.sql import functions as F

from constants import TAXI_PARQUET, ZONES_CSV
from spark_helper import get_spark, print_ui_urls, require_files, show_step


def main() -> None:
    require_files(
        (TAXI_PARQUET, "make download-nyc-cab-data"),
        (ZONES_CSV, "make download-nyc-cab-zones-data"),
    )
    spark = get_spark("cs675-zones-join")
    start = time.time()

    zones = (
        spark.read
        .option("header", "true").option("inferSchema", "true")    # tiny file -> inference is cheap
        .csv(ZONES_CSV)
    )
    trips = spark.read.parquet(TAXI_PARQUET)
    print(f"Zones: {zones.count()} rows    Trips: {trips.count():,} rows")

    # Broadcast join: small dim -> every executor, big fact stays put (no shuffle).
    joined = trips.join(
        F.broadcast(zones),
        trips["PULocationID"] == zones["LocationID"],
        "left",                                                     # keep trips even if no zone matches
    )

    top_zones = (
        joined.groupBy("Borough", "Zone").count()
        .orderBy(F.col("count").desc()).limit(10)
    )
    by_borough = (
        joined.groupBy("Borough")
        .agg(
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.count("*").alias("n_trips"),
        )
        .orderBy(F.col("n_trips").desc())
    )

    # --- Peek ---
    show_step("Top 10 pickup zones by trip count", top_zones, n=10)
    show_step("Average fare by pickup borough", by_borough)

    # --- Verify ---
    assert zones.count() > 0
    assert top_zones.count() == 10

    print(f"\nDone in {time.time() - start:.1f}s.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
