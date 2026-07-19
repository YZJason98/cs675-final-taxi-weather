"""Payment-method analysis on the yellow-taxi Parquet:
trip count, revenue, average ticket by method, and credit-vs-cash by hour.
"""
import time

from pyspark.sql import functions as F

from constants import TAXI_PARQUET
from spark_helper import get_spark, print_ui_urls, require_files, show_step

# TLC payment_type codes; anything else falls through to "other".
PAYMENT_LABELS = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute", 5: "unknown", 6: "voided"}


def main() -> None:
    require_files((TAXI_PARQUET, "make download-nyc-cab-data"))
    spark = get_spark("cs675-taxi-payments")
    start = time.time()

    # Build a CASE-WHEN chain (numeric code -> readable label) and add it as a column.
    label = F.lit("other")
    for code, name in PAYMENT_LABELS.items():
        label = F.when(F.col("payment_type") == code, name).otherwise(label)
    df = spark.read.parquet(TAXI_PARQUET).withColumn("payment_label", label)
    total_rows = df.count()

    by_method = (
        df.groupBy("payment_label")
        .agg(
            F.count("*").alias("n_trips"),
            F.round(F.sum("total_amount"), 0).alias("revenue_usd"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_usd"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
        )
        .orderBy(F.col("n_trips").desc())
    )

    # pivot turns the two payment labels into their own columns, one row per hour.
    by_hour = (
        df.filter(F.col("payment_label").isin("credit_card", "cash"))
        .withColumn("hour", F.hour("tpep_pickup_datetime"))
        .groupBy("hour").pivot("payment_label", ["credit_card", "cash"]).count()
        .orderBy("hour")
    )

    # --- Peek ---
    show_step("Trips, revenue, avg ticket by payment method", by_method)
    show_step("Credit vs cash by hour", by_hour, n=24)

    # Refund-like rows: negative totals.
    n_negative = df.filter(F.col("total_amount") < 0).count()
    print(f"\nTrips with total_amount < 0 (likely refunds): "
          f"{n_negative:,} of {total_rows:,} ({100 * n_negative / total_rows:.2f}%)")

    # --- Verify ---
    assert total_rows > 0
    assert by_method.count() > 0

    print(f"\nDone in {time.time() - start:.1f}s.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
